package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"syscall"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/deliveryrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/replayrpc"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	progresspb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/matcher/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/fencing"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricefeed"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricework"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/progressrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/projection"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/apiprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/matcherprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

func TestMatcherProcess(t *testing.T) {
	if os.Getenv("MATCHER_PROCESS_INTEGRATION") != "1" {
		t.Skip("run python3 scripts/check_matcher_process.py --package ./cmd/matcher")
	}
	if os.Getenv("POSTGRES_TEST") != "1" || os.Getenv("KAFKA_TEST_BROKER") == "" || os.Getenv("KAFKA_TEST_PREFIX") == "" {
		t.Fatal("matcher opt-in requires PostgreSQL/Kafka configuration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 85*time.Second)
	defer cancel()
	must := func(e error) {
		t.Helper()
		if e != nil {
			t.Fatal(e)
		}
	}
	binary := filepath.Join(t.TempDir(), "matcher")
	build := exec.CommandContext(ctx, "go", "build", "-race", "-o", binary, ".")
	if out, e := build.CombinedOutput(); e != nil {
		t.Fatalf("build: %s", out)
	}
	api := postgrestest.Open(t)
	reader := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	f := matcherprocess.New(t, api, os.Getenv("KAFKA_TEST_BROKER"), os.Getenv("KAFKA_TEST_PREFIX"))
	d, e := f.Seed(ctx)
	must(e)
	sort.Strings(d.IDs)
	must(f.PublishChanges(ctx))
	bootstrap, e := f.Bootstrap(ctx)
	must(e)
	b, e := json.Marshal(bootstrap)
	must(e)
	env := validEnv()
	delete(env, "HTTP_ADDR")
	for k, v := range f.ChildEnv(os.Getenv("MATCHER_DATABASE_URL")) {
		env[k] = v
	}
	env["MATCHER_ENVIRONMENT"] = f.Prefix
	env["MATCHER_GROUP"] = f.Prefix + "-matcher"
	env["PRICES_BOOTSTRAP"] = string(b)
	env["HEAD_INTERVAL"] = "50ms"
	readSecret := func(path string) []byte { b, e := os.ReadFile(path); must(e); return b }
	creds, e := serviceauth.ClientCredentials(readSecret(f.CertFile), readSecret(f.KeyFile), readSecret(f.CAFile))
	must(e)
	conn, e := grpc.NewClient(f.RPCAddr, grpc.WithTransportCredentials(creds), grpc.WithAuthority(matcherprocess.ServerName))
	must(e)
	defer conn.Close()
	progress := progressrpc.NewClient(conn)
	var changesOffset int64
	proof := func(kind progresspb.ConsumerKind, id string, sourceTopic string, tid [16]byte, partition int32) (*progresspb.ReadProgressResponse, error) {
		return progress.ReadProgress(ctx, &progresspb.ReadProgressRequest{Kind: kind, ConsumerId: id, ClusterId: f.PricesSource.ClusterID, TopicId: tid[:], Topic: sourceTopic, Partition: partition})
	}
	cfg := apiprocess.Config{Executable: binary, Env: env, StartupProbePath: "/healthz", StartupTimeout: 12 * time.Second, StopTimeout: time.Second}
	wait := func(label string, ok func() bool) {
		t.Helper()
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for !ok() {
			select {
			case <-ctx.Done():
				t.Fatal("timeout: " + label)
			case <-tick.C:
			}
		}
	}
	status := func(p *apiprocess.Process, path string) int {
		t.Helper()
		req, e := http.NewRequestWithContext(ctx, "GET", p.URL+path, nil)
		must(e)
		client := &http.Client{Timeout: time.Second}
		resp, e := client.Do(req)
		must(e)
		defer resp.Body.Close()
		return resp.StatusCode
	}
	// HTTP is live while first evaluation is held after canonical API commit.
	pause := f.PauseDelivery(d.IDs[1])
	defer pause.Release()
	record, e := f.PublishPrice(ctx, f.SetPrice(d.Pair, "101", 2))
	must(e)
	identity := fencing.Identity{Environment: env["MATCHER_ENVIRONMENT"], Group: env["MATCHER_GROUP"], Topic: record.Topic, Partition: pricefeed.PairPartition(d.Pair)}
	var grant fencing.Grant
	var key, trigger string
	var partial pricework.Work
	observer, e := kgo.NewClient(kgo.SeedBrokers(f.Broker))
	must(e)
	defer observer.Close()
	brokerOffset := func() int64 {
		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = env["MATCHER_GROUP"]
		topic := kmsg.NewOffsetFetchRequestTopic()
		topic.Topic = record.Topic
		topic.Partitions = []int32{record.Partition}
		req.Topics = []kmsg.OffsetFetchRequestTopic{topic}
		res, e := req.RequestWith(ctx, observer)
		must(e)
		if len(res.Topics) != 1 || len(res.Topics[0].Partitions) != 1 || res.Topics[0].Partitions[0].ErrorCode != 0 {
			t.Fatal("broker offset response")
		}
		return res.Topics[0].Partitions[0].Offset
	}
	must(apiprocess.With(ctx, cfg, func(p *apiprocess.Process) error {
		var response *pb.RequestDeliveryResponse
		select {
		case response = <-pause.Committed:
		case <-ctx.Done():
			return errors.New("no delayed delivery")
		}
		if status(p, "/readyz") != 503 {
			t.Fatal("ready before first evaluation completed")
		}
		var e error
		grant, _, e = fencing.New(reader).Current(ctx, identity)
		must(e)
		store := pricework.NewSourceBound(reader, grant, f.PricesSource)
		keys, e := store.Pending(ctx)
		must(e)
		if len(keys) != 1 {
			t.Fatal("pending work missing", keys)
		}
		key = keys[0]
		partial, e = store.Get(ctx, key)
		must(e)
		if partial.Completed != 1 || len(partial.Entries) != 3 || partial.State != "pending" {
			t.Fatal("partial durable candidates missing")
		}
		out, err := proof(progresspb.ConsumerKind_CONSUMER_KIND_PRICES, grant.Scope, record.Topic, f.PricesSource.TopicID, record.Partition)
		if out != nil || grpcstatus.Code(err) != codes.FailedPrecondition {
			t.Fatal("exported Pending without Prefix", out, err)
		}
		trigger = response.Workflow.TriggerId
		var count int
		var canonical string
		must(api.QueryRow(ctx, `SELECT count(*),min(trigger_id::text) FROM app.delivery_outbox WHERE subscription_id=$1`, d.IDs[1]).Scan(&count, &canonical))
		if count != 1 || canonical != trigger {
			t.Fatal("canonical API commit missing")
		}
		if brokerOffset() > record.Offset {
			t.Fatal("broker ack overtook Pending")
		}
		// Advance the live changefeed cursor before crash so NewResume must
		// reopen RegisterAt at its exact durable whole-transaction boundary.
		must(f.PublishChanges(ctx))
		wait("durable changefeed cursor", func() bool {
			r, e := projection.New(reader, grant.ProjectionID).Recover(ctx)
			return e == nil && r.Topic == env["SUBSCRIPTION_CHANGES_TOPIC"] && r.Checkpoint.C > 3
		})
		cp, e := projection.New(reader, grant.ProjectionID).Recover(ctx)
		must(e)
		out, err = proof(progresspb.ConsumerKind_CONSUMER_KIND_PROJECTION, grant.ProjectionID, cp.Topic, f.ChangesSource.TopicID, cp.Partition)
		must(err)
		if out.NextOffset == nil || *out.NextOffset < cp.Checkpoint.NextOffset {
			t.Fatal("runtime changefeed proof", out)
		}
		changesOffset = *out.NextOffset
		// No signal handler or graceful assertion: kill the actual executable.
		must(syscall.Kill(-p.PID(), syscall.SIGKILL))
		select {
		case <-p.Done():
		case <-ctx.Done():
			return errors.New("kill not reaped")
		}
		return nil
	}))
	pause.Release()
	f.SetPrice(d.Pair, "99", 3)
	// Same identity/bootstrap, SQL progress and immutable Pending survive crash.
	must(apiprocess.With(ctx, cfg, func(p *apiprocess.Process) error {
		wait("successor ready", func() bool { return status(p, "/readyz") == 200 })
		current, _, e := fencing.New(reader).Current(ctx, identity)
		must(e)
		if current.Token <= grant.Token || current.ProjectionID != grant.ProjectionID || current.Scope != grant.Scope {
			t.Fatal("identity/recovery changed")
		}
		store := pricework.NewSourceBound(reader, grant, f.PricesSource)
		wait("SQL prefix", func() bool {
			v, e := store.Checkpoint(ctx, record.Topic, record.Partition)
			return e == nil && v == record.Offset+1
		})
		wait("broker prefix", func() bool { return brokerOffset() == record.Offset+1 })
		out, err := proof(progresspb.ConsumerKind_CONSUMER_KIND_PRICES, current.Scope, record.Topic, f.PricesSource.TopicID, record.Partition)
		must(err)
		if out.NextOffset == nil || *out.NextOffset != record.Offset+1 {
			t.Fatal("runtime prices proof", out)
		}
		out, err = proof(progresspb.ConsumerKind_CONSUMER_KIND_PROJECTION, current.ProjectionID, f.ChangesSource.Topic, f.ChangesSource.TopicID, 0)
		must(err)
		if out.NextOffset == nil || *out.NextOffset < changesOffset {
			t.Fatal("export lost durable generation", out)
		}
		work, e := store.Get(ctx, key)
		must(e)
		if work.State != "superseded" || work.Completed < partial.Completed || work.Completed > 2 || work.SupersededBy == nil || work.SupersededBy.Revision.Sequence != 3 || work.Entries[0].Result == nil || work.Entries[0].Result.TriggerID != partial.Entries[0].Result.TriggerID {
			t.Fatalf("durable outcomes: state=%s completed=%d superseded=%v first=%v original=%v", work.State, work.Completed, work.SupersededBy, work.Entries[0].Result, partial.Entries[0].Result)
		}
		// Recovery may already persist the second canonical API response while
		// the live view is synchronizing; the third false candidate stays absent.
		if work.Entries[1].Result != nil && work.Entries[1].Result.TriggerID != trigger {
			t.Fatal("recovered result changed canonical trigger")
		}
		recovered, e := deliveryrpc.NewClient(f.Conn).RequestDelivery(ctx, d.OwnerID, d.IDs[1], 1)
		must(e)
		if recovered.Workflow == nil || recovered.Workflow.TriggerId != trigger {
			t.Fatal("canonical workflow changed")
		}
		keys, e := store.Pending(ctx)
		must(e)
		if len(keys) != 0 {
			t.Fatal("pending remains")
		}
		for i, id := range d.IDs {
			var n int
			must(api.QueryRow(ctx, `SELECT count(*) FROM app.delivery_outbox WHERE subscription_id=$1`, id).Scan(&n))
			want := 1
			if i == 2 {
				want = 0
			}
			if n != want {
				t.Fatal("outbox duplicated or false historical workflow", i, n)
			}
		}
		// Revoked SQL authority is terminal; cmd must not silently reset/retry.
		must(fencing.New(reader).Revoke(ctx, current))
		_, e = f.PublishPrice(ctx, f.SetPrice(d.Pair, "99", 4))
		must(e)
		select {
		case <-p.Done():
		case <-time.After(6 * time.Second):
			return errors.New("terminal dependency failure did not exit")
		}
		return nil
	}))
	// Loss of the durable replay pin is terminal across process restarts.
	var lostSnapshot string
	must(apiprocess.With(ctx, cfg, func(p *apiprocess.Process) error {
		wait("ready before pin loss", func() bool { return status(p, "/readyz") == 200 })
		recovered, e := projection.New(reader, grant.ProjectionID).Recover(ctx)
		must(e)
		lostSnapshot = recovered.SnapshotID
		_, e = replayrpc.NewClient(f.Conn).AbandonReplay(ctx, &pb.AbandonReplayRequest{ResourceId: lostSnapshot})
		must(e)
		select {
		case <-p.Done():
		case <-time.After(6 * time.Second):
			return errors.New("pin loss did not stop process")
		}
		return nil
	}))
	lost := cfg
	lost.StartupProbePath = "/readyz"
	e = apiprocess.With(ctx, lost, func(*apiprocess.Process) error { return errors.New("lost pin reset silently") })
	if !errors.Is(e, apiprocess.ErrEarlyExit) {
		t.Fatal("lost pin restart not terminal", e)
	}
	recovered, e := projection.New(reader, grant.ProjectionID).Recover(ctx)
	must(e)
	if recovered.SnapshotID != lostSnapshot {
		t.Fatal("lost pin caused new snapshot")
	}
	// Fail before ready on malformed DSN, inaccessible credential and bad Kafka topic.
	for _, tc := range []struct{ k, v string }{{"KAFKA_CLUSTER_ID", "wrong-cluster"}, {"PRICES_TOPIC_ID", "ffffffffffffffffffffffffffffffff"}, {"SUBSCRIPTION_CHANGES_TOPIC_ID", "ffffffffffffffffffffffffffffffff"}, {"MATCHER_DATABASE_URL", "invalid-dsn"}, {"MATCHER_DATABASE_URL", "postgres://matcher_runtime:secret@127.0.0.1:1/matcher?sslmode=disable"}, {"RPC_KEY_FILE", "/missing/matcher-key"}, {"PRICES_TOPIC", f.Prefix + "-missing"}, {"API_RPC_ADDR", "127.0.0.1:1"}, {"LATEST_RPC_ADDR", "127.0.0.1:1"}} {
		bad := map[string]string{}
		for k, v := range env {
			bad[k] = v
		}
		bad[tc.k] = tc.v
		bad["MATCHER_GROUP"] = env["MATCHER_GROUP"] + "-bad"
		bad["MATCHER_REPLICA_ID"] = "bad"
		failure := cfg
		failure.Env = bad
		failure.StartupProbePath = "/readyz"
		e := apiprocess.With(ctx, failure, func(*apiprocess.Process) error { return errors.New("unexpected ready") })
		if !errors.Is(e, apiprocess.ErrEarlyExit) {
			t.Fatal("startup failure not observed", tc.k, e)
		}
	}
	t.Run("graceful", func(t *testing.T) { testMatcherShutdown(t, binary) })
}
