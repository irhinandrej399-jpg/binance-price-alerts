package matcherprocess

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/deliveryrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/replayrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/snapshotrpc"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/latest"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/fencing"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/live"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricefeed"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricework"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/replica"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/snapshotload"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/postgres"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/apiprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"google.golang.org/grpc"
)

// Connectivity child only: no matcher replica or product signal handling.
func TestChild(t *testing.T) {
	if os.Getenv("MATCHER_PROCESS_CHILD") != "1" {
		return
	}
	fail := func() { os.Exit(25) }
	for _, k := range []string{"DATABASE_URL", "API_DATABASE_URL", "MARKET_DATA_DATABASE_URL", "NOTIFICATIONS_DATABASE_URL", "PARENT_SECRET"} {
		if os.Getenv(k) != "" {
			fail()
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	pool, err := postgres.Open(ctx, os.Getenv("MATCHER_DATABASE_URL"))
	if err != nil {
		fail()
	}
	defer pool.Close()
	var db, role string
	var foreign bool
	err = pool.QueryRow(ctx, `SELECT current_database(),current_user,has_database_privilege(current_user,'api','CONNECT') OR has_database_privilege(current_user,'market_data','CONNECT') OR has_database_privilege(current_user,'notifications','CONNECT')`).Scan(&db, &role, &foreign)
	if err != nil || db != "matcher" || role != "matcher_runtime" || foreign {
		fail()
	}
	read := func(k string) []byte {
		b, e := os.ReadFile(os.Getenv(k))
		if e != nil {
			fail()
		}
		return b
	}
	creds, err := serviceauth.ClientCredentials(read("RPC_CERT_FILE"), read("RPC_KEY_FILE"), read("RPC_CA_FILE"))
	if err != nil {
		fail()
	}
	conn, err := grpc.NewClient(os.Getenv("API_RPC_ADDR"), grpc.WithTransportCredentials(creds), grpc.WithAuthority(os.Getenv("RPC_SERVER_NAME")))
	if err != nil {
		fail()
	}
	defer conn.Close()
	snap, err := pb.NewSubscriptionSnapshotsClient(conn).BuildSnapshot(ctx, &pb.BuildSnapshotRequest{RequestId: "child-probe", TtlMillis: 60000, PageSize: 10})
	if err != nil || len(snap.Subscriptions) != 3 {
		fail()
	}
	_, err = pb.NewSubscriptionSnapshotsClient(conn).ReadSnapshotPage(ctx, &pb.ReadSnapshotPageRequest{SnapshotId: snap.SnapshotId, PageSize: 10})
	if err != nil {
		fail()
	}
	replay := pb.NewSubscriptionReplayClient(conn)
	state, err := replay.BeginReplay(ctx, &pb.BeginReplayRequest{SnapshotId: snap.SnapshotId, TtlMillis: 60000})
	if err != nil {
		fail()
	}
	_, err = replay.ReadReplayPage(ctx, &pb.ReadReplayPageRequest{ResourceId: state.ResourceId, After: state.Progress, MaxTransactions: 10})
	if err != nil {
		fail()
	}
	_, err = replay.AbandonReplay(ctx, &pb.AbandonReplayRequest{ResourceId: state.ResourceId})
	if err != nil {
		fail()
	}
	q, err := latest.NewClient(conn).Latest(ctx, "BTCUSDT")
	if err != nil || q.Quote == nil || q.Quote.Price != "99" {
		fail()
	}
	c, err := kgo.NewClient(kgo.SeedBrokers(os.Getenv("KAFKA_BROKERS")))
	if err != nil {
		fail()
	}
	defer c.Close()
	if c.Ping(ctx) != nil {
		fail()
	}
	meta, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, c)
	if err != nil {
		fail()
	}
	found := false
	for _, topic := range meta.Topics {
		if topic.Topic != nil && *topic.Topic == os.Getenv("PRICES_TOPIC") && len(topic.Partitions) == 16 {
			found = true
		}
	}
	if !found {
		fail()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	_ = http.ListenAndServe(os.Getenv("HTTP_ADDR"), mux)
	os.Exit(26)
}

func TestDisposableFixture(t *testing.T) {
	if os.Getenv("MATCHER_PROCESS_INTEGRATION") != "1" {
		t.Skip("run python3 scripts/check_matcher_process.py")
	}
	if os.Getenv("POSTGRES_TEST") != "1" || os.Getenv("KAFKA_TEST_BROKER") == "" || os.Getenv("KAFKA_TEST_PREFIX") == "" {
		t.Fatal("matcher opt-in requires PostgreSQL/Kafka configuration")
	}
	api := postgrestest.Open(t)
	pool := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	reader := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 65*time.Second)
	defer cancel()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	f := New(t, api, os.Getenv("KAFKA_TEST_BROKER"), os.Getenv("KAFKA_TEST_PREFIX"))
	changes, err := f.Changes(ctx, nil)
	must(err)
	defer changes.Close()
	d, err := f.Seed(ctx)
	must(err)
	sort.Strings(d.IDs)
	f.SetPrice(d.Pair, "99", 1)
	must(f.PublishChanges(ctx))
	batch, err := changes.Next(ctx)
	must(err)
	if len(batch.Records) != 3 || batch.C-batch.First != 2 {
		t.Fatal("canonical change transaction missing")
	}
	for _, record := range batch.Records {
		var body []byte
		must(api.QueryRow(ctx, `SELECT body FROM app.api_journal WHERE position=$1`, record.Change.Position).Scan(&body))
		if !bytes.Equal(body, record.JSON) {
			t.Fatal("change feed bytes differ from API journal")
		}
	}
	must(changes.Confirm(batch.NextOffset))
	bootstrap, err := f.Bootstrap(ctx)
	must(err)
	// Startup verifies SQL privileges, real Snapshot/Replay, Latest and Kafka in
	// a distinct executable with a parent secret deliberately present.
	executable, err := os.Executable()
	must(err)
	t.Setenv("PARENT_SECRET", "must-not-leak")
	env := f.ChildEnv(os.Getenv("MATCHER_DATABASE_URL"))
	env["MATCHER_PROCESS_CHILD"] = "1"
	cfg := apiprocess.Config{Executable: executable, Args: []string{"-test.run=^TestChild$"}, Env: env, StartupTimeout: 10 * time.Second, StopTimeout: time.Second}
	must(apiprocess.With(ctx, cfg, func(*apiprocess.Process) error { return nil }))
	bad := f.ChildEnv("invalid-dsn")
	bad["MATCHER_PROCESS_CHILD"] = "1"
	cfg.Env = bad
	if err = apiprocess.With(ctx, cfg, func(*apiprocess.Process) error { return errors.New("unexpected startup") }); !errors.Is(err, apiprocess.ErrEarlyExit) {
		t.Fatal("startup failure not observed", err)
	}
	// Real replica stays in the parent for harness qualification only. Product
	// process wiring and SIGTERM assertions belong to OPS-053/010.
	pause := f.PauseDelivery(d.IDs[1])
	defer pause.Release()
	base := replica.Config{Pool: pool, ID: "fixture", Environment: f.Prefix, Group: f.Prefix + "-matcher", ChangesSource: f.ChangesSource, Prices: pricefeed.Config{Source: f.PricesSource, Brokers: []string{f.Broker}, Topic: f.Prefix + "-prices", Buffer: 512}, Bootstrap: bootstrap, Replay: replayrpc.NewClient(f.Conn), Latest: latest.NewClient(f.Conn), Delivery: deliveryrpc.NewClient(f.Conn), Live: live.Config{ReplayTTL: time.Minute, HeadInterval: 50 * time.Millisecond}}
	base.Snapshot = func(c context.Context, id string) (string, error) {
		s, e := snapshotload.New(pool, snapshotrpc.NewClient(f.Conn)).Load(c, &pb.BuildSnapshotRequest{RequestId: strings.ReplaceAll(id, ":", "-"), TtlMillis: 60000, PageSize: 1000})
		return s.SnapshotID, e
	}
	base.Changes = f.Changes
	wait := func(label string, ok func() bool) {
		t.Helper()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for !ok() {
			select {
			case <-ctx.Done():
				t.Fatal("timeout: " + label)
			case <-ticker.C:
			}
		}
	}
	var runs []struct {
		stop context.CancelFunc
		done chan error
	}
	defer func() {
		pause.Release()
		for _, run := range runs {
			run.stop()
			select {
			case <-run.done:
			case <-time.After(5 * time.Second):
				t.Error("replica did not stop")
			}
		}
	}()
	start := func() *replica.Replica {
		r, e := replica.New(base)
		must(e)
		c, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- r.Run(c) }()
		runs = append(runs, struct {
			stop context.CancelFunc
			done chan error
		}{stop, done})
		return r
	}
	old := start()
	wait("replica ready", old.Ready)
	var grant fencing.Grant
	for _, g := range old.Grants() {
		if g.Identity.Partition == pricefeed.PairPartition(d.Pair) {
			grant = g
		}
	}
	if grant.Token == 0 {
		t.Fatal("missing grant")
	}
	record, err := f.PublishPrice(ctx, f.SetPrice(d.Pair, "101", 2))
	must(err)
	var response *pb.RequestDeliveryResponse
	select {
	case response = <-pause.Committed:
	case <-ctx.Done():
		t.Fatal("no delayed delivery")
	}
	store := pricework.NewSourceBound(reader, grant, f.PricesSource)
	keys, err := store.Pending(ctx)
	must(err)
	if len(keys) != 1 {
		t.Fatal("expected pending evaluation", keys)
	}
	work, err := store.Get(ctx, keys[0])
	must(err)
	if work.Completed != 1 || len(work.Entries) != 3 || work.State != "pending" {
		t.Fatal("partial durable work not observed", work.Completed, len(work.Entries), work.State)
	}
	// Independent clients observe API commit and broker acknowledgement state.
	var trigger string
	var count int
	must(api.QueryRow(ctx, `SELECT count(*),min(trigger_id::text) FROM app.delivery_outbox WHERE subscription_id=$1`, d.IDs[1]).Scan(&count, &trigger))
	if count != 1 || trigger != response.Workflow.TriggerId {
		t.Fatal("missing canonical API commit")
	}
	observer, err := kgo.NewClient(kgo.SeedBrokers(f.Broker))
	must(err)
	defer observer.Close()
	brokerOffset := func() int64 {
		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = base.Group
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
	if brokerOffset() > record.Offset {
		t.Fatal("broker ack overtook pending")
	}
	runs[0].stop()
	select {
	case e := <-runs[0].done:
		runs[0].done <- e
	case <-ctx.Done():
		t.Fatal("old replica stop")
	}
	pause.Release()
	// The successor sees a false NEW Latest while recovering historical candidates:
	// existing canonical API workflow is recovered, no invented historical trigger.
	f.SetPrice(d.Pair, "99", 3)
	next := start()
	wait("successor ready", next.Ready)
	wait("SQL transport prefix", func() bool {
		v, e := store.Checkpoint(ctx, record.Topic, record.Partition)
		return e == nil && v == record.Offset+1
	})
	wait("broker prefix", func() bool { return brokerOffset() == record.Offset+1 })
	work, err = store.Get(ctx, keys[0])
	must(err)
	if work.State != "superseded" || work.Completed != 1 || work.SupersededBy == nil || work.SupersededBy.Revision.Sequence != 3 || work.Entries[0].Result == nil || work.Entries[0].Result.TriggerID == "" {
		t.Fatal("newer Latest did not supersede historical pending work")
	}
	recovered, err := deliveryrpc.NewClient(f.Conn).RequestDelivery(ctx, d.OwnerID, d.IDs[1], 1)
	must(err)
	if recovered.Workflow == nil || recovered.Workflow.TriggerId != trigger {
		t.Fatal("canonical API recovery changed")
	}
	keys, err = store.Pending(ctx)
	must(err)
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
			t.Fatal("unexpected workflow count", i, n)
		}
	}
}
