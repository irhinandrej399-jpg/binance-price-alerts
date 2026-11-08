package cleaner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	observed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/fencing"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricework"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/intake"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/apiprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/cleanerprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/kafkafault"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/notificationsprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func must(t testing.TB, e error) {
	t.Helper()
	if e != nil {
		t.Fatal(e)
	}
}
func TestExporterChild(t *testing.T) {
	if os.Getenv("CLEANER_FIXTURE_ROLE") == "" {
		return
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	if cleanerprocess.ServeOwner(ctx) != nil {
		os.Exit(23)
	}
}
func TestIntegration(t *testing.T) {
	if os.Getenv("CLEANER_PROCESS_INTEGRATION") != "1" {
		t.Skip("real PostgreSQL/Kafka: python3 scripts/check_cleaner_process.py --package ./internal/cleaner")
	}
	if os.Getenv("POSTGRES_TEST") != "1" || os.Getenv("KAFKA_TEST_BROKER") == "" || os.Getenv("KAFKA_TEST_PREFIX") == "" {
		t.Fatal("opt-in requires all dependencies")
	}
	api := postgrestest.Open(t)
	mpool := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	npool := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
	observer := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
	broker, prefix := os.Getenv("KAFKA_TEST_BROKER"), os.Getenv("KAFKA_TEST_PREFIX")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	direct, e := kgo.NewClient(kgo.SeedBrokers(broker), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	must(t, e)
	defer direct.Close()
	nf := notificationsprocess.New(t, api, broker, prefix)
	must(t, nf.Provision(ctx, npool))
	d1, e := nf.Seed(ctx)
	must(t, e)
	d2, e := nf.Seed(ctx)
	must(t, e)
	deliveries := []*kgo.Record{{Topic: nf.Binding.Source.Topic, Partition: 0, Key: d1.Key, Value: d1.Body}, {Topic: nf.Binding.Source.Topic, Partition: 0, Key: d2.Key, Value: d2.Body}}
	must(t, direct.ProduceSync(ctx, deliveries...).FirstErr())
	got, e := cleanerprocess.ReadCommitted(ctx, broker, nf.Binding.Source.Topic, 0, 0, 2)
	must(t, e)
	for i := range got {
		if !bytes.Equal(got[i].Key, deliveries[i].Key) || !bytes.Equal(got[i].Value, deliveries[i].Value) {
			t.Fatal("canonical delivery bytes changed")
		}
	}
	must(t, intake.NewStore(npool).AcceptBound(ctx, nf.Binding, 0, 0, got[0].Offset, got[0].Key, got[0].Value))
	before, e := notificationsprocess.Observe(ctx, observer, d1.Record.Workflow.TriggerID)
	must(t, e)
	if before.State != "pending" || !bytes.Equal(before.Body, d1.Body) {
		t.Fatal("canonical pending job missing")
	}
	src, _, e := kafkasource.Metadata(ctx, direct, prefix+"-prices")
	must(t, e)
	var records []*kgo.Record
	for i := uint64(1); i <= 3; i++ {
		q := nf.SetPrice("100", i)
		wire, e := observed.Encode(event.Envelope{EventID: "price-" + string(rune('0'+i)), Type: observed.Type, Version: 1, OccurredAt: q.ReceivedAt, CorrelationID: "cleaner", OrderingKey: q.Pair}, q)
		must(t, e)
		records = append(records, &kgo.Record{Topic: src.Topic, Partition: 0, Key: []byte(q.Pair), Value: wire})
	}
	must(t, direct.ProduceSync(ctx, records...).FirstErr())
	// Real completed SQL scopes on the same source with different durable nexts.
	var consumers []Consumer
	for _, name := range []string{"fast", "slow", "new", "late"} {
		grant, e := fencing.New(mpool).Assign(ctx, fencing.Request{Identity: fencing.Identity{Environment: prefix, Group: name, Topic: src.Topic, Partition: 0}, Scope: prefix + "-" + name, ProjectionID: prefix + "-view-" + name, RequestID: "first"})
		must(t, e)
		store := pricework.NewSourceBound(mpool, grant, src)
		count := 1
		if name == "fast" {
			count = 2
		}
		for i := 0; i < count; i++ {
			q := nf.SetPrice("100", uint64(i+1))
			input := pricework.Input{Key: name + string(rune('0'+i)), ProjectionID: grant.ProjectionID, C: 1, Quote: q, Position: &pricework.Position{Source: &src, Topic: src.Topic, Partition: 0, Offset: records[i].Offset}}
			must(t, store.Accept(ctx, input))
			must(t, store.Complete(ctx, input.Key, nil, &pricework.Prefix{Source: src, Topic: src.Topic, Partition: 0, From: records[i].Offset, Next: records[i].Offset + 1}, func(context.Context) error { return nil }))
		}
		consumers = append(consumers, Consumer{Name: name, Kind: "prices", ID: grant.Scope, Endpoint: "matcher"})
	}
	f := cleanerprocess.New(t, broker)
	exe, e := os.Executable()
	must(t, e)
	// Build and exercise the actual one-shot maintenance command with an explicit
	// empty env, not an in-process substitute. It receives only config/state files.
	binary := filepath.Join(t.TempDir(), "kafka-cleaner")
	build := exec.CommandContext(ctx, "go", "build", "-race", "-o", binary, "../../cmd/kafka-cleaner")
	if out, e := build.CombinedOutput(); e != nil {
		t.Fatalf("build cleaner: %v %s", e, out)
	}
	m := f.OwnerConfig("matcher", os.Getenv("MATCHER_DATABASE_URL"), exe, "-test.run=^TestExporterChild$")
	n := f.OwnerConfig("notifications", os.Getenv("NOTIFICATIONS_DATABASE_URL"), exe, "-test.run=^TestExporterChild$")
	must(t, apiprocess.With(ctx, m, func(*apiprocess.Process) error {
		return apiprocess.With(ctx, n, func(*apiprocess.Process) error {
			endpoint := func(ep cleanerprocess.Endpoint) Endpoint {
				return Endpoint{Address: ep.Address, ServerName: "market.test", CAFile: ep.CAFile, CertFile: ep.CertFile, KeyFile: ep.KeyFile}
			}
			endpoints := map[string]Endpoint{"matcher": endpoint(f.Caller.Matcher), "notifications": endpoint(f.Caller.Notifications)}
			reader, e := NewRPCReader(endpoints)
			must(t, e)
			defer reader.Close()
			proxy := kafkafault.New(broker)
			defer proxy.Close()
			client, e := kgo.NewClient(kgo.SeedBrokers(broker), kgo.Dialer(proxy.DialContext), kgo.RequestTimeoutOverhead(time.Second))
			must(t, e)
			defer client.Close()
			// A real decoded broker rejection is distinct from a lost response.
			_, badDelete := (KafkaBroker{direct}).Delete(ctx, src, 0, 1000)
			var rejection *BrokerRejection
			if !errors.As(badDelete, &rejection) || rejection.Code == 0 {
				t.Fatal("broker rejection not classified", badDelete)
			}
			store := NewStore(t.TempDir())
			engine := New(store, KafkaBroker{client}, reader)
			unknown := Consumer{Name: "unknown", Kind: "prices", ID: "unknown", Endpoint: "matcher"}
			inv := Inventory{Source: src, Consumers: []Consumer{consumers[0], consumers[1], unknown}}
			must(t, engine.Init(ctx, inv))
			if r, e := engine.Clean(ctx, 100); !errors.Is(e, ErrProof) || r.Attempted {
				t.Fatal("unknown consumer allowed clean", r, e)
			}
			must(t, engine.Retire(ctx, unknown))
			gate := f.PauseRPC("matcher", true)
			rpcDone := make(chan error, 1)
			go func() { _, e := engine.Clean(ctx, 100); rpcDone <- e }()
			select {
			case <-gate.Entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			gate.Fail()
			if e := <-rpcDone; !errors.Is(e, ErrProof) {
				t.Fatal("lost RPC proof permitted delete", e)
			}
			initial, e := cleanerprocess.LogStart(ctx, broker, src.Topic, 0)
			must(t, e)
			fault, e := proxy.Intercept(ctx, 21, kafkafault.BeforeRequest)
			must(t, e)
			type outcome struct {
				r Result
				e error
			}
			done := make(chan outcome, 1)
			go func() { r, e := engine.Clean(ctx, 100); done <- outcome{r, e} }()
			select {
			case <-fault.Events:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			// Membership changes in a second Engine/Store wait for the same file lock.
			other := New(NewStore(store.dir), KafkaBroker{direct}, reader)
			short, stop := context.WithTimeout(ctx, 70*time.Millisecond)
			if e = other.Register(short, consumers[2]); !errors.Is(e, context.DeadlineExceeded) {
				t.Fatal("registration escaped deletion lock", e)
			}
			stop()
			short, stop = context.WithTimeout(ctx, 70*time.Millisecond)
			if e = other.Retire(short, consumers[1]); !errors.Is(e, context.DeadlineExceeded) {
				t.Fatal("retirement escaped deletion lock", e)
			}
			stop()
			if start, e := cleanerprocess.LogStart(ctx, broker, src.Topic, 0); e != nil || start != initial {
				t.Fatal("pre-request fault changed broker", start, e)
			}
			fault.Fail()
			res := <-done
			fault.Release()
			if !errors.Is(res.e, ErrDelete) || res.r.Target != records[1].Offset || res.r.Status != "unknown" {
				t.Fatal(res)
			}
			state, e := store.Inspect(ctx)
			must(t, e)
			if state.Floor != records[1].Offset {
				t.Fatal("failed delete lost reservation", state)
			}
			must(t, other.Register(ctx, consumers[2]))
			r, e := other.Clean(ctx, 100)
			must(t, e)
			if r.Target != records[1].Offset || r.Status != "deleted" {
				t.Fatal("retry changed minimum", r)
			}
			retained, e := cleanerprocess.ReadCommitted(ctx, broker, src.Topic, 0, records[1].Offset, 2)
			must(t, e)
			if !bytes.Equal(retained[0].Value, records[1].Value) {
				t.Fatal("lagging consumer message lost")
			}
			must(t, other.Retire(ctx, consumers[1]))
			must(t, other.Retire(ctx, consumers[2]))
			fault, e = proxy.Intercept(ctx, 21, kafkafault.AfterResponse)
			must(t, e)
			go func() { r, e := engine.Clean(ctx, 100); done <- outcome{r, e} }()
			var ev kafkafault.Event
			select {
			case ev = <-fault.Events:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			response := ev.Response.(*kmsg.DeleteRecordsResponse)
			if len(response.Topics) != 1 || len(response.Topics[0].Partitions) != 1 || response.Topics[0].Partitions[0].ErrorCode != 0 || response.Topics[0].Partitions[0].LowWatermark != records[2].Offset {
				t.Fatal("real delete was not successful", response)
			}
			if start, e := cleanerprocess.LogStart(ctx, broker, src.Topic, 0); e != nil || start != records[2].Offset {
				t.Fatal(start, e)
			}
			fault.Fail()
			res = <-done
			fault.Release()
			if !errors.Is(res.e, ErrDelete) {
				t.Fatal("lost successful response reported success", res)
			}
			// Fresh engine with persisted state must remember the possible deletion.
			other = New(NewStore(store.dir), KafkaBroker{direct}, reader)
			if e = other.Register(ctx, consumers[3]); !errors.Is(e, ErrProof) {
				t.Fatal("late consumer required pruned history", e)
			}
			r, e = other.Clean(ctx, 100)
			must(t, e)
			if r.Status != "noop" || r.Floor != records[2].Offset {
				t.Fatal("retry after unknown response", r)
			}
			retained, e = cleanerprocess.ReadCommitted(ctx, broker, src.Topic, 0, records[2].Offset, 1)
			must(t, e)
			if !bytes.Equal(retained[0].Key, records[2].Key) || !bytes.Equal(retained[0].Value, records[2].Value) {
				t.Fatal("unprocessed price bytes lost")
			}
			// A successful OffsetCommit loses its real response, independently of SQL.
			ack, ackProxy := f.Caller.Kafka(t)
			ackFault, e := ackProxy.Intercept(ctx, 8, kafkafault.AfterResponse)
			must(t, e)
			commit := kmsg.NewPtrOffsetCommitRequest()
			commit.Group = prefix + "-lost-ack"
			commit.Topics = []kmsg.OffsetCommitRequestTopic{{Topic: nf.Binding.Source.Topic, Partitions: []kmsg.OffsetCommitRequestTopicPartition{{Partition: 0, Offset: got[1].Offset, LeaderEpoch: -1}}}}
			ackCtx, ackStop := context.WithTimeout(ctx, time.Second)
			ackDone := make(chan error, 1)
			go func() { _, e := commit.RequestWith(ackCtx, ack); ackDone <- e }()
			select {
			case ev = <-ackFault.Events:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			cr := ev.Response.(*kmsg.OffsetCommitResponse)
			if len(cr.Topics) != 1 || len(cr.Topics[0].Partitions) != 1 || cr.Topics[0].Partitions[0].ErrorCode != 0 {
				t.Fatal(cr)
			}
			offset, e := nf.BrokerOffset(ctx, commit.Group, 0)
			must(t, e)
			if offset != got[1].Offset {
				t.Fatal("broker commit not observed")
			}
			ackFault.Fail()
			if e = <-ackDone; e == nil {
				t.Fatal("lost ack reported success")
			}
			ackStop()
			ackFault.Release()
			// Execute real CLI init/blocked clean/retire/clean/inspect, no SQL credentials.
			cliDir := t.TempDir()
			files := t.TempDir()
			write := func(name string, v any) string {
				path := filepath.Join(files, name)
				b, e := json.Marshal(v)
				must(t, e)
				must(t, os.WriteFile(path, b, 0600))
				return path
			}
			cfg := write("clients.json", map[string]any{"brokers": []string{broker}, "endpoints": endpoints})
			nc := Consumer{Name: "notifications", Kind: "notifications", ID: nf.Binding.ConsumerID, Generation: nf.Binding.Generation, Endpoint: "notifications"}
			bad := nc
			bad.Name = "lost-generation"
			bad.Generation = "unknown"
			ni := write("inventory.json", Inventory{Source: nf.Binding.Source, Consumers: []Consumer{nc, bad}})
			bc := write("retire.json", bad)
			run := func(op string, code int, extra ...string) []byte {
				t.Helper()
				args := []string{"-operation", op, "-state-dir", cliDir, "-config", cfg, "-timeout", "10s"}
				args = append(args, extra...)
				command := exec.CommandContext(ctx, binary, args...)
				command.Env = []string{}
				var out, errOut bytes.Buffer
				command.Stdout = &out
				command.Stderr = &errOut
				e := command.Run()
				actual := 0
				if e != nil {
					var exit *exec.ExitError
					if !errors.As(e, &exit) {
						t.Fatal(e)
					}
					actual = exit.ExitCode()
				}
				if actual != code {
					t.Fatalf("cleaner %s exit %d: %s %s", op, actual, out.Bytes(), errOut.Bytes())
				}
				return out.Bytes()
			}
			run("init", 0, "-inventory", ni)
			run("clean", 1)
			run("retire", 0, "-consumer", bc)
			var report Result
			must(t, json.Unmarshal(run("clean", 0, "-max-offsets", "1"), &report))
			if report.Status != "deleted" || report.Target != got[1].Offset {
				t.Fatal(report)
			}
			run("clean", 0)
			var cliState State
			must(t, json.Unmarshal(run("inspect", 0), &cliState))
			if cliState.Floor != got[1].Offset {
				t.Fatal(cliState)
			}
			remaining, e := cleanerprocess.ReadCommitted(ctx, broker, nf.Binding.Source.Topic, 0, got[1].Offset, 1)
			must(t, e)
			if !bytes.Equal(remaining[0].Key, d2.Key) || !bytes.Equal(remaining[0].Value, d2.Body) {
				t.Fatal("unprocessed notification bytes lost")
			}
			after, e := notificationsprocess.Observe(ctx, observer, d1.Record.Workflow.TriggerID)
			must(t, e)
			if !reflect.DeepEqual(before, after) {
				t.Fatal("cleaner changed canonical pending job")
			}
			cp, e := nf.Progress(ctx, observer, 0)
			must(t, e)
			if cp.Next != got[1].Offset {
				t.Fatal("cleaner changed SQL Prefix")
			}
			var secondJobs int
			must(t, observer.QueryRow(ctx, `SELECT count(*) FROM app.delivery_jobs WHERE trigger_id=$1`, d2.Record.Workflow.TriggerID).Scan(&secondJobs))
			if secondJobs != 0 {
				t.Fatal("cleaner created unprocessed job")
			}
			return nil
		})
	}))
	t.Log("real SQL/mTLS minimum, unknown/generation/RPC failure; durable floor, concurrent membership, pre-delete failure/lost success/retry; real CLI without DSN; canonical pending job and unprocessed bytes preserved")
}
