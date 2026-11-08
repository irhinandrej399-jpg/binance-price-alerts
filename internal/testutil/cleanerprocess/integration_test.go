package cleanerprocess

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	observed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	mpb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/matcher/v1"
	npb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/notifications/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/fencing"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricework"
	mrpc "github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/progressrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/intake"
	nrpc "github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/progressrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/apiprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/kafkafault"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/notificationsprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func must(t testing.TB, e error) {
	t.Helper()
	if e != nil {
		t.Fatal(e)
	}
}
func TestOwnerChild(t *testing.T) {
	if os.Getenv("CLEANER_FIXTURE_ROLE") == "" {
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if ServeOwner(ctx) != nil {
		os.Exit(23)
	}
}

// TestCallerChild proves the complete caller allowlist in a real child, without
// SQL credentials or parent env. Both real mTLS endpoints and Kafka must work.
func TestCallerChild(t *testing.T) {
	if os.Getenv("CLEANER_CALLER_CHILD") != "1" {
		return
	}
	for _, key := range []string{"DATABASE_URL", "OWNER_DATABASE_URL", "MATCHER_DATABASE_URL", "NOTIFICATIONS_DATABASE_URL", "MARKET_DATA_DATABASE_URL", "PARENT_SECRET", "RPC_CERT_FILE", "RPC_KEY_FILE"} {
		if os.Getenv(key) != "" {
			os.Exit(24)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	endpoint := func(role string) Endpoint {
		return Endpoint{Address: os.Getenv(role + "_RPC_ADDR"), CAFile: os.Getenv(role + "_CA_FILE"), CertFile: os.Getenv(role + "_CERT_FILE"), KeyFile: os.Getenv(role + "_KEY_FILE")}
	}
	// Unknown exact consumer must reach the SQL adapters and return no proof.
	mc := mrpc.NewClient(endpoint("MATCHER").Conn(t))
	nc := nrpc.NewClient(endpoint("NOTIFICATIONS").Conn(t))
	id := make([]byte, 16)
	id[0] = 1
	_, e := mc.ReadProgress(ctx, &mpb.ReadProgressRequest{Kind: mpb.ConsumerKind_CONSUMER_KIND_PRICES, ConsumerId: "unknown", ClusterId: "unknown", Topic: "unknown", TopicId: id})
	if status.Code(e) != codes.FailedPrecondition {
		os.Exit(25)
	}
	_, e = nc.ReadProgress(ctx, &npb.ReadProgressRequest{ConsumerId: "unknown", Generation: "unknown", ClusterId: "unknown", Topic: "unknown", TopicId: id})
	if status.Code(e) != codes.FailedPrecondition {
		os.Exit(26)
	}
	c, e := kgo.NewClient(kgo.SeedBrokers(os.Getenv("KAFKA_BROKERS")))
	if e != nil {
		os.Exit(27)
	}
	defer c.Close()
	if c.Ping(ctx) != nil {
		os.Exit(28)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	_ = http.ListenAndServe(os.Getenv("HTTP_ADDR"), mux)
}

func TestIntegration(t *testing.T) {
	if os.Getenv("CLEANER_PROCESS_INTEGRATION") != "1" {
		t.Skip("real PostgreSQL/Kafka: python3 scripts/check_cleaner_process.py")
	}
	if os.Getenv("POSTGRES_TEST") != "1" || os.Getenv("KAFKA_TEST_BROKER") == "" || os.Getenv("KAFKA_TEST_PREFIX") == "" {
		t.Fatal("opt-in requires all dependencies")
	}
	api := postgrestest.Open(t)
	mpool := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	npool := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
	// Separate observer connections never pass through either RPC endpoint.
	mr := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	nr := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
	broker, prefix := os.Getenv("KAFKA_TEST_BROKER"), os.Getenv("KAFKA_TEST_PREFIX")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	nf := notificationsprocess.New(t, api, broker, prefix)
	must(t, nf.Provision(ctx, npool))
	first, e := nf.Seed(ctx)
	must(t, e)
	second, e := nf.Seed(ctx)
	must(t, e)
	direct, e := kgo.NewClient(kgo.SeedBrokers(broker), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	must(t, e)
	defer direct.Close()
	// Both original API outbox payloads are deliberately placed in one fixture
	// partition. No relay/partitioner or full service runtime is being qualified.
	records := []*kgo.Record{{Topic: nf.Binding.Source.Topic, Partition: 0, Key: first.Key, Value: first.Body}, {Topic: nf.Binding.Source.Topic, Partition: 0, Key: second.Key, Value: second.Body}}
	must(t, direct.ProduceSync(ctx, records...).FirstErr())
	got, e := ReadCommitted(ctx, broker, nf.Binding.Source.Topic, 0, 0, 2)
	must(t, e)
	for i := range records {
		if !bytes.Equal(got[i].Key, records[i].Key) || !bytes.Equal(got[i].Value, records[i].Value) {
			t.Fatal("canonical API bytes changed")
		}
	}
	// Durable job and SQL Prefix before any broker offset acknowledgement.
	must(t, intake.NewStore(npool).AcceptBound(ctx, nf.Binding, 0, 0, got[0].Offset, got[0].Key, got[0].Value))
	before, e := notificationsprocess.Observe(ctx, nr, first.Record.Workflow.TriggerID)
	must(t, e)
	if before.State != "pending" || !bytes.Equal(before.Body, first.Body) {
		t.Fatal("durable pending job missing")
	}
	cp, e := nf.Progress(ctx, nr, 0)
	must(t, e)
	if cp.Next != got[0].Offset+1 {
		t.Fatal(cp)
	}
	var jobs int
	must(t, nr.QueryRow(ctx, `SELECT count(*) FROM app.delivery_jobs WHERE trigger_id=$1`, second.Record.Workflow.TriggerID).Scan(&jobs))
	if jobs != 0 {
		t.Fatal("unprocessed message became job")
	}
	src, _, e := kafkasource.Metadata(ctx, direct, prefix+"-prices")
	must(t, e)
	grant, e := fencing.New(mpool).Assign(ctx, fencing.Request{Identity: fencing.Identity{Environment: prefix, Group: "matcher", Topic: src.Topic, Partition: 0}, Scope: prefix + "-fast", ProjectionID: prefix + "-view", RequestID: "first"})
	must(t, e)
	p, _ := price.Parse("100")
	now := time.Now().UTC()
	work := pricework.NewSourceBound(mpool, grant, src)
	input := pricework.Input{Key: "one", ProjectionID: grant.ProjectionID, C: 1, Quote: observed.Observed{SchemaVersion: 1, Pair: "BTCUSDT", Price: &p, SourceID: "fixture", Revision: observed.Revision{StreamID: "fixture", Generation: 1, Sequence: 1}, TradeTime: now, ReceivedAt: now, StreamState: observed.Live}, Position: &pricework.Position{Source: &src, Topic: src.Topic, Partition: 0, Offset: 0}}
	// Use real canonical PriceObserved records for the matcher fixture position.
	wire, e := observed.Encode(event.Envelope{EventID: "fixture-price", Type: observed.Type, Version: 1, OccurredAt: now, CorrelationID: "fixture", OrderingKey: "BTCUSDT"}, input.Quote)
	must(t, e)
	pr := &kgo.Record{Topic: src.Topic, Partition: 0, Key: []byte("BTCUSDT"), Value: wire}
	must(t, direct.ProduceSync(ctx, pr).FirstErr())
	input.Position.Offset = pr.Offset
	must(t, work.Accept(ctx, input))
	must(t, work.Complete(ctx, input.Key, nil, &pricework.Prefix{Source: src, Topic: src.Topic, Partition: 0, From: pr.Offset, Next: pr.Offset + 1}, func(context.Context) error { return nil }))
	slow, e := fencing.New(mpool).Assign(ctx, fencing.Request{Identity: fencing.Identity{Environment: prefix, Group: "slow", Topic: src.Topic, Partition: 0}, Scope: prefix + "-slow", ProjectionID: prefix + "-slow-view", RequestID: "first"})
	must(t, e)
	input.Key = "slow"
	input.ProjectionID = slow.ProjectionID
	input.Candidates = []pricework.Candidate{{OwnerID: "owner", SubscriptionID: "pending", Version: 1}}
	must(t, pricework.NewSourceBound(mpool, slow, src).Accept(ctx, input)) // Pending has no durable cursor.

	// A second completed consumer on the same source is genuinely behind fast.
	lag, e := fencing.New(mpool).Assign(ctx, fencing.Request{Identity: fencing.Identity{Environment: prefix, Group: "lagging", Topic: src.Topic, Partition: 0}, Scope: prefix + "-lagging", ProjectionID: prefix + "-lag-view", RequestID: "first"})
	must(t, e)
	lagInput := input
	lagInput.Key = "lagging"
	lagInput.ProjectionID = lag.ProjectionID
	lagInput.Candidates = nil
	lagStore := pricework.NewSourceBound(mpool, lag, src)
	must(t, lagStore.Accept(ctx, lagInput))
	must(t, lagStore.Complete(ctx, lagInput.Key, nil, &pricework.Prefix{Source: src, Topic: src.Topic, Partition: 0, From: pr.Offset, Next: pr.Offset + 1}, func(context.Context) error { return nil }))
	// Fast consumes a second real observation; slow Pending remains unconfirmed.
	input.Key = "two"
	input.ProjectionID = grant.ProjectionID
	input.Candidates = nil
	input.Quote.Revision.Sequence = 2
	input.Quote.SourceID = "fixture-2"
	wire, e = observed.Encode(event.Envelope{EventID: "fixture-price-2", Type: observed.Type, Version: 1, OccurredAt: now, CorrelationID: "fixture", OrderingKey: "BTCUSDT"}, input.Quote)
	must(t, e)
	pr2 := &kgo.Record{Topic: src.Topic, Partition: 0, Key: []byte("BTCUSDT"), Value: wire}
	must(t, direct.ProduceSync(ctx, pr2).FirstErr())
	input.Position = &pricework.Position{Source: &src, Topic: src.Topic, Partition: 0, Offset: pr2.Offset}
	must(t, work.Accept(ctx, input))
	must(t, work.Complete(ctx, input.Key, nil, &pricework.Prefix{Source: src, Topic: src.Topic, Partition: 0, From: pr.Offset + 1, Next: pr2.Offset + 1}, func(context.Context) error { return nil }))

	f := New(t, broker)
	exe, e := os.Executable()
	must(t, e)
	mcfg := f.OwnerConfig("matcher", os.Getenv("MATCHER_DATABASE_URL"), exe, "-test.run=^TestOwnerChild$")
	ncfg := f.OwnerConfig("notifications", os.Getenv("NOTIFICATIONS_DATABASE_URL"), exe, "-test.run=^TestOwnerChild$")
	t.Setenv("PARENT_SECRET", "must-not-leak")
	e = apiprocess.With(ctx, mcfg, func(*apiprocess.Process) error {
		return apiprocess.With(ctx, ncfg, func(*apiprocess.Process) error {
			mc := mrpc.NewClient(f.Caller.Matcher.Conn(t))
			nc := nrpc.NewClient(f.Caller.Notifications.Conn(t))
			mq := &mpb.ReadProgressRequest{Kind: mpb.ConsumerKind_CONSUMER_KIND_PRICES, ConsumerId: grant.Scope, ClusterId: src.ClusterID, TopicId: src.TopicID[:], Topic: src.Topic, Partition: 0}
			nq := &npb.ReadProgressRequest{ConsumerId: nf.Binding.ConsumerID, Generation: nf.Binding.Generation, ClusterId: nf.Binding.Source.ClusterID, TopicId: nf.Binding.Source.TopicID[:], Topic: nf.Binding.Source.Topic, Partition: 0}
			check := func() {
				t.Helper()
				a, e := mc.ReadProgress(ctx, mq)
				must(t, e)
				b, e := pricework.New(mr, grant.Scope).ReadCheckpoint(ctx, src.Topic, 0)
				must(t, e)
				if a.NextOffset == nil || *a.NextOffset != b.NextOffset {
					t.Fatal("matcher RPC/SQL mismatch")
				}
				n, e := nc.ReadProgress(ctx, nq)
				must(t, e)
				sql, e := nf.Progress(ctx, nr, 0)
				must(t, e)
				if n.NextOffset == nil || *n.NextOffset != sql.Next {
					t.Fatal("notifications RPC/SQL mismatch")
				}
			}
			check()
			// Both trust domains reject a valid CA-signed caller without cleaner rights.
			_, denied := mrpc.NewClient(f.denied["matcher"].Conn(t)).ReadProgress(ctx, mq)
			if status.Code(denied) != codes.PermissionDenied {
				t.Fatal("matcher mTLS authorization", denied)
			}
			_, denied = nrpc.NewClient(f.denied["notifications"].Conn(t)).ReadProgress(ctx, nq)
			if status.Code(denied) != codes.PermissionDenied {
				t.Fatal("notifications mTLS authorization", denied)
			}
			mq.ConsumerId = lag.Scope
			lagProof, e := mc.ReadProgress(ctx, mq)
			must(t, e)
			lagSQL, e := pricework.New(mr, lag.Scope).ReadCheckpoint(ctx, src.Topic, 0)
			must(t, e)
			if lagProof.NextOffset == nil || *lagProof.NextOffset != lagSQL.NextOffset || *lagProof.NextOffset >= pr2.Offset+1 {
				t.Fatal("lagging consumer not independently proved", lagProof)
			}
			mq.ConsumerId = slow.Scope
			a, e := mc.ReadProgress(ctx, mq)
			if a != nil || status.Code(e) != codes.FailedPrecondition {
				t.Fatal("Pending scope invented proof", a, e)
			}
			mq.ConsumerId = grant.Scope
			nq.ConsumerId = "unknown"
			b, e := nc.ReadProgress(ctx, nq)
			if b != nil || status.Code(e) != codes.FailedPrecondition {
				t.Fatal("unknown consumer proof", b, e)
			}
			nq.ConsumerId = nf.Binding.ConsumerID
			// An explicit lagging partition has SQL zero, independently of fast partition.
			nq.Partition = 1
			b, e = nc.ReadProgress(ctx, nq)
			must(t, e)
			if b.NextOffset == nil || *b.NextOffset != 0 {
				t.Fatal("lagging consumer position lost")
			}
			nq.Partition = 0
			// Gate a genuine response, lose it, then recover using a new RPC.
			gate := f.PauseRPC("notifications", true)
			done := make(chan error, 1)
			go func() { _, e := nc.ReadProgress(ctx, nq); done <- e }()
			select {
			case <-gate.Entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			gate.Fail()
			if status.Code(<-done) != codes.Unavailable {
				t.Fatal("lost response accepted")
			}
			check()
			gate = f.PauseRPC("matcher", false)
			short, stop := context.WithTimeout(ctx, 80*time.Millisecond)
			a, e = mc.ReadProgress(short, mq)
			stop()
			if a != nil || status.Code(e) != codes.DeadlineExceeded {
				t.Fatal("RPC deadline lost", a, e)
			}
			gate.Release()
			check()
			// Caller child receives only RPC/Kafka config and can reach both owners.
			env := f.Caller.Env()
			env["CLEANER_CALLER_CHILD"] = "1"
			must(t, apiprocess.With(ctx, apiprocess.Config{Executable: exe, Args: []string{"-test.run=^TestCallerChild$"}, Env: env, StartupTimeout: 8 * time.Second, StopTimeout: time.Second}, func(*apiprocess.Process) error { return nil }))
			qualifyKafka(t, ctx, f, nf, got, before, nr)
			// A test barrier coordinates future registration/retirement actions without
			// inventing a registry or asserting its safety before OPS-014 exists.
			barrier := NewBarrier()
			actions := make(chan error, 1)
			go func() { actions <- barrier.Arrive(ctx) }()
			<-barrier.Entered
			barrier.Release()
			must(t, <-actions)
			return nil
		})
	})
	must(t, e)
	// Startup failure and callback failure use the same verified lifecycle.
	bad := f.OwnerConfig("matcher", "postgres://invalid@127.0.0.1:1/matcher?sslmode=disable", exe, "-test.run=^TestOwnerChild$")
	if e = apiprocess.With(ctx, bad, func(*apiprocess.Process) error { t.Error("invalid owner started"); return nil }); e == nil {
		t.Fatal("bad DSN accepted")
	}
	sentinel := errors.New("intentional callback failure")
	if e = apiprocess.With(ctx, ncfg, func(*apiprocess.Process) error { return sentinel }); !errors.Is(e, sentinel) {
		t.Fatal(e)
	}
	t.Log("two isolated SQL exporter owners; caller without DSN; RPC fault/recovery; canonical pending job and unprocessed bytes; broker faults/retry; startup/callback cleanup")
}

func qualifyKafka(t *testing.T, ctx context.Context, f *Fixture, nf *notificationsprocess.Fixture, records []*kgo.Record, before notificationsprocess.Observation, nr *pgxpool.Pool) {
	t.Helper()
	caller, proxy := f.Caller.Kafka(t)
	topic := nf.Binding.Source.Topic
	boundary := records[1].Offset
	group := nf.Prefix + "-lost-ack"
	event := func(fault *kafkafault.Fault) kafkafault.Event {
		t.Helper()
		select {
		case v := <-fault.Events:
			return v
		case <-ctx.Done():
			t.Fatal(ctx.Err())
			return kafkafault.Event{}
		}
	}
	invoke := func(q kmsg.Request) (<-chan error, context.CancelFunc) {
		c, stop := context.WithTimeout(ctx, 700*time.Millisecond)
		done := make(chan error, 1)
		go func() { _, e := caller.Request(c, q); done <- e }()
		return done, stop
	}
	failed := func(done <-chan error, stop context.CancelFunc) {
		t.Helper()
		defer stop()
		select {
		case e := <-done:
			if e == nil {
				t.Fatal("faulted request returned success")
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	start := func() int64 { t.Helper(); n, e := LogStart(ctx, f.Caller.Broker, topic, 0); must(t, e); return n }
	commit := kmsg.NewPtrOffsetCommitRequest()
	commit.Group = group
	commit.Topics = []kmsg.OffsetCommitRequestTopic{{Topic: topic, Partitions: []kmsg.OffsetCommitRequestTopicPartition{{Partition: 0, Offset: boundary, LeaderEpoch: -1}}}}
	fault, e := proxy.Intercept(ctx, 8, kafkafault.AfterResponse)
	must(t, e)
	done, stop := invoke(commit)
	ev := event(fault)
	r := ev.Response.(*kmsg.OffsetCommitResponse)
	if len(r.Topics) != 1 || len(r.Topics[0].Partitions) != 1 || r.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatal("real OffsetCommit failed", r)
	}
	offset, e := nf.BrokerOffset(ctx, group, 0)
	must(t, e)
	if offset != boundary {
		t.Fatal("committed offset missing")
	}
	fault.Fail()
	failed(done, stop)
	fault.Release()
	after, e := notificationsprocess.Observe(ctx, nr, beforeTrigger(ctx, t, nr, before.Body))
	must(t, e)
	if !reflect.DeepEqual(after, before) {
		t.Fatal("lost ack changed job")
	}
	initial := start()
	del := kmsg.NewPtrDeleteRecordsRequest()
	del.TimeoutMillis = 1000
	del.Topics = []kmsg.DeleteRecordsRequestTopic{{Topic: topic, Partitions: []kmsg.DeleteRecordsRequestTopicPartition{{Partition: 0, Offset: boundary}}}}
	fault, e = proxy.Intercept(ctx, 21, kafkafault.BeforeRequest)
	must(t, e)
	done, stop = invoke(del)
	ev = event(fault)
	if ev.Response != nil || start() != initial {
		t.Fatal("delete escaped pre-request barrier")
	}
	fault.Fail()
	failed(done, stop)
	fault.Release()
	if start() != initial {
		t.Fatal("failed pre-delete changed log-start")
	}
	// Genuine broker error (invalid offset), then explicit valid request.
	invalid := kmsg.NewPtrDeleteRecordsRequest()
	invalid.TimeoutMillis = 1000
	invalid.Topics = []kmsg.DeleteRecordsRequestTopic{{Topic: topic, Partitions: []kmsg.DeleteRecordsRequestTopicPartition{{Partition: 0, Offset: boundary + 100000}}}}
	bad, e := invalid.RequestWith(ctx, caller)
	must(t, e)
	if len(bad.Topics) != 1 || len(bad.Topics[0].Partitions) != 1 || bad.Topics[0].Partitions[0].ErrorCode == 0 {
		t.Fatal("expected real DeleteRecords broker error")
	}
	if start() != initial {
		t.Fatal("broker error changed start")
	}
	fault, e = proxy.Intercept(ctx, 21, kafkafault.AfterResponse)
	must(t, e)
	done, stop = invoke(del)
	ev = event(fault)
	deleted := ev.Response.(*kmsg.DeleteRecordsResponse)
	if len(deleted.Topics) != 1 || len(deleted.Topics[0].Partitions) != 1 || deleted.Topics[0].Partitions[0].ErrorCode != 0 || deleted.Topics[0].Partitions[0].LowWatermark != boundary {
		t.Fatal("no successful real DeleteRecords response", deleted)
	}
	if start() != boundary {
		t.Fatal("lost delete response has no broker effect")
	}
	fault.Fail()
	failed(done, stop)
	fault.Release()
	deleted, e = del.RequestWith(ctx, caller)
	must(t, e)
	if deleted.Topics[0].Partitions[0].ErrorCode != 0 || start() != boundary {
		t.Fatal("same-boundary retry changed result")
	}
	retained, e := ReadCommitted(ctx, f.Caller.Broker, topic, 0, boundary, 1)
	must(t, e)
	if retained[0].Offset != records[1].Offset || !bytes.Equal(retained[0].Key, records[1].Key) || !bytes.Equal(retained[0].Value, records[1].Value) {
		t.Fatal("unprocessed canonical record lost")
	}
	after, e = notificationsprocess.Observe(ctx, nr, beforeTrigger(ctx, t, nr, before.Body))
	must(t, e)
	if !reflect.DeepEqual(after, before) {
		t.Fatal("delete/retry changed durable job")
	}
	cp, e := nf.Progress(ctx, nr, 0)
	must(t, e)
	if cp.Next != boundary {
		t.Fatal("delete altered SQL Prefix")
	}
}
func beforeTrigger(ctx context.Context, t *testing.T, pool *pgxpool.Pool, body []byte) string {
	t.Helper()
	var trigger string
	must(t, pool.QueryRow(ctx, `SELECT trigger_id::text FROM app.delivery_jobs WHERE body=$1`, body).Scan(&trigger))
	return trigger
}
