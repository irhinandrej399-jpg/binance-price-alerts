package deliveryrelay

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafka"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/kafkafault"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/twmb/franz-go/pkg/kgo"
)

func bounded(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return c
}
func TestKafkaRecovery(t *testing.T) {
	if os.Getenv("KAFKA_INTEGRATION") != "1" {
		t.Skip("run KAFKA_RUN_POSTGRES=1 bash infra/kafka/verify-go.sh")
	}
	pool := postgrestest.Open(t)
	ctx := context.Background()
	broker, prefix := os.Getenv("KAFKA_TEST_BROKER"), os.Getenv("KAFKA_TEST_PREFIX")
	if broker == "" || prefix == "" {
		t.Fatal("Kafka opt-in requires test broker/prefix")
	}
	cfg := kafka.Config{Brokers: []string{broker}, TransactionalID: prefix + "-delivery-relay"}
	topic := prefix + "-delivery-requests"
	// Earlier package fixtures are not this test's input.
	drain := &Relay{pool: pool, topic: topic, open: func(context.Context) (publisher, error) {
		return testPublisher{send: func(context.Context, ...kafka.Record) error { return nil }}, nil
	}}
	for {
		ok, e := drain.Step(ctx)
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			break
		}
	}
	sub1, body1 := insert(t, pool)
	sub2, body2 := insert(t, pool)
	if sub1 >= sub2 {
		t.Fatal("fixture order")
	}
	marked := func(sub string, want bool) {
		t.Helper()
		var got bool
		if e := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.delivery_published WHERE subscription_id=$1)`, sub).Scan(&got); e != nil || got != want {
			t.Fatalf("published %s=%v want=%v err=%v", sub, got, want, e)
		}
	}
	// A newer epoch fences the old producer; relay must retain the same SQL work.
	old, e := kafka.New(bounded(t, 10*time.Second), cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer old.Close()
	replacement, e := kafka.New(bounded(t, 10*time.Second), cfg)
	if e != nil {
		t.Fatal(e)
	}
	replacement.Close()
	fenced := &Relay{pool: pool, topic: topic, open: func(context.Context) (publisher, error) { return old, nil }}
	if _, e = fenced.Step(ctx); !errors.Is(e, kafka.ErrFenced) {
		t.Fatalf("real fencing: %v", e)
	}
	marked(sub1, false)
	// Initialized producer loses all broker connections before publication.
	outage := kafkafault.New(broker)
	outageCfg := cfg
	outageCfg.Dialer = outage.DialContext
	unavailable, _ := New(pool, outageCfg, topic)
	unavailable.open = func(c context.Context) (publisher, error) {
		p, e := kafka.New(c, outageCfg)
		outage.Close()
		return p, e
	}
	if _, e = unavailable.Step(bounded(t, time.Second)); !errors.Is(e, kafka.ErrPublish) {
		t.Fatalf("broker outage: %v", e)
	}
	marked(sub1, false)
	// Initialize a real producer, then lose every EndTxn response after the broker commits.
	proxy := kafkafault.New(broker)
	defer proxy.Close()
	fault := cfg
	fault.Dialer = proxy.DialContext
	r, e := New(pool, fault, topic)
	if e != nil {
		t.Fatal(e)
	}
	normalOpen := r.open
	r.open = func(c context.Context) (publisher, error) {
		p, e := normalOpen(c)
		if e == nil {
			proxy.Arm()
		}
		return p, e
	}
	if _, e = r.Step(bounded(t, 4*time.Second)); !errors.Is(e, kafka.ErrCommitUnknown) {
		t.Fatalf("lost response: %v", e)
	}
	select {
	case code := <-proxy.Observed:
		if code != 0 {
			t.Fatalf("EndTxn code=%d", code)
		}
	case <-time.After(time.Second):
		t.Fatal("no confirmed broker commit")
	}
	marked(sub1, false)
	// Restart uses the same transactional ID, and repeats exactly the first event.
	r, e = New(pool, cfg, topic)
	if e != nil {
		t.Fatal(e)
	}
	if ok, e := r.Step(ctx); e != nil || !ok {
		t.Fatalf("retry %v %v", ok, e)
	}
	marked(sub1, true)
	marked(sub2, false)
	// Commit Kafka, then lose SQL ownership before marking. The second event repeats.
	normalOpen = r.open
	r.open = func(c context.Context) (publisher, error) {
		p, e := normalOpen(c)
		if e != nil {
			return nil, e
		}
		return afterCommit{publisher: p, after: func() error {
			_, e := pool.Exec(ctx, `UPDATE app.delivery_relay SET token=token+1,lease_until=NULL WHERE singleton`)
			return e
		}}, nil
	}
	if _, e = r.Step(ctx); !errors.Is(e, ErrLeaseLost) {
		t.Fatalf("SQL mark: %v", e)
	}
	marked(sub1, true)
	marked(sub2, false)
	r, e = New(pool, cfg, topic)
	if e != nil {
		t.Fatal(e)
	}
	if ok, e := r.Step(ctx); e != nil || !ok {
		t.Fatalf("SQL retry %v %v", ok, e)
	}
	marked(sub1, true)
	marked(sub2, true)
	if ok, e := r.Step(ctx); e != nil || ok {
		t.Fatalf("idle %v %v", ok, e)
	}
	// Independent read_committed consumer proves transaction visibility,
	// stable original wire bytes (including event_id/trigger_id), and key routing.
	consumer, e := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxWait(100*time.Millisecond))
	if e != nil {
		t.Fatal(e)
	}
	defer consumer.Close()
	// Kafka does not order records across the 16 partitions. Check each key's
	// immutable replay independently, rather than depending on fetch order.
	want := map[string][]byte{sub1: body1, sub2: body2}
	counts := map[string]int{}
	partitions := map[string]int32{}
	n := 0
	c := bounded(t, 20*time.Second)
	for n < 4 {
		fs := consumer.PollFetches(c)
		if len(fs.Errors()) > 0 {
			t.Fatal(fs.Errors())
		}
		for _, v := range fs.Records() {
			key := string(v.Key)
			body, ok := want[key]
			if !ok || !bytes.Equal(v.Value, body) || counts[key] >= 2 {
				t.Fatalf("altered/unexpected record key=%s", key)
			}
			if previous, ok := partitions[key]; ok && previous != v.Partition {
				t.Fatal("unstable partition")
			}
			partitions[key] = v.Partition
			counts[key]++
			n++
		}
	}
	if counts[sub1] != 2 || counts[sub2] != 2 {
		t.Fatal("missing retry", counts)
	}
	for _, sub := range []string{sub1, sub2} {
		var body []byte
		if e := pool.QueryRow(ctx, `SELECT body FROM app.delivery_outbox WHERE subscription_id=$1`, sub).Scan(&body); e != nil {
			t.Fatal("lost outbox", e)
		}
	}
}

type afterCommit struct {
	publisher
	after func() error
}

func (p afterCommit) Publish(c context.Context, rs ...kafka.Record) error {
	if e := p.publisher.Publish(c, rs...); e != nil {
		return e
	}
	return p.after()
}
