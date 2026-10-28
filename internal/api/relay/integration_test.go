package relay

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/journal"
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
	cfg := kafka.Config{Brokers: []string{broker}, TransactionalID: prefix + "-subscription-relay"}
	topic := prefix + "-subscription-changes"
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
	start, e := journal.Current(ctx, pool)
	if e != nil {
		t.Fatal(e)
	}
	id := fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	appendBatch := func(ids ...string) uint64 {
		t.Helper()
		tx, e := pool.Begin(ctx)
		if e != nil {
			t.Fatal(e)
		}
		defer tx.Rollback(ctx)
		var es []journal.Entry
		for _, id := range ids {
			es = append(es, fixture(id))
		}
		end, e := journal.Append(ctx, tx, es)
		if e != nil {
			t.Fatal(e)
		}
		if e = tx.Commit(ctx); e != nil {
			t.Fatal(e)
		}
		return end
	}
	end1 := appendBatch(id+"-a", id+"-b")
	end2 := appendBatch(id + "-c")
	var original [][]byte
	rows, e := pool.Query(ctx, `SELECT body FROM app.api_journal WHERE position>$1 ORDER BY position`, int64(start))
	if e != nil {
		t.Fatal(e)
	}
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			t.Fatal(e)
		}
		original = append(original, b)
	}
	if e = rows.Err(); e != nil {
		t.Fatal(e)
	}
	rows.Close()
	cursor := func(want uint64) {
		t.Helper()
		var got uint64
		if e := pool.QueryRow(ctx, `SELECT position FROM app.subscription_relay WHERE singleton`).Scan(&got); e != nil || got != want {
			t.Fatalf("cursor=%d want=%d err=%v", got, want, e)
		}
	}
	// A newer epoch fences the old producer; relay must retain the same SQL prefix.
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
	cursor(start)
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
	cursor(start)
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
	cursor(start)
	// Restart uses the same transactional ID, and repeats exactly the first batch.
	r, e = New(pool, cfg, topic)
	if e != nil {
		t.Fatal(e)
	}
	if ok, e := r.Step(ctx); e != nil || !ok {
		t.Fatalf("retry %v %v", ok, e)
	}
	cursor(end1)
	// Commit Kafka, then lose SQL ownership before marking. The second batch repeats.
	normalOpen = r.open
	r.open = func(c context.Context) (publisher, error) {
		p, e := normalOpen(c)
		if e != nil {
			return nil, e
		}
		return afterCommit{publisher: p, after: func() error {
			_, e := pool.Exec(ctx, `UPDATE app.subscription_relay SET token=token+1,lease_until=NULL WHERE singleton`)
			return e
		}}, nil
	}
	if _, e = r.Step(ctx); !errors.Is(e, ErrLeaseLost) {
		t.Fatalf("SQL mark: %v", e)
	}
	cursor(end1)
	r, e = New(pool, cfg, topic)
	if e != nil {
		t.Fatal(e)
	}
	if ok, e := r.Step(ctx); e != nil || !ok {
		t.Fatalf("SQL retry %v %v", ok, e)
	}
	cursor(end2)
	if ok, e := r.Step(ctx); e != nil || ok {
		t.Fatalf("idle %v %v", ok, e)
	}
	// Independent read_committed consumer proves transaction visibility, ordering,
	// stable original wire bytes (including event_id/C), and single partition/key.
	consumer, e := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxWait(100*time.Millisecond))
	if e != nil {
		t.Fatal(e)
	}
	defer consumer.Close()
	want := [][]byte{original[0], original[1], original[0], original[1], original[2], original[2]}
	n := 0
	c := bounded(t, 20*time.Second)
	for n < len(want) {
		fs := consumer.PollFetches(c)
		if len(fs.Errors()) > 0 {
			t.Fatal(fs.Errors())
		}
		for _, v := range fs.Records() {
			if n >= len(want) || !bytes.Equal(v.Value, want[n]) || v.Partition != 0 || string(v.Key) != "sub-1" {
				t.Fatalf("record %d: altered bytes/order/key/partition", n)
			}
			n++
		}
	}
	replay, e := journal.ReadAfter(ctx, pool, start, 2)
	if e != nil || len(replay) != 3 {
		t.Fatalf("lost replay: %v %v", replay, e)
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
