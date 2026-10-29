package relay

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/journal"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	changed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/subscriptionchanged"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafka"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
)

type testPublisher struct {
	send func(context.Context, ...kafka.Record) error
}

func (p testPublisher) Publish(c context.Context, rs ...kafka.Record) error { return p.send(c, rs...) }
func (p testPublisher) Close()                                              {}
func fixture(id string) journal.Entry {
	p, _ := price.Parse("100.123456789012345678")
	return journal.Entry{Metadata: event.Envelope{EventID: id, Type: changed.Type, Version: 1, OccurredAt: time.Now().UTC(), CorrelationID: id, OrderingKey: "sub-1"}, Change: changed.Changed{SchemaVersion: 2, OwnerID: "canonical-owner", SubscriptionID: "sub-1", SubscriptionVersion: 1, State: domain.Active, Pair: "BTCUSDT", Threshold: &p, Direction: price.Up, Activation: changed.LatestPrice, CreationSnapshot: &changed.Snapshot{Price: &p, TradeTime: time.Now().UTC()}}}
}
func TestRecovery(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	// Drain earlier packages' work without changing the immutable journal.
	r := &Relay{pool: pool, topic: "test-subscription-changes"}
	r.open = func(context.Context) (publisher, error) {
		return testPublisher{send: func(context.Context, ...kafka.Record) error { return nil }}, nil
	}
	for {
		ok, e := r.Step(ctx)
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
	tx, e := pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	id := fmt.Sprintf("relay-%d", time.Now().UnixNano())
	legacy := fixture(id + "-2")
	legacy.Change.SchemaVersion, legacy.Change.OwnerID = 1, ""
	end, e := journal.Append(ctx, tx, []journal.Entry{fixture(id + "-1"), legacy})
	if e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(ctx); e != nil {
		t.Fatal(e)
	}
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
	rows.Close()
	cursor := func(want uint64) {
		t.Helper()
		var got uint64
		if e := pool.QueryRow(ctx, `SELECT position FROM app.subscription_relay WHERE singleton`).Scan(&got); e != nil || got != want {
			t.Fatalf("cursor=%d want=%d err=%v", got, want, e)
		}
	}
	for _, failure := range []error{kafka.ErrPublish, kafka.ErrCommitUnknown, kafka.ErrFenced} {
		r.open = func(context.Context) (publisher, error) {
			return testPublisher{send: func(_ context.Context, rs ...kafka.Record) error {
				if len(rs) != 2 {
					t.Fatalf("split batch: %d", len(rs))
				}
				for i, v := range rs {
					if !bytes.Equal(v.Value, original[i]) || string(v.Key) != "sub-1" {
						t.Fatal("changed identity/key")
					}
				}
				return failure
			}}, nil
		}
		if _, e = r.Step(ctx); !errors.Is(e, failure) {
			t.Fatalf("failure=%v", e)
		}
		cursor(start)
	}
	// A lost SQL acknowledgement/expired owner after confirmed Kafka commit must
	// leave the batch available and reject a stale cursor update.
	r.open = func(context.Context) (publisher, error) {
		return testPublisher{send: func(_ context.Context, rs ...kafka.Record) error {
			_, e := pool.Exec(ctx, `UPDATE app.subscription_relay SET token=token+1,lease_until=NULL WHERE singleton`)
			return e
		}}, nil
	}
	if _, e = r.Step(ctx); !errors.Is(e, ErrLeaseLost) {
		t.Fatalf("stale mark: %v", e)
	}
	cursor(start)
	r.open = func(context.Context) (publisher, error) {
		return testPublisher{send: func(_ context.Context, rs ...kafka.Record) error {
			cursor(start)
			for i, v := range rs {
				if !bytes.Equal(v.Value, original[i]) {
					t.Fatal("retry bytes changed")
				}
			}
			return nil
		}}, nil
	}
	if ok, e := r.Step(ctx); e != nil || !ok {
		t.Fatalf("recovery %v %v", ok, e)
	}
	cursor(end)
	if ok, e := r.Step(ctx); e != nil || ok {
		t.Fatalf("idle %v %v", ok, e)
	}
	replay, e := journal.ReadAfter(ctx, pool, start, 1)
	if e != nil || len(replay) != 2 {
		t.Fatalf("publish removed replay: %v %v", replay, e)
	}
}

func TestLeaseOwnership(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	r := &Relay{pool: pool, topic: "test-subscription-changes"}
	r.open = func(context.Context) (publisher, error) {
		return testPublisher{send: func(context.Context, ...kafka.Record) error { return nil }}, nil
	}
	for {
		ok, e := r.Step(ctx)
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			break
		}
	}
	tx, e := pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	if _, e = journal.Append(ctx, tx, []journal.Entry{fixture(fmt.Sprintf("lease-%d", time.Now().UnixNano()))}); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	// A crash leaves a future lease busy; an expired lease is reclaimable.
	if _, e = pool.Exec(ctx, `UPDATE app.subscription_relay SET lease_until=clock_timestamp()+interval '1 hour' WHERE singleton`); e != nil {
		t.Fatal(e)
	}
	if _, e = r.Step(ctx); !errors.Is(e, ErrBusy) {
		t.Fatalf("live crash lease: %v", e)
	}
	if _, e = pool.Exec(ctx, `UPDATE app.subscription_relay SET lease_until=clock_timestamp()-interval '1 second' WHERE singleton`); e != nil {
		t.Fatal(e)
	}
	send := false
	r.open = func(context.Context) (publisher, error) {
		// Another replica cannot claim during initialization or publication.
		if _, e := r.Step(ctx); !errors.Is(e, ErrBusy) {
			t.Fatalf("parallel owner: %v", e)
		}
		// Simulate the initializer resuming after its lease was replaced.
		if _, e := pool.Exec(ctx, `UPDATE app.subscription_relay SET token=token+1,lease_until=NULL WHERE singleton`); e != nil {
			t.Fatal(e)
		}
		return testPublisher{send: func(context.Context, ...kafka.Record) error { send = true; return nil }}, nil
	}
	if _, e = r.Step(ctx); !errors.Is(e, ErrLeaseLost) || send {
		t.Fatalf("late initializer published=%v err=%v", send, e)
	}
	// A SQL failure after actual publication leaves the durable cursor unchanged.
	// Close the pool connection is not needed: cancellation at the mark boundary
	// makes UPDATE fail and leaves the same batch for a new caller.
	c, cancel := context.WithCancel(ctx)
	r.open = func(context.Context) (publisher, error) {
		return testPublisher{send: func(context.Context, ...kafka.Record) error { cancel(); return nil }}, nil
	}
	if _, e = r.Step(c); !errors.Is(e, context.Canceled) {
		t.Fatalf("cancelled mark: %v", e)
	}
	r.open = func(context.Context) (publisher, error) {
		return testPublisher{send: func(context.Context, ...kafka.Record) error { return nil }}, nil
	}
	if ok, e := r.Step(ctx); e != nil || !ok {
		t.Fatalf("recover cancelled mark %v %v", ok, e)
	}
	// Fatal fencing stops Run rather than starting a replacement and fighting a new owner.
	tx, e = pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	if _, e = journal.Append(ctx, tx, []journal.Entry{fixture(fmt.Sprintf("fence-%d", time.Now().UnixNano()))}); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	calls := 0
	r.open = func(context.Context) (publisher, error) { calls++; return nil, kafka.ErrFenced }
	if e = r.Run(ctx, nil); !errors.Is(e, kafka.ErrFenced) || calls != 1 {
		t.Fatalf("fenced run calls=%d err=%v", calls, e)
	}
	c, cancel = context.WithCancel(ctx)
	r.open = func(context.Context) (publisher, error) { return nil, kafka.ErrPublish }
	if e = r.Run(c, func(error) { cancel() }); !errors.Is(e, context.Canceled) {
		t.Fatalf("cancel backoff: %v", e)
	}
}
