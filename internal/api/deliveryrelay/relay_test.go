package deliveryrelay

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafka"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5/pgxpool"
)

const message = `{"event_id":"outbox-1","type":"alert.triggered","version":1,"occurred_at":"2026-10-27T10:00:01Z","correlation_id":"request-1","ordering_key":"sub-1","payload":{"schema_version":1,"intent":"delivery_requested","trigger_id":"workflow-1","workflow_version":9007199254740993,"user_id":"user-1","subscription_id":"sub-1","subscription_version":9007199254740994,"pair":"BTCUSDT","threshold":"999999999999999999.123456789012345677","direction":"up","candidate_quote":{"schema_version":1,"pair":"BTCUSDT","price":"999999999999999999.123456789012345678","source_id":"9007199254740993","revision":{"stream_id":"binance-spot-trade","generation":2,"sequence":9007199254740993},"trade_time":"2026-10-27T10:00:00.123456Z","received_at":"2026-10-27T10:00:01.123456Z","stream_state":"unconfirmed","gap_detected":true}}}`

type testPublisher struct {
	send func(context.Context, ...kafka.Record) error
}

func (p testPublisher) Publish(c context.Context, rs ...kafka.Record) error { return p.send(c, rs...) }
func (p testPublisher) Close()                                              {}
func insert(t *testing.T, pool *pgxpool.Pool) (string, []byte) {
	t.Helper()
	id := fmt.Sprintf("%032x", time.Now().UnixNano())
	sub := id[:8] + "-" + id[8:12] + "-" + id[12:16] + "-" + id[16:20] + "-" + id[20:]
	body := []byte(strings.NewReplacer("sub-1", sub, "outbox-1", "event-"+id, "workflow-1", "trigger-"+id).Replace(message))
	if _, e := pool.Exec(context.Background(), `INSERT INTO app.delivery_outbox (subscription_id,trigger_id,event_id,body) VALUES ($1,$2,$3,$4)`, sub, "trigger-"+id, "event-"+id, body); e != nil {
		t.Fatal(e)
	}
	return sub, body
}
func drain(t *testing.T, r *Relay) {
	t.Helper()
	r.open = func(context.Context) (publisher, error) {
		return testPublisher{send: func(context.Context, ...kafka.Record) error { return nil }}, nil
	}
	for {
		ok, e := r.Step(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			return
		}
	}
}
func TestRecovery(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	r := &Relay{pool: pool, topic: "test-delivery-requests"}
	drain(t, r)
	sub, body := insert(t, pool)
	marked := func(want bool) {
		t.Helper()
		var got bool
		if e := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.delivery_published WHERE subscription_id=$1)`, sub).Scan(&got); e != nil || got != want {
			t.Fatalf("published=%v want=%v err=%v", got, want, e)
		}
	}
	check := func(rs []kafka.Record) {
		t.Helper()
		if len(rs) != 1 || rs[0].Topic != r.topic || string(rs[0].Key) != sub || !bytes.Equal(rs[0].Value, body) {
			t.Fatal("changed identity/key/bytes", rs)
		}
	}
	for _, failure := range []error{kafka.ErrPublish, kafka.ErrCommitUnknown, kafka.ErrFenced} {
		r.open = func(context.Context) (publisher, error) {
			return testPublisher{send: func(_ context.Context, rs ...kafka.Record) error { check(rs); return failure }}, nil
		}
		if _, e := r.Step(ctx); !errors.Is(e, failure) {
			t.Fatalf("failure=%v", e)
		}
		marked(false)
	}
	// Confirmed publication, then context cancellation prevents SQL acknowledgement.
	c, cancel := context.WithCancel(ctx)
	r.open = func(context.Context) (publisher, error) {
		return testPublisher{send: func(_ context.Context, rs ...kafka.Record) error { check(rs); cancel(); return nil }}, nil
	}
	if _, e := r.Step(c); !errors.Is(e, context.Canceled) {
		t.Fatalf("cancelled ack: %v", e)
	}
	marked(false)
	r.open = func(context.Context) (publisher, error) {
		return testPublisher{send: func(_ context.Context, rs ...kafka.Record) error { check(rs); marked(false); return nil }}, nil
	}
	if ok, e := r.Step(ctx); e != nil || !ok {
		t.Fatalf("retry %v %v", ok, e)
	}
	marked(true)
	if ok, e := r.Step(ctx); e != nil || ok {
		t.Fatalf("idle %v %v", ok, e)
	}
	var saved []byte
	if e := pool.QueryRow(ctx, `SELECT body FROM app.delivery_outbox WHERE subscription_id=$1`, sub).Scan(&saved); e != nil || !bytes.Equal(saved, body) {
		t.Fatal("removed/changed outbox", e)
	}
	// A later commit with a lower UUID must not be skipped by a high-water cursor.
	low := "00000000-0000-0000-0000-000000000001"
	b := []byte(strings.ReplaceAll(string(body), sub, low))
	id := strings.ReplaceAll(sub, "-", "")
	if _, e := pool.Exec(ctx, `INSERT INTO app.delivery_outbox (subscription_id,trigger_id,event_id,body) VALUES ($1,$2,$3,$4)`, low, "trigger-"+id+"-late", "event-"+id+"-late", []byte(strings.NewReplacer("trigger-"+id, "trigger-"+id+"-late", "event-"+id, "event-"+id+"-late").Replace(string(b)))); e != nil {
		t.Fatal(e)
	}
	r.open = func(context.Context) (publisher, error) {
		return testPublisher{send: func(_ context.Context, rs ...kafka.Record) error {
			if len(rs) != 1 || string(rs[0].Key) != low {
				t.Fatal("skipped late lower UUID")
			}
			return nil
		}}, nil
	}
	if ok, e := r.Step(ctx); e != nil || !ok {
		t.Fatalf("late commit %v %v", ok, e)
	}
}

func TestLeaseOwnership(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	r := &Relay{pool: pool, topic: "test-delivery-requests"}
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
	insert(t, pool)
	var e error
	// A crash leaves a future lease busy; an expired lease is reclaimable.
	if _, e = pool.Exec(ctx, `UPDATE app.delivery_relay SET lease_until=clock_timestamp()+interval '1 hour' WHERE singleton`); e != nil {
		t.Fatal(e)
	}
	if _, e = r.Step(ctx); !errors.Is(e, ErrBusy) {
		t.Fatalf("live crash lease: %v", e)
	}
	if _, e = pool.Exec(ctx, `UPDATE app.delivery_relay SET lease_until=clock_timestamp()-interval '1 second' WHERE singleton`); e != nil {
		t.Fatal(e)
	}
	send := false
	r.open = func(context.Context) (publisher, error) {
		// Another replica cannot claim during initialization or publication.
		if _, e := r.Step(ctx); !errors.Is(e, ErrBusy) {
			t.Fatalf("parallel owner: %v", e)
		}
		// Simulate the initializer resuming after its lease was replaced.
		if _, e := pool.Exec(ctx, `UPDATE app.delivery_relay SET token=token+1,lease_until=NULL WHERE singleton`); e != nil {
			t.Fatal(e)
		}
		return testPublisher{send: func(context.Context, ...kafka.Record) error { send = true; return nil }}, nil
	}
	if _, e = r.Step(ctx); !errors.Is(e, ErrLeaseLost) || send {
		t.Fatalf("late initializer published=%v err=%v", send, e)
	}
	// Cancellation after publication prevents acknowledgement and leaves the
	// same event available for a new caller.
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
	insert(t, pool)
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
