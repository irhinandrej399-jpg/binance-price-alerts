package relay

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/storage"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/trade"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafka"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
)

type testPublisher struct {
	send   func(context.Context, []byte) error
	closed bool
}

func (p *testPublisher) Publish(c context.Context, b []byte) error { return p.send(c, b) }
func (p *testPublisher) Close()                                    { p.closed = true }

func TestSQLBacklogOutageRestartAndOrderedRecovery(t *testing.T) {
	pool := postgrestest.OpenWithDSN(t, "MARKET_DATA_DATABASE_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	repo := storage.New(pool)
	gen, err := repo.BeginGeneration(ctx, "SOLUSDT")
	if err != nil {
		t.Fatal(err)
	}
	priceValue, _ := price.Parse("123.45")
	const count = 1030
	for i := 1; i <= count; i++ {
		now := time.Now().UTC()
		q := priceobserved.Observed{SchemaVersion: 1, Pair: "SOLUSDT", Price: &priceValue, SourceID: fmt.Sprint(i), Revision: priceobserved.Revision{StreamID: trade.StreamID, Generation: 1, Sequence: 1}, TradeTime: now, ReceivedAt: now, StreamState: priceobserved.Unconfirmed}
		meta := event.Envelope{EventID: fmt.Sprintf("relay-%d", i), Type: priceobserved.Type, Version: 1, OccurredAt: now, CorrelationID: "relay", OrderingKey: q.Pair}
		if i == 1 {
			huge := meta
			huge.CorrelationID = strings.Repeat("x", 4096)
			if ok, e := repo.Accept(ctx, gen, q, huge); ok || !errors.Is(e, storage.ErrTooLarge) {
				t.Fatalf("oversize accepted: %v %v", ok, e)
			}
		}
		if ok, e := repo.Accept(ctx, gen, q, meta); !ok || e != nil {
			t.Fatalf("accept %d: %v %v", i, ok, e)
		}
	}
	pending := func() int {
		t.Helper()
		var n int
		if e := pool.QueryRow(ctx, "SELECT count(*) FROM app.price_outbox WHERE pair='SOLUSDT' AND NOT published").Scan(&n); e != nil {
			t.Fatal(e)
		}
		return n
	}
	expire := func() {
		t.Helper()
		if _, e := pool.Exec(ctx, "UPDATE app.price_outbox SET lease_until=clock_timestamp()-interval '1 second' WHERE pair='SOLUSDT' AND NOT published"); e != nil {
			t.Fatal(e)
		}
	}
	firstCtx, stop := context.WithCancel(ctx)
	var original []byte
	blocked := &testPublisher{send: func(c context.Context, b []byte) error {
		original = bytes.Clone(b)
		if n := pending(); n != count {
			t.Fatalf("outage lost backlog: %d", n)
		}
		// Competing in-process relay cannot multiply the resident queue.
		if e := run(ctx, repo, nil, nil, timings{}); !errors.Is(e, ErrRunning) {
			t.Fatalf("duplicate relay: %v", e)
		}
		stop()
		<-c.Done()
		return c.Err()
	}}
	tm := timings{time.Millisecond, time.Second, 5 * time.Second}
	if e := run(firstCtx, repo, func(context.Context) (publisher, error) { return blocked, nil }, nil, tm); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if !blocked.closed || pending() != count {
		t.Fatal("failed publish marked or producer leaked")
	}
	expire()
	fenced := &testPublisher{send: func(_ context.Context, b []byte) error {
		if !bytes.Equal(b, original) {
			t.Fatal("fenced owner changed bytes")
		}
		return kafka.ErrFenced
	}}
	if e := run(ctx, repo, func(context.Context) (publisher, error) { return fenced, nil }, nil, tm); !errors.Is(e, kafka.ErrFenced) {
		t.Fatalf("fenced owner continued: %v", e)
	}
	if !fenced.closed || pending() != count {
		t.Fatal("fenced owner acknowledged work")
	}
	expire()
	recoveredCtx, finish := context.WithCancel(ctx)
	defer finish()
	attempts, committed, opens, closes := 0, 0, 0, 0
	var previous uint64
	var instances []*testPublisher
	factory := func(context.Context) (publisher, error) {
		opens++
		p := &testPublisher{send: func(c context.Context, b []byte) error {
			attempts++
			if attempts <= 3 && !bytes.Equal(b, original) {
				t.Fatal("retry changed persisted bytes")
			}
			if attempts == 1 {
				return kafka.ErrCommitUnknown
			}
			if attempts == 2 {
				expire()
				return nil
			} // Kafka committed, but SQL lease expired.
			_, q, e := priceobserved.Decode(b)
			if e != nil {
				t.Fatal(e)
			}
			if q.Revision.Sequence != previous+1 {
				t.Fatalf("reordered sequence: %d after %d", q.Revision.Sequence, previous)
			}
			previous = q.Revision.Sequence
			return nil
		}}
		instances = append(instances, p)
		return p, nil
	}
	observer := func(s Status) {
		if s.InFlight > 1 || s.Bytes > 4096 {
			t.Fatalf("unbounded resident work: %+v", s)
		}
		if s.Phase == "retry" {
			if pending() != count {
				t.Fatal("unknown commit acknowledged")
			}
			expire()
		}
		if s.Phase == "committed" {
			committed++
			if committed == count {
				finish()
			}
		}
	}
	if e := run(recoveredCtx, storage.New(pool), factory, observer, tm); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	for _, p := range instances {
		if p.closed {
			closes++
		}
	}
	if pending() != 0 || attempts != count+2 || opens != 3 || closes != 3 {
		t.Fatalf("recovery: pending=%d attempts=%d opens=%d closes=%d", pending(), attempts, opens, closes)
	}
}

func TestLegacyOversizeClaimStopsWithoutLoadingOrAcknowledging(t *testing.T) {
	pool := postgrestest.OpenWithDSN(t, "MARKET_DATA_DATABASE_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	repo := storage.New(pool)
	if _, err := repo.BeginGeneration(ctx, "OVERSIZEUSDT"); err != nil {
		t.Fatal(err)
	}
	// Simulate work written before the acceptance bound existed.
	if _, err := pool.Exec(ctx, "INSERT INTO app.price_outbox(event_id,pair,data) VALUES('legacy-oversize','OVERSIZEUSDT',$1)", bytes.Repeat([]byte("x"), 32<<20)); err != nil {
		t.Fatal(err)
	}
	defer func() { // Fixture cleanup only; production never marks failed work.
		if _, err := pool.Exec(ctx, "UPDATE app.price_outbox SET published=true WHERE event_id='legacy-oversize'"); err != nil {
			t.Error(err)
		}
	}()
	p := &testPublisher{send: func(context.Context, []byte) error { t.Fatal("oversize reached publisher"); return nil }}
	if err := run(ctx, repo, func(context.Context) (publisher, error) { return p, nil }, nil, timings{time.Millisecond, time.Second, time.Second}); !errors.Is(err, storage.ErrTooLarge) {
		t.Fatalf("oversize claim: %v", err)
	}
	var published bool
	if err := pool.QueryRow(ctx, "SELECT published FROM app.price_outbox WHERE event_id='legacy-oversize'").Scan(&published); err != nil || published {
		t.Fatalf("oversize acknowledged: %v %v", published, err)
	}
	if !p.closed {
		t.Fatal("producer leaked")
	}
}
