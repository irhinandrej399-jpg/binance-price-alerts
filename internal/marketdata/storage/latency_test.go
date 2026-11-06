package storage_test

import (
	"context"
	"testing"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/storage"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/latency"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
)

func TestAcceptanceObservations(t *testing.T) {
	pool := postgrestest.OpenWithDSN(t, "MARKET_DATA_DATABASE_URL")
	ctx := context.Background()
	t.Cleanup(func() {
		if _, e := pool.Exec(ctx, `UPDATE app.price_outbox SET published=true WHERE pair='LATENCYUSDT'`); e != nil {
			t.Error(e)
		}
	})
	var raw []latency.Observation
	rec, e := latency.New(func() (latency.Stamp, error) { return latency.Stamp{Scale: "test", Nanos: 100 + int64(len(raw))}, nil }, func(o latency.Observation) error {
		var n int
		if e := pool.QueryRow(ctx, `SELECT count(*) FROM app.price_outbox WHERE event_id=$1`, o.EventID).Scan(&n); e != nil {
			t.Fatal(e)
		}
		if o.Kind == latency.PriceReceived && o.EventID == "latency-first" && n != 0 {
			t.Fatal("t0 after commit")
		}
		if o.Kind == latency.PriceAccepted && n != 1 {
			t.Fatal("accept before commit")
		}
		raw = append(raw, o)
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	r := storage.NewObserved(pool, rec)
	gen, e := r.BeginGeneration(ctx, "LATENCYUSDT")
	if e != nil {
		t.Fatal(e)
	}
	accept := func(source, event string) (bool, error) {
		q := quote(source)
		q.Pair = "LATENCYUSDT"
		m := meta(event)
		m.OrderingKey = q.Pair
		return r.Accept(ctx, gen, q, m)
	}
	if ok, e := accept("1", "latency-first"); !ok || e != nil {
		t.Fatal(ok, e)
	}
	if ok, e := accept("2", "latency-second"); !ok || e != nil {
		t.Fatal(ok, e)
	}
	if ok, e := accept("2", "latency-repeat"); ok || e != nil {
		t.Fatal(ok, e)
	}
	if ok, e := accept("3", "latency-second"); ok || e == nil {
		t.Fatal("expected unique violation", ok, e)
	}
	if len(raw) != 6 {
		t.Fatal(raw)
	}
	if raw[0].Kind != latency.PriceReceived || raw[1].Kind != latency.PriceAccepted || raw[0].AttemptID != raw[1].AttemptID || raw[1].Revision.Sequence != 1 || raw[3].Revision.Sequence != 2 {
		t.Fatal(raw)
	}
	// Caller-owned oracle binds a subscription to its FIRST sustained source price.
	// Latest now has source 2; its receipt must not replace the oracle's source 1.
	first := raw[0]
	if first.SourceID != "1" || first.At.Nanos != 100 || first.AttemptID == raw[2].AttemptID {
		t.Fatal(raw)
	}
	if d, e := latency.Between(first.At, latency.Stamp{Scale: "test", Nanos: 200000100}); e != nil || d != 200000000 {
		t.Fatal(d, e)
	}
}
