package deliveryrpc_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/deliveryrpc"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/latest"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/storage"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/trade"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/latency"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
)

func TestLatencyBoundariesOverTLS(t *testing.T) {
	ctx := context.Background()
	pool := postgrestest.Open(t)
	mp := postgrestest.OpenWithDSN(t, "MARKET_DATA_DATABASE_URL")
	a, e := account.New(pool).Create(ctx, fmt.Sprintf("latency-%d@example.com", time.Now().UnixNano()), "hash")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=123,telegram_version=1 WHERE id=$1`, a.ID); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if _, e := pool.Exec(ctx, `DELETE FROM app.subscriptions WHERE owner_id=$1`, a.ID); e != nil {
			t.Error(e)
		}
		if _, e := pool.Exec(ctx, `DELETE FROM app.users WHERE id=$1`, a.ID); e != nil {
			t.Error(e)
		}
	})
	v := record()
	v.OwnerID = a.ID
	v.ID = "00000000-0000-4000-8000-000001989001"
	v.Pair = "OBSERVERUSDT"
	v.CreationPrice = v.Threshold
	v.CreationTradeTime = now
	v.Activation = sub.LatestPrice
	tx, e := pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	quota, e := sub.LockQuota(ctx, tx, a.ID)
	if e != nil {
		t.Fatal(e)
	}
	if e = quota.Create(ctx, v); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	var nanos atomic.Int64
	nanos.Store(100)
	var mu sync.Mutex
	var raw []latency.Observation
	clock := func() (latency.Stamp, error) { return latency.Stamp{Scale: "controlled", Nanos: nanos.Load()}, nil }
	sink := func(o latency.Observation) error { mu.Lock(); defer mu.Unlock(); raw = append(raw, o); return nil }
	marketRecorder, e := latency.New(clock, sink)
	if e != nil {
		t.Fatal(e)
	}
	apiRecorder, e := latency.New(clock, sink)
	if e != nil {
		t.Fatal(e)
	}
	market := storage.NewObserved(mp, marketRecorder)
	gen, e := market.BeginGeneration(ctx, v.Pair)
	if e != nil {
		t.Fatal(e)
	}
	accept := func(source string) {
		q := priceobserved.Observed{SchemaVersion: 1, Pair: v.Pair, Price: &v.Threshold, SourceID: source, Revision: priceobserved.Revision{StreamID: trade.StreamID, Generation: 1, Sequence: 1}, TradeTime: now, ReceivedAt: now, StreamState: priceobserved.Unconfirmed}
		ok, e := market.Accept(ctx, gen, q, event.Envelope{EventID: "latency-rpc-" + source, Type: priceobserved.Type, Version: 1, OccurredAt: now, CorrelationID: "latency", OrderingKey: v.Pair})
		if !ok || e != nil {
			t.Fatal(ok, e)
		}
	}
	accept("1")
	// Reference is owned by the control caller, independent of API's later Latest.
	expected := map[string]string{v.ID: "1"}
	nanos.Store(100000100)
	accept("2")
	marketConn := marketConnection(t, market)
	api := connect(t, deliveryrpc.NewServer(deliveryrpc.NewPostgresObserved(pool, apiRecorder), latest.NewClient(marketConn), func() time.Time { return now }), matcherURI)
	client := deliveryrpc.NewClient(api)
	nanos.Store(200000100)
	result, e := client.RequestDelivery(ctx, a.ID, v.ID, 1)
	if e != nil {
		t.Fatal(e)
	}
	nanos.Store(900000100)
	if _, e = client.RequestDelivery(ctx, a.ID, v.ID, 1); e != nil {
		t.Fatal(e)
	}
	mu.Lock()
	defer mu.Unlock()
	var t0, t1 latency.Stamp
	var commits int
	for _, o := range raw {
		if o.Kind == latency.PriceReceived && o.SourceID == expected[v.ID] {
			t0 = o.At
		}
		if o.Kind == latency.WorkflowCommitted && o.SubscriptionID == v.ID {
			commits++
			t1 = o.At
			if o.SourceID != "2" || o.TriggerID != result.Workflow.TriggerId {
				t.Fatal(o)
			}
		}
	}
	if commits != 1 {
		t.Fatal("duplicate/missing commit", raw)
	}
	if d, e := latency.Between(t0, t1); e != nil || d != 200*time.Millisecond {
		t.Fatal(d, e, raw)
	}
	if n, e := marketRecorder.Failures(); n != 0 || e != nil {
		t.Fatal(n, e)
	}
	if n, e := apiRecorder.Failures(); n != 0 || e != nil {
		t.Fatal(n, e)
	}
}
