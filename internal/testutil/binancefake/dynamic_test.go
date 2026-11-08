package binancefake_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/catalog"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/stream"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/trade"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/binancefake"
)

func TestDynamicCatalogReconcileTradesAndReconnect(t *testing.T) {
	f := binancefake.New()
	defer f.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := catalog.New(f.Client(), f.URL(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan priceobserved.Observed, 8)
	r, err := stream.NewReconciler(ctx, stream.ReconcileConfig{Endpoint: f.WSURL(), Limit: 10, Transport: stream.DefaultConfig(), Retry: stream.RetryConfig{InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond}, AckTimeout: time.Second}, func(_ context.Context, b []byte) error {
		o, err := trade.Decode(b, trade.Metadata{Unit: priceobserved.Milliseconds, ReceivedAt: time.Now(), Generation: 1, Sequence: 1, StreamState: priceobserved.Unconfirmed})
		if err == nil {
			received <- o
		}
		return err
	}, func(int, error, time.Duration) {})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	apply := func() {
		t.Helper()
		snap, err := client.Fetch(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Reconcile(ctx, snap); err != nil {
			t.Fatal(err)
		}
	}
	check := func(c *binancefake.Connection, want ...string) {
		t.Helper()
		if got := c.Streams(); !reflect.DeepEqual(got, want) {
			t.Fatalf("streams %v want %v", got, want)
		}
	}
	send := func(c *binancefake.Connection, pair, p string) {
		t.Helper()
		if err := c.TradePair(ctx, pair, binancefake.Trade{ID: 42, Price: p, TradeTime: 1672515782001, EventTime: 1672515782136}); err != nil {
			t.Fatal(err)
		}
		select {
		case o := <-received:
			if o.Pair != pair || o.Price.String() != p {
				t.Fatalf("trade %+v", o)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	apply()
	conn, err := f.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Observe subscriptions concurrently with command processing.
	observedConn := conn
	watchCtx, stopWatch := context.WithCancel(ctx)
	watched := make(chan struct{})
	go func() {
		defer close(watched)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-watchCtx.Done():
				return
			case <-ticker.C:
				_ = observedConn.Streams()
			}
		}
	}()
	defer func() { stopWatch(); <-watched }()
	check(conn, "btcusdt@trade", "solusdt@trade")
	send(conn, "BTCUSDT", "101.000000000000000001")
	send(conn, "SOLUSDT", "23.000000000000000002")
	cancelled, stop := context.WithCancel(ctx)
	stop()
	if err := conn.TradePair(cancelled, "BTCUSDT", binancefake.Trade{Price: "999"}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := conn.Ping(ctx, "dynamic"); err != nil {
		t.Fatal(err)
	}
	pairs := []catalog.Pair{{Symbol: "BTCUSDT", BaseAsset: "BTC", QuoteAsset: "USDT"}, {Symbol: "SOLUSDT", BaseAsset: "SOL", QuoteAsset: "USDT"}, {Symbol: "ETHUSDT", BaseAsset: "ETH", QuoteAsset: "USDT"}}
	if err := f.SetCatalog(pairs); err != nil {
		t.Fatal(err)
	}
	// The fake owns the replacement bytes, not the caller's slice.
	pairs[2].Symbol = "CHANGED"
	apply()
	check(conn, "btcusdt@trade", "ethusdt@trade", "solusdt@trade")
	send(conn, "ETHUSDT", "3000")
	if err := f.SetCatalog([]catalog.Pair{{Symbol: "ETHUSDT", BaseAsset: "ETH", QuoteAsset: "USDT"}, {Symbol: "SOLUSDT", BaseAsset: "SOL", QuoteAsset: "USDT"}}); err != nil {
		t.Fatal(err)
	}
	apply()
	check(conn, "ethusdt@trade", "solusdt@trade")
	if err := conn.TradePair(ctx, "BTCUSDT", binancefake.Trade{Price: "999"}); !errors.Is(err, binancefake.ErrNotSubscribed) {
		t.Fatal(err)
	}
	// A valid message acts as a barrier: the rejected trade must not precede it.
	send(conn, "SOLUSDT", "24")
	if err := conn.Disconnect(); err != nil {
		t.Fatal(err)
	}
	conn, err = f.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	apply()
	check(conn, "ethusdt@trade", "solusdt@trade")
	send(conn, "ETHUSDT", "3001")
	if err := f.SetCatalog(nil); err != nil {
		t.Fatal(err)
	}
	apply()
	if got := conn.Streams(); len(got) != 0 {
		t.Fatalf("empty catalog streams: %v", got)
	}
	r.Close()
	f.Close()
	if _, err := f.Next(ctx); !errors.Is(err, binancefake.ErrClosed) {
		t.Fatal(err)
	}
}

func TestDynamicExactACKAndUnclaimedCleanup(t *testing.T) {
	f := binancefake.New()
	defer f.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ws, _, err := websocket.DefaultDialer.DialContext(ctx, f.WSURL(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	_ = ws.SetReadDeadline(time.Now().Add(time.Second))
	// No Next: handler must read commands and Close must still join it.
	for _, method := range []string{"SUBSCRIBE", "UNSUBSCRIBE"} {
		if err := ws.WriteMessage(websocket.TextMessage, []byte(`{"method":"`+method+`","params":["ethusdt@trade"],"id":18446744073709551615}`)); err != nil {
			t.Fatal(err)
		}
		var ack map[string]json.RawMessage
		if err := ws.ReadJSON(&ack); err != nil {
			t.Fatal(err)
		}
		if string(ack["id"]) != "18446744073709551615" || string(ack["result"]) != "null" {
			t.Fatalf("ack %s", ack)
		}
	}
	done := make(chan struct{})
	go func() { f.Close(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("Close did not join bare socket")
	}
}

func TestConcurrentCatalogReplacement(t *testing.T) {
	f := binancefake.New()
	defer f.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := catalog.New(f.Client(), f.URL(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 30; i++ {
			if err := f.SetCatalog([]catalog.Pair{{Symbol: "ETHUSDT", BaseAsset: "ETH", QuoteAsset: "USDT"}}); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	for i := 0; i < 30; i++ {
		snap, err := client.Fetch(ctx)
		if err != nil || (len(snap.Pairs) != 1 && len(snap.Pairs) != 2) {
			t.Fatalf("snapshot %+v %v", snap, err)
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
