package binancefake_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/catalog"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/stream"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/trade"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/binancefake"
)

func TestCatalogPricesPingDisconnectAndReconnect(t *testing.T) {
	f := binancefake.New()
	defer f.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := catalog.New(f.Client(), f.URL(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := c.Fetch(ctx)
	if err != nil || len(snap.Pairs) != 2 || snap.Pairs[0].Symbol != "BTCUSDT" || snap.Pairs[1].Symbol != "SOLUSDT" {
		t.Fatalf("catalog: %+v, %v", snap, err)
	}
	for round := 0; round < 2; round++ {
		received := make(chan priceobserved.Observed, 2)
		result := make(chan error, 1)
		go func() {
			result <- stream.Read(ctx, f.StreamURL("BTCUSDT"), func(_ context.Context, b []byte) error {
				o, e := trade.Decode(b, trade.Metadata{Unit: priceobserved.Milliseconds, ReceivedAt: time.Now(), Generation: 1, Sequence: 1, StreamState: priceobserved.Live})
				if e == nil {
					received <- o
				}
				return e
			})
		}()
		conn, err := f.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if conn.Symbol != "BTCUSDT" {
			t.Fatal(conn.Symbol)
		}
		for i, p := range []string{"123.000000000000000001", "124.000000000000000002"} {
			id := uint64(18446744073709551614) + uint64(i)
			if err := conn.Trade(ctx, binancefake.Trade{ID: id, Price: p, TradeTime: 1672515782001, EventTime: 1672515782136}); err != nil {
				t.Fatal(err)
			}
			select {
			case o := <-received:
				wantID := []string{"18446744073709551614", "18446744073709551615"}[i]
				if o.SourceID != wantID || o.Price.String() != p || o.TradeTime.UnixMilli() != 1672515782001 || o.SourceEventTime.UnixMilli() != 1672515782136 {
					t.Fatalf("observation: %+v", o)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
		if err := conn.Ping(ctx, "same-payload"); err != nil {
			t.Fatal(err)
		}
		if round == 0 {
			err = conn.Disconnect()
		} else {
			err = conn.Close(ctx, 1001, "rotate")
		}
		if err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-result:
			var ce *websocket.CloseError
			if !errors.As(err, &ce) {
				t.Fatalf("close: %v", err)
			}
			want := 1006
			if round == 1 {
				want = 1001
			}
			if ce.Code != want {
				t.Fatalf("code: %v", err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

func TestCloseJoinsUnclaimedAndClaimedSockets(t *testing.T) {
	f := binancefake.New()
	defer f.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	a, _, err := websocket.DefaultDialer.DialContext(ctx, f.StreamURL("BTCUSDT"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	c, err := f.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := websocket.DefaultDialer.DialContext(ctx, f.StreamURL("SOLUSDT"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	// No reader on a: a ping cannot be acknowledged and must honor its deadline.
	short, stop := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stop()
	if err := c.Ping(short, "unanswered"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ping: %v", err)
	}
	done := make(chan struct{})
	go func() { f.Close(); f.Close(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("Close did not join sockets")
	}
	if _, err := f.Next(ctx); !errors.Is(err, binancefake.ErrClosed) {
		t.Fatalf("Next: %v", err)
	}
	for _, ws := range []*websocket.Conn{a, b} {
		_ = ws.SetReadDeadline(time.Now().Add(time.Second))
		if _, _, err := ws.ReadMessage(); err == nil {
			t.Fatal("socket still open")
		}
	}
}

func TestRoutingAndCancelledNext(t *testing.T) {
	f := binancefake.New()
	defer f.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, path := range []string{"/unknown", "/ws/ethusdt@trade"} {
		r, err := f.Client().Get(f.URL() + path)
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		if r.StatusCode != 404 {
			t.Fatalf("%s: %d", path, r.StatusCode)
		}
	}
}
