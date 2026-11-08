package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/catalog"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/binancefake"
)

// Existing process scenarios now use E2E-008's real subscription-aware fake.
type runtimeFeed struct {
	URL    string
	fake   *binancefake.Fake
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

func newRuntimeFeed(t testing.TB, raw []byte) *runtimeFeed {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	var msg struct {
		Pair                 string
		ID                   uint64
		Price                string
		TradeTime, EventTime int64
	}
	for key, dest := range map[string]any{"s": &msg.Pair, "t": &msg.ID, "p": &msg.Price, "T": &msg.TradeTime, "E": &msg.EventTime} {
		if err := json.Unmarshal(fields[key], dest); err != nil {
			t.Fatal(err)
		}
	}
	f := binancefake.New()
	if err := f.SetCatalog([]catalog.Pair{{Symbol: msg.Pair, BaseAsset: msg.Pair[:len(msg.Pair)-4], QuoteAsset: "USDT"}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	feed := &runtimeFeed{URL: f.WSURL(), fake: f, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(feed.done)
		for {
			c, err := f.Next(ctx)
			if err != nil {
				return
			}
			for len(c.Streams()) == 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Millisecond):
				}
			}
			if err = c.TradePair(ctx, msg.Pair, binancefake.Trade{ID: msg.ID, Price: msg.Price, TradeTime: msg.TradeTime, EventTime: msg.EventTime}); err != nil && ctx.Err() == nil {
				t.Error(err)
				return
			}
		}
	}()
	t.Cleanup(feed.Close)
	return feed
}
func (f *runtimeFeed) Close() { f.once.Do(func() { f.cancel(); f.fake.Close(); <-f.done }) }
func runtimeCatalog(t testing.TB, pair, base string) string {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"symbols":[{"symbol":%q,"baseAsset":%q,"quoteAsset":"USDT","status":"TRADING","isSpotTradingAllowed":true}]}`, pair, base)
	}))
	t.Cleanup(s.Close)
	return s.URL
}
func acknowledgeSubscribe(c *websocket.Conn) error {
	var command struct {
		ID json.RawMessage `json:"id"`
	}
	if err := c.ReadJSON(&command); err != nil {
		return err
	}
	return c.WriteMessage(websocket.TextMessage, []byte(`{"result":null,"id":`+string(command.ID)+`}`))
}
