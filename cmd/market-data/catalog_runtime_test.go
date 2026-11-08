package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/catalog"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/stream"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/binancefake"
)

func TestSupportedCatalogMatchesDurableSymbolContract(t *testing.T) {
	now := time.Now().UTC()
	s, err := supportedSnapshot(catalog.Snapshot{UpdatedAt: now, Pairs: []catalog.Pair{
		{Symbol: "BTCUSDT", BaseAsset: "BTC", QuoteAsset: "USDT"},
		{Symbol: "币USDT", BaseAsset: "币", QuoteAsset: "USDT"},
		{Symbol: "BTCUSDT", BaseAsset: "BTC", QuoteAsset: "USDT"},
		{Symbol: "SOLUSDT", BaseAsset: "SOL", QuoteAsset: "USDT"},
	}})
	want := []catalog.Pair{{Symbol: "BTCUSDT", BaseAsset: "BTC", QuoteAsset: "USDT"}, {Symbol: "SOLUSDT", BaseAsset: "SOL", QuoteAsset: "USDT"}}
	if err != nil || !reflect.DeepEqual(s.Pairs, want) || s.UpdatedAt != now {
		t.Fatalf("unsupported storage symbols leaked: %+v %v", s, err)
	}
}
func TestServedCatalogWithdrawalAndOwnership(t *testing.T) {
	a := catalog.Pair{Symbol: "BTCUSDT"}
	b := catalog.Pair{Symbol: "SOLUSDT"}
	c := catalog.Pair{Symbol: "ADAUSDT"}
	now := time.Now().UTC()
	initial := catalog.Snapshot{Pairs: []catalog.Pair{a, b}, UpdatedAt: now}
	served := new(servedCatalog)
	served.store(initial)
	initial.Pairs[0] = c
	next := catalog.Snapshot{Pairs: []catalog.Pair{b, c}, UpdatedAt: now.Add(time.Minute)}
	served.withdraw(next)
	got, _ := served.Fetch(context.Background())
	if !reflect.DeepEqual(got.Pairs, []catalog.Pair{b}) || got.UpdatedAt != now {
		t.Fatal("removal must precede addition", got)
	}
	served.store(next)
	got, _ = served.Fetch(context.Background())
	got.Pairs[0] = a
	got, _ = served.Fetch(context.Background())
	if !reflect.DeepEqual(got.Pairs, next.Pairs) {
		t.Fatal("RPC caller mutated plan")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := served.Fetch(ctx); err != context.Canceled {
		t.Fatal(err)
	}
}

func TestCatalogRefreshFailureRetainsAppliedPlanAndEmptyRetires(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	f := binancefake.New()
	defer f.Close()
	var fail atomic.Bool
	var failures atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() {
			failures.Add(1)
			w.WriteHeader(503)
			return
		}
		req, _ := http.NewRequestWithContext(r.Context(), "GET", f.URL()+r.URL.Path, nil)
		resp, err := f.Client().Do(req)
		if err != nil {
			w.WriteHeader(502)
			return
		}
		defer resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	defer upstream.Close()
	source, err := catalog.New(upstream.Client(), upstream.URL, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := source.Fetch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	served := new(servedCatalog)
	r, err := stream.NewPreparedReconciler(ctx, stream.ReconcileConfig{Endpoint: f.WSURL(), Limit: 1024, Transport: stream.DefaultConfig(), Retry: stream.DefaultRetryConfig(), AckTimeout: time.Second}, func(context.Context, int, []string) (stream.AttemptCallbacks, error) {
		return stream.AttemptCallbacks{Handle: func(context.Context, []byte) error { return nil }, Disconnected: func(context.Context, error, time.Duration) error { return nil }}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- runCatalog(ctx, source, served, r, initial, 10*time.Millisecond) }()
	defer func() { cancel(); <-done }()
	c, err := f.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wait := func(check func() bool) {
		t.Helper()
		for !check() {
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(time.Millisecond):
			}
		}
	}
	wait(func() bool { s, _ := served.Fetch(ctx); return len(s.Pairs) == 2 })
	fail.Store(true)
	wait(func() bool { return failures.Load() >= 2 })
	s, _ := served.Fetch(ctx)
	if !reflect.DeepEqual(s, initial) || len(c.Streams()) != 2 {
		t.Fatal("failed refresh changed applied plan")
	}
	if err = f.SetCatalog(nil); err != nil {
		t.Fatal(err)
	}
	fail.Store(false)
	wait(func() bool { s, _ := served.Fetch(ctx); return len(s.Pairs) == 0 && len(c.Streams()) == 0 })
}
