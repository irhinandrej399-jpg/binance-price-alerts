package catalog_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/catalog"
)

func TestFetchFiltersAndTimestamps(t *testing.T) {
	body, err := os.ReadFile("testdata/exchange_info.json")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/api/v3/exchangeInfo" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
		}
		_, _ = w.Write(body)
	}))
	defer server.Close()
	now := time.Date(2026, 10, 27, 15, 0, 0, 123, time.FixedZone("MSK", 3*3600))
	client, err := catalog.New(server.Client(), server.URL, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	got, err := client.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []catalog.Pair{{Symbol: "BTCUSDT", BaseAsset: "BTC", QuoteAsset: "USDT"}, {Symbol: "SOLUSDT", BaseAsset: "SOL", QuoteAsset: "USDT"}}
	if !reflect.DeepEqual(got.Pairs, want) {
		t.Fatalf("pairs = %+v, want %+v", got.Pairs, want)
	}
	if got.UpdatedAt != now.UTC() {
		t.Fatalf("updated_at = %v, want %v", got.UpdatedAt, now.UTC())
	}
}

func TestFetchFailuresDoNotReturnSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"rate limit", 429, `{"code":-1003,"msg":"Too many requests"}`},
		{"upstream failure", 503, `unavailable`},
		{"API error", 200, `{"code":-1121,"msg":"Invalid symbol."}`},
		{"malformed JSON", 200, `{"symbols":[`},
		{"missing symbols", 200, `{}`},
		{"null symbols", 200, `{"symbols":null}`},
		{"trailing JSON", 200, `{"symbols":[]} {}`},
		{"invalid pair", 200, `{"symbols":[{"quoteAsset":"USDT","status":"TRADING","isSpotTradingAllowed":true}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			client, err := catalog.New(server.Client(), server.URL, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			got, err := client.Fetch(context.Background())
			if err == nil {
				t.Fatal("expected failure")
			}
			if !got.UpdatedAt.IsZero() || len(got.Pairs) != 0 {
				t.Fatalf("snapshot on failure: %+v", got)
			}
		})
	}
}

func TestFetchEmptyCatalogAndCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"symbols":[]}`)) }))
	defer server.Close()
	client, err := catalog.New(server.Client(), server.URL, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	got, err := client.Fetch(context.Background())
	if err != nil || len(got.Pairs) != 0 || got.UpdatedAt.IsZero() {
		t.Fatalf("empty catalog: %+v, %v", got, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got, err = client.Fetch(ctx)
	if !errors.Is(err, context.Canceled) || !got.UpdatedAt.IsZero() {
		t.Fatalf("cancellation: %+v, %v", got, err)
	}
}

func TestFetchTransportFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	client, err := catalog.New(server.Client(), server.URL, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	server.Close()
	got, err := client.Fetch(context.Background())
	if err == nil || !got.UpdatedAt.IsZero() || len(got.Pairs) != 0 {
		t.Fatalf("transport failure: %+v, %v", got, err)
	}
}

func TestFetchRejectsOversizedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"symbols":[],"padding":"`))
		_, _ = w.Write([]byte(strings.Repeat("x", 16<<20)))
		_, _ = w.Write([]byte(`"}`))
	}))
	defer server.Close()
	client, err := catalog.New(server.Client(), server.URL, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	got, err := client.Fetch(context.Background())
	if err == nil || !got.UpdatedAt.IsZero() {
		t.Fatalf("oversized response: %+v, %v", got, err)
	}
}
