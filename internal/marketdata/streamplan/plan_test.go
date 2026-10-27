package streamplan_test

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/catalog"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/streamplan"
)

func snapshot(symbols ...string) catalog.Snapshot {
	s := catalog.Snapshot{UpdatedAt: time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)}
	for _, symbol := range symbols {
		s.Pairs = append(s.Pairs, catalog.Pair{Symbol: symbol, QuoteAsset: "USDT"})
	}
	return s
}

func TestBuild(t *testing.T) {
	for _, tt := range []struct {
		name  string
		input catalog.Snapshot
		limit int
		want  [][]string
	}{
		{"empty", snapshot(), 2, nil},
		{"duplicate", snapshot("ETHUSDT", "BTCUSDT", "ETHUSDT", "btcusdt"), 2, [][]string{{"btcusdt@trade", "ethusdt@trade"}}},
		{"exact limit", snapshot("ETHUSDT", "BTCUSDT"), 2, [][]string{{"btcusdt@trade", "ethusdt@trade"}}},
		{"limit plus one", snapshot("SOLUSDT", "ETHUSDT", "BTCUSDT"), 2, [][]string{{"btcusdt@trade", "ethusdt@trade"}, {"solusdt@trade"}}},
		{"one per group", snapshot("ETHUSDT", "BTCUSDT"), 1, [][]string{{"btcusdt@trade"}, {"ethusdt@trade"}}},
		{"unicode catalog symbol", snapshot("币USDT"), 2, [][]string{{"币usdt@trade"}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before := tt.input
			before.Pairs = slices.Clone(tt.input.Pairs)
			got, err := streamplan.Build(tt.input, tt.limit)
			if err != nil || !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Build = %v, %v; want %v", got, err, tt.want)
			}
			if !reflect.DeepEqual(tt.input, before) {
				t.Fatal("input modified")
			}
			slices.Reverse(tt.input.Pairs)
			again, err := streamplan.Build(tt.input, tt.limit)
			if err != nil || !reflect.DeepEqual(again, got) {
				t.Fatalf("reordered catalog changed plan: %v, %v", again, err)
			}
		})
	}
}

func TestBuildBinanceLimit(t *testing.T) {
	for _, count := range []int{1024, 1025} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			s := snapshot()
			for i := count - 1; i >= 0; i-- {
				s.Pairs = append(s.Pairs, catalog.Pair{Symbol: fmt.Sprintf("PAIR%04dUSDT", i)})
			}
			got, err := streamplan.Build(s, streamplan.MaxStreamsPerConnection)
			if err != nil {
				t.Fatal(err)
			}
			want := [][]string{make([]string, 1024)}
			for i := range 1024 {
				want[0][i] = fmt.Sprintf("pair%04dusdt@trade", i)
			}
			if count == 1025 {
				want = append(want, []string{"pair1024usdt@trade"})
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatal("unexpected grouping at Binance boundary")
			}
		})
	}
}

func TestBuildInvalidInput(t *testing.T) {
	for _, limit := range []int{-1, 0, 1025} {
		for _, s := range []catalog.Snapshot{snapshot(), snapshot("BTCUSDT")} {
			got, err := streamplan.Build(s, limit)
			if got != nil || !errors.Is(err, streamplan.ErrInvalidLimit) {
				t.Fatalf("limit %d: %v, %v", limit, got, err)
			}
		}
	}
	got, err := streamplan.Build(snapshot("BTCUSDT", ""), 1)
	if got != nil || !errors.Is(err, streamplan.ErrInvalidSymbol) {
		t.Fatalf("empty symbol: %v, %v", got, err)
	}
}
