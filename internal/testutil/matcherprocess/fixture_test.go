package matcherprocess

import (
	"context"
	mpb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/latest"
	"testing"
	"time"
)

func TestControlledLatestOverMTLS(t *testing.T) {
	f := New(t, nil, "", "unit")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	q, err := latest.NewClient(f.Conn).Latest(ctx, "BTCUSDT")
	if err != nil || q.Freshness != mpb.Freshness_FRESHNESS_MISSING {
		t.Fatal("initial Latest")
	}
	f.SetPrice("BTCUSDT", "101", 2)
	q, err = latest.NewClient(f.Conn).Latest(ctx, "BTCUSDT")
	if err != nil || q.Quote == nil || q.Quote.Price != "101" || q.Quote.Revision.Sequence != 2 {
		t.Fatal("controlled Latest")
	}
}
