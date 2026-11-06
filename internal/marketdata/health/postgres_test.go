package health_test

import (
	"context"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/health"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
)

func TestMarketDataPostgresAvailability(t *testing.T) {
	pool := postgrestest.OpenWithDSN(t, "MARKET_DATA_DATABASE_URL")
	h := health.New(pool.Ping, func(context.Context) (health.FeedState, error) {
		return health.Live, nil
	}, time.Second)
	check(t, h, 503, health.Starting)
	h.SetInitialized(true)
	check(t, h, 200, health.Live)
	pool.Close()
	if w := request(h, "/readyz"); w.Code != 503 {
		t.Fatalf("closed market-data pool: %d", w.Code)
	}
	if w := request(h, "/healthz"); w.Code != 200 {
		t.Fatalf("database failure affected liveness: %d", w.Code)
	}
}
