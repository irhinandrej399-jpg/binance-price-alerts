package health_test

import (
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/health"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
)

func TestPostgresAvailability(t *testing.T) {
	pool := postgrestest.Open(t)
	h := health.New(pool.Ping, time.Second)
	if w := request(h, "/readyz"); w.Code != 503 {
		t.Fatalf("startup: %d", w.Code)
	}
	h.SetInitialized(true)
	if w := request(h, "/readyz"); w.Code != 200 {
		t.Fatalf("available PostgreSQL: %d", w.Code)
	}
	pool.Close()
	if w := request(h, "/readyz"); w.Code != 503 {
		t.Fatalf("closed PostgreSQL pool: %d", w.Code)
	}
	if w := request(h, "/healthz"); w.Code != 200 {
		t.Fatalf("liveness: %d", w.Code)
	}
}
