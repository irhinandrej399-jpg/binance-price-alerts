// Package postgrestest connects opt-in storage tests to a disposable PostgreSQL.
package postgrestest

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Open skips only when POSTGRES_TEST is unset. Once opted in, missing or invalid
// configuration and connection failures fail the test. Never print the DSN.
func Open(t testing.TB) *pgxpool.Pool {
	t.Helper()
	if os.Getenv("POSTGRES_TEST") == "" {
		t.Skip("PostgreSQL integration test: run python3 scripts/check_postgres.py")
	}
	if os.Getenv("POSTGRES_TEST") != "1" {
		t.Fatal("POSTGRES_TEST must be 1")
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Fatal("POSTGRES_TEST requires DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := postgres.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open test PostgreSQL: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}
