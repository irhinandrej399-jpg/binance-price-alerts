package postgres_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/postgres"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
)

func TestInvalidConfigDoesNotExposeCredentials(t *testing.T) {
	pool, err := postgres.Open(context.Background(), "postgres://user:secret@localhost/db?connect_timeout=invalid")
	if pool != nil || err == nil {
		t.Fatal("expected configuration error and no pool")
	}
	if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "postgres://") {
		t.Fatal("configuration error exposed credentials")
	}
}

func TestTransactions(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	second, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	var user, database string
	if err := first.QueryRow(ctx, "SELECT current_user, current_database()").Scan(&user, &database); err != nil {
		t.Fatal(err)
	}
	if user != "api_runtime" || database != "api" {
		t.Fatal("test requires api_runtime in api")
	}
	if first.Conn().PgConn().PID() == second.Conn().PgConn().PID() {
		t.Fatal("expected independent PostgreSQL sessions")
	}
	// Every run owns its rows. Cleanup also runs after Fatal; the disposable
	// Compose volume is removed by the runner even if this process is killed.
	id := time.Now().UnixNano()
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, err := pool.Exec(cleanup, "DELETE FROM app.connection_probe WHERE id=$1", id); err != nil {
			t.Errorf("cleanup probe: %v", err)
		}
	})
	assertCount := func(want int) {
		t.Helper()
		var count int
		if err := second.QueryRow(ctx, "SELECT count(*) FROM app.connection_probe WHERE id=$1", id).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != want {
			t.Fatalf("visible rows = %d, want %d", count, want)
		}
	}
	tx, err := first.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, "INSERT INTO app.connection_probe(id) VALUES ($1)", id); err != nil {
		t.Fatal(err)
	}
	assertCount(0)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	assertCount(1)
	tx, err = first.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, "DELETE FROM app.connection_probe WHERE id=$1", id); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	assertCount(1)
}
