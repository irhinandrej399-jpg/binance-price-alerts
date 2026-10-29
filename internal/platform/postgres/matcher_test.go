package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestMatcherTransactions(t *testing.T) {
	writer := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	reader := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var role, database string
	if err := writer.QueryRow(ctx, "SELECT current_user, current_database()").Scan(&role, &database); err != nil {
		t.Fatal(err)
	}
	if role != "matcher_runtime" || database != "matcher" {
		t.Fatal("test requires matcher_runtime in matcher")
	}
	var writerPID, readerPID uint32
	if err := writer.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&writerPID); err != nil {
		t.Fatal(err)
	}
	if err := reader.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&readerPID); err != nil {
		t.Fatal(err)
	}
	if writerPID == readerPID {
		t.Fatal("expected independent pools and sessions")
	}
	id := time.Now().UnixNano()
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, err := writer.Exec(cleanup, "DELETE FROM app.matcher_connection_probe WHERE id=$1", id); err != nil {
			t.Errorf("cleanup probe: %v", err)
		}
	})
	assertCount := func(want int) {
		t.Helper()
		var count int
		if err := reader.QueryRow(ctx, "SELECT count(*) FROM app.matcher_connection_probe WHERE id=$1", id).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != want {
			t.Fatalf("visible rows = %d, want %d", count, want)
		}
	}
	// Both attempts insert the same row: rollback leaves nothing, commit makes
	// the row visible through an independently opened runtime pool.
	for _, commit := range []bool{false, true} {
		tx, err := writer.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(context.Background())
		if _, err := tx.Exec(ctx, "INSERT INTO app.matcher_connection_probe(id) VALUES ($1)", id); err != nil {
			t.Fatal(err)
		}
		assertCount(0)
		if commit {
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			assertCount(1)
		} else {
			if err := tx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			assertCount(0)
		}
	}
	// Assert the database's grants as this runtime role, without obtaining
	// another owner's credentials or treating connection errors as permission errors.
	for _, other := range []string{"api", "market_data", "notifications"} {
		var allowed bool
		if err := reader.QueryRow(ctx, "SELECT has_database_privilege(current_user, $1, 'CONNECT')", other).Scan(&allowed); err != nil {
			t.Fatal(err)
		}
		if allowed {
			t.Fatalf("matcher can connect to %s", other)
		}
	}
	_, err := reader.Exec(ctx, "SELECT * FROM migration.history")
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Fatalf("expected permission denied for migration history, got %v", err)
	}
	var apiFixture, apiTable *string
	if err := reader.QueryRow(ctx, "SELECT to_regclass('app.connection_probe')::text, to_regclass('app.users')::text").Scan(&apiFixture, &apiTable); err != nil {
		t.Fatal(err)
	}
	if apiFixture != nil || apiTable != nil {
		t.Fatal("API fixtures or application migrations leaked into matcher")
	}
}
