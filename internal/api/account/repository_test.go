package account_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestConcurrentEmailAndRead(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	email := fmt.Sprintf("iam-%d@example.com", time.Now().UnixNano())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := pool.Exec(ctx, "DELETE FROM app.users WHERE email=$1", email); err != nil {
			t.Error(err)
		}
	})
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
	if first.Conn().PgConn().PID() == second.Conn().PgConn().PID() {
		t.Fatal("need independent sessions")
	}
	tx, err := first.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = tx.Rollback(cleanup)
	}()
	// A fake precomputed hash: IAM-002 supplies real password hashing separately.
	const hash = "test-only-encoded-hash-with-salt"
	created, err := account.New(tx).Create(ctx, "  "+strings.ToUpper(email)+"  ", hash)
	if err != nil {
		t.Fatal(err)
	}
	if created.ID == "" || created.Email != email || created.PasswordHash != hash {
		t.Fatal("wrong saved account")
	}
	if _, err := account.New(second).ByEmail(ctx, email); !errors.Is(err, account.ErrNotFound) {
		t.Fatalf("uncommitted account visible: %v", err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := account.New(second).Create(ctx, email, "different-test-hash")
		result <- err
	}()
	// Confirm real unique-index contention before releasing the winning insert.
	// A mere simultaneous goroutine start would not prove overlapping SQL writes.
	for {
		var waiting bool
		err := pool.QueryRow(ctx, "SELECT coalesce(wait_event_type = 'Lock', false) FROM pg_stat_activity WHERE pid=$1", second.Conn().PgConn().PID()).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err := <-result:
			t.Fatalf("insert did not wait for concurrent transaction: %v", err)
		case <-ctx.Done():
			t.Fatal("concurrent insert never waited on lock")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, account.ErrEmailExists) {
			t.Fatalf("duplicate error = %v", err)
		}
	case <-ctx.Done():
		t.Fatal("duplicate insert did not finish")
	}
	got, err := account.New(pool).ByEmail(ctx, "\t"+strings.ToUpper(email)+"\n")
	if err != nil {
		t.Fatal(err)
	}
	if got != created {
		t.Fatal("read differs from committed account")
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM app.users WHERE email=$1", email).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("saved %d accounts, want 1", count)
	}
}

func TestStorageConstraints(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, tc := range []struct{ name, sql, code string }{
		{"null email", "INSERT INTO app.users(email,password_hash) VALUES (NULL,'hash')", "23502"},
		{"null hash", "INSERT INTO app.users(email,password_hash) VALUES ('constraint@example.com',NULL)", "23502"},
		{"empty email", "INSERT INTO app.users(email,password_hash) VALUES ('','hash')", "23514"},
		{"noncanonical email", "INSERT INTO app.users(email,password_hash) VALUES (' Alice@Example.com ','hash')", "23514"},
		{"email with trailing tab", "INSERT INTO app.users(email,password_hash) VALUES ('alice@example.com' || chr(9),'hash')", "23514"},
		{"email with leading newline", "INSERT INTO app.users(email,password_hash) VALUES (chr(10) || 'alice@example.com','hash')", "23514"},
		{"empty hash", "INSERT INTO app.users(email,password_hash) VALUES ('constraint@example.com','')", "23514"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			_, err = tx.Exec(ctx, tc.sql)
			var pgerr *pgconn.PgError
			if !errors.As(err, &pgerr) || pgerr.Code != tc.code {
				t.Fatalf("constraint error = %v, want SQLSTATE %s", err, tc.code)
			}
		})
	}
	repo := account.New(pool)
	if _, err := repo.ByEmail(ctx, "missing@example.com"); !errors.Is(err, account.ErrNotFound) {
		t.Fatalf("missing account error = %v", err)
	}
	for _, input := range []struct{ email, hash string }{{" \t", "hash"}, {"someone@example.com", ""}, {"someone@example.com", " \n"}} {
		if _, err := repo.Create(ctx, input.email, input.hash); !errors.Is(err, account.ErrInvalidInput) {
			t.Fatalf("invalid input error = %v", err)
		}
	}
}
