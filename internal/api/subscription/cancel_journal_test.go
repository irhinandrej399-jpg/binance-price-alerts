package subscription_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/auth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/journal"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/session"
	store "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type failedCancelJournalDB struct {
	*pgxpool.Pool
	t         *testing.T
	owner, id string
}
type failedCancelJournalTx struct {
	pgx.Tx
	db failedCancelJournalDB
}

func (db failedCancelJournalDB) BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	tx, err := db.Pool.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return failedCancelJournalTx{tx, db}, nil
}
func (tx failedCancelJournalTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if strings.Contains(sql, "INSERT INTO app.api_journal (") {
		saved, err := store.New(tx.Tx).ByID(ctx, tx.db.owner, tx.db.id)
		if err != nil || saved.State != domain.Cancelled || saved.Version != 2 {
			tx.db.t.Fatalf("expected provisional cancellation: %+v %v", saved, err)
		}
		var count int
		var reserved bool
		if err := tx.QueryRow(ctx, `SELECT u.active_subscriptions,s.quota_reserved FROM app.users u JOIN app.subscriptions s ON s.owner_id=u.id WHERE s.id=$1`, tx.db.id).Scan(&count, &reserved); err != nil || count != 0 || reserved {
			tx.db.t.Fatalf("expected provisional slot release: %d %v %v", count, reserved, err)
		}
		return tx.Tx.Exec(ctx, `SELECT 1/0`)
	}
	return tx.Tx.Exec(ctx, sql, args...)
}
func TestCancelHTTPJournalFailure(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	owner := quotaOwner(t, pool)
	v := quotaRecord(owner, 930001)
	if err := quotaTx(ctx, pool, owner, func(q *store.OwnerQuota) error { return q.Create(ctx, v) }); err != nil {
		t.Fatal(err)
	}
	sessions := session.New(pool, nil)
	credentials, err := sessions.Issue(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	before, err := journal.Current(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	var batches int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM app.api_journal_batches`).Scan(&batches); err != nil {
		t.Fatal(err)
	}
	h := auth.New(sessions, store.NewCancelHandler(failedCancelJournalDB{pool, t, owner, v.ID}))
	w := deleteSubscription(h, credentials.AccessToken, v.ID)
	if w.Code != http.StatusInternalServerError || strings.TrimSpace(w.Body.String()) != `{"error":{"code":"internal_error"}}` {
		t.Fatal(w.Code, w.Body.String())
	}
	saved, err := store.New(pool).ByID(ctx, owner, v.ID)
	if err != nil || saved.State != domain.Active || saved.Version != 1 {
		t.Fatal(saved, err)
	}
	assertQuota(t, pool, owner, 1)
	var reserved bool
	if err := pool.QueryRow(ctx, `SELECT quota_reserved FROM app.subscriptions WHERE id=$1`, v.ID).Scan(&reserved); err != nil || !reserved {
		t.Fatal("slot lost", reserved, err)
	}
	if c, err := journal.Current(ctx, pool); err != nil || c != before {
		t.Fatal("C changed", c, err)
	}
	entries, err := journal.ReadAfter(ctx, pool, before, 10)
	if err != nil || len(entries) != 0 {
		t.Fatal("event survived rollback", entries, err)
	}
	var after int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM app.api_journal_batches`).Scan(&after); err != nil || after != batches {
		t.Fatal("batch survived rollback", after, err)
	}
	// A later retry performs the transition once, after the failed transaction.
	h = auth.New(sessions, store.NewCancelHandler(pool))
	decodeCancel(t, deleteSubscription(h, credentials.AccessToken, v.ID))
	entries, err = journal.ReadAfter(ctx, pool, before, 10)
	if err != nil || len(entries) != 1 || entries[0].Change.OwnerID != owner || entries[0].Change.SchemaVersion != 2 || entries[0].Change.SubscriptionVersion != 2 {
		t.Fatal(entries, err)
	}
	assertQuota(t, pool, owner, 0)
}
