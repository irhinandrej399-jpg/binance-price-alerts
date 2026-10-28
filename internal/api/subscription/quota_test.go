package subscription_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	store "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func quotaOwner(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	a, err := account.New(pool).Create(context.Background(), fmt.Sprintf("quota-%d@example.com", time.Now().UnixNano()), "hash")
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(context.Background(), `UPDATE app.users SET telegram_chat_id=$2,telegram_version=1 WHERE id=$1`, a.ID, time.Now().UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), `DELETE FROM app.subscriptions WHERE owner_id=$1`, a.ID); err != nil {
			t.Error(err)
		}
		if _, err := pool.Exec(context.Background(), `DELETE FROM app.users WHERE id=$1`, a.ID); err != nil {
			t.Error(err)
		}
	})
	return a.ID
}
func quotaRecord(owner string, i int) store.Record {
	p, _ := price.Parse("100")
	return store.Record{ID: fmt.Sprintf("00000000-0000-4000-8000-%012d", i), OwnerID: owner, Pair: "BTCUSDT", Threshold: p, Direction: price.Up, CreationPrice: p, CreationTradeTime: time.Now().UTC().Truncate(time.Microsecond), State: domain.Active, Version: 1, Activation: store.LatestPrice}
}
func quotaTx(ctx context.Context, pool *pgxpool.Pool, owner string, fn func(*store.OwnerQuota) error) error {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	q, err := store.LockQuota(ctx, tx, owner)
	if err != nil {
		return err
	}
	if err = fn(q); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func assertQuota(t *testing.T, pool *pgxpool.Pool, owner string, want int) {
	t.Helper()
	var count, rows, unfinished int
	err := pool.QueryRow(context.Background(), `SELECT active_subscriptions,(SELECT count(*) FROM app.subscriptions WHERE owner_id=$1 AND quota_reserved),(SELECT count(*) FROM app.subscriptions WHERE owner_id=$1 AND state NOT IN ('delivered','cancelled')) FROM app.users WHERE id=$1`, owner).Scan(&count, &rows, &unfinished)
	if err != nil || count != want || rows != want || unfinished != want {
		t.Fatalf("quota count=%d reserved=%d unfinished=%d want=%d err=%v", count, rows, unfinished, want, err)
	}
}
func TestQuotaConcurrentCreation(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, initial := range []int{0, 14} {
		t.Run(fmt.Sprint(initial), func(t *testing.T) {
			owner := quotaOwner(t, pool)
			for i := 0; i < initial; i++ {
				if err := quotaTx(ctx, pool, owner, func(q *store.OwnerQuota) error { return q.Create(ctx, quotaRecord(owner, 200000+i)) }); err != nil {
					t.Fatal(err)
				}
			}
			n := 30
			if initial == 14 {
				n = 2
			}
			start := make(chan struct{})
			results := make(chan error, n)
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					results <- quotaTx(ctx, pool, owner, func(q *store.OwnerQuota) error { return q.Create(ctx, quotaRecord(owner, 300000+initial*100+i)) })
				}(i)
			}
			close(start)
			wg.Wait()
			close(results)
			successes := 0
			for err := range results {
				if err == nil {
					successes++
				} else if !errors.Is(err, store.ErrQuotaExceeded) {
					t.Fatal(err)
				}
			}
			if successes != 15-initial {
				t.Fatalf("successes=%d want=%d", successes, 15-initial)
			}
			assertQuota(t, pool, owner, 15)

			// At the limit, cancellation makes exactly one creation possible.
			tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(context.Background())
			q, err := store.LockQuota(ctx, tx, owner)
			if err != nil {
				t.Fatal(err)
			}
			var id string
			err = tx.QueryRow(ctx, `UPDATE app.subscriptions SET state='cancelled' WHERE id=(SELECT id FROM app.subscriptions WHERE owner_id=$1 ORDER BY id LIMIT 1) RETURNING id::text`, owner).Scan(&id)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				if err = q.ReleaseTerminal(ctx, id); err != nil {
					t.Fatal(err)
				}
			}
			if err = tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			assertQuota(t, pool, owner, 14)
			if err = quotaTx(ctx, pool, owner, func(q *store.OwnerQuota) error { return q.Create(ctx, quotaRecord(owner, 700000+initial)) }); err != nil {
				t.Fatal(err)
			}
			if err = quotaTx(ctx, pool, owner, func(q *store.OwnerQuota) error { return q.Create(ctx, quotaRecord(owner, 710000+initial)) }); !errors.Is(err, store.ErrQuotaExceeded) {
				t.Fatal(err)
			}
			assertQuota(t, pool, owner, 15)
		})
	}
}

func TestQuotaRollbackAndChannel(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	owner := quotaOwner(t, pool)
	sentinel := errors.New("later operation failed")
	err := quotaTx(ctx, pool, owner, func(q *store.OwnerQuota) error {
		if err := q.Create(ctx, quotaRecord(owner, 400001)); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	assertQuota(t, pool, owner, 0)
	if _, err := store.New(pool).ByID(ctx, owner, quotaRecord(owner, 400001).ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("rollback: %v", err)
	}
	_, err = pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=NULL WHERE id=$1`, owner)
	if err != nil {
		t.Fatal(err)
	}
	err = quotaTx(ctx, pool, owner, func(q *store.OwnerQuota) error { return q.Create(ctx, quotaRecord(owner, 400002)) })
	if !errors.Is(err, store.ErrTelegramNotLinked) {
		t.Fatalf("channel: %v", err)
	}
	assertQuota(t, pool, owner, 0)
}

func TestQuotaReleaseTerminal(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	owner := quotaOwner(t, pool)
	v := quotaRecord(owner, 500001)
	// State protocols belong to subsequent tickets. Compose their SQL and quota
	// release under the SAME user lock/transaction; never release on reactivation.
	for _, terminal := range []domain.State{domain.Cancelled, domain.Delivered} {
		v.ID = quotaRecord(owner, 500001+int(terminal[0])).ID
		if err := quotaTx(ctx, pool, owner, func(q *store.OwnerQuota) error { return q.Create(ctx, v) }); err != nil {
			t.Fatal(err)
		}
		for _, state := range []domain.State{domain.PendingDelivery, domain.Sending, domain.DeliveryBlocked, domain.Active, terminal, terminal} {
			tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
			if err != nil {
				t.Fatal(err)
			}
			q, err := store.LockQuota(ctx, tx, owner)
			if err != nil {
				tx.Rollback(ctx)
				t.Fatal(err)
			}
			reason := any(nil)
			binding := any(nil)
			if state == domain.DeliveryBlocked {
				reason = "channel_unavailable"
				binding = 1
			}
			_, err = tx.Exec(ctx, `UPDATE app.subscriptions SET state=$2,workflow_id=$5,workflow_version=1,block_reason=$3,blocked_binding_version=$4 WHERE id=$1`, v.ID, state, reason, binding, "quota-"+v.ID)
			if err == nil {
				err = q.ReleaseTerminal(ctx, v.ID)
			}
			if err != nil {
				tx.Rollback(ctx)
				t.Fatal(err)
			}
			if err = tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			want := 1
			if state == terminal {
				want = 0
			}
			assertQuota(t, pool, owner, want)
		}
	}
	if err := quotaTx(ctx, pool, owner, func(q *store.OwnerQuota) error { return q.Create(ctx, quotaRecord(owner, 500002)) }); err != nil {
		t.Fatal(err)
	}
	assertQuota(t, pool, owner, 1)
}

func TestQuotaOwnerLocksAndChannelRecheck(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first, second := quotaOwner(t, pool), quotaOwner(t, pool)
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = store.LockQuota(ctx, tx, first); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `UPDATE app.users SET telegram_chat_id=NULL WHERE id=$1`, first); err != nil {
		t.Fatal(err)
	}
	// Another owner can commit while the first owner's binding/quota is locked.
	otherCtx, stop := context.WithTimeout(ctx, 3*time.Second)
	defer stop()
	if err := quotaTx(otherCtx, pool, second, func(q *store.OwnerQuota) error { return q.Create(otherCtx, quotaRecord(second, 600001)) }); err != nil {
		t.Fatal(err)
	}
	waiting, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	defer waiting.Rollback(context.Background())
	if _, err = waiting.Exec(ctx, `SET LOCAL lock_timeout='100ms'`); err != nil {
		t.Fatal(err)
	}
	if _, err = store.LockQuota(ctx, waiting, first); !errors.Is(err, store.ErrStorage) {
		t.Fatalf("same owner not serialized: %v", err)
	}
	if err = waiting.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	err = quotaTx(ctx, pool, first, func(q *store.OwnerQuota) error { return q.Create(ctx, quotaRecord(first, 600002)) })
	if !errors.Is(err, store.ErrTelegramNotLinked) {
		t.Fatalf("binding not rechecked: %v", err)
	}
	assertQuota(t, pool, first, 0)
	assertQuota(t, pool, second, 1)
}

func TestQuotaFailureAtomicity(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	owner := quotaOwner(t, pool)
	v := quotaRecord(owner, 800001)
	bad := v
	bad.Pair = "invalid pair"
	if err := quotaTx(ctx, pool, owner, func(q *store.OwnerQuota) error { return q.Create(ctx, bad) }); !errors.Is(err, store.ErrInvalidInput) {
		t.Fatalf("insert failure: %v", err)
	}
	assertQuota(t, pool, owner, 0)
	if err := quotaTx(ctx, pool, owner, func(q *store.OwnerQuota) error { return q.Create(ctx, v) }); err != nil {
		t.Fatal(err)
	}
	if err := quotaTx(ctx, pool, owner, func(q *store.OwnerQuota) error { return q.Create(ctx, v) }); !errors.Is(err, store.ErrExists) {
		t.Fatalf("duplicate: %v", err)
	}
	assertQuota(t, pool, owner, 1)
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	q, err := store.LockQuota(ctx, tx, owner)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `UPDATE app.subscriptions SET state='cancelled' WHERE id=$1`, v.ID); err != nil {
		t.Fatal(err)
	}
	if err = q.ReleaseTerminal(ctx, v.ID); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	assertQuota(t, pool, owner, 1)
	got, err := store.New(pool).ByID(ctx, owner, v.ID)
	if err != nil || got.State != domain.Active {
		t.Fatalf("release rollback: state=%s err=%v", got.State, err)
	}
	for _, value := range []int{-1, 16} {
		if _, err := pool.Exec(ctx, `UPDATE app.users SET active_subscriptions=$2 WHERE id=$1`, owner, value); err == nil {
			t.Fatalf("counter accepted %d", value)
		}
	}
	assertQuota(t, pool, owner, 1)
	other := quotaOwner(t, pool)
	if err := quotaTx(ctx, pool, other, func(q *store.OwnerQuota) error { return q.ReleaseTerminal(ctx, v.ID) }); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("foreign release: %v", err)
	}
	assertQuota(t, pool, owner, 1)
	cancelled, stop := context.WithCancel(ctx)
	stop()
	if err := quotaTx(cancelled, pool, owner, func(q *store.OwnerQuota) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
}
