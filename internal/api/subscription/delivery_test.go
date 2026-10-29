package subscription_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/auth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/journal"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/session"
	store "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/alerttriggered"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func deliveryCandidate(v store.Record) store.DeliveryCandidate {
	return store.DeliveryCandidate{OwnerID: v.OwnerID, SubscriptionID: v.ID, ExpectedVersion: v.Version,
		Quote: priceobserved.Observed{SchemaVersion: 1, Pair: v.Pair, Price: &v.Threshold, SourceID: "source-42",
			Revision:  priceobserved.Revision{StreamID: "durable", Generation: 1, Sequence: 42},
			TradeTime: v.CreationTradeTime.Add(-time.Hour), ReceivedAt: v.CreationTradeTime, StreamState: priceobserved.Unconfirmed, GapDetected: true}}
}

func TestDeliveryConcurrent(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	owner := quotaOwner(t, pool)
	v := quotaRecord(owner, 940001)
	for i := 0; i < 15; i++ {
		r := quotaRecord(owner, 940001+i)
		if i == 0 {
			r = v
		}
		if err := quotaTx(ctx, pool, owner, func(q *store.OwnerQuota) error { return q.Create(ctx, r) }); err != nil {
			t.Fatal(err)
		}
	}
	before, err := journal.Current(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		v   store.Record
		err error
	}
	results := make(chan result, 20)
	start := make(chan struct{})
	for i := 0; i < 20; i++ {
		go func() {
			<-start
			r, e := store.RequestDelivery(ctx, pool, deliveryCandidate(v))
			results <- result{r, e}
		}()
	}
	close(start)
	var first store.Record
	for i := 0; i < 20; i++ {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		if i == 0 {
			first = r.v
		}
		if !reflect.DeepEqual(first, r.v) {
			t.Fatal("different concurrent response", r.v)
		}
	}
	if first.State != domain.PendingDelivery || first.Version != 2 || first.Workflow == nil || first.Workflow.Version != 1 {
		t.Fatal(first)
	}
	assertQuota(t, pool, owner, 15)
	if err := quotaTx(ctx, pool, owner, func(q *store.OwnerQuota) error { return q.Create(ctx, quotaRecord(owner, 940100)) }); !errors.Is(err, store.ErrQuotaExceeded) {
		t.Fatal(err)
	}
	entries, err := journal.ReadAfter(ctx, pool, before, 10)
	if err != nil || len(entries) != 1 {
		t.Fatal(entries, err)
	}
	c := entries[0].Change
	if c.SchemaVersion != 2 || c.OwnerID != v.OwnerID || c.State != first.State || c.SubscriptionVersion != 2 || c.Workflow.TriggerID != first.Workflow.TriggerID || c.Workflow.Version != 1 || c.Position != before+1 || c.TransactionEnd != before+1 || c.Threshold.String() != v.Threshold.String() || c.CreationSnapshot.Price.String() != v.CreationPrice.String() || !c.CreationSnapshot.TradeTime.Equal(v.CreationTradeTime) {
		t.Fatal(c)
	}
	var body, again []byte
	var eventID, triggerID string
	var n int
	if err := pool.QueryRow(ctx, `SELECT event_id,trigger_id,body FROM app.delivery_outbox WHERE subscription_id=$1`, v.ID).Scan(&eventID, &triggerID, &body); err != nil {
		t.Fatal(err)
	}
	meta, work, err := alerttriggered.Decode(body)
	if err != nil || meta.EventID != eventID || meta.OrderingKey != v.ID || work.TriggerID != triggerID || triggerID != first.Workflow.TriggerID || work.SubscriptionVersion != 2 || work.WorkflowVersion != 1 || work.UserID != owner || work.Intent != alerttriggered.DeliveryRequested || !reflect.DeepEqual(*work.CandidateQuote, deliveryCandidate(v).Quote) {
		t.Fatal(meta, work, err)
	}
	if _, err = store.RequestDelivery(ctx, pool, deliveryCandidate(v)); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT body FROM app.delivery_outbox WHERE subscription_id=$1`, v.ID).Scan(&again); err != nil || !bytes.Equal(body, again) {
		t.Fatal("unstable bytes", err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM app.delivery_outbox WHERE subscription_id=$1`, v.ID).Scan(&n); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if _, err = pool.Exec(ctx, `UPDATE app.delivery_outbox SET body=body WHERE subscription_id=$1`, v.ID); err == nil {
		t.Fatal("mutable outbox")
	}
	if _, err = pool.Exec(ctx, `DELETE FROM app.delivery_outbox WHERE subscription_id=$1`, v.ID); err == nil {
		t.Fatal("deletable outbox")
	}

	// Canonical fixture deletion must not cascade into immutable retained work.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, `DELETE FROM app.subscriptions WHERE id=$1`, v.ID); err != nil {
		t.Fatal(err)
	}
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM app.delivery_outbox WHERE subscription_id=$1`, v.ID).Scan(&n); err != nil || n != 1 {
		t.Fatal("cascade deleted work", n, err)
	}
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	// A retained workflow in active/blocked/terminal state is returned without mutation.
	for _, state := range []domain.State{domain.Active, domain.Sending, domain.DeliveryBlocked, domain.Delivered, domain.Cancelled} {
		var reason, binding any
		if state == domain.DeliveryBlocked {
			reason = "channel_unavailable"
			binding = 1
		}
		if _, err = pool.Exec(ctx, `UPDATE app.subscriptions SET state=$2,block_reason=$3,blocked_binding_version=$4 WHERE id=$1`, v.ID, state, reason, binding); err != nil {
			t.Fatal(err)
		}
		r, e := store.RequestDelivery(ctx, pool, deliveryCandidate(v))
		if e != nil || r.State != state || r.Workflow.TriggerID != triggerID || r.Version != 2 {
			t.Fatal(r, e)
		}
	}
	if c, e := journal.Current(ctx, pool); e != nil || c != before+1 {
		t.Fatal(c, e)
	}
}

func TestDeliveryRejected(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	owner := quotaOwner(t, pool)
	other := quotaOwner(t, pool)
	for i, name := range []string{"version", "false_up", "false_down", "pair", "invalid_quote", "cancelled", "blocked", "foreign", "overflow"} {
		t.Run(name, func(t *testing.T) {
			v := quotaRecord(owner, 950001+i)
			if name == "false_down" {
				v.Direction = price.Down
			}
			if err := quotaTx(ctx, pool, owner, func(q *store.OwnerQuota) error { return q.Create(ctx, v) }); err != nil {
				t.Fatal(err)
			}
			candidate := deliveryCandidate(v)
			want := store.ErrCandidateRejected
			switch name {
			case "version":
				candidate.ExpectedVersion = 2
			case "false_up":
				p, _ := price.Parse("99.999999999999999999")
				candidate.Quote.Price = &p
			case "false_down":
				p, _ := price.Parse("100.000000000000000001")
				candidate.Quote.Price = &p
			case "pair":
				candidate.Quote.Pair = "ETHUSDT"
				want = store.ErrInvalidInput
			case "invalid_quote":
				candidate.Quote.TradeTime = time.Time{}
				want = store.ErrInvalidInput
			case "cancelled":
				sessions := session.New(pool, nil)
				cred, e := sessions.Issue(ctx, owner)
				if e != nil {
					t.Fatal(e)
				}
				decodeCancel(t, deleteSubscription(auth.New(sessions, store.NewCancelHandler(pool)), cred.AccessToken, v.ID))
			case "blocked":
				if _, e := pool.Exec(ctx, `UPDATE app.subscriptions SET state='delivery_blocked',block_reason='channel_unavailable',blocked_binding_version=1 WHERE id=$1`, v.ID); e != nil {
					t.Fatal(e)
				}
			case "foreign":
				candidate.OwnerID = other
				want = store.ErrNotFound
			case "overflow":
				if _, e := pool.Exec(ctx, `UPDATE app.subscriptions SET version=9223372036854775807 WHERE id=$1`, v.ID); e != nil {
					t.Fatal(e)
				}
				candidate.ExpectedVersion = 9223372036854775807
				want = store.ErrInvalidInput
			}
			before, e := journal.Current(ctx, pool)
			if e != nil {
				t.Fatal(e)
			}
			saved, e := store.New(pool).ByID(ctx, owner, v.ID)
			if e != nil {
				t.Fatal(e)
			}
			_, e = store.RequestDelivery(ctx, pool, candidate)
			if !errors.Is(e, want) {
				t.Fatalf("want %v got %v", want, e)
			}
			after, e := store.New(pool).ByID(ctx, owner, v.ID)
			if e != nil || !reflect.DeepEqual(saved, after) {
				t.Fatal(after, e)
			}
			if c, e := journal.Current(ctx, pool); e != nil || c != before {
				t.Fatal(c, e)
			}
			var n int
			if e := pool.QueryRow(ctx, `SELECT count(*) FROM app.delivery_outbox WHERE subscription_id=$1`, v.ID).Scan(&n); e != nil || n != 0 {
				t.Fatal(n, e)
			}
		})
	}
}

// Inject a real SQL failure after observing provisional writes in the same tx.
type failedDeliveryDB struct {
	*pgxpool.Pool
	t  *testing.T
	v  store.Record
	at string
}
type failedDeliveryTx struct {
	pgx.Tx
	db failedDeliveryDB
}

func (db failedDeliveryDB) BeginTx(ctx context.Context, o pgx.TxOptions) (pgx.Tx, error) {
	tx, e := db.Pool.BeginTx(ctx, o)
	return failedDeliveryTx{tx, db}, e
}
func (tx failedDeliveryTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if strings.Contains(sql, tx.db.at) {
		r, e := store.New(tx.Tx).ByID(ctx, tx.db.v.OwnerID, tx.db.v.ID)
		if e != nil || r.State != domain.PendingDelivery || r.Workflow == nil || r.Version != 2 {
			tx.db.t.Fatalf("provisional state %+v %v", r, e)
		}
		return tx.Tx.Exec(ctx, `SELECT 1/0`)
	}
	return tx.Tx.Exec(ctx, sql, args...)
}
func TestDeliveryRollback(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	owner := quotaOwner(t, pool)
	for i, at := range []string{"INSERT INTO app.delivery_outbox", "INSERT INTO app.api_journal (", "commit"} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			v := quotaRecord(owner, 960001+i)
			if e := quotaTx(ctx, pool, owner, func(q *store.OwnerQuota) error { return q.Create(ctx, v) }); e != nil {
				t.Fatal(e)
			}
			before, e := journal.Current(ctx, pool)
			if e != nil {
				t.Fatal(e)
			}
			var db store.Beginner = failedDeliveryDB{pool, t, v, at}
			if at == "commit" {
				db = failedCommitDB{pool}
			}
			if _, e = store.RequestDelivery(ctx, db, deliveryCandidate(v)); e == nil {
				t.Fatal("wanted failure")
			}
			saved, e := store.New(pool).ByID(ctx, owner, v.ID)
			if e != nil || !reflect.DeepEqual(saved, v) {
				t.Fatal(saved, e)
			}
			if c, e := journal.Current(ctx, pool); e != nil || c != before {
				t.Fatal(c, e)
			}
			var n int
			if e := pool.QueryRow(ctx, `SELECT count(*) FROM app.delivery_outbox WHERE subscription_id=$1`, v.ID).Scan(&n); e != nil || n != 0 {
				t.Fatal(n, e)
			}
			if _, e = store.RequestDelivery(ctx, pool, deliveryCandidate(v)); e != nil {
				t.Fatal(e)
			}
			assertQuota(t, pool, owner, i+1)
		})
	}
}

// Keep the real cancellation transaction open immediately before commit.
type pausedCancelDB struct {
	*pgxpool.Pool
	reached chan struct{}
	release chan struct{}
}
type pausedCancelTx struct {
	pgx.Tx
	db pausedCancelDB
}

func (db pausedCancelDB) BeginTx(ctx context.Context, o pgx.TxOptions) (pgx.Tx, error) {
	tx, e := db.Pool.BeginTx(ctx, o)
	if e != nil {
		return nil, e
	}
	return pausedCancelTx{tx, db}, nil
}
func (tx pausedCancelTx) Commit(ctx context.Context) error {
	close(tx.db.reached)
	select {
	case <-tx.db.release:
		return tx.Tx.Commit(ctx)
	case <-ctx.Done():
		return ctx.Err()
	}
}
func TestDeliveryWaitsForCancellation(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	owner := quotaOwner(t, pool)
	v := quotaRecord(owner, 970001)
	if e := quotaTx(ctx, pool, owner, func(q *store.OwnerQuota) error { return q.Create(ctx, v) }); e != nil {
		t.Fatal(e)
	}
	sessions := session.New(pool, nil)
	cred, e := sessions.Issue(ctx, owner)
	if e != nil {
		t.Fatal(e)
	}
	db := pausedCancelDB{pool, make(chan struct{}), make(chan struct{})}
	cancelled := make(chan int, 1)
	go func() {
		cancelled <- deleteSubscription(auth.New(sessions, store.NewCancelHandler(db)), cred.AccessToken, v.ID).Code
	}()
	select {
	case <-db.reached:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	defer func() {
		select {
		case <-db.release:
		default:
			close(db.release)
		}
	}()
	result := make(chan error, 1)
	go func() { _, e := store.RequestDelivery(ctx, pool, deliveryCandidate(v)); result <- e }()
	for {
		var waiting bool
		e := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT id::text FROM app.users%')`).Scan(&waiting)
		if e != nil {
			t.Fatal(e)
		}
		if waiting {
			break
		}
		select {
		case <-time.After(5 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	close(db.release)
	select {
	case code := <-cancelled:
		if code != 200 {
			t.Fatal(code)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case e := <-result:
		if !errors.Is(e, store.ErrCandidateRejected) {
			t.Fatal(e)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	saved, e := store.New(pool).ByID(ctx, owner, v.ID)
	if e != nil || saved.State != domain.Cancelled || saved.Version != 2 || saved.Workflow != nil {
		t.Fatal(saved, e)
	}
	assertQuota(t, pool, owner, 0)
	var n int
	if e := pool.QueryRow(ctx, `SELECT count(*) FROM app.delivery_outbox WHERE subscription_id=$1`, v.ID).Scan(&n); e != nil || n != 0 {
		t.Fatal(n, e)
	}
}

type lostDeliveryCommitDB struct{ *pgxpool.Pool }
type lostDeliveryCommitTx struct{ pgx.Tx }

func (db lostDeliveryCommitDB) BeginTx(ctx context.Context, o pgx.TxOptions) (pgx.Tx, error) {
	tx, e := db.Pool.BeginTx(ctx, o)
	if e != nil {
		return nil, e
	}
	return lostDeliveryCommitTx{tx}, nil
}
func (tx lostDeliveryCommitTx) Commit(ctx context.Context) error {
	if e := tx.Tx.Commit(ctx); e != nil {
		return e
	}
	return errors.New("lost commit response")
}
func TestDeliveryRecoversCommittedWorkflow(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	owner := quotaOwner(t, pool)
	v := quotaRecord(owner, 980001)
	v.Direction = price.Down
	if e := quotaTx(ctx, pool, owner, func(q *store.OwnerQuota) error { return q.Create(ctx, v) }); e != nil {
		t.Fatal(e)
	}
	before, e := journal.Current(ctx, pool)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = store.RequestDelivery(ctx, lostDeliveryCommitDB{pool}, deliveryCandidate(v)); !errors.Is(e, store.ErrStorage) {
		t.Fatal(e)
	}
	saved, e := store.New(pool).ByID(ctx, owner, v.ID)
	if e != nil || saved.Workflow == nil || saved.State != domain.PendingDelivery {
		t.Fatal(saved, e)
	}
	recovered, e := store.RequestDelivery(ctx, pool, deliveryCandidate(v))
	if e != nil || !reflect.DeepEqual(recovered, saved) {
		t.Fatal(recovered, e)
	}
	entries, e := journal.ReadAfter(ctx, pool, before, 10)
	if e != nil || len(entries) != 1 || entries[0].Change.Workflow.TriggerID != saved.Workflow.TriggerID {
		t.Fatal(entries, e)
	}
	var count int
	if e := pool.QueryRow(ctx, `SELECT count(*) FROM app.delivery_outbox WHERE subscription_id=$1 AND trigger_id=$2`, v.ID, saved.Workflow.TriggerID).Scan(&count); e != nil || count != 1 {
		t.Fatal(count, e)
	}
	assertQuota(t, pool, owner, 1)
}
