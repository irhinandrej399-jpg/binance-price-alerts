package subscription_test

import (
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
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/telegramcode"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/telegramlink"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func admissionFixture(t *testing.T, pool *pgxpool.Pool, i int) (store.Record, store.AdmissionCommand) {
	t.Helper()
	ctx := context.Background()
	v := quotaRecord(quotaOwner(t, pool), i)
	if e := quotaTx(ctx, pool, v.OwnerID, func(q *store.OwnerQuota) error { return q.Create(ctx, v) }); e != nil {
		t.Fatal(e)
	}
	v, e := store.RequestDelivery(ctx, pool, deliveryCandidate(v))
	if e != nil {
		t.Fatal(e)
	}
	return v, store.AdmissionCommand{OwnerID: v.OwnerID, SubscriptionID: v.ID, WorkflowID: v.Workflow.TriggerID, RequestID: "check-1", Quote: deliveryCandidate(v).Quote, PermitTTL: time.Minute}
}

func TestAdmissionConcurrentAndReplay(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	v, c := admissionFixture(t, pool, 991001)
	before, e := journal.Current(ctx, pool)
	if e != nil {
		t.Fatal(e)
	}
	type result struct {
		r store.AdmissionResult
		e error
	}
	results := make(chan result, 20)
	start := make(chan struct{})
	for i := 0; i < 20; i++ {
		go func() { <-start; r, e := store.AdmitDelivery(ctx, pool, c); results <- result{r, e} }()
	}
	close(start)
	var first store.AdmissionResult
	for i := 0; i < 20; i++ {
		r := <-results
		if r.e != nil {
			t.Fatal(r.e)
		}
		if i == 0 {
			first = r.r
		}
		if !reflect.DeepEqual(first, r.r) {
			t.Fatal("unstable replay", first, r.r)
		}
	}
	if first.Outcome != store.Admitted || first.Record.State != domain.Sending || first.Record.Version != 3 || first.Attempt == nil || first.Attempt.Fence != 1 || first.Attempt.ChatID <= 0 || first.Attempt.BindingVersion != 1 || !reflect.DeepEqual(first.Attempt.Quote, c.Quote) {
		t.Fatal(first)
	}
	c.RequestID = "check-2"
	busy, e := store.AdmitDelivery(ctx, pool, c)
	if e != nil || busy.Outcome != store.AttemptBusy || busy.Attempt != nil {
		t.Fatal(busy, e)
	}
	entries, e := journal.ReadAfter(ctx, pool, before, 10)
	if e != nil || len(entries) != 1 || entries[0].Change.SchemaVersion != 2 || entries[0].Change.OwnerID != v.OwnerID || entries[0].Change.State != domain.Sending || entries[0].Change.SubscriptionVersion != 3 || entries[0].Change.Workflow.TriggerID != v.Workflow.TriggerID {
		t.Fatal(entries, e)
	}
	assertQuota(t, pool, v.OwnerID, 1)
	// Recovery needs command identity only, including after a lost commit reply.
	c.RequestID = "check-1"
	c.Quote = priceobserved.Observed{}
	c.PermitTTL = 0
	replay, e := store.AdmitDelivery(ctx, pool, c)
	if e != nil || !reflect.DeepEqual(first, replay) {
		t.Fatal(replay, e)
	}
	var n int
	if e = pool.QueryRow(ctx, `SELECT count(*) FROM app.delivery_admissions WHERE subscription_id=$1`, v.ID).Scan(&n); e != nil || n != 2 {
		t.Fatal(n, e)
	}
	if _, e = pool.Exec(ctx, `UPDATE app.delivery_admissions SET response=response WHERE subscription_id=$1`, v.ID); e == nil {
		t.Fatal("mutable command/permission")
	}
	if _, e = pool.Exec(ctx, `DELETE FROM app.delivery_admissions WHERE subscription_id=$1`, v.ID); e == nil {
		t.Fatal("deletable command/permission")
	}
}

func waitAdmissionExpiry(t *testing.T, pool *pgxpool.Pool, expires time.Time) {
	t.Helper()
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	for {
		var expired bool
		if e := pool.QueryRow(ctx, `SELECT clock_timestamp()>=$1`, expires).Scan(&expired); e != nil {
			t.Fatal(e)
		}
		if expired {
			return
		}
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

func TestAdmissionExpirationAndFencing(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	v, c := admissionFixture(t, pool, 991002)
	c.PermitTTL = 20 * time.Millisecond
	first, e := store.AdmitDelivery(ctx, pool, c)
	if e != nil {
		t.Fatal(e)
	}
	waitAdmissionExpiry(t, pool, first.Attempt.ExpiresAt)
	replay, e := store.AdmitDelivery(ctx, pool, c)
	if e != nil || !reflect.DeepEqual(replay, first) {
		t.Fatal(replay, e)
	}
	c.RequestID = "new-after-expiry"
	c.PermitTTL = time.Minute
	second, e := store.AdmitDelivery(ctx, pool, c)
	if e != nil || second.Attempt == nil || second.Attempt.ID == first.Attempt.ID || second.Attempt.Fence != 2 || second.Record.Version != 4 {
		t.Fatal(second, e)
	}
	c.RequestID = "check-1"
	old, e := store.AdmitDelivery(ctx, pool, c)
	if e != nil || !reflect.DeepEqual(old, first) {
		t.Fatal(old, e)
	}
	var id string
	var fence int64
	if e = pool.QueryRow(ctx, `SELECT current_attempt_id,admission_fence FROM app.subscriptions WHERE id=$1`, v.ID).Scan(&id, &fence); e != nil || id != second.Attempt.ID || fence != second.Attempt.Fence || id == first.Attempt.ID {
		t.Fatal(id, fence, e)
	}
	assertQuota(t, pool, v.OwnerID, 1)
}

func TestAdmissionWaitAndChannelRecovery(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	for i, truth := range []bool{true, false} {
		t.Run(fmt.Sprint(truth), func(t *testing.T) {
			v, c := admissionFixture(t, pool, 991010+i)
			percent := "+5" // Fixed threshold must survive recovery, independently of current price.
			if _, e := pool.Exec(ctx, `UPDATE app.subscriptions SET percent=$2 WHERE id=$1`, v.ID, percent); e != nil {
				t.Fatal(e)
			}
			if _, e := pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=NULL WHERE id=$1`, v.OwnerID); e != nil {
				t.Fatal(e)
			}
			blocked, e := store.AdmitDelivery(ctx, pool, c)
			if e != nil || blocked.Outcome != store.AdmissionBlocked || blocked.Record.Blocked == nil || blocked.Record.Blocked.BindingVersion != 1 || blocked.Record.State != domain.DeliveryBlocked || blocked.Attempt != nil {
				t.Fatal(blocked, e)
			}
			c.RequestID = "same-binding"
			again, e := store.AdmitDelivery(ctx, pool, c)
			if e != nil || again.Outcome != store.AdmissionBlocked || again.Record.Version != blocked.Record.Version {
				t.Fatal(again, e)
			}
			// Restoring the same version is insufficient.
			if _, e = pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=$2 WHERE id=$1`, v.OwnerID, time.Now().UnixNano()); e != nil {
				t.Fatal(e)
			}
			c.RequestID = "same-version"
			again, e = store.AdmitDelivery(ctx, pool, c)
			if e != nil || again.Attempt != nil || again.Record.State != domain.DeliveryBlocked {
				t.Fatal(again, e)
			}
			if _, e = pool.Exec(ctx, `UPDATE app.users SET telegram_version=2 WHERE id=$1`, v.OwnerID); e != nil {
				t.Fatal(e)
			}
			c.RequestID = "recover"
			if !truth {
				p := v.CreationPrice // threshold 100, choose 99
				if e := p.UnmarshalText([]byte("99")); e != nil {
					t.Fatal(e)
				}
				c.Quote.Price = &p
			}
			before, e := journal.Current(ctx, pool)
			if e != nil {
				t.Fatal(e)
			}
			recovered, e := store.AdmitDelivery(ctx, pool, c)
			if e != nil {
				t.Fatal(e)
			}
			want := domain.Active
			if truth {
				want = domain.Sending
			}
			if recovered.Record.State != want || recovered.Record.Blocked != nil || recovered.Record.Workflow.TriggerID != v.Workflow.TriggerID || recovered.Record.Threshold.Cmp(v.Threshold) != 0 || *recovered.Record.Percent != percent || recovered.Record.Version != blocked.Record.Version+1 {
				t.Fatal(recovered)
			}
			if truth && (recovered.Attempt == nil || recovered.Attempt.BindingVersion != 2) {
				t.Fatal(recovered)
			}
			if !truth && (recovered.Outcome != store.WaitCondition || recovered.Attempt != nil) {
				t.Fatal(recovered)
			}
			replay, e := store.AdmitDelivery(ctx, pool, c)
			if e != nil || !reflect.DeepEqual(replay, recovered) {
				t.Fatal(replay, e)
			}
			entries, e := journal.ReadAfter(ctx, pool, before, 10)
			if e != nil || len(entries) != 1 || entries[0].Change.State != want {
				t.Fatal(entries, e)
			}
			assertQuota(t, pool, v.OwnerID, 1)
		})
	}
}

func TestAdmissionRollbackAndLostReply(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	v, c := admissionFixture(t, pool, 991020)
	before, e := journal.Current(ctx, pool)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = store.AdmitDelivery(ctx, failedCommitDB{pool}, c); e == nil {
		t.Fatal("wanted commit failure")
	}
	saved, e := store.New(pool).ByID(ctx, v.OwnerID, v.ID)
	if e != nil || !reflect.DeepEqual(saved, v) {
		t.Fatal(saved, e)
	}
	if pos, e := journal.Current(ctx, pool); e != nil || pos != before {
		t.Fatal(pos, e)
	}
	var n int
	if e = pool.QueryRow(ctx, `SELECT count(*) FROM app.delivery_admissions WHERE subscription_id=$1`, v.ID).Scan(&n); e != nil || n != 0 {
		t.Fatal(n, e)
	}
	if _, e = store.AdmitDelivery(ctx, lostDeliveryCommitDB{pool}, c); !errors.Is(e, store.ErrStorage) {
		t.Fatal(e)
	}
	recovered, e := store.AdmitDelivery(ctx, pool, c)
	if e != nil || recovered.Attempt == nil || recovered.Attempt.Fence != 1 {
		t.Fatal(recovered, e)
	}
	if pos, e := journal.Current(ctx, pool); e != nil || pos != before+1 {
		t.Fatal(pos, e)
	}
	assertQuota(t, pool, v.OwnerID, 1)
}

func TestAdmissionCompetingCommands(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	v, c := admissionFixture(t, pool, 991030)
	type result struct {
		r store.AdmissionResult
		e error
	}
	ch := make(chan result, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		go func(i int) {
			<-start
			cmd := c
			cmd.RequestID = fmt.Sprint(i)
			r, e := store.AdmitDelivery(ctx, pool, cmd)
			ch <- result{r, e}
		}(i)
	}
	close(start)
	admitted, busy := 0, 0
	for i := 0; i < 2; i++ {
		r := <-ch
		if r.e != nil {
			t.Fatal(r.e)
		}
		switch r.r.Outcome {
		case store.Admitted:
			admitted++
		case store.AttemptBusy:
			busy++
		}
	}
	if admitted != 1 || busy != 1 {
		t.Fatal(admitted, busy)
	}
	assertQuota(t, pool, v.OwnerID, 1)
}

func TestAdmissionRejectsAndWaits(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	for i, name := range []string{"false", "false_down", "terminal_cancelled", "terminal_delivered", "other_block", "workflow", "pair", "quote", "ttl", "overflow", "foreign", "no_workflow"} {
		t.Run(name, func(t *testing.T) {
			v, c := admissionFixture(t, pool, 991040+i)
			outcome := store.WaitCondition
			var wantErr error
			switch name {
			case "false", "false_down":
				p := v.Threshold
				value := "99"
				if name == "false_down" {
					value = "101"
					if _, e := pool.Exec(ctx, `UPDATE app.subscriptions SET direction='down' WHERE id=$1`, v.ID); e != nil {
						t.Fatal(e)
					}
				}
				if e := p.UnmarshalText([]byte(value)); e != nil {
					t.Fatal(e)
				}
				c.Quote.Price = &p
			case "terminal_cancelled", "terminal_delivered":
				state := "cancelled"
				if name == "terminal_delivered" {
					state = "delivered"
				}
				if e := quotaTx(ctx, pool, v.OwnerID, func(q *store.OwnerQuota) error {
					_, e := pool.Exec(ctx, `UPDATE app.subscriptions SET state=$2 WHERE id=$1`, v.ID, state)
					if e != nil {
						return e
					}
					return q.ReleaseTerminal(ctx, v.ID)
				}); e != nil {
					t.Fatal(e)
				}
				outcome = store.AdmissionTerminal
			case "other_block":
				if _, e := pool.Exec(ctx, `UPDATE app.subscriptions SET state='delivery_blocked',block_reason='permanent_failure',blocked_binding_version=0 WHERE id=$1`, v.ID); e != nil {
					t.Fatal(e)
				}
				outcome = store.AdmissionBlocked
			case "workflow":
				c.WorkflowID = "foreign-workflow"
				wantErr = store.ErrAdmissionRejected
			case "pair":
				c.Quote.Pair = "ETHUSDT"
				wantErr = store.ErrInvalidInput
			case "quote":
				c.Quote.TradeTime = time.Time{}
				wantErr = store.ErrInvalidInput
			case "ttl":
				c.PermitTTL = time.Hour
				wantErr = store.ErrInvalidInput
			case "overflow":
				if _, e := pool.Exec(ctx, `UPDATE app.subscriptions SET admission_fence=9223372036854775807 WHERE id=$1`, v.ID); e != nil {
					t.Fatal(e)
				}
				wantErr = store.ErrInvalidInput
			case "foreign":
				c.OwnerID = quotaOwner(t, pool)
				wantErr = store.ErrNotFound
			case "no_workflow":
				if _, e := pool.Exec(ctx, `UPDATE app.subscriptions SET state='active',workflow_id=NULL,workflow_version=NULL WHERE id=$1`, v.ID); e != nil {
					t.Fatal(e)
				}
				wantErr = store.ErrAdmissionRejected
			}
			before, e := journal.Current(ctx, pool)
			if e != nil {
				t.Fatal(e)
			}
			saved, e := store.New(pool).ByID(ctx, v.OwnerID, v.ID)
			if e != nil {
				t.Fatal(e)
			}
			r, e := store.AdmitDelivery(ctx, pool, c)
			if !errors.Is(e, wantErr) {
				t.Fatal(r, e, wantErr)
			}
			mutated := name == "false" || name == "false_down"
			if wantErr == nil && (r.Outcome != outcome || r.Attempt != nil) {
				t.Fatal(r)
			}
			after, e := store.New(pool).ByID(ctx, v.OwnerID, v.ID)
			if e != nil {
				t.Fatal(e)
			}
			if mutated {
				if after.State != domain.Active || after.Version != saved.Version+1 || after.Workflow.TriggerID != saved.Workflow.TriggerID {
					t.Fatal(after)
				}
			} else if !reflect.DeepEqual(saved, after) {
				t.Fatal(saved, after)
			}
			expected := before
			if mutated {
				expected++
			}
			if pos, e := journal.Current(ctx, pool); e != nil || pos != expected {
				t.Fatal(pos, expected, e)
			}
			quota := 1
			if outcome == store.AdmissionTerminal {
				quota = 0
			}
			assertQuota(t, pool, v.OwnerID, quota)
		})
	}
}

// Fail real SQL after provisional canonical writes, including the command row
// before journal failure. Verify the entire transaction disappears on rollback.
type failedAdmissionDB struct {
	*pgxpool.Pool
	t  *testing.T
	v  store.Record
	at string
}
type failedAdmissionTx struct {
	pgx.Tx
	db failedAdmissionDB
}

func (db failedAdmissionDB) BeginTx(ctx context.Context, o pgx.TxOptions) (pgx.Tx, error) {
	tx, e := db.Pool.BeginTx(ctx, o)
	if e != nil {
		return nil, e
	}
	return failedAdmissionTx{tx, db}, nil
}
func (tx failedAdmissionTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if strings.Contains(sql, tx.db.at) {
		r, e := store.New(tx.Tx).ByID(ctx, tx.db.v.OwnerID, tx.db.v.ID)
		if e != nil || r.State != domain.Sending || r.Version != 3 {
			tx.db.t.Fatal(r, e)
		}
		if strings.Contains(tx.db.at, "api_journal") {
			var n int
			if e = tx.Tx.QueryRow(ctx, `SELECT count(*) FROM app.delivery_admissions WHERE subscription_id=$1`, r.ID).Scan(&n); e != nil || n != 1 {
				tx.db.t.Fatal(n, e)
			}
		}
		return tx.Tx.Exec(ctx, `SELECT 1/0`)
	}
	return tx.Tx.Exec(ctx, sql, args...)
}
func TestAdmissionSQLRollback(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	for i, at := range []string{"INSERT INTO app.delivery_admissions", "INSERT INTO app.api_journal ("} {
		t.Run(at, func(t *testing.T) {
			v, c := admissionFixture(t, pool, 991060+i)
			before, e := journal.Current(ctx, pool)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = store.AdmitDelivery(ctx, failedAdmissionDB{pool, t, v, at}, c); e == nil {
				t.Fatal("wanted SQL failure")
			}
			saved, e := store.New(pool).ByID(ctx, v.OwnerID, v.ID)
			if e != nil || !reflect.DeepEqual(saved, v) {
				t.Fatal(saved, e)
			}
			var n, fence int64
			var current *string
			if e = pool.QueryRow(ctx, `SELECT admission_fence,current_attempt_id FROM app.subscriptions WHERE id=$1`, v.ID).Scan(&fence, &current); e != nil || fence != 0 || current != nil {
				t.Fatal(fence, current, e)
			}
			if e = pool.QueryRow(ctx, `SELECT count(*) FROM app.delivery_admissions WHERE subscription_id=$1`, v.ID).Scan(&n); e != nil || n != 0 {
				t.Fatal(n, e)
			}
			if pos, e := journal.Current(ctx, pool); e != nil || pos != before {
				t.Fatal(pos, e)
			}
			good, e := store.AdmitDelivery(ctx, pool, c)
			if e != nil || good.Attempt == nil || good.Attempt.Fence != 1 {
				t.Fatal(good, e)
			}
			assertQuota(t, pool, v.OwnerID, 1)
		})
	}
}

func waitAdmissionOwnerLock(t *testing.T, pool *pgxpool.Pool, ctx context.Context) {
	t.Helper()
	for {
		var waiting bool
		if e := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT id::text FROM app.users%')`).Scan(&waiting); e != nil {
			t.Fatal(e)
		}
		if waiting {
			return
		}
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}
func releasePausedOnExit(db pausedCancelDB) func() {
	return func() {
		select {
		case <-db.release:
		default:
			close(db.release)
		}
	}
}

func TestAdmissionCancellationBoundary(t *testing.T) {
	pool := postgrestest.Open(t)
	for i, cancelFirst := range []bool{true, false} {
		t.Run(fmt.Sprint(cancelFirst), func(t *testing.T) {
			ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			v, c := admissionFixture(t, pool, 991070+i)
			sessions := session.New(pool, nil)
			cred, e := sessions.Issue(ctx, v.OwnerID)
			if e != nil {
				t.Fatal(e)
			}
			paused := pausedCancelDB{pool, make(chan struct{}), make(chan struct{})}
			defer releasePausedOnExit(paused)()
			admissions := make(chan store.AdmissionResult, 1)
			errs := make(chan error, 1)
			cancelled := make(chan int, 1)
			if cancelFirst {
				go func() {
					cancelled <- deleteSubscription(auth.New(sessions, store.NewCancelHandler(paused)), cred.AccessToken, v.ID).Code
				}()
				select {
				case <-paused.reached:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				go func() { r, e := store.AdmitDelivery(ctx, pool, c); admissions <- r; errs <- e }()
			} else {
				go func() { r, e := store.AdmitDelivery(ctx, paused, c); admissions <- r; errs <- e }()
				select {
				case <-paused.reached:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				go func() {
					cancelled <- deleteSubscription(auth.New(sessions, store.NewCancelHandler(pool)), cred.AccessToken, v.ID).Code
				}()
			}
			waitAdmissionOwnerLock(t, pool, ctx)
			close(paused.release)
			select {
			case code := <-cancelled:
				if code != 200 {
					t.Fatal(code)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			var r store.AdmissionResult
			select {
			case r = <-admissions:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if e := <-errs; e != nil {
				t.Fatal(e)
			}
			if cancelFirst {
				if r.Attempt != nil || r.Outcome != store.AdmissionTerminal {
					t.Fatal(r)
				}
			} else if r.Attempt == nil || r.Outcome != store.Admitted {
				t.Fatal(r)
			}
			saved, e := store.New(pool).ByID(ctx, v.OwnerID, v.ID)
			if e != nil || saved.State != domain.Cancelled {
				t.Fatal(saved, e)
			}
			// Saved grants remain historical; new commands can never resurrect cancelled.
			c.RequestID = "after-cancel"
			r, e = store.AdmitDelivery(ctx, pool, c)
			if e != nil || r.Attempt != nil || r.Outcome != store.AdmissionTerminal {
				t.Fatal(r, e)
			}
			assertQuota(t, pool, v.OwnerID, 0)
		})
	}
}

func TestAdmissionBindingBoundary(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	v, c := admissionFixture(t, pool, 991080)
	code, e := telegramcode.New(pool, nil).Issue(ctx, v.OwnerID)
	if e != nil {
		t.Fatal(e)
	}
	c.PermitTTL = time.Second
	newChat := time.Now().UnixNano()
	paused := pausedCancelDB{pool, make(chan struct{}), make(chan struct{})}
	defer releasePausedOnExit(paused)()
	linked := make(chan error, 1)
	go func() { linked <- telegramlink.New(paused, nil).Consume(ctx, code.Value, newChat) }()
	select {
	case <-paused.reached:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	type result struct {
		r store.AdmissionResult
		e error
	}
	ch := make(chan result, 1)
	go func() { r, e := store.AdmitDelivery(ctx, pool, c); ch <- result{r, e} }()
	waitAdmissionOwnerLock(t, pool, ctx)
	close(paused.release)
	if e := <-linked; e != nil {
		t.Fatal(e)
	}
	first := <-ch
	if first.e != nil || first.r.Attempt == nil || first.r.Attempt.ChatID != newChat || first.r.Attempt.BindingVersion != 2 {
		t.Fatal(first)
	}
	// Rebinding after admission preserves the old grant and does not permit
	// another active call. The next check after expiry captures the new channel.
	code, e = telegramcode.New(pool, nil).Issue(ctx, v.OwnerID)
	if e != nil {
		t.Fatal(e)
	}
	if e = telegramlink.New(pool, nil).Consume(ctx, code.Value, newChat+1); e != nil {
		t.Fatal(e)
	}
	replay, e := store.AdmitDelivery(ctx, pool, c)
	if e != nil || !reflect.DeepEqual(replay, first.r) {
		t.Fatal(replay, e)
	}
	c.RequestID = "after-rebind"
	busy, e := store.AdmitDelivery(ctx, pool, c)
	if e != nil || busy.Outcome != store.AttemptBusy || busy.Attempt != nil {
		t.Fatal(busy, e)
	}
	waitAdmissionExpiry(t, pool, first.r.Attempt.ExpiresAt)
	c.RequestID = "new-channel-after-expiry"
	fresh, e := store.AdmitDelivery(ctx, pool, c)
	if e != nil || fresh.Attempt == nil || fresh.Attempt.ChatID != newChat+1 || fresh.Attempt.BindingVersion != 3 || fresh.Attempt.Fence != 2 {
		t.Fatal(fresh, e)
	}
	assertQuota(t, pool, v.OwnerID, 1)
}

func TestAdmissionExpiryWaitAndMissingChannel(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	for i, missing := range []bool{true, false} {
		t.Run(fmt.Sprint(missing), func(t *testing.T) {
			v, c := admissionFixture(t, pool, 991090+i)
			c.PermitTTL = 20 * time.Millisecond
			first, e := store.AdmitDelivery(ctx, pool, c)
			if e != nil {
				t.Fatal(e)
			}
			waitAdmissionExpiry(t, pool, first.Attempt.ExpiresAt)
			if missing {
				if _, e = pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=NULL WHERE id=$1`, v.OwnerID); e != nil {
					t.Fatal(e)
				}
			} else {
				p := v.Threshold
				if e = p.UnmarshalText([]byte("99")); e != nil {
					t.Fatal(e)
				}
				c.Quote.Price = &p
			}
			c.RequestID = "recheck-after-expiry"
			next, e := store.AdmitDelivery(ctx, pool, c)
			if e != nil || next.Attempt != nil || next.Record.Version != 4 {
				t.Fatal(next, e)
			}
			if missing {
				if next.Outcome != store.AdmissionBlocked || next.Record.Blocked == nil {
					t.Fatal(next)
				}
			} else if next.Outcome != store.WaitCondition || next.Record.State != domain.Active {
				t.Fatal(next)
			}
			var current *string
			if e = pool.QueryRow(ctx, `SELECT current_attempt_id FROM app.subscriptions WHERE id=$1`, v.ID).Scan(&current); e != nil || current != nil {
				t.Fatal(current, e)
			}
			assertQuota(t, pool, v.OwnerID, 1)
		})
	}
}

// Preparation must release locks before network I/O, never persist a command
// without a price, and detect rebinding while the caller reads that price.
func TestAdmissionPreparationAndBindingCAS(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	v, c := admissionFixture(t, pool, 991100)
	before, e := journal.Current(ctx, pool)
	if e != nil {
		t.Fatal(e)
	}
	empty := c
	empty.Quote = priceobserved.Observed{}
	_, e = store.PrepareAdmission(ctx, pool, empty)
	var need *store.AdmissionQuoteRequired
	if !errors.As(e, &need) || need.Record.ID != v.ID || need.BindingVersion != 1 {
		t.Fatal(need, e)
	}
	var n int
	if e = pool.QueryRow(ctx, `SELECT count(*) FROM app.delivery_admissions WHERE subscription_id=$1`, v.ID).Scan(&n); e != nil || n != 0 {
		t.Fatal(n, e)
	}
	if pos, e := journal.Current(ctx, pool); e != nil || pos != before {
		t.Fatal(pos, e)
	}
	c.ExpectedBindingVersion = &need.BindingVersion
	if _, e = pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=456,telegram_version=2 WHERE id=$1`, v.OwnerID); e != nil {
		t.Fatal(e)
	}
	if _, e = store.AdmitDelivery(ctx, pool, c); !errors.Is(e, store.ErrAdmissionChanged) {
		t.Fatal(e)
	}
	_, e = store.PrepareAdmission(ctx, pool, empty)
	if !errors.As(e, &need) || need.BindingVersion != 2 {
		t.Fatal(need, e)
	}
	c.ExpectedBindingVersion = &need.BindingVersion
	first, e := store.AdmitDelivery(ctx, pool, c)
	if e != nil || first.Attempt == nil || first.Attempt.ChatID != 456 || first.Attempt.BindingVersion != 2 {
		t.Fatal(first, e)
	}
	replay, e := store.PrepareAdmission(ctx, pool, empty)
	if e != nil || !reflect.DeepEqual(first, replay) {
		t.Fatal(replay, e)
	}
	c.RequestID = "busy-without-price"
	c.Quote = priceobserved.Observed{}
	busy, e := store.PrepareAdmission(ctx, pool, c)
	if e != nil || busy.Outcome != store.AttemptBusy || busy.Attempt != nil {
		t.Fatal(busy, e)
	}
}
