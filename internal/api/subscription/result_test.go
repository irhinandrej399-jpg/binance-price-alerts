package subscription_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/auth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/session"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/telegramcode"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/telegramlink"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/journal"
	store "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
)

func TestDeliveryResultSuccessReplay(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	v, c := admissionFixture(t, pool, 992001)
	admitted, e := store.AdmitDelivery(ctx, pool, c)
	if e != nil {
		t.Fatal(e)
	}
	cmd := store.DeliveryResultCommand{OwnerID: v.OwnerID, SubscriptionID: v.ID, WorkflowID: c.WorkflowID, AttemptID: admitted.Attempt.ID, Outcome: store.DeliverySucceeded, MessageID: 123}
	before, e := journal.Current(ctx, pool)
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 3; i++ {
		r, e := store.SaveDeliveryResult(ctx, pool, cmd)
		if e != nil || r.State != domain.Delivered || r.Version != admitted.Record.Version+1 {
			t.Fatal(r, e)
		}
	}
	assertQuota(t, pool, v.OwnerID, 0)
	entries, e := journal.ReadAfter(ctx, pool, before, 10)
	if e != nil || len(entries) != 1 || entries[0].Change.SchemaVersion != 2 || entries[0].Change.OwnerID != v.OwnerID || entries[0].Change.State != domain.Delivered {
		t.Fatal(entries, e)
	}
	var message int64
	if e = pool.QueryRow(ctx, `SELECT delivered_message_id FROM app.subscriptions WHERE id=$1`, v.ID).Scan(&message); e != nil || message != 123 {
		t.Fatal(message, e)
	}
	var n int
	if e = pool.QueryRow(ctx, `SELECT count(*) FROM app.delivery_results WHERE subscription_id=$1`, v.ID).Scan(&n); e != nil || n != 1 {
		t.Fatal(n, e)
	}
}

func resultCommand(v store.Record, a store.AdmissionResult, outcome store.DeliveryOutcome) store.DeliveryResultCommand {
	c := store.DeliveryResultCommand{OwnerID: v.OwnerID, SubscriptionID: v.ID, WorkflowID: v.Workflow.TriggerID, AttemptID: a.Attempt.ID, Outcome: outcome}
	if outcome == store.DeliverySucceeded {
		c.MessageID = 123
	}
	if outcome == store.DeliveryFailed {
		c.Reason = domain.ChannelUnavailable
	}
	return c
}

func TestDeliveryResultNegativeAndLateSuccess(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	for i, outcome := range []store.DeliveryOutcome{store.DeliveryRetryable, store.DeliveryUncertain, store.DeliveryFailed} {
		t.Run(string(outcome), func(t *testing.T) {
			v, c := admissionFixture(t, pool, 992010+i)
			a, e := store.AdmitDelivery(ctx, pool, c)
			if e != nil {
				t.Fatal(e)
			}
			cmd := resultCommand(v, a, outcome)
			r, e := store.SaveDeliveryResult(ctx, pool, cmd)
			if e != nil {
				t.Fatal(e)
			}
			want := domain.PendingDelivery
			if outcome == store.DeliveryFailed {
				want = domain.DeliveryBlocked
			}
			if r.State != want || r.Version != a.Record.Version+1 || r.Workflow.TriggerID != v.Workflow.TriggerID || r.Threshold.Cmp(v.Threshold) != 0 {
				t.Fatal(r)
			}
			if outcome == store.DeliveryFailed && (r.Blocked == nil || r.Blocked.BindingVersion != 1 || r.Blocked.Reason != domain.ChannelUnavailable) {
				t.Fatal(r)
			}
			assertQuota(t, pool, v.OwnerID, 1)
			var current *string
			if e = pool.QueryRow(ctx, `SELECT current_attempt_id FROM app.subscriptions WHERE id=$1`, v.ID).Scan(&current); e != nil || current != nil {
				t.Fatal(current, e)
			}
			// A repeated negative cannot clear a newer admission or alter its version.
			if outcome == store.DeliveryFailed {
				if _, e = pool.Exec(ctx, `UPDATE app.users SET telegram_version=2 WHERE id=$1`, v.OwnerID); e != nil {
					t.Fatal(e)
				}
			}
			c.RequestID = "next"
			b, e := store.AdmitDelivery(ctx, pool, c)
			if e != nil || b.Attempt == nil {
				t.Fatal(b, e)
			}
			r, e = store.SaveDeliveryResult(ctx, pool, cmd)
			if e != nil || r.Version != b.Record.Version || r.State != domain.Sending {
				t.Fatal(r, e)
			}
			// Late success for the previous attempt wins even over an active newer one.
			success := resultCommand(v, a, store.DeliverySucceeded)
			r, e = store.SaveDeliveryResult(ctx, pool, success)
			if e != nil || r.State != domain.Delivered {
				t.Fatal(r, e)
			}
			version := r.Version
			// A second successful external call is audited but preserves the first receipt.
			second := resultCommand(v, b, store.DeliverySucceeded)
			second.MessageID = 456
			r, e = store.SaveDeliveryResult(ctx, pool, second)
			if e != nil || r.Version != version {
				t.Fatal(r, e)
			}
			negative := resultCommand(v, b, store.DeliveryFailed)
			r, e = store.SaveDeliveryResult(ctx, pool, negative)
			if e != nil || r.State != domain.Delivered || r.Version != version {
				t.Fatal(r, e)
			}
			var id string
			var message int64
			if e = pool.QueryRow(ctx, `SELECT delivered_attempt_id,delivered_message_id FROM app.subscriptions WHERE id=$1`, v.ID).Scan(&id, &message); e != nil || id != a.Attempt.ID || message != 123 {
				t.Fatal(id, message, e)
			}
			assertQuota(t, pool, v.OwnerID, 0)
		})
	}
}

func TestDeliveryResultExpiredAttemptAndStaleFailure(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	for i, newAttempt := range []bool{false, true} {
		t.Run(fmt.Sprint(newAttempt), func(t *testing.T) {
			v, c := admissionFixture(t, pool, 992020+i)
			c.PermitTTL = 20 * time.Millisecond
			a, e := store.AdmitDelivery(ctx, pool, c)
			if e != nil {
				t.Fatal(e)
			}
			waitAdmissionExpiry(t, pool, a.Attempt.ExpiresAt)
			if newAttempt {
				c.RequestID = "new"
				c.PermitTTL = time.Minute
				b, e := store.AdmitDelivery(ctx, pool, c)
				if e != nil || b.Attempt == nil {
					t.Fatal(b, e)
				}
				// Rebind + old failure after new admission must not block the new work.
				before, e := journal.Current(ctx, pool)
				if e != nil {
					t.Fatal(e)
				}
				r, e := store.SaveDeliveryResult(ctx, pool, resultCommand(v, a, store.DeliveryFailed))
				if e != nil || r.State != domain.Sending || r.Version != b.Record.Version {
					t.Fatal(r, e)
				}
				var current string
				if e = pool.QueryRow(ctx, `SELECT current_attempt_id FROM app.subscriptions WHERE id=$1`, v.ID).Scan(&current); e != nil || current != b.Attempt.ID {
					t.Fatal(current, e)
				}
				if pos, e := journal.Current(ctx, pool); e != nil || pos != before {
					t.Fatal(pos, e)
				}
			}
			r, e := store.SaveDeliveryResult(ctx, pool, resultCommand(v, a, store.DeliverySucceeded))
			if e != nil || r.State != domain.Delivered {
				t.Fatal(r, e)
			}
			assertQuota(t, pool, v.OwnerID, 0)
		})
	}
}

func TestDeliveryResultBindingBeforeBlocked(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	for i, reason := range []domain.BlockReason{domain.ChannelUnavailable, domain.PermanentFailure, domain.AttemptsExhausted} {
		t.Run(string(reason), func(t *testing.T) {
			v, c := admissionFixture(t, pool, 992030+i)
			a, e := store.AdmitDelivery(ctx, pool, c)
			if e != nil {
				t.Fatal(e)
			}
			code, e := telegramcode.New(pool, nil).Issue(ctx, v.OwnerID)
			if e != nil {
				t.Fatal(e)
			}
			if e = telegramlink.New(pool, nil).Consume(ctx, code.Value, time.Now().UnixNano()); e != nil {
				t.Fatal(e)
			}
			cmd := resultCommand(v, a, store.DeliveryFailed)
			cmd.Reason = reason
			r, e := store.SaveDeliveryResult(ctx, pool, cmd)
			if e != nil || r.Blocked == nil || r.Blocked.BindingVersion != 1 || r.Blocked.Reason != reason {
				t.Fatal(r, e)
			}
			var binding int64
			if e = pool.QueryRow(ctx, `SELECT binding_version FROM app.delivery_results WHERE attempt_id=$1`, a.Attempt.ID).Scan(&binding); e != nil || binding != 1 {
				t.Fatal(binding, e)
			}
			c.RequestID = "recover"
			b, e := store.AdmitDelivery(ctx, pool, c)
			if e != nil {
				t.Fatal(e)
			}
			if reason == domain.ChannelUnavailable {
				if b.Attempt == nil || b.Attempt.BindingVersion != 2 {
					t.Fatal(b)
				}
			} else if b.Outcome != store.AdmissionBlocked || b.Attempt != nil {
				t.Fatal(b)
			}
			r, e = store.SaveDeliveryResult(ctx, pool, cmd)
			if e != nil || r.Version != b.Record.Version {
				t.Fatal(r, e)
			}
			assertQuota(t, pool, v.OwnerID, 1)
		})
	}
}

func TestDeliveryResultCancellationBoundary(t *testing.T) {
	pool := postgrestest.Open(t)
	for i, cancelFirst := range []bool{true, false} {
		t.Run(fmt.Sprint(cancelFirst), func(t *testing.T) {
			ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			v, c := admissionFixture(t, pool, 992040+i)
			a, e := store.AdmitDelivery(ctx, pool, c)
			if e != nil {
				t.Fatal(e)
			}
			cmd := resultCommand(v, a, store.DeliverySucceeded)
			sessions := session.New(pool, nil)
			cred, e := sessions.Issue(ctx, v.OwnerID)
			if e != nil {
				t.Fatal(e)
			}
			paused := pausedCancelDB{pool, make(chan struct{}), make(chan struct{})}
			defer releasePausedOnExit(paused)()
			cancelled := make(chan int, 1)
			results := make(chan store.Record, 1)
			errs := make(chan error, 1)
			cancel := func(db store.Beginner) {
				cancelled <- deleteSubscription(auth.New(sessions, store.NewCancelHandler(db)), cred.AccessToken, v.ID).Code
			}
			save := func(db store.Beginner) { r, e := store.SaveDeliveryResult(ctx, db, cmd); results <- r; errs <- e }
			if cancelFirst {
				go cancel(paused)
			} else {
				go save(paused)
			}
			select {
			case <-paused.reached:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if cancelFirst {
				go save(pool)
			} else {
				go cancel(pool)
			}
			waitAdmissionOwnerLock(t, pool, ctx)
			close(paused.release)
			if code := <-cancelled; code != 200 {
				t.Fatal(code)
			}
			r := <-results
			if e := <-errs; e != nil {
				t.Fatal(e)
			}
			want := domain.Delivered
			if cancelFirst {
				want = domain.Cancelled
			}
			if r.State != want {
				t.Fatal(r)
			}
			after, e := store.New(pool).ByID(ctx, v.OwnerID, v.ID)
			if e != nil || after.State != want {
				t.Fatal(after, e)
			}
			var count int
			if e = pool.QueryRow(ctx, `SELECT count(*) FROM app.delivery_results WHERE attempt_id=$1 AND message_id=123`, a.Attempt.ID).Scan(&count); e != nil || count != 1 {
				t.Fatal(count, e)
			}
			before, e := journal.Current(ctx, pool)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = store.SaveDeliveryResult(ctx, pool, cmd); e != nil {
				t.Fatal(e)
			}
			if pos, e := journal.Current(ctx, pool); e != nil || pos != before {
				t.Fatal(pos, e)
			}
			assertQuota(t, pool, v.OwnerID, 0)
		})
	}
}

func TestDeliveryResultConcurrent(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	v, c := admissionFixture(t, pool, 992050)
	a, e := store.AdmitDelivery(ctx, pool, c)
	if e != nil {
		t.Fatal(e)
	}
	before, e := journal.Current(ctx, pool)
	if e != nil {
		t.Fatal(e)
	}
	start := make(chan struct{})
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		go func(i int) {
			<-start
			outcome := store.DeliverySucceeded
			if i%2 == 1 {
				outcome = store.DeliveryUncertain
			}
			_, e := store.SaveDeliveryResult(ctx, pool, resultCommand(v, a, outcome))
			errs <- e
		}(i)
	}
	close(start)
	for i := 0; i < 20; i++ {
		if e := <-errs; e != nil {
			t.Fatal(e)
		}
	}
	after, e := store.New(pool).ByID(ctx, v.OwnerID, v.ID)
	if e != nil || after.State != domain.Delivered {
		t.Fatal(after, e)
	}
	entries, e := journal.ReadAfter(ctx, pool, before, 10)
	if e != nil || len(entries) < 1 || len(entries) > 2 || entries[len(entries)-1].Change.State != domain.Delivered {
		t.Fatal(entries, e)
	}
	assertQuota(t, pool, v.OwnerID, 0)
}

type failedResultDB struct {
	*pgxpool.Pool
	at string
}
type failedResultTx struct {
	pgx.Tx
	at string
}

func (db failedResultDB) BeginTx(ctx context.Context, o pgx.TxOptions) (pgx.Tx, error) {
	tx, e := db.Pool.BeginTx(ctx, o)
	if e != nil {
		return nil, e
	}
	return failedResultTx{tx, db.at}, nil
}
func (tx failedResultTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if strings.Contains(sql, tx.at) {
		return tx.Tx.Exec(ctx, `SELECT 1/0`)
	}
	return tx.Tx.Exec(ctx, sql, args...)
}
func TestDeliveryResultRollbackAndLostReply(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	for i, at := range []string{"INSERT INTO app.delivery_results", "INSERT INTO app.api_journal (", "commit"} {
		t.Run(at, func(t *testing.T) {
			v, c := admissionFixture(t, pool, 992060+i)
			a, e := store.AdmitDelivery(ctx, pool, c)
			if e != nil {
				t.Fatal(e)
			}
			cmd := resultCommand(v, a, store.DeliverySucceeded)
			before, e := journal.Current(ctx, pool)
			if e != nil {
				t.Fatal(e)
			}
			var db store.Beginner = failedResultDB{pool, at}
			if at == "commit" {
				db = failedCommitDB{pool}
			}
			if _, e = store.SaveDeliveryResult(ctx, db, cmd); e == nil {
				t.Fatal("expected SQL/commit failure")
			}
			r, e := store.New(pool).ByID(ctx, v.OwnerID, v.ID)
			if e != nil || !reflect.DeepEqual(r, a.Record) {
				t.Fatal(r, e)
			}
			var n int
			var current string
			var receipt *int64
			if e = pool.QueryRow(ctx, `SELECT current_attempt_id,delivered_message_id FROM app.subscriptions WHERE id=$1`, v.ID).Scan(&current, &receipt); e != nil || current != a.Attempt.ID || receipt != nil {
				t.Fatal(current, receipt, e)
			}
			if e = pool.QueryRow(ctx, `SELECT count(*) FROM app.delivery_results WHERE subscription_id=$1`, v.ID).Scan(&n); e != nil || n != 0 {
				t.Fatal(n, e)
			}
			if pos, e := journal.Current(ctx, pool); e != nil || pos != before {
				t.Fatal(pos, e)
			}
			assertQuota(t, pool, v.OwnerID, 1)
			// Ambiguous commit: retrying only storage restores the one saved result.
			if _, e = store.SaveDeliveryResult(ctx, lostDeliveryCommitDB{pool}, cmd); !errors.Is(e, store.ErrStorage) {
				t.Fatal(e)
			}
			r, e = store.SaveDeliveryResult(ctx, pool, cmd)
			if e != nil || r.State != domain.Delivered {
				t.Fatal(r, e)
			}
			if pos, e := journal.Current(ctx, pool); e != nil || pos != before+1 {
				t.Fatal(pos, e)
			}
			assertQuota(t, pool, v.OwnerID, 0)
		})
	}
}

func TestDeliveryResultRejects(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	v, c := admissionFixture(t, pool, 992070)
	a, e := store.AdmitDelivery(ctx, pool, c)
	if e != nil {
		t.Fatal(e)
	}
	good := resultCommand(v, a, store.DeliverySucceeded)
	for _, name := range []string{"unknown", "workflow", "owner", "message", "negative_message", "reason", "outcome", "empty", "overflow"} {
		t.Run(name, func(t *testing.T) {
			cmd := good
			want := store.ErrInvalidInput
			switch name {
			case "unknown":
				cmd.AttemptID = "not-admitted"
				want = store.ErrAttemptUnknown
			case "workflow":
				cmd.WorkflowID = "foreign"
				want = store.ErrAttemptUnknown
			case "owner":
				cmd.OwnerID = quotaOwner(t, pool)
				want = store.ErrNotFound
			case "message":
				cmd.MessageID = 0
			case "negative_message":
				cmd.Outcome = store.DeliveryUncertain
			case "reason":
				cmd.Outcome = store.DeliveryFailed
				cmd.MessageID = 0
			case "outcome":
				cmd.Outcome = "nonsense"
			case "empty":
				cmd.AttemptID = " "
			case "overflow":
				if _, e = pool.Exec(ctx, `UPDATE app.subscriptions SET version=9223372036854775807 WHERE id=$1`, v.ID); e != nil {
					t.Fatal(e)
				}
				defer func() {
					if _, e := pool.Exec(ctx, `UPDATE app.subscriptions SET version=3 WHERE id=$1`, v.ID); e != nil {
						t.Fatal(e)
					}
				}()
			}
			before, e := journal.Current(ctx, pool)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = store.SaveDeliveryResult(ctx, pool, cmd); !errors.Is(e, want) {
				t.Fatal(e, want)
			}
			if pos, e := journal.Current(ctx, pool); e != nil || pos != before {
				t.Fatal(pos, e)
			}
			var n int
			if e = pool.QueryRow(ctx, `SELECT count(*) FROM app.delivery_results WHERE subscription_id=$1`, v.ID).Scan(&n); e != nil || n != 0 {
				t.Fatal(n, e)
			}
			assertQuota(t, pool, v.OwnerID, 1)
		})
	}
	if _, e = store.SaveDeliveryResult(ctx, pool, good); e != nil {
		t.Fatal(e)
	}
	good.MessageID = 999
	if _, e = store.SaveDeliveryResult(ctx, pool, good); !errors.Is(e, store.ErrResultConflict) {
		t.Fatal(e)
	}
	for _, sql := range []string{`UPDATE app.delivery_results SET message_id=999 WHERE subscription_id=$1`, `DELETE FROM app.delivery_results WHERE subscription_id=$1`} {
		if _, e = pool.Exec(ctx, sql, v.ID); e == nil {
			t.Fatal("audit must be immutable")
		}
	}
}
