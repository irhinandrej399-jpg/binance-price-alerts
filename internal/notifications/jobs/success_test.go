package jobs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSuccessSurvivesRestartAndLostResponse(t *testing.T) {
	ctx, p, a, b, _, id := fixture(t)
	l, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.RegisterAttempt(ctx, l, "api-"+id); e != nil {
		t.Fatal(e)
	}
	// The caller discards the committed response, then recovers on another pool.
	if e = a.SaveSuccess(ctx, l, "api-"+id, 123); e != nil {
		t.Fatal(e)
	}
	b, e = New(b.pool, time.Second) // new process state, independent SQL pool
	if e != nil {
		t.Fatal(e)
	}
	if e = b.SaveSuccess(ctx, l, "api-"+id, 123); e != nil {
		t.Fatal(e)
	}
	if e = b.SaveSuccess(ctx, l, "api-"+id, 456); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	if _, e = b.Claim(ctx, id); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	var state, status string
	if e = p.QueryRow(ctx, `SELECT j.state,c.status FROM app.delivery_jobs j JOIN app.delivery_claims c ON c.trigger_id=j.trigger_id WHERE j.trigger_id=$1`, id).Scan(&state, &status); e != nil || state != "succeeded" || status != "completed" {
		t.Fatal(state, status, e)
	}
	assertSuccess(t, p, id, 1)
}

func TestLateSuccessPreservesNewLease(t *testing.T) {
	ctx, p, a, b, clock, id := fixture(t)
	old, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.RegisterAttempt(ctx, old, "api-"+id); e != nil {
		t.Fatal(e)
	}
	clock.Add(int64(time.Second / time.Microsecond))
	fresh, e := b.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.SaveSuccess(ctx, old, "api-"+id, 789); e != nil {
		t.Fatal(e)
	}
	var active bool
	if e = p.QueryRow(ctx, `SELECT lease_id=$2 AND lease_fence=$3 AND lease_expires_at=$4 AND state='leased' FROM app.delivery_jobs WHERE trigger_id=$1`, id, fresh.ID, fresh.Fence, fresh.ExpiresAt).Scan(&active); e != nil || !active {
		t.Fatal(active, e)
	}
	if e = b.RegisterAttempt(ctx, fresh, "another-"+id); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	if e = b.Park(ctx, fresh, WaitLimiter, fresh.ExpiresAt); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	clock.Add(int64(time.Second / time.Microsecond))
	if _, e = b.Claim(ctx, id); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	assertSuccess(t, p, id, 1)
}

func TestExpiredSuccessWithoutTakeover(t *testing.T) {
	ctx, p, a, _, clock, id := fixture(t)
	l, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.RegisterAttempt(ctx, l, "api-"+id); e != nil {
		t.Fatal(e)
	}
	clock.Add(int64(time.Second / time.Microsecond))
	if e = a.SaveSuccess(ctx, l, "api-"+id, 1); e != nil {
		t.Fatal(e)
	}
	var active bool
	if e = p.QueryRow(ctx, `SELECT lease_id=$2 AND state='leased' FROM app.delivery_jobs WHERE trigger_id=$1`, id, l.ID).Scan(&active); e != nil || !active {
		t.Fatal(active, e)
	}
	assertSuccess(t, p, id, 1)
}

func TestKnownAttemptValidation(t *testing.T) {
	ctx, _, a, b, clock, id := fixture(t)
	l, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.SaveSuccess(ctx, l, "unknown", 1); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	if e = a.RegisterAttempt(ctx, l, "api-"+id); e != nil {
		t.Fatal(e)
	}
	if e = b.RegisterAttempt(ctx, l, "api-"+id); e != nil {
		t.Fatal(e)
	}
	if e = a.RegisterAttempt(ctx, l, "different"); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	forged := l
	forged.Fence++
	if e = a.SaveSuccess(ctx, forged, "api-"+id, 1); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	if e = a.SaveSuccess(ctx, l, "api-"+id, 0); e == nil {
		t.Fatal("zero message accepted")
	}
	if e = a.Park(ctx, l, WaitCondition, l.ExpiresAt); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	clock.Add(int64(time.Second / time.Microsecond))
	if e = a.RegisterAttempt(ctx, l, "api-"+id); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
}

func TestConcurrentSuccesses(t *testing.T) {
	ctx, p, a, b, clock, id := fixture(t)
	old, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.RegisterAttempt(ctx, old, "old-"+id); e != nil {
		t.Fatal(e)
	}
	clock.Add(int64(time.Second / time.Microsecond))
	fresh, e := b.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = b.RegisterAttempt(ctx, fresh, "new-"+id); e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			l, attempt, s := old, "old-"+id, a
			if i%2 == 0 {
				l, attempt, s = fresh, "new-"+id, b
			}
			if e := s.SaveSuccess(ctx, l, attempt, 11); e != nil {
				t.Error(e)
			}
		})
	}
	wg.Wait()
	assertSuccess(t, p, id, 2)
}

func TestSuccessRollback(t *testing.T) {
	ctx, p, a, b, _, id := fixture(t)
	l, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.RegisterAttempt(ctx, l, "api-"+id); e != nil {
		t.Fatal(e)
	}
	if e = a.SaveSuccess(ctx, l, "api-"+id, 123); e == nil {
		t.Fatal("commit failure accepted")
	}
	assertSuccess(t, p, id, 0)
	if e = b.RegisterAttempt(ctx, l, "api-"+id); e != nil {
		t.Fatal(e)
	}
	var active bool
	if e = p.QueryRow(ctx, `SELECT j.state='leased' AND j.lease_id=$2 AND c.status='claimed' FROM app.delivery_jobs j JOIN app.delivery_claims c ON c.claim_id=j.lease_id WHERE j.trigger_id=$1`, id, l.ID).Scan(&active); e != nil || !active {
		t.Fatal(active, e)
	}
}

func assertSuccess(t *testing.T, p *pgxpool.Pool, id string, want int) {
	t.Helper()
	var receipts, reports int
	e := p.QueryRow(context.Background(), `SELECT
 (SELECT count(*) FROM app.delivery_successes s JOIN app.delivery_attempts a USING(attempt_id) WHERE a.trigger_id=$1),
 (SELECT count(*) FROM app.delivery_success_reports r JOIN app.delivery_attempts a USING(attempt_id) WHERE a.trigger_id=$1 AND r.pending)`, id).Scan(&receipts, &reports)
	if e != nil || receipts != want || reports != want {
		t.Fatal(receipts, reports, e)
	}
}

func TestSuccessKeepsReceiptAndAcknowledgedReport(t *testing.T) {
	ctx, p, a, b, _, id := fixture(t)
	l, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.RegisterAttempt(ctx, l, "api-"+id); e != nil {
		t.Fatal(e)
	}
	if e = a.SaveSuccess(ctx, l, "api-"+id, 9223372036854775807); e != nil {
		t.Fatal(e)
	}
	// Model the later reporter acknowledgement; replay must never reset it.
	if _, e = p.Exec(ctx, `UPDATE app.delivery_success_reports SET pending=false WHERE attempt_id=$1`, "api-"+id); e != nil {
		t.Fatal(e)
	}
	if e = b.SaveSuccess(ctx, l, "api-"+id, 9223372036854775807); e != nil {
		t.Fatal(e)
	}
	var message int64
	var pending bool
	if e = p.QueryRow(ctx, `SELECT s.message_id,r.pending FROM app.delivery_successes s JOIN app.delivery_success_reports r USING(attempt_id) WHERE s.attempt_id=$1`, "api-"+id).Scan(&message, &pending); e != nil || message != 9223372036854775807 || pending {
		t.Fatal(message, pending, e)
	}
}

func TestAttemptCannotMoveBetweenWorkflows(t *testing.T) {
	ctx, p, a, _, _, id := fixture(t)
	first, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.RegisterAttempt(ctx, first, "shared-"+id); e != nil {
		t.Fatal(e)
	}
	if _, e = p.Exec(ctx, `INSERT INTO app.delivery_jobs(trigger_id,subscription_id,user_id,event_id,body) VALUES($1,'sub','user','event',$2)`, id+"-other", []byte("request")); e != nil {
		t.Fatal(e)
	}
	second, e := a.Claim(ctx, id+"-other")
	if e != nil {
		t.Fatal(e)
	}
	if e = a.RegisterAttempt(ctx, second, "shared-"+id); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	if e = a.SaveSuccess(ctx, second, "shared-"+id, 1); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	if e = a.RegisterAttempt(ctx, second, "unique-"+id); e != nil {
		t.Fatal(e)
	}
}

func TestCancelledJobKeepsLateEvidence(t *testing.T) {
	ctx, p, a, _, _, id := fixture(t)
	l, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.RegisterAttempt(ctx, l, "api-"+id); e != nil {
		t.Fatal(e)
	}
	if _, e = p.Exec(ctx, `UPDATE app.delivery_jobs SET state='cancelled',lease_id=NULL,lease_expires_at=NULL WHERE trigger_id=$1`, id); e != nil {
		t.Fatal(e)
	}
	if e = a.SaveSuccess(ctx, l, "api-"+id, 3); e != nil {
		t.Fatal(e)
	}
	var state string
	if e = p.QueryRow(ctx, `SELECT state FROM app.delivery_jobs WHERE trigger_id=$1`, id).Scan(&state); e != nil || state != "cancelled" {
		t.Fatal(state, e)
	}
	assertSuccess(t, p, id, 1)
}
