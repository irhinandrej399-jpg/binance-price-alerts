package jobs

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// UnresolvedAttempt means an expired claim registered an API permission but
// never persisted its outcome. Recover with SaveUncertain, never replay HTTP.
// Lease is historical correlation, not renewed permission to send.
type UnresolvedAttempt struct {
	Lease     Lease
	AttemptID string
}

func (*UnresolvedAttempt) Error() string {
	return "delivery attempt requires uncertain outcome recovery"
}

// SaveUncertain records possible delivery and a pending API report atomically.
// next is a future condition-check deadline, normally Policy.WaitCondition's
// deadline. It grants no send permission and does not consume a proven temporary
// Telegram failure. The first committed schedule wins, including after restart.
// Historical evidence is accepted, but only a live matching lease can be parked.
// A confirmed success is never overwritten or downgraded by this operation.
func (s *Store) SaveUncertain(ctx context.Context, l Lease, attempt string, next time.Time) error {
	if !validAttempt(l, attempt) || next.IsZero() {
		return errors.New("invalid uncertain outcome")
	}
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(context.Background())
	j, e := lockJob(ctx, tx, l.TriggerID)
	if e != nil {
		return e
	}
	var known bool
	e = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.delivery_attempts WHERE attempt_id=$1 AND trigger_id=$2 AND claim_id=$3 AND fence=$4)`, attempt, l.TriggerID, l.ID, l.Fence).Scan(&known)
	if e != nil {
		return e
	}
	if !known {
		return ErrStale
	}
	var done bool
	e = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.delivery_uncertainties WHERE attempt_id=$1) OR EXISTS(SELECT 1 FROM app.delivery_successes WHERE attempt_id=$1)`, attempt).Scan(&done)
	if e != nil {
		return e
	}
	if done {
		return tx.Commit(ctx)
	}
	now, e := s.clock(ctx, tx)
	if e != nil {
		return e
	}
	if !next.After(now) {
		return errors.New("uncertain check deadline must be in the future")
	}
	if next.Nanosecond()%1000 != 0 {
		next = next.Truncate(time.Microsecond).Add(time.Microsecond)
	}
	if _, e = tx.Exec(ctx, `INSERT INTO app.delivery_uncertainties(attempt_id,next_attempt_at) VALUES($1,$2)`, attempt, next); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `INSERT INTO app.delivery_uncertain_reports(attempt_id) VALUES($1)`, attempt); e != nil {
		return e
	}
	succeeded, e := hasSuccess(ctx, tx, l.TriggerID)
	if e != nil {
		return e
	}
	if j.owns(l, now) && !succeeded {
		if _, e = tx.Exec(ctx, `UPDATE app.delivery_jobs SET state='uncertain',next_attempt_at=$2,lease_id=NULL,lease_expires_at=NULL WHERE trigger_id=$1`, l.TriggerID, next); e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, `UPDATE app.delivery_claims SET status='completed' WHERE claim_id=$1`, l.ID); e != nil {
			return e
		}
	}
	return tx.Commit(ctx)
}

// UncertainAttempts is a durable, deduplicated metric of possible deliveries,
// not a count of actual Telegram duplicates. Evidence survives success and report
// acknowledgement. Export as a database-wide gauge (do not sum replica copies).
// Retention/export and runtime wiring belong to OPS-003.
func (s *Store) UncertainAttempts(ctx context.Context) (int64, error) {
	var n int64
	e := s.pool.QueryRow(ctx, `SELECT count(*) FROM app.delivery_uncertainties`).Scan(&n)
	return n, e
}

// checkPreviousAttempt runs with the job locked. A crashed registered attempt
// must acquire evidence/report before takeover. An unregistered claim never
// implies that HTTP happened. Respect the recovered deadline without touching
// the expired claim or granting the old worker renewed ownership.
func checkPreviousAttempt(ctx context.Context, tx pgx.Tx, l Lease, oldID *string, now time.Time) error {
	if oldID == nil {
		return nil
	}
	var attempt string
	var next *time.Time
	e := tx.QueryRow(ctx, `SELECT a.attempt_id,u.next_attempt_at FROM app.delivery_attempts a LEFT JOIN app.delivery_uncertainties u USING(attempt_id) WHERE a.claim_id=$1`, *oldID).Scan(&attempt, &next)
	if errors.Is(e, pgx.ErrNoRows) {
		return nil
	}
	if e != nil {
		return e
	}
	if next == nil {
		l.ID = *oldID
		return &UnresolvedAttempt{Lease: l, AttemptID: attempt}
	}
	if now.Before(*next) {
		return ErrUnavailable
	}
	return nil
}
