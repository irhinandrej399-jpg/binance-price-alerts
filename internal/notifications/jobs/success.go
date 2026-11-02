package jobs

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

var ErrConflict = errors.New("conflicting delivery attempt or success")

type lockedJob struct {
	id      *string
	fence   int64
	expires *time.Time
	state   string
}

func lockJob(ctx context.Context, tx pgx.Tx, trigger string) (lockedJob, error) {
	var j lockedJob
	e := tx.QueryRow(ctx, `SELECT lease_id,lease_fence,lease_expires_at,state FROM app.delivery_jobs WHERE trigger_id=$1 FOR UPDATE`, trigger).Scan(&j.id, &j.fence, &j.expires, &j.state)
	if errors.Is(e, pgx.ErrNoRows) {
		e = ErrStale
	}
	return j, e
}
func (j lockedJob) owns(l Lease, now time.Time) bool {
	return j.id != nil && *j.id == l.ID && j.fence == l.Fence && j.expires != nil && now.Before(*j.expires) && j.state == "leased"
}
func validAttempt(l Lease, attempt string) bool {
	return strings.TrimSpace(l.TriggerID) != "" && strings.TrimSpace(l.ID) != "" && l.Fence > 0 && strings.TrimSpace(attempt) != ""
}
func hasSuccess(ctx context.Context, tx pgx.Tx, trigger string) (bool, error) {
	var yes bool
	e := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.delivery_attempts a JOIN app.delivery_successes s USING(attempt_id) WHERE a.trigger_id=$1)`, trigger).Scan(&yes)
	return yes, e
}

// RegisterAttempt durably correlates an API attempt ID before HTTP. Caller
// supplies an actual permission from API; this method neither obtains nor
// validates that remote permission. A live local lease is mandatory, including
// on repeats. There is at most one API attempt per local claim.
func (s *Store) RegisterAttempt(ctx context.Context, l Lease, attempt string) error {
	if !validAttempt(l, attempt) {
		return errors.New("invalid attempt registration")
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
	now, e := s.clock(ctx, tx)
	if e != nil {
		return e
	}
	if !j.owns(l, now) {
		return ErrStale
	}
	success, e := hasSuccess(ctx, tx, l.TriggerID)
	if e != nil {
		return e
	}
	if success {
		return ErrUnavailable
	}
	var previous string
	e = tx.QueryRow(ctx, `SELECT attempt_id FROM app.delivery_attempts WHERE claim_id=$1`, l.ID).Scan(&previous)
	if e == nil {
		if previous != attempt {
			return ErrConflict
		}
		return tx.Commit(ctx)
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return e
	}
	tag, e := tx.Exec(ctx, `INSERT INTO app.delivery_attempts(attempt_id,claim_id,trigger_id,fence) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, attempt, l.ID, l.TriggerID, l.Fence)
	if e != nil {
		return e
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	return tx.Commit(ctx)
}

// SaveSuccess atomically records immutable evidence and a pending API report.
// Known historical attempts are accepted after expiry/takeover. Only a live
// matching lease may close current work. A repeated matching receipt is a no-op;
// conflicting message IDs are rejected. On any DB error retry this same call,
// never Telegram: commit may have succeeded despite a lost response.
func (s *Store) SaveSuccess(ctx context.Context, l Lease, attempt string, messageID int64) error {
	if !validAttempt(l, attempt) || messageID <= 0 {
		return errors.New("invalid delivery success")
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
	var previous int64
	e = tx.QueryRow(ctx, `SELECT message_id FROM app.delivery_successes WHERE attempt_id=$1`, attempt).Scan(&previous)
	if e == nil {
		if previous != messageID {
			return ErrConflict
		}
		return tx.Commit(ctx)
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return e
	}
	if _, e = tx.Exec(ctx, `INSERT INTO app.delivery_successes(attempt_id,message_id) VALUES($1,$2)`, attempt, messageID); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `INSERT INTO app.delivery_success_reports(attempt_id) VALUES($1)`, attempt); e != nil {
		return e
	}
	now, e := s.clock(ctx, tx)
	if e != nil {
		return e
	}
	if j.owns(l, now) {
		if _, e = tx.Exec(ctx, `UPDATE app.delivery_jobs SET state='succeeded',lease_id=NULL,lease_expires_at=NULL WHERE trigger_id=$1`, l.TriggerID); e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, `UPDATE app.delivery_claims SET status='completed' WHERE claim_id=$1`, l.ID); e != nil {
			return e
		}
	}
	return tx.Commit(ctx)
}
