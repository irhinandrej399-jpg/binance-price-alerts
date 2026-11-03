package jobs

import (
	"context"
	"errors"

	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/telegram"
	"github.com/jackc/pgx/v5"
)

// PermanentReason maps safe sender diagnostics to the existing API reasons.
// Exhaustion is supplied separately from retry.Policy's Exhausted plan. Unknown,
// temporary and local failures must never be classified as permanent here.
func PermanentReason(cause error) (domain.BlockReason, bool) {
	var e *telegram.Error
	if !errors.As(cause, &e) || e == nil {
		return "", false
	}
	if e.Kind == telegram.Forbidden || (e.Kind == telegram.Rejected && e.InvalidChat) {
		return domain.ChannelUnavailable, true
	}
	if e.Kind == telegram.Rejected {
		return domain.PermanentFailure, true
	}
	return "", false
}
func validBlock(reason domain.BlockReason, version int64) bool {
	return version >= 0 && (reason == domain.ChannelUnavailable || reason == domain.PermanentFailure || reason == domain.AttemptsExhausted)
}

// SaveBlocked atomically persists immutable evidence and the pending API report.
// version is the binding of this registered API attempt, never the job event's
// binding or a later channel lookup. Caller retains it from the permission.
// A historical refusal never changes a newer lease, cancellation or success.
// On a DB error replay the same arguments, never the HTTP request.
func (s *Store) SaveBlocked(ctx context.Context, l Lease, attempt string, reason domain.BlockReason, version int64) error {
	if !validAttempt(l, attempt) || !validBlock(reason, version) {
		return errors.New("invalid blocked outcome")
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
	e = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.delivery_attempts WHERE attempt_id=$1 AND claim_id=$2 AND trigger_id=$3 AND fence=$4)`, attempt, l.ID, l.TriggerID, l.Fence).Scan(&known)
	if e != nil {
		return e
	}
	if !known {
		return ErrStale
	}
	var previous string
	var binding int64
	e = tx.QueryRow(ctx, `SELECT reason,binding_version FROM app.delivery_blocks WHERE attempt_id=$1`, attempt).Scan(&previous, &binding)
	if e == nil {
		if previous != string(reason) || binding != version {
			return ErrConflict
		}
		return tx.Commit(ctx)
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return e
	}
	succeeded, e := hasSuccess(ctx, tx, l.TriggerID)
	if e != nil {
		return e
	}
	if succeeded {
		return tx.Commit(ctx)
	}
	if _, e = tx.Exec(ctx, `INSERT INTO app.delivery_blocks(attempt_id,reason,binding_version) VALUES($1,$2,$3)`, attempt, string(reason), version); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `INSERT INTO app.delivery_blocked_reports(attempt_id) VALUES($1)`, attempt); e != nil {
		return e
	}
	now, e := s.clock(ctx, tx)
	if e != nil {
		return e
	}
	if j.owns(l, now) {
		if e = finishBlocked(ctx, tx, l, reason, version); e != nil {
			return e
		}
	}
	return tx.Commit(ctx)
}

// ParkBlocked persists an already blocked API admission without a send attempt.
// API owns the canonical transition (including a missing binding); no outcome
// report is invented. Only a live claim with no registered attempt may park.
// Immutable claim receipts make lost-response retries safe after restart.
func (s *Store) ParkBlocked(ctx context.Context, l Lease, reason domain.BlockReason, version int64) error {
	if !validAttempt(l, "admission") || !validBlock(reason, version) {
		return errors.New("invalid blocked admission")
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
	var previous string
	var binding int64
	var fence int64
	e = tx.QueryRow(ctx, `SELECT p.reason,p.binding_version,c.fence FROM app.delivery_block_parks p JOIN app.delivery_claims c USING(claim_id) WHERE p.claim_id=$1 AND c.trigger_id=$2`, l.ID, l.TriggerID).Scan(&previous, &binding, &fence)
	if e == nil {
		if fence != l.Fence {
			return ErrStale
		}
		if previous != string(reason) || binding != version {
			return ErrConflict
		}
		return tx.Commit(ctx)
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return e
	}
	now, e := s.clock(ctx, tx)
	if e != nil {
		return e
	}
	if !j.owns(l, now) {
		return ErrStale
	}
	succeeded, e := hasSuccess(ctx, tx, l.TriggerID)
	if e != nil {
		return e
	}
	if succeeded {
		return ErrUnavailable
	}
	var attempted bool
	if e = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.delivery_attempts WHERE claim_id=$1)`, l.ID).Scan(&attempted); e != nil {
		return e
	}
	if attempted {
		return ErrConflict
	}
	if _, e = tx.Exec(ctx, `INSERT INTO app.delivery_block_parks(claim_id,reason,binding_version) VALUES($1,$2,$3)`, l.ID, string(reason), version); e != nil {
		return e
	}
	if e = finishBlocked(ctx, tx, l, reason, version); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func finishBlocked(ctx context.Context, tx pgx.Tx, l Lease, reason domain.BlockReason, version int64) error {
	if _, e := tx.Exec(ctx, `UPDATE app.delivery_jobs SET state='blocked',blocked_reason=$2,blocked_binding_version=$3,next_attempt_at='infinity',lease_id=NULL,lease_expires_at=NULL WHERE trigger_id=$1`, l.TriggerID, string(reason), version); e != nil {
		return e
	}
	_, e := tx.Exec(ctx, `UPDATE app.delivery_claims SET status='completed' WHERE claim_id=$1`, l.ID)
	return e
}
