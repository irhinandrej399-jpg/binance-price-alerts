package jobs

import (
	"context"
	"errors"
	"math"
	"time"

	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/retry"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/telegram"
	"github.com/jackc/pgx/v5"
)

// failureCount runs under the job lock. Evidence, rather than process memory or
// a mutable intake field, owns the budget across waits, unknowns and restarts.
func failureCount(ctx context.Context, tx pgx.Tx, trigger string) (uint32, error) {
	var n int64
	e := tx.QueryRow(ctx, `SELECT count(*) FROM app.delivery_retryables r JOIN app.delivery_attempts a USING(attempt_id) WHERE a.trigger_id=$1`, trigger).Scan(&n)
	if e != nil {
		return 0, e
	}
	if n >= math.MaxUint32 {
		return 0, errors.New("temporary failure budget overflow")
	}
	return uint32(n), nil
}

// SaveRetryable records a proven 429/5xx and calculates retry.Policy's plan with
// the SQL clock and durable error count while holding the job lock. Only safe
// typed cause fields are stored. entropy is used once; replay returns the first
// committed plan even if time, entropy or policy configuration have changed.
// version comes from this attempt's API permission, for atomic exhaustion.
// On an error retry this operation, never Telegram. A returned deadline grants
// a new CHECK, not permission for HTTP or reuse of an old API attempt.
func (s *Store) SaveRetryable(ctx context.Context, l Lease, attempt string, cause error, policy *retry.Policy, entropy uint64, version int64) (retry.Plan, error) {
	var c *telegram.Error
	if !validAttempt(l, attempt) || policy == nil || version < 0 || !errors.As(cause, &c) || c == nil || (c.Kind != telegram.RateLimited && c.Kind != telegram.Server) || c.RetryAfterSeconds < 0 {
		return retry.Plan{}, errors.New("invalid retryable outcome")
	}
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return retry.Plan{}, e
	}
	defer tx.Rollback(context.Background())
	j, e := lockJob(ctx, tx, l.TriggerID)
	if e != nil {
		return retry.Plan{}, e
	}
	var known bool
	e = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.delivery_attempts WHERE attempt_id=$1 AND trigger_id=$2 AND claim_id=$3 AND fence=$4)`, attempt, l.TriggerID, l.ID, l.Fence).Scan(&known)
	if e != nil {
		return retry.Plan{}, e
	}
	if !known {
		return retry.Plan{}, ErrStale
	}
	var plan retry.Plan
	var kind string
	var after, binding int64
	var next *time.Time
	e = tx.QueryRow(ctx, `SELECT kind,retry_after_seconds,binding_version,failures,exhausted,next_attempt_at FROM app.delivery_retryables WHERE attempt_id=$1`, attempt).Scan(&kind, &after, &binding, &plan.Failures, &plan.Exhausted, &next)
	if e == nil {
		if kind != string(c.Kind) || after != c.RetryAfterSeconds || binding != version {
			return retry.Plan{}, ErrConflict
		}
		if next != nil {
			plan.NextAttemptAt = next.Round(0).UTC()
		}
		if e = tx.Commit(ctx); e != nil {
			return retry.Plan{}, e
		}
		return plan, nil
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return retry.Plan{}, e
	}
	var blocked bool
	if e = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.delivery_blocks WHERE attempt_id=$1)`, attempt).Scan(&blocked); e != nil {
		return retry.Plan{}, e
	}
	if blocked {
		return retry.Plan{}, ErrConflict
	}
	failures, e := failureCount(ctx, tx, l.TriggerID)
	if e != nil {
		return retry.Plan{}, e
	}
	now, e := s.clock(ctx, tx)
	if e != nil {
		return retry.Plan{}, e
	}
	plan, e = policy.Retry(now, failures, cause, entropy)
	if e != nil {
		return retry.Plan{}, e
	}
	if !plan.Exhausted {
		plan.NextAttemptAt = plan.NextAttemptAt.Round(0).UTC()
		if plan.NextAttemptAt.Nanosecond()%1000 != 0 {
			plan.NextAttemptAt = plan.NextAttemptAt.Truncate(time.Microsecond).Add(time.Microsecond)
		}
		next = &plan.NextAttemptAt
	}
	if _, e = tx.Exec(ctx, `INSERT INTO app.delivery_retryables(attempt_id,kind,retry_after_seconds,binding_version,failures,exhausted,next_attempt_at) VALUES($1,$2,$3,$4,$5,$6,$7)`, attempt, string(c.Kind), c.RetryAfterSeconds, version, plan.Failures, plan.Exhausted, next); e != nil {
		return retry.Plan{}, e
	}
	if _, e = tx.Exec(ctx, `INSERT INTO app.delivery_retryable_reports(attempt_id) VALUES($1)`, attempt); e != nil {
		return retry.Plan{}, e
	}
	// Exhaustion is inseparable from its outcome: there is no crash window between
	// recording the last error and the existing current-fence blocked protocol.
	if plan.Exhausted {
		if _, e = tx.Exec(ctx, `INSERT INTO app.delivery_blocks(attempt_id,reason,binding_version) VALUES($1,'attempts_exhausted',$2)`, attempt, version); e != nil {
			return retry.Plan{}, e
		}
		if _, e = tx.Exec(ctx, `INSERT INTO app.delivery_blocked_reports(attempt_id) VALUES($1)`, attempt); e != nil {
			return retry.Plan{}, e
		}
	}
	succeeded, e := hasSuccess(ctx, tx, l.TriggerID)
	if e != nil {
		return retry.Plan{}, e
	}
	if j.owns(l, now) && !succeeded {
		if plan.Exhausted {
			e = finishBlocked(ctx, tx, l, domain.AttemptsExhausted, version)
		} else {
			_, e = tx.Exec(ctx, `UPDATE app.delivery_jobs SET state='retryable',next_attempt_at=$2,lease_id=NULL,lease_expires_at=NULL WHERE trigger_id=$1`, l.TriggerID, plan.NextAttemptAt)
			if e == nil {
				_, e = tx.Exec(ctx, `UPDATE app.delivery_claims SET status='completed' WHERE claim_id=$1`, l.ID)
			}
		}
		if e != nil {
			return retry.Plan{}, e
		}
	}
	if e = tx.Commit(ctx); e != nil {
		return retry.Plan{}, e
	}
	return plan, nil
}
