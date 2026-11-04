package jobs

import (
	"context"
	"errors"
	"strings"
	"time"
)

// ScheduleRecovery durably schedules a channel check for the current fence.
// interval is configurable per call (1 second..1 hour, whole microseconds).
// A fixed interval suffices; callers may increase it for bounded backoff. The
// first committed deadline wins, so retries never postpone or accelerate it.
// No lease/limiter is held while waiting; no HTTP failure budget is consumed.
// Scheduler must also discover unscheduled current-fence channel blocks, since
// a crash or historical outcome may leave job metadata unchanged. New checks
// go through Claim and a fresh API admission request, never directly to HTTP.
func (s *Store) ScheduleRecovery(ctx context.Context, trigger string, fence int64, interval time.Duration) error {
	if strings.TrimSpace(trigger) == "" || fence <= 0 || interval < time.Second || interval > time.Hour || interval%time.Microsecond != 0 {
		return errors.New("invalid channel recovery schedule")
	}
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(context.Background())
	j, e := lockJob(ctx, tx, trigger)
	if e != nil {
		return e
	}
	if j.fence != fence {
		return ErrStale
	}
	switch j.state {
	case "blocked", "pending", "leased", "retryable", "uncertain", "wait_condition", "wait_limiter":
	default:
		return ErrUnavailable
	}
	success, e := hasSuccess(ctx, tx, trigger)
	if e != nil {
		return e
	}
	if success {
		return ErrUnavailable
	}
	now, e := s.clock(ctx, tx)
	if e != nil {
		return e
	}
	if j.expires != nil && now.Before(*j.expires) {
		return ErrUnavailable
	}
	// Evidence of the current fence takes precedence over job metadata. Negative
	// evidence from a previous fence cannot schedule or stop a recovered cycle.
	var reason *string
	var version *int64
	e = tx.QueryRow(ctx, `SELECT COALESCE(b.reason,j.blocked_reason),COALESCE(b.binding_version,j.blocked_binding_version)
 FROM app.delivery_jobs j LEFT JOIN app.delivery_attempts a ON a.trigger_id=j.trigger_id AND a.fence=j.lease_fence
 LEFT JOIN app.delivery_blocks b USING(attempt_id) WHERE j.trigger_id=$1`, trigger).Scan(&reason, &version)
	if e != nil {
		return e
	}
	if reason == nil || *reason != "channel_unavailable" || version == nil {
		return ErrUnavailable
	}
	if j.id != nil {
		if _, e = tx.Exec(ctx, `UPDATE app.delivery_claims SET status='expired' WHERE claim_id=$1 AND status='claimed'`, *j.id); e != nil {
			return e
		}
	}
	_, e = tx.Exec(ctx, `UPDATE app.delivery_jobs SET state='blocked',blocked_reason=$2,blocked_binding_version=$3,
 next_check_at=COALESCE(next_check_at,$4),next_attempt_at='infinity',lease_id=NULL,lease_expires_at=NULL WHERE trigger_id=$1`, trigger, *reason, *version, now.Add(interval))
	if e != nil {
		return e
	}
	return tx.Commit(ctx)
}
