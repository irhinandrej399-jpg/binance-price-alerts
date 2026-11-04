package jobs

import (
	"context"
	"errors"
)

// Work is a scheduling hint, never a lease or permission. RecoveryFence > 0
// asks the caller to repair a missing channel check instead of invoking Handle.
type Work struct {
	TriggerID     string
	RecoveryFence int64
}

// Discover returns a bounded keyset page. An empty page asks the caller to
// wrap its cursor on the next paced poll. Claim/ScheduleRecovery recheck races.
func (s *Store) Discover(ctx context.Context, after string, limit int) ([]Work, error) {
	if limit < 1 || limit > 1000 {
		return nil, errors.New("invalid discovery limit")
	}
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback(context.Background())
	now, e := s.clock(ctx, tx)
	if e != nil {
		return nil, e
	}
	rows, e := tx.Query(ctx, `SELECT j.trigger_id,CASE WHEN recover THEN j.lease_fence ELSE 0 END
 FROM app.delivery_jobs j
 LEFT JOIN app.delivery_attempts a ON a.trigger_id=j.trigger_id AND a.fence=j.lease_fence
 LEFT JOIN app.delivery_blocks b USING(attempt_id)
 LEFT JOIN app.delivery_retryables r USING(attempt_id)
 CROSS JOIN LATERAL (SELECT
   EXISTS(SELECT 1 FROM app.delivery_attempts x JOIN app.delivery_successes s USING(attempt_id) WHERE x.trigger_id=j.trigger_id) AS success,
   EXISTS(SELECT 1 FROM app.delivery_attempts x WHERE x.trigger_id=j.trigger_id AND
       (EXISTS(SELECT 1 FROM app.delivery_success_reports WHERE attempt_id=x.attempt_id AND pending)
        OR EXISTS(SELECT 1 FROM app.delivery_uncertain_reports WHERE attempt_id=x.attempt_id AND pending)
        OR EXISTS(SELECT 1 FROM app.delivery_blocked_reports WHERE attempt_id=x.attempt_id AND pending)
        OR EXISTS(SELECT 1 FROM app.delivery_retryable_reports WHERE attempt_id=x.attempt_id AND pending))) AS reports
 ) evidence
 CROSS JOIN LATERAL (SELECT NOT success
    AND j.state IN ('pending','leased','retryable','uncertain','wait_condition','wait_limiter','blocked')
    AND (j.lease_expires_at IS NULL OR j.lease_expires_at <= $2)
    AND j.next_check_at IS NULL AND COALESCE(b.reason,j.blocked_reason)='channel_unavailable' AS recover) repair
 WHERE j.trigger_id > $1 AND (
    reports OR recover OR (NOT success AND (j.lease_expires_at IS NULL OR j.lease_expires_at <= $2) AND (
       (j.state='blocked' AND j.blocked_reason='channel_unavailable' AND j.next_check_at <= $2)
       OR (j.state IN ('pending','leased','retryable','uncertain','wait_condition','wait_limiter')
           AND b.attempt_id IS NULL AND j.next_attempt_at <= $2
           AND (r.next_attempt_at IS NULL OR r.next_attempt_at <= $2)))))
 ORDER BY j.trigger_id LIMIT $3`, after, now, limit)
	if e != nil {
		return nil, e
	}
	var work []Work
	for rows.Next() {
		var w Work
		if e = rows.Scan(&w.TriggerID, &w.RecoveryFence); e != nil {
			rows.Close()
			return nil, e
		}
		work = append(work, w)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return nil, e
	}
	if e = tx.Commit(ctx); e != nil {
		return nil, e
	}
	return work, nil
}
