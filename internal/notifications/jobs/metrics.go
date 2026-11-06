package jobs

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// QueueSnapshot is database-wide. Copies from multiple replicas must not be summed.
// Evidence totals overlap: a historical uncertain attempt may later succeed.
type QueueSnapshot struct {
	At                                     time.Time
	States                                 map[string]QueueState
	PendingReports                         map[string]int64
	Outcomes                               map[string]int64
	Blocks                                 map[string]int64
	Backlog, Completed, PossibleDuplicates int64
	OldestSeconds                          float64
}
type QueueState struct {
	Count         int64
	OldestSeconds float64
}

// QueueMetrics observes a consistent read-only snapshot. Errors return no partial
// values; reading never claims work, acknowledges reports or calls Telegram/API.
func (s *Store) QueueMetrics(ctx context.Context) (QueueSnapshot, error) {
	tx, e := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if e != nil {
		return QueueSnapshot{}, e
	}
	defer tx.Rollback(context.Background())
	now, e := s.clock(ctx, tx)
	if e != nil {
		return QueueSnapshot{}, e
	}
	rows, e := tx.Query(ctx, `WITH effective AS (
 SELECT j.created_at,CASE
 WHEN EXISTS(SELECT 1 FROM app.delivery_attempts a JOIN app.delivery_successes s USING(attempt_id) WHERE a.trigger_id=j.trigger_id) THEN 'succeeded'
 WHEN j.state IN ('pending','leased','retryable','uncertain','wait_condition','wait_limiter','blocked') AND EXISTS(
 SELECT 1 FROM app.delivery_attempts a JOIN app.delivery_blocks b USING(attempt_id) WHERE a.trigger_id=j.trigger_id AND a.fence=j.lease_fence) THEN 'blocked'
 WHEN j.state IN ('pending','leased','retryable','uncertain','wait_condition','wait_limiter','blocked','succeeded','cancelled','stopped','delivered') THEN j.state
 ELSE 'other' END AS state FROM app.delivery_jobs j
 ), reports AS (
 SELECT 'success' AS kind,pending FROM app.delivery_success_reports UNION ALL
 SELECT 'uncertain',pending FROM app.delivery_uncertain_reports UNION ALL
 SELECT 'blocked',pending FROM app.delivery_blocked_reports UNION ALL
 SELECT 'retryable',pending FROM app.delivery_retryable_reports
 ), outcomes AS (
 SELECT 'success' AS kind FROM app.delivery_successes UNION ALL
 SELECT 'uncertain' FROM app.delivery_uncertainties UNION ALL
 SELECT reason FROM app.delivery_blocks UNION ALL
 SELECT kind FROM app.delivery_retryables
 )
 SELECT 'state',state,count(*),GREATEST(0,EXTRACT(EPOCH FROM ($1::timestamptz-min(created_at))))::double precision FROM effective GROUP BY state
 UNION ALL SELECT 'report',kind,count(*),0 FROM reports WHERE pending GROUP BY kind
 UNION ALL SELECT 'outcome',kind,count(*),0 FROM outcomes GROUP BY kind
 UNION ALL SELECT 'block',reason,count(*),0 FROM app.delivery_jobs j
 CROSS JOIN LATERAL(SELECT COALESCE((SELECT b.reason FROM app.delivery_attempts a JOIN app.delivery_blocks b USING(attempt_id) WHERE a.trigger_id=j.trigger_id AND a.fence=j.lease_fence),j.blocked_reason) reason) b
 WHERE j.state IN ('pending','leased','retryable','uncertain','wait_condition','wait_limiter','blocked') AND b.reason IS NOT NULL
 AND NOT EXISTS(SELECT 1 FROM app.delivery_attempts a JOIN app.delivery_successes s USING(attempt_id) WHERE a.trigger_id=j.trigger_id) GROUP BY reason
 UNION ALL SELECT 'total','backlog',count(*),COALESCE(GREATEST(0,EXTRACT(EPOCH FROM ($1::timestamptz-min(created_at)))),0)::double precision FROM effective WHERE state NOT IN ('succeeded','cancelled','stopped','delivered')
 UNION ALL SELECT 'total','completed',count(DISTINCT a.trigger_id),0 FROM app.delivery_attempts a JOIN app.delivery_success_reports r USING(attempt_id) WHERE NOT r.pending
 UNION ALL SELECT 'total','duplicates',count(*),0 FROM app.delivery_uncertainties`, now)
	if e != nil {
		return QueueSnapshot{}, e
	}
	defer rows.Close()
	m := QueueSnapshot{At: now, States: map[string]QueueState{}, PendingReports: map[string]int64{}, Outcomes: map[string]int64{}, Blocks: map[string]int64{}}
	for rows.Next() {
		var group, key string
		var n int64
		var age float64
		if e = rows.Scan(&group, &key, &n, &age); e != nil {
			return QueueSnapshot{}, e
		}
		switch group {
		case "state":
			m.States[key] = QueueState{n, age}
		case "report":
			m.PendingReports[key] = n
		case "outcome":
			m.Outcomes[key] = n
		case "block":
			m.Blocks[key] = n
		case "total":
			switch key {
			case "backlog":
				m.Backlog = n
				m.OldestSeconds = age
			case "completed":
				m.Completed = n
			case "duplicates":
				m.PossibleDuplicates = n
			}
		}
	}
	if e = rows.Err(); e != nil {
		return QueueSnapshot{}, e
	}
	if e = tx.Commit(ctx); e != nil {
		return QueueSnapshot{}, e
	}
	return m, nil
}
