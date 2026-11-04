// Package report reconciles durable observations with the subscription owner.
package report

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/resultrpc"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Client interface {
	SaveDeliveryResult(context.Context, *pb.SaveDeliveryResultRequest) (*pb.SaveDeliveryResultResponse, error)
}
type Reporter struct {
	pool   *pgxpool.Pool
	client Client
}

func New(p *pgxpool.Pool, c Client) (*Reporter, error) {
	if p == nil || c == nil {
		return nil, errors.New("report pool and client required")
	}
	return &Reporter{p, c}, nil
}

// Reconcile delivers at most limit saved observations for a scheduler-selected
// workflow. It never holds a SQL transaction/lease over RPC. Concurrent reporters
// may repeat an identical observation; API idempotency and per-outcome ack make
// this safe. The returned count is successful RPC+ack operations in this pass.
// Any RPC or ack error retains the obligation; the caller schedules another pass
// with backoff. Invalid identity/conflict errors require operator investigation.
func (r *Reporter) Reconcile(ctx context.Context, trigger string, limit int) (int, error) {
	if strings.TrimSpace(trigger) == "" || limit < 1 || limit > 1000 {
		return 0, errors.New("workflow and report limit 1..1000 required")
	}
	rows, err := r.pool.Query(ctx, `
 SELECT j.user_id,j.subscription_id,a.trigger_id,o.attempt_id,o.outcome,o.message_id,o.reason
 FROM (
 SELECT r.attempt_id,'succeeded'::text AS outcome,s.message_id,''::text AS reason,s.recorded_at
 FROM app.delivery_success_reports r JOIN app.delivery_successes s USING(attempt_id) WHERE r.pending
 UNION ALL
 SELECT r.attempt_id,'uncertain',0,'',s.recorded_at
 FROM app.delivery_uncertain_reports r JOIN app.delivery_uncertainties s USING(attempt_id) WHERE r.pending
 UNION ALL
 SELECT r.attempt_id,'retryable',0,'',s.recorded_at
 FROM app.delivery_retryable_reports r JOIN app.delivery_retryables s USING(attempt_id) WHERE r.pending
 UNION ALL
 SELECT r.attempt_id,'blocked',0,s.reason,s.recorded_at
 FROM app.delivery_blocked_reports r JOIN app.delivery_blocks s USING(attempt_id) WHERE r.pending
 ) o JOIN app.delivery_attempts a USING(attempt_id)
 JOIN app.delivery_jobs j ON j.trigger_id=a.trigger_id
 WHERE a.trigger_id=$1
 ORDER BY (o.outcome='succeeded') DESC,o.recorded_at,o.attempt_id,o.outcome LIMIT $2`, trigger, limit)
	if err != nil {
		return 0, err
	}
	// Fully release the query connection before RPC/ack, including pool size one.
	var requests []*pb.SaveDeliveryResultRequest
	for rows.Next() {
		req := new(pb.SaveDeliveryResultRequest)
		if err = rows.Scan(&req.OwnerId, &req.SubscriptionId, &req.WorkflowId, &req.AttemptId, &req.Outcome, &req.MessageId, &req.BlockReason); err != nil {
			rows.Close()
			return 0, err
		}
		requests = append(requests, req)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	n := 0
	var failures []error
	for _, req := range requests {
		if ctx.Err() != nil {
			failures = append(failures, ctx.Err())
			break
		}
		rpcCtx, cancel := context.WithTimeout(ctx, resultrpc.Budget)
		response, e := r.client.SaveDeliveryResult(rpcCtx, req)
		cancel()
		if e == nil && response == nil {
			e = errors.New("missing result acknowledgement")
		}
		if e == nil {
			// Only fixed internal identifiers enter SQL. No request or remote response
			// can select another table or ack another attempt/outcome.
			table := reportTable(req.Outcome)
			_, e = r.pool.Exec(ctx, "UPDATE app."+table+" SET pending=false WHERE attempt_id=$1", req.AttemptId)
		}
		if e != nil {
			failures = append(failures, fmt.Errorf("report %s/%s: %w", req.AttemptId, req.Outcome, e))
			continue
		}
		n++
	}
	return n, errors.Join(failures...)
}
func reportTable(outcome string) string {
	switch outcome {
	case "succeeded":
		return "delivery_success_reports"
	case "uncertain":
		return "delivery_uncertain_reports"
	case "retryable":
		return "delivery_retryable_reports"
	case "blocked":
		return "delivery_blocked_reports"
	default:
		panic("unknown stored outcome")
	}
}
