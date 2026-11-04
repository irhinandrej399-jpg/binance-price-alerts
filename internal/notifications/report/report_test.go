package report_test

import (
	"context"
	"sync"
	"testing"
	"time"

	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/jobs"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/report"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/retry"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/telegram"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type api struct {
	mu    sync.Mutex
	calls []*pb.SaveDeliveryResultRequest
	fail  string
}

func (a *api) SaveDeliveryResult(ctx context.Context, r *pb.SaveDeliveryResultRequest) (*pb.SaveDeliveryResultResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, r)
	if r.Outcome == a.fail {
		return nil, status.Error(codes.DeadlineExceeded, "response lost after commit")
	}
	return &pb.SaveDeliveryResultResponse{SubscriptionId: r.SubscriptionId, WorkflowId: r.WorkflowId, State: "cancelled"}, nil
}
func setup(t *testing.T) (context.Context, *pgxpool.Pool, *jobs.Store, jobs.Lease, string) {
	t.Helper()
	p := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
	ctx := context.Background()
	id := "report-" + t.Name()
	if _, e := p.Exec(ctx, `INSERT INTO app.delivery_jobs(trigger_id,subscription_id,user_id,event_id,body) VALUES($1,'subscription','owner','event',$2)`, id, []byte("original")); e != nil {
		t.Fatal(e)
	}
	s, e := jobs.New(p, time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	l, e := s.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.RegisterAttempt(ctx, l, id+"-attempt"); e != nil {
		t.Fatal(e)
	}
	return ctx, p, s, l, id + "-attempt"
}
func pending(t *testing.T, p *pgxpool.Pool, table, attempt string) bool {
	t.Helper()
	var v bool
	if e := p.QueryRow(context.Background(), `SELECT pending FROM app.`+table+` WHERE attempt_id=$1`, attempt).Scan(&v); e != nil {
		t.Fatal(e)
	}
	return v
}
func TestRestartLostAPIResponseAndLateSuccess(t *testing.T) {
	ctx, p, s, l, id := setup(t)
	a := &api{fail: "uncertain"}
	r, e := report.New(p, a)
	if e != nil {
		t.Fatal(e)
	}
	// Crash before local commit: an attempt alone is not an observation to send.
	if n, e := r.Reconcile(ctx, l.TriggerID, 10); e != nil || n != 0 || len(a.calls) != 0 {
		t.Fatal(n, e, a.calls)
	}
	if e = s.SaveUncertain(ctx, l, id, time.Now().Add(time.Hour)); e != nil {
		t.Fatal(e)
	}
	if n, e := r.Reconcile(ctx, l.TriggerID, 10); e == nil || n != 0 {
		t.Fatal(n, e)
	}
	if !pending(t, p, "delivery_uncertain_reports", id) {
		t.Fatal("lost API response acknowledged")
	}
	// Restart from another pool; no Telegram sender/admission is involved.
	p2 := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
	a.fail = ""
	r, e = report.New(p2, a)
	if e != nil {
		t.Fatal(e)
	}
	if n, e := r.Reconcile(ctx, l.TriggerID, 1); e != nil || n != 1 {
		t.Fatal(n, e)
	}
	if pending(t, p, "delivery_uncertain_reports", id) {
		t.Fatal("not acknowledged")
	}
	// Replay cannot resurrect an ack; late success is a distinct pending outcome.
	if e = s.SaveUncertain(ctx, l, id, time.Now().Add(2*time.Hour)); e != nil {
		t.Fatal(e)
	}
	if e = s.SaveSuccess(ctx, l, id, 321); e != nil {
		t.Fatal(e)
	}
	if n, e := r.Reconcile(ctx, l.TriggerID, 10); e != nil || n != 1 {
		t.Fatal(n, e)
	}
	if n, e := r.Reconcile(ctx, l.TriggerID, 10); e != nil || n != 0 {
		t.Fatal(n, e)
	}
	if len(a.calls) != 3 {
		t.Fatal(len(a.calls))
	}
	for i, c := range a.calls {
		if c.OwnerId != "owner" || c.SubscriptionId != "subscription" || c.WorkflowId != l.TriggerID || c.AttemptId != id {
			t.Fatal(c)
		}
		if i == 2 && (c.Outcome != "succeeded" || c.MessageId != 321) {
			t.Fatal(c)
		}
	}
}
func TestIndependentOutcomesAndBoundedPass(t *testing.T) {
	ctx, p, s, l, id := setup(t)
	policy, e := retry.New(retry.Config{BaseDelay: time.Second, MaxDelay: time.Second, ConditionInterval: time.Second, MaxFailures: 1})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.SaveRetryable(ctx, l, id, &telegram.Error{Kind: telegram.Server}, policy, 0, 7); e != nil {
		t.Fatal(e)
	}
	a := &api{fail: "retryable"}
	r, e := report.New(p, a)
	if e != nil {
		t.Fatal(e)
	}
	// A retryable RPC error must not prevent the independent exhausted report.
	if n, e := r.Reconcile(ctx, l.TriggerID, 10); e == nil || n != 1 {
		t.Fatal(n, e)
	}
	if !pending(t, p, "delivery_retryable_reports", id) || pending(t, p, "delivery_blocked_reports", id) {
		t.Fatal("wrong ack key")
	}
	var reason string
	for _, c := range a.calls {
		if c.Outcome == "blocked" {
			reason = c.BlockReason
		}
	}
	if reason != string(domain.AttemptsExhausted) {
		t.Fatal(a.calls)
	}
	a.fail = ""
	if n, e := r.Reconcile(ctx, l.TriggerID, 1); e != nil || n != 1 {
		t.Fatal(n, e)
	}
	if n, e := r.Reconcile(ctx, l.TriggerID, 10); e != nil || n != 0 {
		t.Fatal(n, e)
	}
}
func TestConcurrentReplayAndHistoricalBlock(t *testing.T) {
	ctx, p, s, l, id := setup(t)
	if e := s.SaveBlocked(ctx, l, id, domain.ChannelUnavailable, 3); e != nil {
		t.Fatal(e)
	}
	// New channel/job metadata is owned by the future recovery worker. Reporter
	// must only ack evidence of the original attempt, never write job state.
	if _, e := p.Exec(ctx, `UPDATE app.delivery_jobs SET state='wait_condition',lease_fence=lease_fence+1,blocked_reason=NULL,blocked_binding_version=NULL WHERE trigger_id=$1`, l.TriggerID); e != nil {
		t.Fatal(e)
	}
	a := &api{}
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			p2 := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
			r, e := report.New(p2, a)
			if e != nil {
				t.Error(e)
				return
			}
			if _, e = r.Reconcile(ctx, l.TriggerID, 10); e != nil {
				t.Error(e)
			}
		})
	}
	wg.Wait()
	if pending(t, p, "delivery_blocked_reports", id) {
		t.Fatal("still pending")
	}
	var state string
	var fence int64
	if e := p.QueryRow(ctx, `SELECT state,lease_fence FROM app.delivery_jobs WHERE trigger_id=$1`, l.TriggerID).Scan(&state, &fence); e != nil || state != "wait_condition" || fence != l.Fence+1 {
		t.Fatal(state, fence, e)
	}
	for _, c := range a.calls {
		if c.AttemptId != id || c.BlockReason != "channel_unavailable" {
			t.Fatal(c)
		}
	}
}
func TestInvalidConfiguration(t *testing.T) {
	if _, e := report.New(nil, &api{}); e == nil {
		t.Fatal("nil pool")
	}
}

type clientFunc func(context.Context, *pb.SaveDeliveryResultRequest) (*pb.SaveDeliveryResultResponse, error)

func (f clientFunc) SaveDeliveryResult(c context.Context, r *pb.SaveDeliveryResultRequest) (*pb.SaveDeliveryResultResponse, error) {
	return f(c, r)
}
func TestAckFailureRetainsObligation(t *testing.T) {
	ctx, p, s, l, id := setup(t)
	if e := s.SaveSuccess(ctx, l, id, 77); e != nil {
		t.Fatal(e)
	}
	cancelled, cancel := context.WithCancel(ctx)
	r, e := report.New(p, clientFunc(func(c context.Context, req *pb.SaveDeliveryResultRequest) (*pb.SaveDeliveryResultResponse, error) {
		cancel() // API accepted; local ack cannot commit under a cancelled context.
		return &pb.SaveDeliveryResultResponse{SubscriptionId: req.SubscriptionId}, nil
	}))
	if e != nil {
		t.Fatal(e)
	}
	if n, e := r.Reconcile(cancelled, l.TriggerID, 10); e == nil || n != 0 {
		t.Fatal(n, e)
	}
	if !pending(t, p, "delivery_success_reports", id) {
		t.Fatal("ack failure lost obligation")
	}
	a := &api{}
	r, e = report.New(p, a)
	if e != nil {
		t.Fatal(e)
	}
	if n, e := r.Reconcile(ctx, l.TriggerID, 10); e != nil || n != 1 {
		t.Fatal(n, e)
	}
	if len(a.calls) != 1 || a.calls[0].MessageId != 77 {
		t.Fatal(a.calls)
	}
	for _, limit := range []int{-1, 0, 1001} {
		if _, e = r.Reconcile(ctx, l.TriggerID, limit); e == nil {
			t.Fatal("invalid limit")
		}
	}
	if _, e = r.Reconcile(ctx, " ", 1); e == nil {
		t.Fatal("empty workflow")
	}
}

func TestBatchLimitAndSuccessPriority(t *testing.T) {
	ctx, p, s, l, id := setup(t)
	if e := s.SaveUncertain(ctx, l, id, time.Now().Add(time.Hour)); e != nil {
		t.Fatal(e)
	}
	policy, e := retry.New(retry.Config{BaseDelay: time.Second, MaxDelay: time.Second, ConditionInterval: time.Second, MaxFailures: 1})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.SaveRetryable(ctx, l, id, &telegram.Error{Kind: telegram.Server}, policy, 0, 1); e != nil {
		t.Fatal(e)
	}
	if e = s.SaveSuccess(ctx, l, id, 99); e != nil {
		t.Fatal(e)
	}
	a := &api{}
	r, e := report.New(p, a)
	if e != nil {
		t.Fatal(e)
	}
	if n, e := r.Reconcile(ctx, l.TriggerID, 1); e != nil || n != 1 {
		t.Fatal(n, e)
	}
	if len(a.calls) != 1 || a.calls[0].Outcome != "succeeded" {
		t.Fatal(a.calls)
	}
	if !pending(t, p, "delivery_uncertain_reports", id) || !pending(t, p, "delivery_retryable_reports", id) || !pending(t, p, "delivery_blocked_reports", id) {
		t.Fatal("batch ack leaked")
	}
	if n, e := r.Reconcile(ctx, l.TriggerID, 2); e != nil || n != 2 {
		t.Fatal(n, e)
	}
	if n, e := r.Reconcile(ctx, l.TriggerID, 2); e != nil || n != 1 {
		t.Fatal(n, e)
	}
	if n, e := r.Reconcile(ctx, l.TriggerID, 2); e != nil || n != 0 {
		t.Fatal(n, e)
	}
}
