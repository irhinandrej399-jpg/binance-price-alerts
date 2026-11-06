package jobs

import (
	"context"
	"testing"
	"time"

	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/telegram"
)

func TestQueueMetricsLifecycle(t *testing.T) {
	ctx, p, s, restarted, clock, id := fixture(t)
	before, e := s.QueueMetrics(ctx)
	if e != nil {
		t.Fatal(e)
	}
	now := time.UnixMicro(clock.Load())
	// Oldest age is creation age, including delayed/blocked jobs, not next retry age.
	if _, e = p.Exec(ctx, `UPDATE app.delivery_jobs SET created_at=$2 WHERE trigger_id=$1`, id, now.Add(-200*365*24*time.Hour)); e != nil {
		t.Fatal(e)
	}
	l, e := s.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Park(ctx, l, WaitCondition, now.Add(time.Second)); e != nil {
		t.Fatal(e)
	}
	m, e := s.QueueMetrics(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if m.States["pending"].Count != before.States["pending"].Count-1 || m.States["wait_condition"].Count != before.States["wait_condition"].Count+1 || m.States["wait_condition"].OldestSeconds != float64(200*365*24*60*60) {
		t.Fatal(m)
	}
	clock.Add(int64(time.Second / time.Microsecond))
	l, e = s.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Park(ctx, l, WaitLimiter, now.Add(time.Minute)); e != nil {
		t.Fatal(e)
	}
	m, e = s.QueueMetrics(ctx)
	if e != nil || m.States["wait_limiter"].Count != before.States["wait_limiter"].Count+1 {
		t.Fatal(m, e)
	}
	clock.Store(now.Add(time.Minute).UnixMicro())
	l, e = s.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	attempt := "metrics-" + id
	if e = s.RegisterAttempt(ctx, l, attempt); e != nil {
		t.Fatal(e)
	}
	cause := &telegram.Error{Kind: telegram.RateLimited, RetryAfterSeconds: 10}
	for range 2 {
		if _, e = s.SaveRetryable(ctx, l, attempt, cause, retryPolicy(t, 5), 0, 1); e != nil {
			t.Fatal(e)
		}
	}
	m, e = restarted.QueueMetrics(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if m.Outcomes["rate_limited"] != before.Outcomes["rate_limited"]+1 || m.PendingReports["retryable"] != before.PendingReports["retryable"]+1 || m.States["retryable"].Count != before.States["retryable"].Count+1 {
		t.Fatal(m)
	}
	// A late block changes effective state even after the lease was released.
	if e = s.SaveBlocked(ctx, l, attempt, domain.PermanentFailure, 1); e != nil {
		t.Fatal(e)
	}
	m, e = s.QueueMetrics(ctx)
	if e != nil || m.States["blocked"].Count != before.States["blocked"].Count+1 || m.Blocks["permanent_failure"] != before.Blocks["permanent_failure"]+1 {
		t.Fatal(m, e)
	}
	// Success dominates stale job fields and historical block evidence.
	if e = s.SaveSuccess(ctx, l, attempt, 42); e != nil {
		t.Fatal(e)
	}
	m, e = s.QueueMetrics(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if m.Backlog != before.Backlog-1 || m.States["succeeded"].Count != before.States["succeeded"].Count+1 || m.Completed != before.Completed || m.PendingReports["success"] != before.PendingReports["success"]+1 {
		t.Fatal(m)
	}
	if _, e = p.Exec(ctx, `UPDATE app.delivery_success_reports SET pending=false WHERE attempt_id=$1`, attempt); e != nil {
		t.Fatal(e)
	}
	m, e = restarted.QueueMetrics(ctx)
	if e != nil || m.Completed != before.Completed+1 || m.PendingReports["success"] != before.PendingReports["success"] {
		t.Fatal(m, e)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, e = s.QueueMetrics(cancelled); e == nil {
		t.Fatal("cancelled snapshot succeeded")
	}
}

func TestQueueMetricsUncertainSurvivesSuccessAndAck(t *testing.T) {
	ctx, p, s, _, clock, id := fixture(t)
	before, e := s.QueueMetrics(ctx)
	if e != nil {
		t.Fatal(e)
	}
	l, e := s.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	a := "uncertain-metrics-" + id
	if e = s.RegisterAttempt(ctx, l, a); e != nil {
		t.Fatal(e)
	}
	for range 2 {
		if e = s.SaveUncertain(ctx, l, a, time.UnixMicro(clock.Load()).Add(time.Second)); e != nil {
			t.Fatal(e)
		}
	}
	m, e := s.QueueMetrics(ctx)
	if e != nil || m.PossibleDuplicates != before.PossibleDuplicates+1 || m.Outcomes["uncertain"] != before.Outcomes["uncertain"]+1 {
		t.Fatal(m, e)
	}
	if e = s.SaveSuccess(ctx, l, a, 99); e != nil {
		t.Fatal(e)
	}
	for _, table := range []string{"delivery_success_reports", "delivery_uncertain_reports"} {
		if _, e = p.Exec(ctx, `UPDATE app.`+table+` SET pending=false WHERE attempt_id=$1`, a); e != nil {
			t.Fatal(e)
		}
	}
	m, e = s.QueueMetrics(ctx)
	if e != nil || m.PossibleDuplicates != before.PossibleDuplicates+1 || m.Completed != before.Completed+1 {
		t.Fatal(m, e)
	}
}

func TestQueueMetricsRecoveryAndDistinctCompletion(t *testing.T) {
	ctx, p, s, _, clock, id := fixture(t)
	before, e := s.QueueMetrics(ctx)
	if e != nil {
		t.Fatal(e)
	}
	old, e := s.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	first := "old-metrics-" + id
	if e = s.RegisterAttempt(ctx, old, first); e != nil {
		t.Fatal(e)
	}
	if e = s.SaveUncertain(ctx, old, first, time.UnixMicro(clock.Load()).Add(time.Second)); e != nil {
		t.Fatal(e)
	}
	if e = s.SaveBlocked(ctx, old, first, domain.ChannelUnavailable, 1); e != nil {
		t.Fatal(e)
	}
	m, e := s.QueueMetrics(ctx)
	if e != nil || m.Blocks["channel_unavailable"] != before.Blocks["channel_unavailable"]+1 {
		t.Fatal(m, e)
	}
	if e = s.ScheduleRecovery(ctx, id, old.Fence, time.Second); e != nil {
		t.Fatal(e)
	}
	clock.Add(int64(time.Second / time.Microsecond))
	fresh, e := s.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	m, e = s.QueueMetrics(ctx)
	if e != nil || m.Blocks["channel_unavailable"] != before.Blocks["channel_unavailable"] || m.States["leased"].Count != before.States["leased"].Count+1 {
		t.Fatal(m, e)
	}
	second := "new-metrics-" + id
	if e = s.RegisterAttempt(ctx, fresh, second); e != nil {
		t.Fatal(e)
	}
	if e = s.SaveSuccess(ctx, old, first, 1); e != nil {
		t.Fatal(e)
	}
	if e = s.SaveSuccess(ctx, fresh, second, 2); e != nil {
		t.Fatal(e)
	}
	// Two successful external attempts, but only one completed workflow.
	if _, e = p.Exec(ctx, `UPDATE app.delivery_success_reports SET pending=false WHERE attempt_id IN ($1,$2)`, first, second); e != nil {
		t.Fatal(e)
	}
	m, e = s.QueueMetrics(ctx)
	if e != nil || m.Completed != before.Completed+1 || m.Outcomes["success"] != before.Outcomes["success"]+2 || m.Backlog != before.Backlog-1 {
		t.Fatal(m, e)
	}
}
