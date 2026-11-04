package jobs

import (
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/telegram"
	"testing"
	"time"
)

func TestDiscoverLifecycle(t *testing.T) {
	ctx, p, s, _, clock, id := fixture(t)
	find := func() *Work {
		t.Helper()
		rows, e := s.Discover(ctx, "", 1000)
		if e != nil {
			t.Fatal(e)
		}
		for _, w := range rows {
			if w.TriggerID == id {
				return &w
			}
		}
		return nil
	}
	if find() == nil {
		t.Fatal("missing pending job")
	}
	l, e := s.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if find() != nil {
		t.Fatal("live lease selected")
	}
	if e = s.ParkBlocked(ctx, l, domain.ChannelUnavailable, 1); e != nil {
		t.Fatal(e)
	}
	if w := find(); w == nil || w.RecoveryFence != l.Fence {
		t.Fatal("missing unscheduled block", w)
	}
	if e = s.ScheduleRecovery(ctx, id, l.Fence, time.Second); e != nil {
		t.Fatal(e)
	}
	if find() != nil {
		t.Fatal("early recovery")
	}
	clock.Add(int64(time.Second / time.Microsecond))
	if w := find(); w == nil || w.RecoveryFence != 0 {
		t.Fatal("missing due check", w)
	}
	l, e = s.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	attempt := "discover-" + id
	if e = s.RegisterAttempt(ctx, l, attempt); e != nil {
		t.Fatal(e)
	}
	if e = s.SaveSuccess(ctx, l, attempt, 123); e != nil {
		t.Fatal(e)
	}
	if find() == nil {
		t.Fatal("terminal pending report lost")
	}
	if _, e = p.Exec(ctx, `UPDATE app.delivery_success_reports SET pending=false WHERE attempt_id=$1`, attempt); e != nil {
		t.Fatal(e)
	}
	if find() != nil {
		t.Fatal("acknowledged success selected")
	}
}

func TestDiscoverWaitsPaginationAndExpiredLease(t *testing.T) {
	ctx, p, s, _, clock, id := fixture(t)
	// Restrict keyset to our prefix so other storage fixtures do not define order.
	for _, suffix := range []string{"-a", "-b", "-c"} {
		if _, e := p.Exec(ctx, `INSERT INTO app.delivery_jobs(trigger_id,subscription_id,user_id,event_id,body) VALUES($1,'s','u','e',$2)`, id+suffix, []byte("request")); e != nil {
			t.Fatal(e)
		}
	}
	page, e := s.Discover(ctx, id, 2)
	if e != nil || len(page) != 2 || page[0].TriggerID != id+"-a" || page[1].TriggerID != id+"-b" {
		t.Fatal(page, e)
	}
	page, e = s.Discover(ctx, page[1].TriggerID, 1)
	if e != nil || len(page) != 1 || page[0].TriggerID != id+"-c" {
		t.Fatal(page, e)
	}
	selected := func() bool {
		t.Helper()
		rows, e := s.Discover(ctx, "", 1000)
		if e != nil {
			t.Fatal(e)
		}
		for _, w := range rows {
			if w.TriggerID == id {
				return true
			}
		}
		return false
	}
	l, e := s.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if selected() {
		t.Fatal("live lease")
	}
	clock.Add(int64(time.Second / time.Microsecond))
	if !selected() {
		t.Fatal("expired lease not discovered")
	}
	l, e = s.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Park(ctx, l, WaitCondition, time.UnixMicro(clock.Load()).Add(time.Second)); e != nil {
		t.Fatal(e)
	}
	if selected() {
		t.Fatal("future wait selected")
	}
	clock.Add(int64(time.Second / time.Microsecond))
	if !selected() {
		t.Fatal("due wait lost")
	}
	if _, e = s.Discover(ctx, "", 0); e == nil {
		t.Fatal("invalid bound accepted")
	}
}

func TestDiscoverHistoricalBlockAndReports(t *testing.T) {
	ctx, p, s, _, clock, id := fixture(t)
	l, e := s.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	attempt := "historical-" + id
	if e = s.RegisterAttempt(ctx, l, attempt); e != nil {
		t.Fatal(e)
	}
	if e = s.SaveUncertain(ctx, l, attempt, time.UnixMicro(clock.Load()).Add(time.Second)); e != nil {
		t.Fatal(e)
	}
	if e = s.SaveBlocked(ctx, l, attempt, domain.ChannelUnavailable, 1); e != nil {
		t.Fatal(e)
	}
	found := false
	rows, e := s.Discover(ctx, "", 1000)
	if e != nil {
		t.Fatal(e)
	}
	for _, w := range rows {
		if w.TriggerID == id {
			found = true
			if w.RecoveryFence != l.Fence {
				t.Fatal(w)
			}
		}
	}
	if !found {
		t.Fatal("historical channel block lost")
	}
	// Reports must remain discoverable independently of all job scheduling fields.
	if _, e = p.Exec(ctx, `UPDATE app.delivery_jobs SET state='stopped',next_attempt_at='infinity' WHERE trigger_id=$1`, id); e != nil {
		t.Fatal(e)
	}
	rows, e = s.Discover(ctx, "", 1000)
	if e != nil {
		t.Fatal(e)
	}
	found = false
	for _, w := range rows {
		if w.TriggerID == id {
			found = true
			if w.RecoveryFence != 0 {
				t.Fatal(w)
			}
		}
	}
	if !found {
		t.Fatal("stopped reports lost")
	}
	for _, table := range []string{"delivery_uncertain_reports", "delivery_blocked_reports"} {
		if _, e = p.Exec(ctx, `UPDATE app.`+table+` SET pending=false WHERE attempt_id=$1`, attempt); e != nil {
			t.Fatal(e)
		}
	}
	rows, e = s.Discover(ctx, "", 1000)
	if e != nil {
		t.Fatal(e)
	}
	for _, w := range rows {
		if w.TriggerID == id {
			t.Fatal("acked stopped job", w)
		}
	}
}

func TestDiscoverRetryableReportAndDurableDeadline(t *testing.T) {
	ctx, p, s, restarted, clock, id := fixture(t)
	l, e := s.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	attempt := "retry-" + id
	if e = s.RegisterAttempt(ctx, l, attempt); e != nil {
		t.Fatal(e)
	}
	plan, e := s.SaveRetryable(ctx, l, attempt, &telegram.Error{Kind: telegram.RateLimited, RetryAfterSeconds: 20}, retryPolicy(t, 5), 0, 1)
	if e != nil {
		t.Fatal(e)
	}
	selected := func() bool {
		t.Helper()
		rows, e := restarted.Discover(ctx, "", 1000)
		if e != nil {
			t.Fatal(e)
		}
		for _, w := range rows {
			if w.TriggerID == id {
				return true
			}
		}
		return false
	}
	if !selected() {
		t.Fatal("retryable report lost")
	}
	if _, e = p.Exec(ctx, `UPDATE app.delivery_retryable_reports SET pending=false WHERE attempt_id=$1`, attempt); e != nil {
		t.Fatal(e)
	}
	if selected() {
		t.Fatal("retry before deadline")
	}
	clock.Store(plan.NextAttemptAt.UnixMicro())
	if !selected() {
		t.Fatal("due retry lost")
	}
	// A permanent current-fence block prevents automatic attempts after report ack.
	l, e = s.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.ParkBlocked(ctx, l, domain.PermanentFailure, 1); e != nil {
		t.Fatal(e)
	}
	if selected() {
		t.Fatal("permanent block selected")
	}
}
