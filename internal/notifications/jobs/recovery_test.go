package jobs

import (
	"errors"
	"sync"
	"testing"
	"time"

	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/telegram"
)

func TestChannelRecoveryScheduleRestartAndRepark(t *testing.T) {
	ctx, p, a, b, clock, id := fixture(t)
	l, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.ParkBlocked(ctx, l, domain.ChannelUnavailable, 4); e != nil {
		t.Fatal(e)
	}
	next := time.UnixMicro(clock.Load()).Add(time.Minute)
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			if e := b.ScheduleRecovery(ctx, id, l.Fence, time.Minute); e != nil {
				t.Error(e)
			}
		})
	}
	wg.Wait()
	clock.Add(1)
	if e = a.ScheduleRecovery(ctx, id, l.Fence, time.Hour); e != nil {
		t.Fatal(e)
	} // first commit wins
	var saved time.Time
	if e = p.QueryRow(ctx, `SELECT next_check_at FROM app.delivery_jobs WHERE trigger_id=$1`, id).Scan(&saved); e != nil || !saved.Equal(next) {
		t.Fatal(saved, e)
	}
	if _, e = b.Claim(ctx, id); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	clock.Store(next.UnixMicro())
	recovered, e := b.Claim(ctx, id)
	if e != nil || recovered.Fence != l.Fence+1 || recovered.Failures != 0 || string(recovered.Body) != "request" {
		t.Fatal(recovered, e)
	}
	if e = a.ScheduleRecovery(ctx, id, l.Fence, time.Minute); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	// Same binding / missing channel: API returns blocked, never a send permission.
	if e = b.ParkBlocked(ctx, recovered, domain.ChannelUnavailable, 4); e != nil {
		t.Fatal(e)
	}
	if e = b.ScheduleRecovery(ctx, id, recovered.Fence, 2*time.Minute); e != nil {
		t.Fatal(e)
	}
	clock.Add(int64(2 * time.Minute / time.Microsecond))
	recovered, e = a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	// New binding with false predicate: handler uses ordinary wait_condition.
	if e = a.Park(ctx, recovered, WaitCondition, time.UnixMicro(clock.Load()).Add(time.Second)); e != nil {
		t.Fatal(e)
	}
	clock.Add(int64(time.Second / time.Microsecond))
	recovered, e = b.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	// A true current API result would allow a newly registered attempt on same job.
	if e = b.RegisterAttempt(ctx, recovered, "new-"+id); e != nil {
		t.Fatal(e)
	}
	var jobs, attempts int
	if e = p.QueryRow(ctx, `SELECT count(*),(SELECT count(*) FROM app.delivery_attempts WHERE trigger_id=$1) FROM app.delivery_jobs WHERE trigger_id=$1`, id).Scan(&jobs, &attempts); e != nil || jobs != 1 || attempts != 1 {
		t.Fatal(jobs, attempts, e)
	}
}

func TestRecoveryHistoricalBlockAndLateOldFailure(t *testing.T) {
	ctx, p, a, b, clock, id := fixture(t)
	old, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	attempt := "old-" + id
	if e = a.RegisterAttempt(ctx, old, attempt); e != nil {
		t.Fatal(e)
	}
	if e = a.SaveUncertain(ctx, old, attempt, time.UnixMicro(clock.Load()).Add(time.Second)); e != nil {
		t.Fatal(e)
	}
	if e = a.SaveBlocked(ctx, old, attempt, domain.ChannelUnavailable, 7); e != nil {
		t.Fatal(e)
	}
	if e = b.ScheduleRecovery(ctx, id, old.Fence, time.Minute); e != nil {
		t.Fatal(e)
	}
	clock.Add(int64(time.Minute / time.Microsecond))
	fresh, e := b.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.SaveBlocked(ctx, old, attempt, domain.ChannelUnavailable, 7); e != nil {
		t.Fatal(e)
	}
	if e = a.ScheduleRecovery(ctx, id, old.Fence, time.Minute); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	if e = b.Park(ctx, fresh, WaitCondition, time.UnixMicro(clock.Load()).Add(time.Second)); e != nil {
		t.Fatal(e)
	}
	var state string
	if e = p.QueryRow(ctx, `SELECT state FROM app.delivery_jobs WHERE trigger_id=$1`, id).Scan(&state); e != nil || state != "wait_condition" {
		t.Fatal(state, e)
	}
}

func TestRecoveryRejectsOtherReasonsTerminalSuccessAndLiveLease(t *testing.T) {
	for _, reason := range []domain.BlockReason{domain.PermanentFailure, domain.AttemptsExhausted, domain.ChannelUnavailable} {
		t.Run(string(reason), func(t *testing.T) {
			ctx, p, a, b, clock, id := fixture(t)
			l, e := a.Claim(ctx, id)
			if e != nil {
				t.Fatal(e)
			}
			if e = a.ScheduleRecovery(ctx, id, l.Fence, time.Minute); !errors.Is(e, ErrUnavailable) {
				t.Fatal(e)
			}
			if e = a.ParkBlocked(ctx, l, reason, 1); e != nil {
				t.Fatal(e)
			}
			e = b.ScheduleRecovery(ctx, id, l.Fence, time.Minute)
			if reason != domain.ChannelUnavailable {
				if !errors.Is(e, ErrUnavailable) {
					t.Fatal(e)
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			if _, e = p.Exec(ctx, `UPDATE app.delivery_jobs SET state='cancelled' WHERE trigger_id=$1`, id); e != nil {
				t.Fatal(e)
			}
			clock.Add(int64(time.Hour / time.Microsecond))
			if e = b.ScheduleRecovery(ctx, id, l.Fence, time.Minute); !errors.Is(e, ErrUnavailable) {
				t.Fatal(e)
			}
			if _, e = b.Claim(ctx, id); !errors.Is(e, ErrUnavailable) {
				t.Fatal(e)
			}
		})
	}
	ctx, _, a, b, clock, id := fixture(t)
	l, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	attempt := "success-" + id
	if e = a.RegisterAttempt(ctx, l, attempt); e != nil {
		t.Fatal(e)
	}
	if e = a.SaveBlocked(ctx, l, attempt, domain.ChannelUnavailable, 1); e != nil {
		t.Fatal(e)
	}
	if e = a.ScheduleRecovery(ctx, id, l.Fence, time.Minute); e != nil {
		t.Fatal(e)
	}
	if e = a.SaveSuccess(ctx, l, attempt, 123); e != nil {
		t.Fatal(e)
	}
	clock.Add(int64(time.Hour / time.Microsecond))
	if e = b.ScheduleRecovery(ctx, id, l.Fence, time.Minute); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	if _, e = b.Claim(ctx, id); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
}

func TestRecoveryClaimCrashAndConcurrentTakeover(t *testing.T) {
	ctx, _, a, b, clock, id := fixture(t)
	l, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.ParkBlocked(ctx, l, domain.ChannelUnavailable, 0); e != nil {
		t.Fatal(e)
	}
	if e = a.ScheduleRecovery(ctx, id, l.Fence, time.Second); e != nil {
		t.Fatal(e)
	}
	clock.Add(int64(time.Second / time.Microsecond))
	check, e := b.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	// No API/HTTP took place; process dies immediately after recovery claim.
	clock.Add(int64(time.Second / time.Microsecond))
	var wg sync.WaitGroup
	wins := make(chan Lease, 20)
	for range 20 {
		wg.Go(func() {
			got, e := a.Claim(ctx, id)
			if e == nil {
				wins <- got
			} else if !errors.Is(e, ErrUnavailable) {
				t.Error(e)
			}
		})
	}
	wg.Wait()
	close(wins)
	var winner Lease
	n := 0
	for got := range wins {
		winner = got
		n++
	}
	if n != 1 || winner.Fence != check.Fence+1 {
		t.Fatalf("takeover winners=%d lease=%+v", n, winner)
	}
	if e = b.ParkBlocked(ctx, check, domain.ChannelUnavailable, 0); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
}

func TestRecoveryScheduleValidation(t *testing.T) {
	for _, interval := range []time.Duration{0, -1, time.Second - time.Microsecond, time.Hour + time.Microsecond, time.Second + time.Nanosecond} {
		if e := (&Store{}).ScheduleRecovery(t.Context(), "job", 1, interval); e == nil {
			t.Fatal(interval)
		}
	}
	for _, args := range []struct {
		id    string
		fence int64
	}{{"", 1}, {" ", 1}, {"job", 0}, {"job", -1}} {
		if e := (&Store{}).ScheduleRecovery(t.Context(), args.id, args.fence, time.Minute); e == nil {
			t.Fatal(args)
		}
	}
}

func TestRecoveryScheduleRollback(t *testing.T) {
	ctx, p, a, b, clock, id := fixture(t)
	l, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	attempt := "api-" + id
	if e = a.RegisterAttempt(ctx, l, attempt); e != nil {
		t.Fatal(e)
	}
	if e = a.SaveBlocked(ctx, l, attempt, domain.ChannelUnavailable, 2); e != nil {
		t.Fatal(e)
	}
	if e = a.ScheduleRecovery(ctx, id, l.Fence, time.Second); e == nil {
		t.Fatal("injected commit failure accepted")
	}
	var unchanged bool
	if e = p.QueryRow(ctx, `SELECT next_check_at IS NULL AND blocked_reason='channel_unavailable' AND blocked_binding_version=2 AND lease_fence=1 AND state='blocked' FROM app.delivery_jobs WHERE trigger_id=$1`, id).Scan(&unchanged); e != nil || !unchanged {
		t.Fatal(unchanged, e)
	}
	clock.Add(int64(time.Minute / time.Microsecond))
	if _, e = b.Claim(ctx, id); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
}

func TestRecoveryClaimRollback(t *testing.T) {
	ctx, p, a, b, clock, id := fixture(t)
	l, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.ParkBlocked(ctx, l, domain.ChannelUnavailable, 2); e != nil {
		t.Fatal(e)
	}
	if e = a.ScheduleRecovery(ctx, id, l.Fence, time.Second); e != nil {
		t.Fatal(e)
	}
	clock.Add(int64(time.Second / time.Microsecond))
	if _, e = b.Claim(ctx, id); e == nil {
		t.Fatal("injected commit failure accepted")
	}
	var unchanged bool
	if e = p.QueryRow(ctx, `SELECT next_check_at IS NOT NULL AND blocked_reason='channel_unavailable' AND blocked_binding_version=2 AND lease_id IS NULL AND lease_fence=1 AND state='blocked' AND (SELECT count(*) FROM app.delivery_claims WHERE trigger_id=$1)=1 FROM app.delivery_jobs WHERE trigger_id=$1`, id).Scan(&unchanged); e != nil || !unchanged {
		t.Fatal(unchanged, e)
	}
}

func TestRecoveryPreservesFailureBudgetAndReports(t *testing.T) {
	ctx, p, a, b, clock, id := fixture(t)
	l, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	attempt := "api-" + id
	if e = a.RegisterAttempt(ctx, l, attempt); e != nil {
		t.Fatal(e)
	}
	plan, e := a.SaveRetryable(ctx, l, attempt, &telegram.Error{Kind: telegram.Server}, retryPolicy(t, 5), 0, 3)
	if e != nil {
		t.Fatal(e)
	}
	// A stronger late channel failure of the same attempt, after lease release.
	if e = a.SaveBlocked(ctx, l, attempt, domain.ChannelUnavailable, 3); e != nil {
		t.Fatal(e)
	}
	if e = a.ScheduleRecovery(ctx, id, l.Fence, time.Hour); e != nil {
		t.Fatal(e)
	}
	clock.Add(int64(time.Hour / time.Microsecond))
	fresh, e := b.Claim(ctx, id)
	if e != nil || fresh.Failures != plan.Failures || fresh.Failures != 1 {
		t.Fatal(fresh, e)
	}
	var reports int
	if e = p.QueryRow(ctx, `SELECT (SELECT count(*) FROM app.delivery_retryable_reports WHERE attempt_id=$1 AND pending)+(SELECT count(*) FROM app.delivery_blocked_reports WHERE attempt_id=$1 AND pending)`, attempt).Scan(&reports); e != nil || reports != 2 {
		t.Fatal(reports, e)
	}
}
