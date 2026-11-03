package jobs

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/retry"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/telegram"
)

func retryPolicy(t *testing.T, max uint32) *retry.Policy {
	t.Helper()
	p, e := retry.New(retry.Config{BaseDelay: 4 * time.Second, MaxDelay: time.Minute, ConditionInterval: time.Second, MaxFailures: max})
	if e != nil {
		t.Fatal(e)
	}
	return p
}

func TestRetryableReplayRestartBudgetAndWaits(t *testing.T) {
	ctx, p, a, b, clock, id := fixture(t)
	policy := retryPolicy(t, 5)
	l, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	attempt := "api-" + id
	if e = a.RegisterAttempt(ctx, l, attempt); e != nil {
		t.Fatal(e)
	}
	cause := &telegram.Error{Kind: telegram.RateLimited, RetryAfterSeconds: 10}
	plan, e := a.SaveRetryable(ctx, l, attempt, cause, policy, 0, 7)
	if e != nil || plan.Failures != 1 || !plan.NextAttemptAt.Equal(time.UnixMicro(clock.Load()).Add(10*time.Second)) {
		t.Fatal(plan, e)
	}
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			s := a
			if i%2 == 0 {
				s = b
			}
			got, e := s.SaveRetryable(ctx, l, attempt, cause, policy, 999, 7)
			if e != nil || got != plan {
				t.Error(got, e)
			}
		})
	}
	wg.Wait()
	if _, e = p.Exec(ctx, `UPDATE app.delivery_retryable_reports SET pending=false WHERE attempt_id=$1`, attempt); e != nil {
		t.Fatal(e)
	}
	restarted, _ := New(b.pool, time.Second)
	restarted.now = b.now
	// Expired original deadline and a changed policy/entropy cannot recalculate replay.
	clock.Store(plan.NextAttemptAt.UnixMicro())
	got, e := restarted.SaveRetryable(ctx, l, attempt, cause, retryPolicy(t, 1), 123, 7)
	if e != nil || got != plan {
		t.Fatal(got, e)
	}
	var pending bool
	if e = p.QueryRow(ctx, `SELECT pending FROM app.delivery_retryable_reports WHERE attempt_id=$1`, attempt).Scan(&pending); e != nil || pending {
		t.Fatal(pending, e)
	}
	fresh, e := restarted.Claim(ctx, id)
	if e != nil || fresh.Failures != 1 || fresh.PossibleDuplicate {
		t.Fatal(fresh, e)
	}
	// wait_condition and limiter waits retain the durable budget.
	for _, reason := range []WaitReason{WaitCondition, WaitLimiter} {
		next := time.UnixMicro(clock.Load()).Add(time.Second)
		if e = b.Park(ctx, fresh, reason, next); e != nil {
			t.Fatal(e)
		}
		clock.Store(next.UnixMicro())
		fresh, e = b.Claim(ctx, id)
		if e != nil || fresh.Failures != 1 {
			t.Fatal(fresh, e)
		}
	}
	if e = b.RegisterAttempt(ctx, fresh, "unknown-"+id); e != nil {
		t.Fatal(e)
	}
	next := time.UnixMicro(clock.Load()).Add(time.Second)
	if e = b.SaveUncertain(ctx, fresh, "unknown-"+id, next); e != nil {
		t.Fatal(e)
	}
	clock.Store(next.UnixMicro())
	fresh, e = b.Claim(ctx, id)
	if e != nil || fresh.Failures != 1 || !fresh.PossibleDuplicate {
		t.Fatal(fresh, e)
	}
	if e = b.RegisterAttempt(ctx, fresh, "server-"+id); e != nil {
		t.Fatal(e)
	}
	plan, e = b.SaveRetryable(ctx, fresh, "server-"+id, &telegram.Error{Kind: telegram.Server}, policy, 1, 7)
	if e != nil || plan.Failures != 2 || plan.Exhausted {
		t.Fatal(plan, e)
	}
	if _, e = b.Claim(ctx, id); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
}

func TestRetryableExpiredRecoveryAndLateSuccess(t *testing.T) {
	ctx, p, a, b, clock, id := fixture(t)
	l, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	attempt := "api-" + id
	if e = a.RegisterAttempt(ctx, l, attempt); e != nil {
		t.Fatal(e)
	}
	clock.Store(l.ExpiresAt.UnixMicro())
	cause := &telegram.Error{Kind: telegram.Server}
	plan, e := a.SaveRetryable(ctx, l, attempt, cause, retryPolicy(t, 3), 0, 8)
	if e != nil {
		t.Fatal(e)
	}
	// Historical evidence never changes even the expired lease.
	var active bool
	if e = p.QueryRow(ctx, `SELECT lease_id=$2 AND state='leased' FROM app.delivery_jobs WHERE trigger_id=$1`, id, l.ID).Scan(&active); e != nil || !active {
		t.Fatal(active, e)
	}
	if _, e = b.Claim(ctx, id); !errors.Is(e, ErrUnavailable) {
		t.Fatalf("schedule bypass/unresolved: %v", e)
	}
	clock.Store(plan.NextAttemptAt.UnixMicro())
	fresh, e := b.Claim(ctx, id)
	if e != nil || fresh.Failures != 1 {
		t.Fatal(fresh, e)
	}
	if e = a.SaveSuccess(ctx, l, attempt, 44); e != nil {
		t.Fatal(e)
	}
	if e = b.RegisterAttempt(ctx, fresh, "new-"+id); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	if _, e = b.Claim(ctx, id); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	if e = p.QueryRow(ctx, `SELECT lease_id=$2 AND state='leased' FROM app.delivery_jobs WHERE trigger_id=$1`, id, fresh.ID).Scan(&active); e != nil || !active {
		t.Fatal(active, e)
	}
}

func TestRetryableHistoricalOutcome(t *testing.T) {
	for _, state := range []string{"takeover", "cancelled", "success"} {
		t.Run(state, func(t *testing.T) {
			ctx, p, a, b, clock, id := fixture(t)
			old, e := a.Claim(ctx, id)
			if e != nil {
				t.Fatal(e)
			}
			attempt := "api-" + id
			if e = a.RegisterAttempt(ctx, old, attempt); e != nil {
				t.Fatal(e)
			}
			next := time.UnixMicro(clock.Load()).Add(time.Second)
			if e = a.SaveUncertain(ctx, old, attempt, next); e != nil {
				t.Fatal(e)
			}
			clock.Store(next.UnixMicro())
			fresh, e := b.Claim(ctx, id)
			if e != nil {
				t.Fatal(e)
			}
			if state == "cancelled" {
				if _, e = p.Exec(ctx, `UPDATE app.delivery_jobs SET state='cancelled',lease_id=NULL,lease_expires_at=NULL WHERE trigger_id=$1`, id); e != nil {
					t.Fatal(e)
				}
			}
			if state == "success" {
				if e = b.SaveSuccess(ctx, old, attempt, 1); e != nil {
					t.Fatal(e)
				}
			}
			var before string
			if e = p.QueryRow(ctx, `SELECT row_to_json(j)::text FROM app.delivery_jobs j WHERE trigger_id=$1`, id).Scan(&before); e != nil {
				t.Fatal(e)
			}
			_, e = a.SaveRetryable(ctx, old, attempt, &telegram.Error{Kind: telegram.Server}, retryPolicy(t, 4), 0, 9)
			if e != nil {
				t.Fatal(e)
			}
			var after string
			if e = p.QueryRow(ctx, `SELECT row_to_json(j)::text FROM app.delivery_jobs j WHERE trigger_id=$1`, id).Scan(&after); e != nil || before != after {
				t.Fatal(before, after, e)
			}
			var n int
			if e = p.QueryRow(ctx, `SELECT count(*) FROM app.delivery_retryable_reports WHERE attempt_id=$1`, attempt).Scan(&n); e != nil || n != 1 {
				t.Fatal(n, e)
			}
			if state == "takeover" {
				if e = b.RegisterAttempt(ctx, fresh, "new-"+id); e != nil {
					t.Fatal(e)
				}
			}
		})
	}
}

func TestRetryableExhaustionAtomicAndRecovery(t *testing.T) {
	for _, expired := range []bool{false, true} {
		name := "live"
		if expired {
			name = "expired"
		}
		t.Run(name, func(t *testing.T) {
			ctx, p, a, b, clock, id := fixture(t)
			policy := retryPolicy(t, 2)
			for i := range 2 {
				l, e := a.Claim(ctx, id)
				if e != nil {
					t.Fatal(e)
				}
				attempt := l.ID
				if e = a.RegisterAttempt(ctx, l, attempt); e != nil {
					t.Fatal(e)
				}
				if expired && i == 1 {
					clock.Store(l.ExpiresAt.UnixMicro())
				}
				plan, e := a.SaveRetryable(ctx, l, attempt, &telegram.Error{Kind: telegram.Server}, policy, 0, 99)
				if e != nil || plan.Failures != uint32(i+1) || plan.Exhausted != (i == 1) {
					t.Fatal(plan, e)
				}
				if i == 0 {
					clock.Store(plan.NextAttemptAt.UnixMicro())
					continue
				}
				// No separate SaveBlocked call: crash immediately after SaveRetryable is safe.
				clock.Add(int64(time.Hour / time.Microsecond))
				if _, e = b.Claim(ctx, id); !errors.Is(e, ErrUnavailable) {
					t.Fatal(e)
				}
				var pending bool
				var reason string
				var version int64
				if e = p.QueryRow(ctx, `SELECT r.pending,b.reason,b.binding_version FROM app.delivery_blocks b JOIN app.delivery_blocked_reports r USING(attempt_id) WHERE attempt_id=$1`, attempt).Scan(&pending, &reason, &version); e != nil || !pending || reason != "attempts_exhausted" || version != 99 {
					t.Fatal(pending, reason, version, e)
				}
				if !plan.NextAttemptAt.IsZero() {
					t.Fatal(plan)
				}
			}
		})
	}
}

func TestRetryableRollback(t *testing.T) {
	for _, max := range []uint32{1, 3} {
		name := "retry"
		if max == 1 {
			name = "exhausted"
		}
		t.Run(name, func(t *testing.T) {
			ctx, p, a, _, _, id := fixture(t)
			l, e := a.Claim(ctx, id)
			if e != nil {
				t.Fatal(e)
			}
			attempt := "api-" + id
			if e = a.RegisterAttempt(ctx, l, attempt); e != nil {
				t.Fatal(e)
			}
			if _, e = a.SaveRetryable(ctx, l, attempt, &telegram.Error{Kind: telegram.Server}, retryPolicy(t, max), 0, 1); e == nil {
				t.Fatal("commit failure accepted")
			}
			var outcomes, reports, blocks int
			var active bool
			if e = p.QueryRow(ctx, `SELECT (SELECT count(*) FROM app.delivery_retryables WHERE attempt_id=$2),(SELECT count(*) FROM app.delivery_retryable_reports WHERE attempt_id=$2),(SELECT count(*) FROM app.delivery_blocks WHERE attempt_id=$2),state='leased' AND lease_id=$3 FROM app.delivery_jobs WHERE trigger_id=$1`, id, attempt, l.ID).Scan(&outcomes, &reports, &blocks, &active); e != nil || outcomes != 0 || reports != 0 || blocks != 0 || !active {
				t.Fatal(outcomes, reports, blocks, active, e)
			}
		})
	}
}

func TestRetryableValidationConflictAndUncertain(t *testing.T) {
	ctx, p, a, _, clock, id := fixture(t)
	l, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	attempt := "api-" + id
	policy := retryPolicy(t, 3)
	cause := &telegram.Error{Kind: telegram.Server}
	if _, e = a.SaveRetryable(ctx, l, attempt, cause, policy, 0, 1); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	if e = a.RegisterAttempt(ctx, l, attempt); e != nil {
		t.Fatal(e)
	}
	forged := l
	forged.Fence++
	if _, e = a.SaveRetryable(ctx, forged, attempt, cause, policy, 0, 1); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	for _, kind := range []telegram.Kind{telegram.Unknown, telegram.Forbidden, telegram.NotSent} {
		if _, e = a.SaveRetryable(ctx, l, attempt, &telegram.Error{Kind: kind}, policy, 0, 1); e == nil {
			t.Fatal(kind)
		}
	}
	if _, e = a.SaveRetryable(ctx, l, attempt, cause, nil, 0, 1); e == nil {
		t.Fatal("nil policy")
	}
	if _, e = a.SaveRetryable(ctx, l, attempt, cause, policy, 0, -1); e == nil {
		t.Fatal("negative binding")
	}
	if _, e = a.SaveRetryable(ctx, l, attempt, cause, policy, 0, 1); e != nil {
		t.Fatal(e)
	}
	for _, changed := range []*telegram.Error{{Kind: telegram.RateLimited}, {Kind: telegram.Server, RetryAfterSeconds: 2}} {
		if _, e = a.SaveRetryable(ctx, l, attempt, changed, policy, 0, 1); !errors.Is(e, ErrConflict) {
			t.Fatal(e)
		}
	}
	if _, e = a.SaveRetryable(ctx, l, attempt, cause, policy, 0, 2); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	if e = a.SaveUncertain(ctx, l, attempt, time.UnixMicro(clock.Load()).Add(time.Second)); e != nil {
		t.Fatal(e)
	}
	var n int
	if e = p.QueryRow(ctx, `SELECT count(*) FROM app.delivery_uncertainties WHERE attempt_id=$1`, attempt).Scan(&n); e != nil || n != 0 {
		t.Fatal(n, e)
	}
}

func TestRetryableHTTPAndReleasedFenceSchedule(t *testing.T) {
	for _, status := range []int{429, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			ctx, p, a, b, clock, id := fixture(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				fmt.Fprint(w, `{"ok":false,"parameters":{"retry_after":12},"description":"private-text"}`)
			}))
			defer server.Close()
			client, e := telegram.New(telegram.Config{Token: "fixture", Timeout: time.Second, BaseURL: server.URL})
			if e != nil {
				t.Fatal(e)
			}
			l, e := a.Claim(ctx, id)
			if e != nil {
				t.Fatal(e)
			}
			attempt := "api-" + id
			if e = a.RegisterAttempt(ctx, l, attempt); e != nil {
				t.Fatal(e)
			}
			_, cause := client.Send(ctx, 1, "fixture")
			if cause == nil {
				t.Fatal("HTTP fixture unexpectedly succeeded")
			}
			// Recovery may have recorded uncertain before the definitive HTTP result.
			uncertainNext := time.UnixMicro(clock.Load()).Add(time.Second)
			if e = b.SaveUncertain(ctx, l, attempt, uncertainNext); e != nil {
				t.Fatal(e)
			}
			plan, e := a.SaveRetryable(ctx, l, attempt, cause, retryPolicy(t, 4), 1, 7)
			if e != nil {
				t.Fatal(e)
			}
			clock.Store(uncertainNext.UnixMicro())
			if _, e = b.Claim(ctx, id); !errors.Is(e, ErrUnavailable) {
				t.Fatal("released-fence retry deadline bypassed", e)
			}
			clock.Store(plan.NextAttemptAt.UnixMicro())
			fresh, e := b.Claim(ctx, id)
			if e != nil || fresh.Failures != 1 {
				t.Fatal(fresh, e)
			}
			var denied bool
			if e = p.QueryRow(ctx, `SELECT NOT has_table_privilege(current_user,'app.delivery_retryables','UPDATE') AND NOT has_table_privilege(current_user,'app.delivery_retryables','DELETE')`).Scan(&denied); e != nil || !denied {
				t.Fatal(denied, e)
			}
		})
	}
}
