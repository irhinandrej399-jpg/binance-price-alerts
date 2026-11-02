package retry_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/retry"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/telegram"
)

func policy(t *testing.T) *retry.Policy {
	t.Helper()
	p, err := retry.New(retry.Config{BaseDelay: 2 * time.Second, MaxDelay: 8 * time.Second, ConditionInterval: 3 * time.Second, MaxFailures: 5})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestTemporaryFailuresAndLimit(t *testing.T) {
	p := policy(t)
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC) // caller-owned fake clock
	failures := uint32(0)
	for i, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 4 * time.Second, 0} {
		kind := telegram.Server
		if i%2 == 0 {
			kind = telegram.RateLimited
		}
		plan, err := p.Retry(now, failures, fmt.Errorf("wrapped: %w", &telegram.Error{Kind: kind}), 0)
		if err != nil {
			t.Fatal(err)
		}
		if plan.Failures != failures+1 || plan.Exhausted != (i == 4) {
			t.Fatalf("attempt %d: %+v", i, plan)
		}
		if i == 4 {
			if !plan.NextAttemptAt.IsZero() {
				t.Fatal("exhausted job scheduled")
			}
			break
		}
		if !plan.NextAttemptAt.Equal(now.Add(want)) {
			t.Fatalf("attempt %d: %+v want delay %v", i, plan, want)
		}
		failures = plan.Failures
		// Many condition polls never consume or reset the Telegram failure budget.
		for j := 0; j < 20; j++ {
			wait := p.WaitCondition(now, failures)
			if wait.Failures != failures || wait.Exhausted || !wait.NextAttemptAt.Equal(now.Add(3*time.Second)) {
				t.Fatalf("wait: %+v %v", wait, err)
			}
			now = wait.NextAttemptAt
		}
		now = now.Add(time.Hour) // scheduling uses current time, not the previous deadline
	}
}

func TestRetryAfterAndJitter(t *testing.T) {
	p := policy(t)
	now := time.Unix(1000, 0)
	for _, tc := range []struct {
		seconds int64
		entropy uint64
		want    time.Duration
	}{
		{0, 0, time.Second}, {0, uint64(time.Second), 2 * time.Second},
		{1, uint64(time.Second), 2 * time.Second}, {90, 0, 90 * time.Second},
		{-1, 0, time.Second},
	} {
		plan, err := p.Retry(now, 0, &telegram.Error{Kind: telegram.RateLimited, RetryAfterSeconds: tc.seconds}, tc.entropy)
		if err != nil || !plan.NextAttemptAt.Equal(now.Add(tc.want)) {
			t.Fatalf("%+v: %+v %v", tc, plan, err)
		}
	}
	for _, failures := range []uint32{0, 1, 2, 3} {
		capDelay := 2 * time.Second
		for i := uint32(0); i < failures; i++ {
			capDelay *= 2
		}
		if capDelay > 8*time.Second {
			capDelay = 8 * time.Second
		}
		seen := map[time.Duration]bool{}
		for entropy := uint64(0); entropy < 100; entropy++ {
			plan, err := p.Retry(now, failures, &telegram.Error{Kind: telegram.Server}, entropy*123456789)
			delay := plan.NextAttemptAt.Sub(now)
			if err != nil || delay < capDelay/2 || delay > capDelay {
				t.Fatalf("jitter %v %v", delay, err)
			}
			seen[delay] = true
		}
		if len(seen) < 2 {
			t.Fatal("jitter is constant")
		}
	}
}

func TestUnsupportedAndOverflowDoNotSchedule(t *testing.T) {
	p := policy(t)
	for _, cause := range []error{nil, errors.New("transport"), &telegram.Error{Kind: telegram.Unknown, Timeout: true}, &telegram.Error{Kind: telegram.Forbidden}, &telegram.Error{Kind: telegram.NotSent}, &telegram.Error{Kind: telegram.Rejected}, &telegram.Error{Kind: telegram.RateLimited, RetryAfterSeconds: math.MaxInt64}} {
		plan, err := p.Retry(time.Now(), 0, cause, 0)
		if err == nil || !plan.NextAttemptAt.IsZero() || plan.Exhausted {
			t.Fatalf("unsafe schedule: %+v %v", plan, err)
		}
	}
	// Restored exhausted budget cannot be incremented, wrapped or scheduled again.
	for _, n := range []uint32{5, 6, math.MaxUint32} {
		plan, err := p.Retry(time.Now(), n, &telegram.Error{Kind: telegram.Server}, 0)
		if err != nil || !plan.Exhausted || plan.Failures != n || !plan.NextAttemptAt.IsZero() {
			t.Fatalf("limit: %+v %v", plan, err)
		}
	}
}

func TestConfigurationAndDurationBoundaries(t *testing.T) {
	for _, cfg := range []retry.Config{{}, {BaseDelay: time.Second, MaxDelay: time.Second, ConditionInterval: time.Second}, {BaseDelay: -1, MaxDelay: time.Second, ConditionInterval: time.Second, MaxFailures: 1}, {BaseDelay: 2, MaxDelay: 1, ConditionInterval: 1, MaxFailures: 1}, {BaseDelay: 1, MaxDelay: 1, ConditionInterval: 0, MaxFailures: 1}} {
		if _, err := retry.New(cfg); err == nil {
			t.Fatalf("accepted %+v", cfg)
		}
	}
	for _, d := range []time.Duration{1, 3, time.Duration(math.MaxInt64)} {
		p, err := retry.New(retry.Config{BaseDelay: d, MaxDelay: d, ConditionInterval: d, MaxFailures: math.MaxUint32})
		if err != nil {
			t.Fatal(err)
		}
		now := time.Unix(0, 0)
		plan, err := p.Retry(now, math.MaxUint32-2, &telegram.Error{Kind: telegram.Server}, math.MaxUint64)
		if err != nil || plan.NextAttemptAt.Sub(now) < d/2 || plan.NextAttemptAt.Sub(now) > d {
			t.Fatalf("duration overflow: %+v %v", plan, err)
		}
	}
}

func TestSenderResults(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
		delay  time.Duration
	}{
		{429, `{"ok":false,"error_code":429,"parameters":{"retry_after":12}}`, 12 * time.Second},
		{503, `unavailable`, time.Second},
	} {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			}))
			defer server.Close()
			sender, err := telegram.New(telegram.Config{Token: "test", Timeout: time.Second, BaseURL: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			_, outcome := sender.Send(context.Background(), 123, "message")
			now := time.Unix(1000, 0)
			plan, err := policy(t).Retry(now, 0, outcome, 0)
			if err != nil || calls != 1 || !plan.NextAttemptAt.Equal(now.Add(tc.delay)) {
				t.Fatalf("calls=%d plan=%+v err=%v", calls, plan, err)
			}
		})
	}
}

func TestWaitCannotReopenExhaustedBudget(t *testing.T) {
	plan := policy(t).WaitCondition(time.Now(), 5)
	if !plan.Exhausted || plan.Failures != 5 || !plan.NextAttemptAt.IsZero() {
		t.Fatalf("%+v", plan)
	}
}

func TestBackoffDoublingSaturatesWithoutOverflow(t *testing.T) {
	d := time.Duration(math.MaxInt64)
	p, err := retry.New(retry.Config{BaseDelay: d/2 + 1, MaxDelay: d, ConditionInterval: time.Second, MaxFailures: 100})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0)
	plan, err := p.Retry(now, 98, &telegram.Error{Kind: telegram.Server}, 0)
	if err != nil || plan.NextAttemptAt.Sub(now) != d/2+1 {
		t.Fatalf("%+v %v", plan, err)
	}
}
