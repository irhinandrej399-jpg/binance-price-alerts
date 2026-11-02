// Package retry calculates retry deadlines without sleeping, sending or storing jobs.
package retry

import (
	"errors"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/telegram"
)

// Config is shared by all workers. MaxFailures counts temporary Telegram errors,
// including the first failed send, rather than condition polls or total job age.
// All intervals and MaxFailures must be positive; MaxDelay >= BaseDelay.
type Config struct {
	BaseDelay         time.Duration
	MaxDelay          time.Duration
	ConditionInterval time.Duration
	MaxFailures       uint32
}

// Policy is immutable and safe for concurrent use. The caller supplies its clock
// and fresh random entropy for each new outcome; replay restores the stored plan.
type Policy struct{ cfg Config }

func New(cfg Config) (*Policy, error) {
	if cfg.BaseDelay <= 0 || cfg.MaxDelay < cfg.BaseDelay || cfg.ConditionInterval <= 0 || cfg.MaxFailures == 0 {
		return nil, errors.New("invalid retry configuration")
	}
	return &Policy{cfg: cfg}, nil
}

// Plan must be persisted with the attempt outcome under the worker's fencing
// token before releasing the lease. Exhausted has no deadline: automatic sends
// stop and the API must be informed of delivery_blocked; the slot stays occupied.
// A nonzero deadline only makes work eligible for a NEW API admission after both
// limiters. It never permits reusing a previous attempt ID or send authorization.
type Plan struct {
	Failures      uint32
	NextAttemptAt time.Time
	Exhausted     bool
}

// Retry handles only rate limits and server errors from one completed HTTP
// attempt. failures is the durable count BEFORE this outcome. Applying an outcome
// twice must be prevented by the caller's attempt ID transaction. Unknown outcomes,
// permanent errors and local NotSent errors require their own policies.
// Backoff ceiling is min(BaseDelay*2^failures, MaxDelay), with equal jitter in
// [ceil(ceiling/2), ceiling]. Positive retry_after is a lower bound, even above
// MaxDelay. An unrepresentable retry_after fails explicitly without a deadline.
func (p *Policy) Retry(now time.Time, failures uint32, cause error, entropy uint64) (Plan, error) {
	var e *telegram.Error
	if !errors.As(cause, &e) || e == nil || (e.Kind != telegram.RateLimited && e.Kind != telegram.Server) {
		return Plan{}, errors.New("unsupported retry outcome")
	}
	if failures >= p.cfg.MaxFailures {
		return Plan{Failures: failures, Exhausted: true}, nil
	}
	plan := Plan{Failures: failures + 1}
	if plan.Failures == p.cfg.MaxFailures {
		plan.Exhausted = true
		return plan, nil
	}
	ceiling := p.cfg.BaseDelay
	// Saturating doubling is bounded by the duration width, not the failure count.
	for i := uint32(0); i < failures && ceiling < p.cfg.MaxDelay; i++ {
		if ceiling > p.cfg.MaxDelay/2 {
			ceiling = p.cfg.MaxDelay
		} else {
			ceiling *= 2
		}
	}
	lower := ceiling/2 + ceiling%2
	delay := lower + time.Duration(entropy%uint64(ceiling-lower+1))
	if e.Kind == telegram.RateLimited && e.RetryAfterSeconds > 0 {
		const maxSeconds = int64((1<<63 - 1) / time.Second)
		if e.RetryAfterSeconds > maxSeconds {
			return Plan{}, errors.New("retry_after exceeds supported duration")
		}
		if wait := time.Duration(e.RetryAfterSeconds) * time.Second; wait > delay {
			delay = wait
		}
	}
	plan.NextAttemptAt = now.Add(delay)
	return plan, nil
}

// WaitCondition parks the same workflow with an independent fixed, configurable
// poll interval. It neither consumes nor resets the Telegram error budget. Even
// a restored exhausted budget cannot restart automatic sending through this path.
func (p *Policy) WaitCondition(now time.Time, failures uint32) Plan {
	if failures >= p.cfg.MaxFailures {
		return Plan{Failures: failures, Exhausted: true}
	}
	return Plan{Failures: failures, NextAttemptAt: now.Add(p.cfg.ConditionInterval)}
}
