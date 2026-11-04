// Package worker runs paced, bounded waves of notification work.
package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/jobs"
)

type Source interface {
	Discover(context.Context, string, int) ([]jobs.Work, error)
	ScheduleRecovery(context.Context, string, int64, time.Duration) error
}
type Handler interface {
	Handle(context.Context, string) error
}
type Config struct {
	Source           Source
	Handler          Handler
	Concurrency      int
	Interval         time.Duration // minimum pause after every wave, including errors/empty pages
	Timeout          time.Duration // discovery and each handler/recovery invocation
	WriteTimeout     time.Duration // each persistence retry, including shutdown
	RecoveryInterval time.Duration
	OnError          func(error) // optional concurrent observer; must not block
}
type pendingWrite interface {
	error
	Retry(context.Context) error
}
type knownWrite struct {
	id    string
	write pendingWrite
}

type Worker struct {
	cfg     Config
	wait    func(context.Context, time.Duration) error
	running atomic.Bool
	pending []knownWrite
}

func New(c Config) (*Worker, error) {
	if c.Source == nil || c.Handler == nil || c.Concurrency < 1 || c.Concurrency > 1000 || c.Interval <= 0 || c.Timeout <= 0 || c.WriteTimeout <= 0 || c.RecoveryInterval < time.Second || c.RecoveryInterval > time.Hour || c.RecoveryInterval%time.Microsecond != 0 {
		return nil, errors.New("invalid notification worker configuration")
	}
	return &Worker{cfg: c, wait: pause}, nil
}
func pause(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}
func (w *Worker) observe(e error) {
	if e != nil && w.cfg.OnError != nil {
		w.cfg.OnError(e)
	}
}

// Run owns at most Concurrency calls, including pending writes. It waits for
// each wave before advancing the keyset cursor, so a workflow is never handled
// twice concurrently by this worker. It may be run again after it returns.
// Dependencies must honor context deadlines; this does not forcibly stop HTTP.
func (w *Worker) Run(ctx context.Context) error {
	if !w.running.CompareAndSwap(false, true) {
		return errors.New("notification worker already running")
	}
	defer w.running.Store(false)
	if e := ctx.Err(); e != nil {
		return e
	}
	// A failed final write remains owned by this Worker across Run calls.
	for len(w.pending) > 0 {
		p := w.pending[0]
		if e := w.persist(ctx, p.write); e != nil {
			return errors.Join(ctx.Err(), fmt.Errorf("workflow %s: %w", p.id, e))
		}
		w.pending = w.pending[1:]
	}
	after := ""
	for {
		if e := ctx.Err(); e != nil {
			return e
		}
		scan, cancel := context.WithTimeout(ctx, w.cfg.Timeout)
		work, e := w.cfg.Source.Discover(scan, after, w.cfg.Concurrency)
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var final error
		if e != nil {
			w.observe(e)
		} else {
			if len(work) > w.cfg.Concurrency {
				return errors.New("discovery exceeded worker limit")
			}
			if len(work) == 0 {
				after = ""
			} else {
				after = work[len(work)-1].TriggerID
			}
			var wg sync.WaitGroup
			var mu sync.Mutex
			seen := map[string]bool{}
			for _, item := range work {
				if ctx.Err() != nil {
					break
				}
				if seen[item.TriggerID] {
					continue
				}
				seen[item.TriggerID] = true
				wg.Go(func() {
					if ctx.Err() != nil {
						return
					}
					call, stop := context.WithTimeout(ctx, w.cfg.Timeout)
					var err error
					if item.RecoveryFence > 0 {
						err = w.cfg.Source.ScheduleRecovery(call, item.TriggerID, item.RecoveryFence, w.cfg.RecoveryInterval)
					} else {
						err = w.cfg.Handler.Handle(call, item.TriggerID)
					}
					stop()
					var pending pendingWrite
					if errors.As(err, &pending) {
						err = w.persist(ctx, pending)
					} else {
						w.observe(err)
						err = nil
					}
					if err != nil {
						mu.Lock()
						w.pending = append(w.pending, knownWrite{id: item.TriggerID, write: pending})
						final = errors.Join(final, fmt.Errorf("workflow %s: %w", item.TriggerID, err))
						mu.Unlock()
					}
				})
			}
			wg.Wait()
		}
		if ctx.Err() != nil {
			return errors.Join(ctx.Err(), final)
		}
		if final != nil {
			return final
		}
		if e = w.wait(ctx, w.cfg.Interval); e != nil {
			return e
		}
	}
}

// Retain the original object across bounded retries. No new Handle may replace
// a known outcome. Cancellation grants one final fresh, bounded write context;
// failure returns the object in the error chain for the caller to retain.
func (w *Worker) persist(ctx context.Context, p pendingWrite) error {
	for {
		if ctx.Err() == nil {
			_ = w.wait(ctx, w.cfg.Interval)
		}
		stopping := ctx.Err() != nil
		fresh, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.cfg.WriteTimeout)
		e := p.Retry(fresh)
		cancel()
		if e == nil {
			return nil
		}
		w.observe(e)
		if stopping {
			return errors.Join(p, e)
		}
	}
}
