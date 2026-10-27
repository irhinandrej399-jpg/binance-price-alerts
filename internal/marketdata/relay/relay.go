// Package relay drains the market-data outbox serially. The durable outbox is
// the queue; at most one payload is resident, with no prefetch or goroutine fanout.
package relay

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/publish"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/storage"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafka"
)

var ErrRunning = errors.New("market relay: one relay per process allowed")
var active atomic.Bool

type publisher interface {
	Publish(context.Context, []byte) error
	Close()
}
type factory func(context.Context) (publisher, error)
type timings struct{ pause, operation, lease time.Duration }

// Status is delivered synchronously, without payloads or an unbounded channel.
// Observers must return promptly. InFlight is 0 or 1. Bytes is the resident payload
// size, not total process RSS. Phase reports idle, claimed, committed or retry.
type Status struct {
	Phase           string
	InFlight, Bytes int
	Err             error
}

// Run owns the single fixed shard (all pairs) and a stable transactional ID.
// All replicas MUST use the same cfg.TransactionalID. A new epoch is established
// before any claim. ErrFenced stops this owner permanently; other failures retry
// with capped backoff. No caller-side queue is accepted. Cancellation leaves SQL
// work intact; its DB-clock lease makes it eligible for a subsequent run.
func Run(ctx context.Context, repo *storage.Repository, cfg kafka.Config, topic string, observe func(Status)) error {
	if repo == nil {
		return storage.ErrInvalid
	}
	return run(ctx, repo, func(c context.Context) (publisher, error) { return publish.New(c, cfg, topic) }, observe, timings{250 * time.Millisecond, 5 * time.Second, 30 * time.Second})
}

func run(ctx context.Context, repo *storage.Repository, open factory, observe func(Status), tm timings) error {
	if !active.CompareAndSwap(false, true) {
		return ErrRunning
	}
	defer active.Store(false)
	emit := func(s Status) {
		if observe != nil {
			observe(s)
		}
	}
	var p publisher
	defer func() {
		if p != nil {
			p.Close()
		}
	}()
	delay := tm.pause
	retry := func(err error) error {
		emit(Status{Phase: "retry", Err: err})
		if p != nil {
			p.Close()
			p = nil
		}
		if errors.Is(err, kafka.ErrFenced) || errors.Is(err, kafka.ErrConfig) || errors.Is(err, storage.ErrTooLarge) {
			return err
		}
		if err = wait(ctx, delay); err != nil {
			return err
		}
		delay = min(delay*2, 5*time.Second)
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if p == nil {
			c, cancel := context.WithTimeout(ctx, tm.operation)
			next, err := open(c)
			cancel()
			if err != nil {
				if err = retry(err); err != nil {
					return err
				}
				continue
			}
			p = next
		}
		c, cancel := context.WithTimeout(ctx, tm.operation)
		w, err := repo.Claim(c, tm.lease)
		cancel()
		if errors.Is(err, storage.ErrNotFound) {
			emit(Status{Phase: "idle"})
			if err = wait(ctx, tm.pause); err != nil {
				return err
			}
			continue
		}
		if err == nil {
			if len(w.Data) > storage.MaxEventBytes {
				err = storage.ErrTooLarge
			} else {
				emit(Status{Phase: "claimed", InFlight: 1, Bytes: len(w.Data)})
				c, cancel = context.WithTimeout(ctx, tm.operation)
				err = p.Publish(c, w.Data)
				cancel()
				if err == nil {
					c, cancel = context.WithTimeout(ctx, tm.operation)
					err = repo.MarkPublished(c, w.EventID, w.Token)
					cancel()
				}
			}
		}
		// Drop the only resident payload before any wait/re-initialization. A failed
		// claim/publish/mark is recovered through SQL, never through a RAM retry queue.
		w = storage.Work{}
		if err != nil {
			if err = retry(err); err != nil {
				return err
			}
			continue
		}
		delay = tm.pause
		emit(Status{Phase: "committed"})
	}
}
func wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
