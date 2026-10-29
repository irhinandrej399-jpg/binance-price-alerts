// Package priceconnection reconciles a registered price stream with Latest.
// It owns ephemeral price readiness, never durable checkpoints or candidates.
package priceconnection

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/latest"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricefeed"
)

var (
	ErrConfig      = errors.New("priceconnection: invalid configuration")
	ErrUnavailable = errors.New("priceconnection: latest quote unavailable")
	ErrQuote       = errors.New("priceconnection: invalid quote")
)

// Registration is implemented by pricefeed.Registration. Close must release
// registration resources, and Next/Latest must respect context cancellation.
type Registration interface {
	Next(context.Context) (pricefeed.Update, error)
	TryNext() (pricefeed.Update, bool, error)
	Done() <-chan struct{}
	Err() error
	Close()
}
type RegisterFunc func(context.Context, string, int32) (Registration, error)
type QuoteSource interface {
	Latest(context.Context, string) (*pb.LatestResponse, error)
}

// Connection has one serialized Next reader; Current and Close may be concurrent.
// A successful Current/Next is price readiness at that instant, not permission
// to deliver. A candidate consumer must still use the canonical API check.
type Connection struct {
	reg     Registration
	ctx     context.Context
	cancel  context.CancelFunc
	stopped chan struct{}
	mu      sync.Mutex
	nextMu  sync.Mutex
	current priceobserved.Observed
	failure error
	now     func() time.Time
}

// Connect is also the reconnect operation: each call requires a fresh registration
// followed by a fresh Latest. A failed call closes its registration and returns
// no ready connection. The caller closes the previous connection before retrying;
// after source failure its RegisterFunc must supply a new source/assignment.
func Connect(ctx context.Context, pair string, partition int32, register RegisterFunc, quotes QuoteSource, now func() time.Time) (*Connection, error) {
	if register == nil || quotes == nil || now == nil || len(pair) == 0 || len(pair) > 64 || partition < 0 || partition >= 16 {
		return nil, ErrConfig
	}
	for _, r := range pair {
		if !(r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
			return nil, ErrConfig
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	life, cancel := context.WithCancel(ctx)
	reg, err := register(life, pair, partition)
	if err != nil {
		cancel()
		return nil, err
	}
	if reg == nil {
		cancel()
		return nil, ErrConfig
	}
	c := &Connection{reg: reg, ctx: life, cancel: cancel, stopped: make(chan struct{}), now: now}
	go func() {
		defer close(c.stopped)
		select {
		case <-reg.Done():
		case <-life.Done():
		}
		cancel()
		reg.Close()
	}()
	success := false
	defer func() {
		if !success {
			c.Close()
		}
	}()
	readCtx, stop := context.WithTimeout(life, latest.Budget)
	res, err := quotes.Latest(readCtx, pair)
	readErr := readCtx.Err()
	stop()
	if e := c.err(); e != nil {
		return nil, e
	}
	if err != nil || readErr != nil {
		return nil, ErrUnavailable
	}
	q, err := decode(res, pair, now())
	if err != nil {
		return nil, err
	}
	// Events buffered before/during the RPC are folded into one initial state.
	// Bound the drain so a hot producer cannot indefinitely delay readiness.
	q, _, err = c.drain(q)
	if err != nil {
		return nil, err
	}
	if err = c.err(); err != nil {
		return nil, err
	}
	c.current = copyQuote(q)
	success = true
	return c, nil
}

// Done revokes price readiness for ongoing candidate work. It closes on lifetime
// cancellation, stream loss, contract failure, or Close.
func (c *Connection) Done() <-chan struct{} { return c.ctx.Done() }
func (c *Connection) Err() error            { return c.err() }

func (c *Connection) err() error {
	c.mu.Lock()
	err := c.failure
	c.mu.Unlock()
	if err != nil {
		return err
	}
	if err = c.reg.Err(); err != nil {
		return err
	}
	return c.ctx.Err()
}

// Current never returns a quote after continuity/lifetime loss, even if Next
// has not been called since the loss. Quote pointers are independent copies.
func (c *Connection) Current() (priceobserved.Observed, error) {
	if err := c.err(); err != nil {
		return priceobserved.Observed{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return copyQuote(c.current), nil
}

// Next waits for a newer revision and coalesces buffered superseded prices.
// Cancelling only this wait leaves the connection usable. Contract/stream errors
// invalidate readiness; the caller must Connect again before handling candidates.
func (c *Connection) Next(ctx context.Context) (priceobserved.Observed, error) {
	c.nextMu.Lock()
	defer c.nextMu.Unlock()
	wait, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(c.ctx, cancel)
	defer stop()
	for {
		if err := c.err(); err != nil {
			return priceobserved.Observed{}, err
		}
		if err := wait.Err(); err != nil {
			return priceobserved.Observed{}, err
		}
		u, err := c.reg.Next(wait)
		if e := c.err(); e != nil {
			return priceobserved.Observed{}, e
		}
		if err != nil {
			if wait.Err() != nil {
				return priceobserved.Observed{}, wait.Err()
			}
			return priceobserved.Observed{}, c.fail(err)
		}
		c.mu.Lock()
		q := c.current
		c.mu.Unlock()
		q, changed, err := c.merge(q, u.Quote)
		if err != nil {
			return priceobserved.Observed{}, c.fail(err)
		}
		q, more, err := c.drain(q)
		if err != nil {
			return priceobserved.Observed{}, c.fail(err)
		}
		if err = c.err(); err != nil {
			return priceobserved.Observed{}, err
		}
		// Once consumed, a valid revision is retained even if this wait was
		// cancelled concurrently. Cancellation must not drop stream work.
		if changed || more {
			c.mu.Lock()
			c.current = copyQuote(q)
			c.mu.Unlock()
			return copyQuote(q), nil
		}
	}
}
func (c *Connection) drain(q priceobserved.Observed) (priceobserved.Observed, bool, error) {
	changed := false
	for range pricefeed.MaxBuffer {
		u, ok, err := c.reg.TryNext()
		if err != nil {
			return q, false, err
		}
		if !ok {
			break
		}
		var newer bool
		q, newer, err = c.merge(q, u.Quote)
		if err != nil {
			return q, false, err
		}
		changed = changed || newer
	}
	return q, changed, nil
}
func (c *Connection) merge(current, next priceobserved.Observed) (priceobserved.Observed, bool, error) {
	if next.Validate() != nil {
		return current, false, ErrQuote
	}
	if price.CheckFreshness(true, next.TradeTime, c.now()) == price.InvalidTime {
		return current, false, ErrQuote
	}
	newer, err := next.NewerThan(current)
	if err != nil {
		return current, false, err
	}
	if newer {
		return next, true, nil
	}
	return current, false, nil
}
func (c *Connection) fail(err error) error {
	c.mu.Lock()
	c.failure = err
	c.mu.Unlock()
	c.Close()
	return err
}
func (c *Connection) Close() { c.cancel(); c.reg.Close(); <-c.stopped }

func decode(res *pb.LatestResponse, pair string, now time.Time) (priceobserved.Observed, error) {
	if res == nil || (res.Freshness != pb.Freshness_FRESHNESS_FRESH && res.Freshness != pb.Freshness_FRESHNESS_STALE) || res.Quote == nil {
		return priceobserved.Observed{}, ErrUnavailable
	}
	q := res.Quote
	if q.Revision == nil || q.Pair != pair {
		return priceobserved.Observed{}, ErrQuote
	}
	p, err := price.Parse(q.Price)
	if err != nil {
		return priceobserved.Observed{}, ErrQuote
	}
	trade, err := time.Parse(time.RFC3339Nano, q.TradeTime)
	if err != nil {
		return priceobserved.Observed{}, ErrQuote
	}
	received, err := time.Parse(time.RFC3339Nano, q.ReceivedAt)
	if err != nil {
		return priceobserved.Observed{}, ErrQuote
	}
	out := priceobserved.Observed{SchemaVersion: 1, Pair: q.Pair, Price: &p, SourceID: q.SourceId, Revision: priceobserved.Revision{StreamID: q.Revision.StreamId, Generation: q.Revision.Generation, Sequence: q.Revision.Sequence}, TradeTime: trade, ReceivedAt: received, StreamState: priceobserved.StreamState(q.StreamState), GapDetected: q.GapDetected}
	if out.Validate() != nil || price.CheckFreshness(true, trade, now) == price.InvalidTime {
		return priceobserved.Observed{}, ErrQuote
	}
	return out, nil
}
func copyQuote(q priceobserved.Observed) priceobserved.Observed {
	if q.Price != nil {
		p := *q.Price
		q.Price = &p
	}
	if q.SourceEventTime != nil {
		t := *q.SourceEventTime
		q.SourceEventTime = &t
	}
	return q
}

var _ Registration = (*pricefeed.Registration)(nil)
var _ QuoteSource = (*latest.Client)(nil)
