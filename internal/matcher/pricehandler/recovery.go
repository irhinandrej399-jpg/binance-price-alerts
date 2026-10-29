package pricehandler

import (
	"context"
	"fmt"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricework"
)

// Recover makes one pass over durable pending work in this handler's stable
// scope. The caller first restores the live projection and latest prices using
// MAT-008/009. Handle rechecks Current before each remaining candidate; a lost
// view or transient API failure stops the pass and leaves work retryable.
//
// The owner serializes this call with Handle/Resume. Key order is not transport
// order: this method neither advances a prices prefix nor acknowledges Kafka.
// New current-view evaluations and broker redelivery remain the owner's job.
func (h *Handler) Recover(ctx context.Context) error {
	if h.work == nil {
		return ErrProtocol
	}
	keys, err := h.work.Pending(ctx)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := h.Resume(ctx, key, nil, nil); err != nil {
			return fmt.Errorf("pricehandler: recover %q: %w", key, err)
		}
	}
	return nil
}

// Resume reads the original request identity from SQL, including its real
// transport position, and retries Handle. It never accepts a missing key or
// rebuilds old candidates from the current generation. Terminal keys support
// retrying a lost ack even though Pending no longer lists them. Prefix must be
// proven independently by the partition owner; nil stores only work outcomes.
func (h *Handler) Resume(ctx context.Context, key string, prefix *pricework.Prefix, ack func(context.Context) error) error {
	if h.work == nil || key == "" {
		return ErrProtocol
	}
	w, err := h.work.Get(ctx, key)
	if err != nil {
		return err
	}
	return h.Handle(ctx, Request{
		Key: key, Pair: w.Input.Quote.Pair, Position: w.Input.Position, Prefix: prefix,
	}, ack)
}
