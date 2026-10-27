package relay

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafka"
)

func TestFencedOwnerStopsAndCancellationInterruptsBackoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := run(ctx, nil, func(context.Context) (publisher, error) { return nil, kafka.ErrFenced }, nil, timings{time.Millisecond, time.Second, time.Second})
	if !errors.Is(err, kafka.ErrFenced) {
		t.Fatalf("fenced owner: %v", err)
	}
	ctx2, stop := context.WithCancel(context.Background())
	calls := 0
	err = run(ctx2, nil, func(context.Context) (publisher, error) { calls++; stop(); return nil, kafka.ErrPublish }, nil, timings{time.Hour, time.Second, time.Second})
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("backoff cancel: %d %v", calls, err)
	}
}
