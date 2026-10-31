package botlimit

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
)

func TestSharedRateAndRestart(t *testing.T) {
	p1 := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
	p2 := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
	ctx := context.Background()
	bot := fmt.Sprint(time.Now().UnixNano())
	var micros atomic.Int64
	micros.Store(time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC).UnixMicro())
	clock := func(context.Context) (time.Time, error) { return time.UnixMicro(micros.Load()), nil }
	newWorker := func(second bool) *Limiter {
		p := p1
		if second {
			p = p2
		}
		l, err := New(p, bot, 25)
		if err != nil {
			t.Fatal(err)
		}
		l.now = clock
		return l
	}
	workers := []*Limiter{newWorker(false), newWorker(true)}
	for tick := 0; tick < 100; tick++ {
		var allowed atomic.Int64
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				wait, err := workers[i%2].TryAcquire(ctx)
				if err != nil {
					t.Error(err)
					return
				}
				if wait == 0 {
					allowed.Add(1)
				} else if wait <= 0 || wait > 40*time.Millisecond {
					t.Errorf("wait %v", wait)
				}
			}(i)
		}
		wg.Wait()
		if allowed.Load() != 1 {
			t.Fatalf("tick %d: %d grants", tick, allowed.Load())
		}
		// New process/pool wrapper at the same instant must not refill the budget.
		workers[0] = newWorker(false)
		wait, err := workers[0].TryAcquire(ctx)
		if err != nil || wait != 40*time.Millisecond {
			t.Fatalf("restart wait=%v err=%v", wait, err)
		}
		micros.Add(40000)
	}
	// Long idle permits one attempt, never accumulated credits.
	micros.Add(int64(time.Hour / time.Microsecond))
	if wait, err := workers[1].TryAcquire(ctx); err != nil || wait != 0 {
		t.Fatalf("idle: %v %v", wait, err)
	}
	if wait, err := workers[0].TryAcquire(ctx); err != nil || wait != 40*time.Millisecond {
		t.Fatalf("burst: %v %v", wait, err)
	}
	// A differently configured replica must fail closed.
	other, _ := New(p2, bot, 30)
	if _, err := other.TryAcquire(ctx); err == nil {
		t.Fatal("accepted inconsistent rate")
	}
	// Cancellation consumes no new grant; errors never mean permission.
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := workers[0].TryAcquire(canceled); err == nil {
		t.Fatal("canceled acquire succeeded")
	}
	// Backward time cannot refill the budget.
	micros.Add(-1000000)
	if wait, err := workers[0].TryAcquire(ctx); err != nil || wait != 1040*time.Millisecond {
		t.Fatalf("backward clock: %v %v", wait, err)
	}
}

func TestDatabaseClockAndIndependentBots(t *testing.T) {
	p := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
	ctx := context.Background()
	bot := fmt.Sprint(time.Now().UnixNano())
	a, _ := New(p, bot, 1)
	if wait, err := a.TryAcquire(ctx); err != nil || wait != 0 {
		t.Fatalf("first: %v %v", wait, err)
	}
	if wait, err := a.TryAcquire(ctx); err != nil || wait <= 0 || wait > time.Second {
		t.Fatalf("second: %v %v", wait, err)
	}
	b, _ := New(p, fmt.Sprint(time.Now().UnixNano()+1), 1)
	if wait, err := b.TryAcquire(ctx); err != nil || wait != 0 {
		t.Fatalf("other bot: %v %v", wait, err)
	}
}

func TestConservativeRoundingAndBoundary(t *testing.T) {
	p := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
	l, err := New(p, fmt.Sprint(time.Now().UnixNano()), 30)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	l.now = func(context.Context) (time.Time, error) { return now, nil }
	ctx := context.Background()
	if wait, err := l.TryAcquire(ctx); wait != 0 || err != nil {
		t.Fatalf("initial: %v %v", wait, err)
	}
	now = now.Add(33333 * time.Microsecond)
	if wait, err := l.TryAcquire(ctx); wait != time.Microsecond || err != nil {
		t.Fatalf("early: %v %v", wait, err)
	}
	now = now.Add(time.Microsecond)
	if wait, err := l.TryAcquire(ctx); wait != 0 || err != nil {
		t.Fatalf("boundary: %v %v", wait, err)
	}
}
