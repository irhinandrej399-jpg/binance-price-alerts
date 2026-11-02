package chatlimit

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
)

func TestScheduleHotChatWithoutBlockingOthers(t *testing.T) {
	p := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
	l, err := New(p, fmt.Sprint(time.Now().UnixNano()), 0)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	l.now = func(context.Context) (time.Time, error) { return now, nil }
	chats := make([]int64, 15, 17)
	for i := range chats {
		chats[i] = 101
	}
	chats = append(chats, 202, 303)
	ctx := context.Background()
	counts := map[int64]int{}
	last := map[int64]time.Time{}
	start := now
	for len(chats) > 0 {
		i, wait, err := l.Next(ctx, chats)
		if err != nil {
			t.Fatal(err)
		}
		if i < 0 {
			if wait != time.Second {
				t.Fatalf("wait=%v", wait)
			}
			now = now.Add(wait)
			continue
		}
		if wait != 0 {
			t.Fatalf("grant with wait %v", wait)
		}
		chat := chats[i]
		if prev, ok := last[chat]; ok && now.Sub(prev) < time.Second {
			t.Fatal("chat burst")
		}
		if chat != 101 && !now.Equal(start) {
			t.Fatal("other chat blocked by hot chat")
		}
		last[chat] = now
		counts[chat]++
		chats = append(chats[:i], chats[i+1:]...)
	}
	if counts[101] != 15 || counts[202] != 1 || counts[303] != 1 || now.Sub(start) != 14*time.Second {
		t.Fatalf("counts=%v elapsed=%v", counts, now.Sub(start))
	}
	i, wait, err := l.Next(ctx, nil)
	if i != -1 || wait != 0 || err != nil {
		t.Fatalf("empty: %d %v %v", i, wait, err)
	}
}

func TestSharedChatRestartAndIsolation(t *testing.T) {
	p1 := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
	p2 := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
	bot := fmt.Sprint(time.Now().UnixNano())
	var micros atomic.Int64
	micros.Store(time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC).UnixMicro())
	clock := func(context.Context) (time.Time, error) { return time.UnixMicro(micros.Load()), nil }
	newWorker := func(second bool) *Limiter {
		p := p1
		if second {
			p = p2
		}
		l, err := New(p, bot, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		l.now = clock
		return l
	}
	workers := []*Limiter{newWorker(false), newWorker(true)}
	ctx := context.Background()
	for tick := 0; tick < 15; tick++ {
		var grants atomic.Int64
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				wait, err := workers[i%2].TryAcquire(ctx, 101)
				if err != nil {
					t.Error(err)
					return
				}
				if wait == 0 {
					grants.Add(1)
				} else if wait < 0 || wait > time.Second {
					t.Errorf("wait=%v", wait)
				}
			}(i)
		}
		wg.Wait()
		if grants.Load() != 1 {
			t.Fatalf("tick %d grants %d", tick, grants.Load())
		}
		workers[0] = newWorker(false)
		if wait, err := workers[0].TryAcquire(ctx, 101); err != nil || wait != time.Second {
			t.Fatalf("restart %v %v", wait, err)
		}
		micros.Add(1000000)
	}
	// No refill before the exact boundary, no accumulated credits after idle.
	micros.Add(-1)
	if wait, err := workers[0].TryAcquire(ctx, 101); err != nil || wait != time.Microsecond {
		t.Fatalf("early %v %v", wait, err)
	}
	micros.Add(int64(time.Hour / time.Microsecond))
	if wait, err := workers[0].TryAcquire(ctx, 101); err != nil || wait != 0 {
		t.Fatalf("idle %v %v", wait, err)
	}
	if wait, err := workers[1].TryAcquire(ctx, 101); err != nil || wait != time.Second {
		t.Fatalf("burst %v %v", wait, err)
	}
	other, _ := New(p2, fmt.Sprint(time.Now().UnixNano()+1), 0)
	other.now = clock
	if wait, err := other.TryAcquire(ctx, 101); err != nil || wait != 0 {
		t.Fatalf("other bot %v %v", wait, err)
	}
	mismatch, _ := New(p2, bot, 2*time.Second)
	if _, err := mismatch.TryAcquire(ctx, 101); err != ErrConfiguration {
		t.Fatalf("config %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := workers[0].TryAcquire(canceled, 101); err == nil {
		t.Fatal("canceled granted")
	}
	if _, err := workers[0].TryAcquire(ctx, 0); err == nil {
		t.Fatal("zero chat granted")
	}
}

func TestBusyChatDoesNotBlockSelection(t *testing.T) {
	p1 := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
	p2 := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
	bot := fmt.Sprint(time.Now().UnixNano())
	l, _ := New(p2, bot, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Keep the first chat locked even before its initial row exists.
	tx, err := p1.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, lockKey(bot, 101)); err != nil {
		t.Fatal(err)
	}
	i, wait, err := l.Next(ctx, []int64{101, 101, 202})
	if err != nil || i != 2 || wait != 0 {
		t.Fatalf("selection: %d %v %v", i, wait, err)
	}
	i, wait, err = l.Next(ctx, []int64{101})
	if err != nil || i != -1 || wait <= 0 {
		t.Fatalf("busy: %d %v %v", i, wait, err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	// Busy selection did not consume the first chat's admission.
	if wait, err := l.TryAcquire(ctx, 101); err != nil || wait != 0 {
		t.Fatalf("released: %v %v", wait, err)
	}
	// A later invalid candidate must not consume an earlier valid admission.
	if _, _, err := l.Next(ctx, []int64{303, 0}); err == nil {
		t.Fatal("invalid batch granted")
	}
	if wait, err := l.TryAcquire(ctx, 303); err != nil || wait != 0 {
		t.Fatalf("validation consumed grant: %v %v", wait, err)
	}
	// Production uses the database clock and supports signed chat identifiers.
	if wait, err := l.TryAcquire(ctx, -404); err != nil || wait != 0 {
		t.Fatalf("negative chat: %v %v", wait, err)
	}
	if wait, err := l.TryAcquire(ctx, -404); err != nil || wait <= 0 || wait > time.Second {
		t.Fatalf("SQL clock: %v %v", wait, err)
	}
}

func TestConfigurationValidation(t *testing.T) {
	if _, err := New(nil, "123", 0); err == nil {
		t.Fatal("nil pool accepted")
	}
	p := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
	for _, bot := range []string{"", "0", "-1", "01", "123:secret"} {
		if _, err := New(p, bot, 0); err == nil {
			t.Fatalf("bot %q accepted", bot)
		}
	}
	for _, interval := range []time.Duration{-time.Second, time.Nanosecond} {
		if _, err := New(p, "123", interval); err == nil {
			t.Fatalf("interval %v accepted", interval)
		}
	}
}
