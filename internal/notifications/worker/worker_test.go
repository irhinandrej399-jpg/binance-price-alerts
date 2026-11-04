package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/jobs"
)

type sourceFunc func(context.Context, string, int) ([]jobs.Work, error)

func (f sourceFunc) Discover(c context.Context, s string, n int) ([]jobs.Work, error) {
	return f(c, s, n)
}
func (f sourceFunc) ScheduleRecovery(context.Context, string, int64, time.Duration) error { return nil }

type handleFunc func(context.Context, string) error

func (f handleFunc) Handle(c context.Context, s string) error { return f(c, s) }
func receive[T any](t *testing.T, c <-chan T) T {
	t.Helper()
	select {
	case v := <-c:
		return v
	case <-time.After(time.Second * 3):
		t.Fatal("timed out")
		var v T
		return v
	}
}
func config(s Source, h Handler) Config {
	return Config{Source: s, Handler: h, Concurrency: 2, Interval: time.Second, Timeout: time.Second, WriteTimeout: time.Second, RecoveryInterval: time.Second}
}
func TestEmptyQueueWaitAndCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := make(chan string, 10)
	wait := make(chan struct{}, 10)
	tick := make(chan struct{})
	s := sourceFunc(func(_ context.Context, after string, _ int) ([]jobs.Work, error) { calls <- after; return nil, nil })
	w, e := New(config(s, handleFunc(func(context.Context, string) error { t.Error("unexpected Handle"); return nil })))
	if e != nil {
		t.Fatal(e)
	}
	w.wait = func(ctx context.Context, _ time.Duration) error {
		wait <- struct{}{}
		select {
		case <-tick:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	receive(t, calls)
	receive(t, wait)
	select {
	case <-calls:
		t.Fatal("busy loop")
	default:
	}
	tick <- struct{}{}
	receive(t, calls)
	receive(t, wait)
	cancel()
	if e := receive(t, done); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	select {
	case <-calls:
		t.Fatal("discovery after cancellation")
	default:
	}
}
func TestParallelBoundAndNoNewCallsAfterCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var active, maximum atomic.Int32
	started := make(chan string, 10)
	s := sourceFunc(func(_ context.Context, _ string, n int) ([]jobs.Work, error) {
		if n != 2 {
			t.Error(n)
		}
		return []jobs.Work{{TriggerID: "a"}, {TriggerID: "b"}}, nil
	})
	h := handleFunc(func(c context.Context, id string) error {
		n := active.Add(1)
		defer active.Add(-1)
		for old := maximum.Load(); n > old; old = maximum.Load() {
			if maximum.CompareAndSwap(old, n) {
				break
			}
		}
		if _, ok := c.Deadline(); !ok {
			t.Error("missing deadline")
		}
		started <- id
		<-c.Done()
		return c.Err()
	})
	w, e := New(config(s, h))
	if e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	receive(t, started)
	receive(t, started)
	cancel()
	receive(t, done)
	if maximum.Load() != 2 || active.Load() != 0 {
		t.Fatal(maximum.Load(), active.Load())
	}
	if e = w.Run(ctx); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	select {
	case <-started:
		t.Fatal("new call after cancellation")
	default:
	}
}

type pending struct{ retry func(context.Context) error }

func (p *pending) Error() string                 { return "pending" }
func (p *pending) Retry(c context.Context) error { return p.retry(c) }
func TestPendingWriteRetainedAndShutdownUsesFreshContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var handles, retries atomic.Int32
	entered := make(chan struct{}, 1)
	p := &pending{retry: func(c context.Context) error {
		retries.Add(1)
		if c.Err() != nil {
			t.Error("cancelled persistence context")
		}
		if _, ok := c.Deadline(); !ok {
			t.Error("missing write deadline")
		}
		return nil
	}}
	s := sourceFunc(func(context.Context, string, int) ([]jobs.Work, error) { return []jobs.Work{{TriggerID: "a"}}, nil })
	h := handleFunc(func(context.Context, string) error { handles.Add(1); cancel(); entered <- struct{}{}; return p })
	w, e := New(config(s, h))
	if e != nil {
		t.Fatal(e)
	}
	e = w.Run(ctx)
	receive(t, entered)
	if !errors.Is(e, context.Canceled) || handles.Load() != 1 || retries.Load() != 1 {
		t.Fatal(e, handles.Load(), retries.Load())
	}
}

func TestCursorFairnessAndDiscoveryErrorBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var scans, waits int
	var cursors []string
	s := sourceFunc(func(_ context.Context, after string, _ int) ([]jobs.Work, error) {
		cursors = append(cursors, after)
		scans++
		switch scans {
		case 1:
			return nil, errors.New("database unavailable")
		case 2:
			return []jobs.Work{{TriggerID: "a"}, {TriggerID: "b"}}, nil
		case 3:
			return []jobs.Work{{TriggerID: "c"}}, nil
		default:
			return nil, nil
		}
	})
	var handled atomic.Int32
	w, e := New(config(s, handleFunc(func(context.Context, string) error { handled.Add(1); return errors.New("transient") })))
	if e != nil {
		t.Fatal(e)
	}
	w.wait = func(context.Context, time.Duration) error {
		waits++
		if waits == 4 {
			cancel()
			return context.Canceled
		}
		return nil
	}
	if e = w.Run(ctx); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if scans != 4 || waits != 4 || handled.Load() != 3 {
		t.Fatal(scans, waits, handled.Load())
	}
	want := []string{"", "", "b", "c"}
	for i := range want {
		if cursors[i] != want[i] {
			t.Fatal(cursors)
		}
	}
}

func TestPendingRetriesNeverReenterHandlerAndPreserveErrorOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var handles, retries, waits int
	p := &pending{}
	p.retry = func(c context.Context) error {
		retries++
		if c.Err() != nil {
			t.Fatal(c.Err())
		}
		if retries == 2 {
			cancel()
		}
		return errors.New("write unavailable")
	}
	s := sourceFunc(func(context.Context, string, int) ([]jobs.Work, error) { return []jobs.Work{{TriggerID: "a"}}, nil })
	w, e := New(config(s, handleFunc(func(context.Context, string) error { handles++; return p })))
	if e != nil {
		t.Fatal(e)
	}
	w.wait = func(context.Context, time.Duration) error { waits++; return nil }
	e = w.Run(ctx)
	var retained *pending
	if !errors.Is(e, context.Canceled) || !errors.As(e, &retained) || retained != p || handles != 1 || retries != 3 || waits != 2 {
		t.Fatal(e, handles, retries, waits)
	}
}

type recoverySource struct {
	sourceFunc
	scheduled atomic.Int32
}

func (s *recoverySource) ScheduleRecovery(ctx context.Context, id string, fence int64, d time.Duration) error {
	if ctx.Err() != nil || id != "block" || fence != 7 || d != time.Second {
		return errors.New("bad schedule")
	}
	s.scheduled.Add(1)
	return nil
}
func TestRecoveryWorkSchedulesWithoutHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &recoverySource{sourceFunc: func(context.Context, string, int) ([]jobs.Work, error) {
		return []jobs.Work{{TriggerID: "block", RecoveryFence: 7}}, nil
	}}
	w, e := New(config(s, handleFunc(func(context.Context, string) error { t.Error("recovery must not invoke handler"); return nil })))
	if e != nil {
		t.Fatal(e)
	}
	w.wait = func(context.Context, time.Duration) error { cancel(); return context.Canceled }
	if e = w.Run(ctx); !errors.Is(e, context.Canceled) || s.scheduled.Load() != 1 {
		t.Fatal(e, s.scheduled.Load())
	}
}

func TestFailedFinalWriteSurvivesRunRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fail := true
	var retries, handles, scans int
	p := &pending{retry: func(context.Context) error {
		retries++
		if fail {
			return errors.New("SQL unavailable")
		}
		return nil
	}}
	nextCtx, nextCancel := context.WithCancel(context.Background())
	defer nextCancel()
	s := sourceFunc(func(context.Context, string, int) ([]jobs.Work, error) {
		scans++
		if scans > 1 {
			nextCancel()
			return nil, nil
		}
		return []jobs.Work{{TriggerID: "a"}}, nil
	})
	w, e := New(config(s, handleFunc(func(context.Context, string) error { handles++; cancel(); return p })))
	if e != nil {
		t.Fatal(e)
	}
	if e = w.Run(ctx); !errors.Is(e, p) {
		t.Fatal(e)
	}
	fail = false
	w.wait = func(context.Context, time.Duration) error { return nil }
	if e = w.Run(nextCtx); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if retries != 2 || handles != 1 || scans != 2 || len(w.pending) != 0 {
		t.Fatal(retries, handles, scans, len(w.pending))
	}
}

func TestRejectInvalidConfiguration(t *testing.T) {
	s := sourceFunc(func(context.Context, string, int) ([]jobs.Work, error) { return nil, nil })
	h := handleFunc(func(context.Context, string) error { return nil })
	cases := []func(*Config){func(c *Config) { c.Concurrency = 0 }, func(c *Config) { c.Concurrency = 1001 }, func(c *Config) { c.Interval = 0 }, func(c *Config) { c.Timeout = 0 }, func(c *Config) { c.WriteTimeout = 0 }, func(c *Config) { c.RecoveryInterval = 0 }, func(c *Config) { c.Source = nil }, func(c *Config) { c.Handler = nil }}
	for _, change := range cases {
		c := config(s, h)
		change(&c)
		if _, e := New(c); e == nil {
			t.Fatal("invalid configuration accepted")
		}
	}
}
