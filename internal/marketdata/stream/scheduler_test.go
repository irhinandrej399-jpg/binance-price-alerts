package stream

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type fakeClock struct {
	mu    sync.Mutex
	now   time.Time
	armed chan time.Duration
	timer chan time.Time
	due   time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(100, 0), armed: make(chan time.Duration, 1)}
}
func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.timer = make(chan time.Time, 1)
	c.due = c.now.Add(d)
	c.armed <- d
	return c.timer
}
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	if c.timer != nil && !c.now.Before(c.due) {
		c.timer <- c.now
		c.timer = nil
	}
}
func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("timed out")
		var zero T
		return zero
	}
}

type sentFrame struct {
	kind int
	data string
	at   time.Time
}

func TestSchedulerBurstAndPongShareBudget(t *testing.T) {
	clock := newFakeClock()
	commands := make(chan []byte, 20)
	for i := 0; i < 12; i++ {
		commands <- []byte(`{"method":"SUBSCRIBE","params":["btcusdt@trade"],"id":1}`)
	}
	close(commands)
	pongs := make(chan []byte, 4)
	sent := make(chan sentFrame, 20)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runWriter(ctx, Config{Messages: 5, Window: time.Second, WriteTimeout: time.Second, ControlQueue: 4}, clock, commands, pongs, func(k int, b []byte) error { sent <- sentFrame{k, string(b), clock.Now()}; return nil })
	}()
	frames := []sentFrame{receive(t, sent)}
	if frames[0].kind != websocket.TextMessage {
		t.Fatal(frames[0])
	}
	wait := receive(t, clock.armed)
	if wait != 200*time.Millisecond {
		t.Fatalf("wait = %v", wait)
	}
	pongs <- []byte("\x00ping\xff")
	clock.advance(wait - time.Nanosecond)
	select {
	case f := <-sent:
		t.Fatalf("early send: %+v", f)
	default:
	}
	clock.advance(time.Nanosecond)
	frames = append(frames, receive(t, sent))
	if frames[1].kind != websocket.PongMessage || frames[1].data != "\x00ping\xff" {
		t.Fatalf("pong priority/payload: %+v", frames[1])
	}
	for i := 0; i < 11; i++ {
		wait = receive(t, clock.armed)
		clock.advance(wait)
		frames = append(frames, receive(t, sent))
	}
	for i, f := range frames {
		n := 0
		for _, g := range frames {
			if !g.at.Before(f.at) && g.at.Before(f.at.Add(time.Second)) {
				n++
			}
		}
		if n > 5 {
			t.Fatalf("window %d: %d messages", i, n)
		}
	}
	cancel()
	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestSchedulerNoCatchupAndCancellation(t *testing.T) {
	clock := newFakeClock()
	commands := make(chan []byte, 3)
	pongs := make(chan []byte, 1)
	for i := 0; i < 3; i++ {
		commands <- []byte("command")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sent := make(chan sentFrame, 3)
	done := make(chan error, 1)
	go func() {
		done <- runWriter(ctx, Config{Messages: 2, Window: time.Second, WriteTimeout: time.Second, ControlQueue: 1}, clock, commands, pongs, func(k int, b []byte) error { sent <- sentFrame{k, string(b), clock.Now()}; return nil })
	}()
	receive(t, sent)
	receive(t, clock.armed)
	clock.advance(10 * time.Second)
	receive(t, sent)
	if wait := receive(t, clock.armed); wait != 500*time.Millisecond {
		t.Fatalf("catchup wait %v", wait)
	}
	cancel()
	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	select {
	case f := <-sent:
		t.Fatalf("sent after cancellation: %+v", f)
	default:
	}
}

func TestSchedulerWriteError(t *testing.T) {
	commands := make(chan []byte, 1)
	commands <- []byte("command")
	want := errors.New("write failed")
	err := runWriter(context.Background(), DefaultConfig(), newFakeClock(), commands, make(chan []byte, 1), func(int, []byte) error { return want })
	if !errors.Is(err, want) {
		t.Fatal(err)
	}
}

func TestConfigValidation(t *testing.T) {
	for _, c := range []Config{{}, {Messages: 6, Window: time.Second, WriteTimeout: time.Second, ControlQueue: 1}, {Messages: 5, Window: time.Second - time.Nanosecond, WriteTimeout: time.Second, ControlQueue: 1}, {Messages: 1, Window: time.Second, WriteTimeout: 0, ControlQueue: 1}, {Messages: 1, Window: time.Second, WriteTimeout: time.Second, ControlQueue: 0}} {
		if _, err := c.interval(); err == nil {
			t.Fatalf("accepted %+v", c)
		}
	}
	c := DefaultConfig()
	c.Messages = 3
	if d, err := c.interval(); err != nil || d != 333333334*time.Nanosecond {
		t.Fatalf("rounded interval = %v, %v", d, err)
	}
}

func TestPongBurstKeepsFIFOAndCommandsResume(t *testing.T) {
	clock := newFakeClock()
	commands := make(chan []byte, 2)
	commands <- []byte("first")
	commands <- []byte("second")
	close(commands)
	pongs := make(chan []byte, 3)
	sent := make(chan sentFrame, 5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runWriter(ctx, DefaultConfig(), clock, commands, pongs, func(k int, b []byte) error { sent <- sentFrame{k, string(b), clock.Now()}; return nil })
	}()
	if f := receive(t, sent); f.data != "first" {
		t.Fatal(f)
	}
	receive(t, clock.armed)
	for _, s := range []string{"ping-1", "ping-2", "ping-3"} {
		pongs <- []byte(s)
	}
	for i, want := range []string{"ping-1", "ping-2", "ping-3", "second"} {
		clock.advance(200 * time.Millisecond)
		f := receive(t, sent)
		if f.data != want {
			t.Fatalf("frame %d: %+v", i, f)
		}
		kind := websocket.PongMessage
		if i == 3 {
			kind = websocket.TextMessage
		}
		if f.kind != kind {
			t.Fatalf("kind %d", f.kind)
		}
		if i < 3 {
			receive(t, clock.armed)
		}
	}
	// An exhausted, closed command channel must leave pong handling alive.
	pongs <- []byte("after-close")
	receive(t, clock.armed)
	clock.advance(200 * time.Millisecond)
	if f := receive(t, sent); f.kind != websocket.PongMessage || f.data != "after-close" {
		t.Fatal(f)
	}
	cancel()
	receive(t, done)
}
