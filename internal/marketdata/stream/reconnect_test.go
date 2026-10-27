package stream

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestReconnectRestoresCommandsAfterDelay(t *testing.T) {
	clock := newFakeClock()
	var connections atomic.Int32
	received := make(chan string, 2)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RequestURI() != "/ws?tag=same" {
			t.Errorf("URL = %s", r.URL)
		}
		c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer c.Close()
		n := connections.Add(1)
		c.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, b, err := c.ReadMessage()
		if err != nil {
			t.Error(err)
			return
		}
		received <- string(b)
		if n == 1 {
			return
		}
		c.WriteMessage(websocket.TextMessage, []byte("price"))
		c.ReadMessage()
	}))
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gaps := make(chan error, 2)
	done := make(chan error, 1)
	commands := [][]byte{[]byte(`{"method":"SUBSCRIBE","params":["btcusdt@trade","ethusdt@trade"],"id":1}`)}
	go func() {
		done <- readReconnect(ctx, "ws"+strings.TrimPrefix(s.URL, "http")+"/ws?tag=same", DefaultConfig(), RetryConfig{time.Second, 4 * time.Second}, commands, func(context.Context, []byte) error { cancel(); return nil }, func(err error, _ time.Duration) { gaps <- err }, clock, func(n int64) int64 { return n - 1 })
	}()
	first := receive(t, received)
	if receive(t, gaps) == nil {
		t.Fatal("missing gap cause")
	}
	if d := receive(t, clock.armed); d != time.Second {
		t.Fatalf("delay = %v", d)
	}
	// No new attempt can occur until fake time reaches the deadline.
	clock.advance(time.Second - time.Nanosecond)
	if connections.Load() != 1 {
		t.Fatal("early reconnect")
	}
	// The caller's input is snapshotted before the first attempt.
	commands[0][0] = '!'
	clock.advance(time.Nanosecond)
	if second := receive(t, received); second != first {
		t.Fatalf("restored %q, want %q", second, first)
	}
	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if connections.Load() != 2 {
		t.Fatal(connections.Load())
	}
}

func TestReconnectBackoffBoundedAndCancelable(t *testing.T) {
	for _, low := range []bool{false, true} {
		t.Run(map[bool]string{false: "upper", true: "lower"}[low], func(t *testing.T) {
			clock := newFakeClock()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			attempts := make(chan struct{}, 10)
			gaps := make(chan time.Duration, 10)
			done := make(chan error, 1)
			cause := errors.New("offline")
			go func() {
				done <- retryConnections(ctx, RetryConfig{time.Second, 3 * time.Second}, clock, func(n int64) int64 {
					if low {
						return 0
					}
					return n - 1
				}, func() (bool, error) { attempts <- struct{}{}; return true, cause }, func(err error, d time.Duration) {
					if !errors.Is(err, cause) {
						t.Error(err)
					}
					gaps <- d
				})
			}()
			for _, cap := range []time.Duration{time.Second, 2 * time.Second, 3 * time.Second, 3 * time.Second} {
				receive(t, attempts)
				d := receive(t, gaps)
				want := cap
				if low {
					want = cap / 2
				}
				if d != want {
					t.Fatalf("delay %v want %v", d, want)
				}
				if armed := receive(t, clock.armed); armed != d {
					t.Fatal(armed)
				}
				if cap == 3*time.Second && len(gaps) == 0 { /* advance except on final iteration below */
				}
				clock.advance(d)
			}
			receive(t, attempts)
			receive(t, gaps)
			receive(t, clock.armed)
			cancel()
			if err := receive(t, done); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			select {
			case <-attempts:
				t.Fatal("attempt after cancellation")
			default:
			}
		})
	}
}

func TestReconnectTerminalAndAlreadyCanceled(t *testing.T) {
	cause := errors.New("handler failed")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	err := retryConnections(ctx, DefaultRetryConfig(), realClock{}, func(int64) int64 { return 0 }, func() (bool, error) { called = true; return false, cause }, func(error, time.Duration) { t.Error("gap") })
	if called || !errors.Is(err, context.Canceled) {
		t.Fatal(called, err)
	}
	err = retryConnections(context.Background(), DefaultRetryConfig(), realClock{}, func(int64) int64 { return 0 }, func() (bool, error) { return false, cause }, func(error, time.Duration) { t.Error("gap") })
	if !errors.Is(err, cause) {
		t.Fatal(err)
	}
}

func TestReconnectHandshakeFailureThenTerminalHandler(t *testing.T) {
	clock := newFakeClock()
	var attempts atomic.Int32
	stopped := errors.New("stop processing")
	serverDone := make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			http.Error(w, "offline", http.StatusServiceUnavailable)
			return
		}
		c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer c.Close()
		defer close(serverDone)
		c.SetReadDeadline(time.Now().Add(3 * time.Second))
		if err := c.WriteMessage(websocket.TextMessage, []byte("price")); err != nil {
			t.Error(err)
			return
		}
		c.ReadMessage()
	}))
	defer s.Close()
	gaps := make(chan error, 2)
	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() {
		done <- readReconnect(ctx, "ws"+strings.TrimPrefix(s.URL, "http"), DefaultConfig(), DefaultRetryConfig(), nil, func(context.Context, []byte) error { return stopped }, func(err error, _ time.Duration) { gaps <- err }, clock, func(int64) int64 { return 0 })
	}()
	if err := receive(t, gaps); !errors.Is(err, websocket.ErrBadHandshake) {
		t.Fatal(err)
	}
	clock.advance(receive(t, clock.armed))
	if err := receive(t, done); !errors.Is(err, stopped) {
		t.Fatal(err)
	}
	receive(t, serverDone)
	if attempts.Load() != 2 {
		t.Fatal(attempts.Load())
	}
	select {
	case err := <-gaps:
		t.Fatalf("handler failure reported as gap: %v", err)
	default:
	}
}

func TestReconnectValidation(t *testing.T) {
	handle := func(context.Context, []byte) error { return nil }
	observer := func(error, time.Duration) { t.Error("unexpected disconnect") }
	for _, retry := range []RetryConfig{{}, {-1, time.Second}, {time.Second, time.Nanosecond}} {
		if err := Reconnect(context.Background(), "unused", DefaultConfig(), retry, nil, handle, observer); err == nil {
			t.Fatal("accepted retry config", retry)
		}
	}
	if err := Reconnect(context.Background(), "unused", Config{}, DefaultRetryConfig(), nil, handle, observer); err == nil {
		t.Fatal("accepted scheduler config")
	}
	if err := Reconnect(context.Background(), "unused", DefaultConfig(), DefaultRetryConfig(), nil, nil, observer); err == nil {
		t.Fatal("accepted nil handler")
	}
	if err := Reconnect(context.Background(), "unused", DefaultConfig(), DefaultRetryConfig(), nil, handle, nil); err == nil {
		t.Fatal("accepted nil observer")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Reconnect(ctx, "unused", DefaultConfig(), DefaultRetryConfig(), nil, handle, observer); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestReconnectCancelFromObserverPreventsRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	err := retryConnections(ctx, DefaultRetryConfig(), realClock{}, func(int64) int64 { return 0 }, func() (bool, error) { calls++; return true, errors.New("disconnect") }, func(error, time.Duration) { cancel() })
	if calls != 1 || !errors.Is(err, context.Canceled) {
		t.Fatal(calls, err)
	}
}

func TestReconnectBackoffDoesNotOverflow(t *testing.T) {
	const max = time.Duration(1<<63 - 1)
	clock := newFakeClock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- retryConnections(ctx, RetryConfig{max - 1, max}, clock, func(n int64) int64 { return n - 1 }, func() (bool, error) { return true, errors.New("offline") }, func(error, time.Duration) {})
	}()
	if d := receive(t, clock.armed); d != max-1 {
		t.Fatal(d)
	}
	// fakeClock's absolute time cannot advance by two MaxInt64 durations; fire
	// each timer directly to exercise the saturating backoff arithmetic instead.
	clock.mu.Lock()
	clock.timer <- clock.now
	clock.mu.Unlock()
	if d := receive(t, clock.armed); d != max {
		t.Fatal(d)
	}
	cancel()
	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
