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

func TestReconnectPreparedBeforeDialAndCancellation(t *testing.T) {
	for _, mode := range []string{"error", "canceled", "cancel-during", "nil-handle", "nil-disconnect"} {
		t.Run(mode, func(t *testing.T) {
			var dials atomic.Int32
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { dials.Add(1); http.Error(w, "unexpected", 503) }))
			defer s.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "canceled" {
				cancel()
			}
			cause := errors.New("generation failed")
			calls := 0
			err := ReconnectPrepared(ctx, "ws"+strings.TrimPrefix(s.URL, "http"), DefaultConfig(), DefaultRetryConfig(), nil, func(got context.Context) (AttemptCallbacks, error) {
				calls++
				if got != ctx {
					t.Error("prepare context changed")
				}
				cb := AttemptCallbacks{Handle: func(context.Context, []byte) error { return nil }, Disconnected: func(context.Context, error, time.Duration) error { t.Error("unexpected gap"); return nil }}
				switch mode {
				case "error":
					return AttemptCallbacks{}, cause
				case "cancel-during":
					cancel()
				case "nil-handle":
					cb.Handle = nil
				case "nil-disconnect":
					cb.Disconnected = nil
				}
				return cb, nil
			})
			if err == nil || dials.Load() != 0 {
				t.Fatal(err, dials.Load())
			}
			if mode == "error" && !errors.Is(err, cause) {
				t.Fatal(err)
			}
			if (mode == "canceled" || mode == "cancel-during") && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			want := 1
			if mode == "canceled" {
				want = 0
			}
			if calls != want {
				t.Fatal(calls)
			}
		})
	}
}

// The server observes preparation before every handshake. Every callback closes
// over its own generation, including the attempt whose handshake fails.
func TestReconnectPreparedAttemptIsolation(t *testing.T) {
	for _, mode := range []string{"dial", "loss", "shutdown", "age"} {
		t.Run(mode, func(t *testing.T) {
			clock := newFakeClock()
			var prepared, dials atomic.Int32
			var retired atomic.Bool
			timerArmed := make(chan *fakeClock, 2)
			timer := func(time.Duration) (<-chan time.Time, func()) {
				c := newFakeClock()
				ticks := c.After(RotationAge)
				<-c.armed
				timerArmed <- c
				return ticks, func() { retired.Store(true) }
			}
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := dials.Add(1)
				if prepared.Load() != n {
					t.Errorf("dial %d before prepare %d", n, prepared.Load())
				}
				if mode == "dial" && n == 1 {
					http.Error(w, "offline", 503)
					return
				}
				c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer c.Close()
				c.SetReadDeadline(time.Now().Add(3 * time.Second))
				c.WriteMessage(websocket.TextMessage, []byte("price"))
				if n == 1 {
					switch mode {
					case "loss":
						return
					case "shutdown":
						c.WriteMessage(websocket.TextMessage, []byte(`{"e":"serverShutdown"}`))
					}
				}
				c.ReadMessage()
			}))
			defer s.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			entered := make(chan int32, 2)
			release := make(chan struct{})
			gaps := make(chan error, 2)
			done := make(chan error, 1)
			terminal := errors.New("terminal handle")
			var finished, disconnected bool
			go func() {
				done <- readReconnectPrepared(ctx, "ws"+strings.TrimPrefix(s.URL, "http"), DefaultConfig(), DefaultRetryConfig(), nil, func(context.Context) (AttemptCallbacks, error) {
					n := prepared.Add(1)
					if n == 2 && (!disconnected || (mode != "dial" && (!finished || !retired.Load()))) {
						t.Error("next preparation before previous cleanup")
					}
					retired.Store(false)
					return AttemptCallbacks{
						Handle: func(hctx context.Context, b []byte) error {
							if string(b) != "price" || prepared.Load() != n {
								t.Error("wrong attempt payload", n, string(b))
							}
							if n == 2 {
								return terminal
							}
							entered <- n
							if mode == "age" {
								<-hctx.Done()
								<-release
								finished = true
								return hctx.Err()
							}
							finished = true
							return nil
						},
						Disconnected: func(dctx context.Context, err error, d time.Duration) error {
							if dctx != ctx || n != 1 || d != 500*time.Millisecond {
								t.Error("disconnect context/generation/delay", n, d)
							}
							disconnected = true
							gaps <- err
							return nil
						},
					}, nil
				}, clock, func(int64) int64 { return 0 }, timer)
			}()
			if mode != "dial" {
				receive(t, entered)
				age := receive(t, timerArmed)
				if mode == "age" {
					age.advance(RotationAge)
					// A blocked previous handler must keep the next generation from starting.
					if prepared.Load() != 1 {
						t.Fatal("overlapping generations")
					}
					close(release)
				}
			}
			gap := receive(t, gaps)
			if (mode == "age" || mode == "shutdown") && !errors.Is(gap, ErrRotation) {
				t.Fatal(gap)
			}
			if mode == "dial" && !errors.Is(gap, websocket.ErrBadHandshake) {
				t.Fatal(gap)
			}
			clock.advance(receive(t, clock.armed))
			if err := receive(t, done); !errors.Is(err, terminal) {
				t.Fatal(err)
			}
			if prepared.Load() != 2 || dials.Load() != 2 {
				t.Fatal(prepared.Load(), dials.Load())
			}
		})
	}
}

func TestReconnectPreparedDisconnectError(t *testing.T) {
	var dials atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { dials.Add(1); http.Error(w, "offline", 503) }))
	defer s.Close()
	cause := errors.New("durable status failed")
	err := ReconnectPrepared(context.Background(), "ws"+strings.TrimPrefix(s.URL, "http"), DefaultConfig(), DefaultRetryConfig(), nil, func(context.Context) (AttemptCallbacks, error) {
		return AttemptCallbacks{Handle: func(context.Context, []byte) error { return nil }, Disconnected: func(_ context.Context, err error, _ time.Duration) error {
			if !errors.Is(err, websocket.ErrBadHandshake) {
				t.Error(err)
			}
			return cause
		}}, nil
	})
	if !errors.Is(err, cause) || dials.Load() != 1 {
		t.Fatal(err, dials.Load())
	}
}

func TestReconnectPreparedCancelWhilePreparing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- ReconnectPrepared(ctx, "unused", DefaultConfig(), DefaultRetryConfig(), nil, func(ctx context.Context) (AttemptCallbacks, error) {
			close(entered)
			<-ctx.Done()
			return AttemptCallbacks{}, ctx.Err()
		})
	}()
	receive(t, entered)
	cancel()
	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestReconnectPreparedFailureAfterRetry(t *testing.T) {
	clock := newFakeClock()
	var dials atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { dials.Add(1); http.Error(w, "offline", 503) }))
	defer s.Close()
	cause := errors.New("next generation failed")
	calls := 0
	done := make(chan error, 1)
	go func() {
		done <- readReconnectPrepared(context.Background(), "ws"+strings.TrimPrefix(s.URL, "http"), DefaultConfig(), DefaultRetryConfig(), nil, func(context.Context) (AttemptCallbacks, error) {
			calls++
			if calls == 2 {
				return AttemptCallbacks{}, cause
			}
			return AttemptCallbacks{Handle: func(context.Context, []byte) error { return nil }, Disconnected: func(context.Context, error, time.Duration) error { return nil }}, nil
		}, clock, func(int64) int64 { return 0 }, newRotationTimer)
	}()
	clock.advance(receive(t, clock.armed))
	if err := receive(t, done); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	if calls != 2 || dials.Load() != 1 {
		t.Fatal(calls, dials.Load())
	}
}

func TestReconnectPreparedValidationBeforePreparation(t *testing.T) {
	prepare := func(context.Context) (AttemptCallbacks, error) {
		t.Error("unexpected preparation")
		return AttemptCallbacks{}, nil
	}
	for _, test := range []struct {
		config  Config
		retry   RetryConfig
		prepare PrepareAttempt
	}{
		{Config{}, DefaultRetryConfig(), prepare},
		{DefaultConfig(), RetryConfig{}, prepare},
		{DefaultConfig(), DefaultRetryConfig(), nil},
	} {
		if err := ReconnectPrepared(context.Background(), "unused", test.config, test.retry, nil, test.prepare); err == nil {
			t.Fatal("invalid configuration accepted")
		}
	}
}
