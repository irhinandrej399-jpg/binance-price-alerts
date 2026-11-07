package main

import (
	"context"
	"errors"
	"net"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/health"
)

func TestSupervisorWithdrawsReadinessAndJoins(t *testing.T) {
	for _, failed := range []int{0, 1} {
		t.Run(string(rune('0'+failed)), func(t *testing.T) {
			var sqlFailed atomic.Bool
			probe := health.New(func(context.Context) error {
				if sqlFailed.Load() {
					return errors.New("SQL unavailable")
				}
				return nil
			}, func() bool { return true }, time.Second)
			status := func(path string) int {
				r := httptest.NewRecorder()
				probe.ServeHTTP(r, httptest.NewRequest("GET", path, nil))
				return r.Code
			}
			if status("/readyz") != 503 {
				t.Fatal("ready before initialization")
			}
			listener, e := net.Listen("tcp", "127.0.0.1:0")
			if e != nil {
				t.Fatal(e)
			}
			defer listener.Close()
			started := make(chan struct{}, 2)
			exit := []chan struct{}{make(chan struct{}), make(chan struct{})}
			joined := make(chan struct{})
			release := make(chan struct{})
			defer close(release)
			component := func(i int) func(context.Context) error {
				return func(ctx context.Context) error {
					started <- struct{}{}
					select {
					case <-exit[i]:
						return errors.New("terminal failure")
					case <-ctx.Done():
						close(joined)
						<-release
						return ctx.Err()
					}
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- supervise(ctx, listener, probe, component(0), component(1)) }()
			for i := 0; i < 2; i++ {
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal("start timeout")
				}
			}
			if status("/readyz") != 200 {
				t.Fatal("not ready after start")
			}
			sqlFailed.Store(true)
			if status("/readyz") != 503 || status("/healthz") != 200 {
				t.Fatal("SQL readiness/liveness wrong")
			}
			sqlFailed.Store(false)
			close(exit[failed])
			select {
			case <-joined:
			case <-ctx.Done():
				t.Fatal("sibling not canceled")
			}
			if status("/readyz") != 503 {
				t.Fatal("ready after component exit")
			}
			select {
			case <-done:
				t.Fatal("owner returned before sibling joined")
			default:
			}
			release <- struct{}{}
			select {
			case e := <-done:
				if e == nil {
					t.Fatal("terminal failure hidden")
				}
			case <-ctx.Done():
				t.Fatal("owner not joined")
			}
		})
	}
}

// A final persistence error can wrap cancellation; it must still fail shutdown.
type failedFinalWrite struct{}

func (failedFinalWrite) Error() string               { return "final write failed" }
func (failedFinalWrite) Retry(context.Context) error { return errors.New("SQL unavailable") }

func TestSupervisorCancellationResult(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "clean", true: "failed_final_write"}[fail], func(t *testing.T) {
			listener, e := net.Listen("tcp", "127.0.0.1:0")
			if e != nil {
				t.Fatal(e)
			}
			defer listener.Close()
			probe := health.New(func(context.Context) error { return nil }, func() bool { return true }, time.Second)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			started := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				done <- supervise(ctx, listener, probe, func(ctx context.Context) error {
					close(started)
					<-ctx.Done()
					if fail {
						return errors.Join(ctx.Err(), failedFinalWrite{})
					}
					return ctx.Err()
				})
			}()
			<-started
			cancel()
			select {
			case e := <-done:
				if (e != nil) != fail {
					t.Fatalf("fail=%v error=%v", fail, e)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("join timeout")
			}
		})
	}
}

func TestShutdownBudget(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan struct{})
	done := make(chan bool, 1)
	go func() { done <- shutdownExpired(ctx, finished, 10*time.Millisecond) }()
	select {
	case <-done:
		t.Fatal("budget started before signal")
	case <-time.After(30 * time.Millisecond):
	}
	cancel()
	select {
	case expired := <-done:
		if !expired {
			t.Fatal("stuck drain not expired")
		}
	case <-time.After(time.Second):
		t.Fatal("missing deadline")
	}
	for _, canceled := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		if canceled {
			cancel()
		}
		finished := make(chan struct{})
		close(finished)
		if shutdownExpired(ctx, finished, time.Second) {
			t.Fatal("finished drain expired")
		}
		cancel()
	}
}
