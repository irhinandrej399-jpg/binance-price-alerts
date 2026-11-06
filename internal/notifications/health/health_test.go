package health_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/health"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/jobs"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/telegram"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/worker"
)

func request(h http.Handler, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	return w
}

func TestInitializationStorageFailureAndRecovery(t *testing.T) {
	var storageErr error
	calls := 0
	h := health.New(func(context.Context) error { calls++; return storageErr }, func() bool { return true }, time.Second)
	if w := request(h, "/healthz"); w.Code != 200 || calls != 0 {
		t.Fatalf("liveness before initialization: status=%d probes=%d", w.Code, calls)
	}
	if w := request(h, "/readyz"); w.Code != 503 || calls != 0 {
		t.Fatalf("before initialization: status=%d probes=%d", w.Code, calls)
	}
	h.SetInitialized(true)
	if w := request(h, "/readyz"); w.Code != 200 {
		t.Fatalf("available storage: %d", w.Code)
	}
	storageErr = errors.New("write failed: disk full postgres://secret")
	if w := request(h, "/readyz"); w.Code != 503 || strings.Contains(w.Body.String(), "secret") {
		t.Fatalf("failed storage: %d %s", w.Code, w.Body.String())
	}
	before := calls
	if w := request(h, "/healthz"); w.Code != 200 || calls != before {
		t.Fatalf("liveness must not probe dependencies: %d probes=%d", w.Code, calls)
	}
	storageErr = nil
	if w := request(h, "/readyz"); w.Code != 200 {
		t.Fatalf("recovered storage: %d", w.Code)
	}
	h.SetInitialized(false)
	if w := request(h, "/readyz"); w.Code != 503 {
		t.Fatalf("withdrawn initialization: %d", w.Code)
	}
}

func TestInitializationWithdrawnDuringProbe(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	h := health.New(func(context.Context) error {
		close(started)
		<-release
		return nil
	}, func() bool { return true }, time.Second)
	h.SetInitialized(true)
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() { result <- request(h, "/readyz") }()
	<-started
	h.SetInitialized(false)
	close(release)
	if w := <-result; w.Code != 503 {
		t.Fatalf("initialization withdrawn during probe: %d", w.Code)
	}
}

func TestDeadlineAndRequestCancellation(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(map[bool]string{false: "timeout", true: "canceled"}[canceled], func(t *testing.T) {
			h := health.New(func(ctx context.Context) error {
				if _, ok := ctx.Deadline(); !ok {
					t.Error("missing probe deadline")
				}
				<-ctx.Done()
				return nil // A canceled check cannot report success, even with a nil result.
			}, func() bool { return true }, 10*time.Millisecond)
			h.SetInitialized(true)
			r := httptest.NewRequest(http.MethodGet, "/readyz", nil)
			if canceled {
				ctx, cancel := context.WithCancel(r.Context())
				cancel()
				r = r.WithContext(ctx)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != 503 {
				t.Fatalf("status=%d", w.Code)
			}
		})
	}
}

func TestMissingProbeAndHTTPContract(t *testing.T) {
	h := health.New(nil, func() bool { return true }, time.Second)
	h.SetInitialized(true)
	if w := request(h, "/readyz"); w.Code != 503 {
		t.Fatalf("nil probe: %d", w.Code)
	}
	w := request(h, "/healthz")
	if w.Code != 200 || w.Body.String() != "{\"status\":\"ok\"}\n" || w.Header().Get("Content-Type") != "application/json" || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("liveness response: %v %s", w.Result(), w.Body.String())
	}
	if w := request(h, "/other"); w.Code != 404 {
		t.Fatalf("unknown route: %d", w.Code)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/readyz", nil))
	if w.Code != 405 {
		t.Fatalf("POST: %d", w.Code)
	}
}

func TestWorkerObservationFailsClosed(t *testing.T) {
	for _, running := range []func() bool{nil, func() bool { return false }} {
		h := health.New(func(context.Context) error { return nil }, running, 0)
		h.SetInitialized(true)
		if w := request(h, "/readyz"); w.Code != 503 {
			t.Fatalf("missing/stopped worker: %d", w.Code)
		}
		if w := request(h, "/healthz"); w.Code != 200 {
			t.Fatalf("liveness: %d", w.Code)
		}
	}
	var running atomic.Bool
	running.Store(true)
	h := health.New(func(context.Context) error { running.Store(false); return nil }, running.Load, time.Second)
	h.SetInitialized(true)
	if w := request(h, "/readyz"); w.Code != 503 {
		t.Fatalf("worker exited during probe: %d", w.Code)
	}
}

type sourceFunc func(context.Context, string, int) ([]jobs.Work, error)

func (f sourceFunc) Discover(c context.Context, s string, n int) ([]jobs.Work, error) {
	return f(c, s, n)
}
func (sourceFunc) ScheduleRecovery(context.Context, string, int64, time.Duration) error { return nil }

type handleFunc func(context.Context, string) error

func (f handleFunc) Handle(c context.Context, s string) error { return f(c, s) }
func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("timeout")
		var zero T
		return zero
	}
}

func TestRealWorkerSurvivesTelegram429AndStorageFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
		_, _ = w.Write([]byte(`{"ok":false,"error_code":429,"parameters":{"retry_after":2}}`))
	}))
	defer server.Close()
	sender, err := telegram.New(telegram.Config{Token: "test", BaseURL: server.URL, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	observed := make(chan error, 10)
	var unavailable atomic.Bool
	var first atomic.Bool

	w, err := worker.New(worker.Config{
		Source: sourceFunc(func(ctx context.Context, _ string, _ int) ([]jobs.Work, error) {
			if unavailable.Load() {
				return nil, errors.New("storage down")
			}
			if first.CompareAndSwap(false, true) {
				return []jobs.Work{{TriggerID: "test"}}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}),
		Handler:     handleFunc(func(ctx context.Context, _ string) error { _, err := sender.Send(ctx, 1, "test"); return err }),
		Concurrency: 1, Interval: time.Millisecond, Timeout: time.Second, WriteTimeout: time.Second, RecoveryInterval: time.Second,
		OnError: func(err error) { observed <- err },
	})
	if err != nil {
		t.Fatal(err)
	}
	h := health.New(func(context.Context) error {
		if unavailable.Load() {
			return errors.New("storage down")
		}
		return nil
	}, w.Running, time.Second)
	h.SetInitialized(true)
	if request(h, "/readyz").Code != 503 {
		t.Fatal("worker has not started")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	var te *telegram.Error
	if err := receive(t, observed); !errors.As(err, &te) || te.Kind != telegram.RateLimited || te.RetryAfterSeconds != 2 {
		t.Fatalf("unexpected Telegram error: %v", err)
	}
	for range 10 {
		if request(h, "/healthz").Code != 200 || request(h, "/readyz").Code != 200 {
			t.Fatal("429 changed health/readiness")
		}
	}
	unavailable.Store(true)
	if request(h, "/readyz").Code != 503 || request(h, "/healthz").Code != 200 {
		t.Fatal("storage failure contract")
	}
	unavailable.Store(false)
	if request(h, "/readyz").Code != 200 {
		t.Fatal("storage recovery")
	}
	cancel()
	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if w.Running() || request(h, "/readyz").Code != 503 || request(h, "/healthz").Code != 200 {
		t.Fatal("worker exit contract")
	}
}

func TestConcurrentRequestsAndInitialization(t *testing.T) {
	h := health.New(func(context.Context) error { return nil }, func() bool { return true }, time.Second)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 50 {
				h.SetInitialized(true)
				code := request(h, "/readyz").Code
				if code != 200 && code != 503 {
					t.Errorf("status %d", code)
				}
				h.SetInitialized(false)
			}
		})
	}
	wg.Wait()
}
