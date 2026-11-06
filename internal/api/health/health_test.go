package health_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/health"
)

func request(h http.Handler, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	return w
}

func TestInitializationStorageFailureAndRecovery(t *testing.T) {
	var storageErr error
	calls := 0
	h := health.New(func(context.Context) error { calls++; return storageErr }, time.Second)
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
	}, time.Second)
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
			}, 10*time.Millisecond)
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
	h := health.New(nil, time.Second)
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
