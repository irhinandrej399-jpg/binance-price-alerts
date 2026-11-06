package health_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/health"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/live"
)

func request(h http.Handler, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
	return w
}
func TestLifecycle(t *testing.T) {
	var s live.Status
	var dbErr error
	calls := 0
	h := health.New(func(context.Context) error { calls++; return dbErr }, func() live.Status { return s }, time.Second)
	check := func(code int, lag *uint64) {
		t.Helper()
		w := request(h, "/readyz")
		var b struct {
			Status     string
			Projection live.Status
		}
		if err := json.Unmarshal(w.Body.Bytes(), &b); err != nil {
			t.Fatal(err)
		}
		if w.Code != code || (b.Projection.Lag == nil) != (lag == nil) {
			t.Fatalf("%d %s", w.Code, w.Body.String())
		}
		if lag != nil && *b.Projection.Lag != *lag {
			t.Fatal(w.Body.String())
		}
	}
	check(503, nil) // empty start
	h.SetInitialized(true)
	check(503, nil) // incomplete snapshot
	n := uint64(3)
	s = live.Status{AppliedC: 7, Head: 10, Lag: &n}
	check(503, &n) // replay incomplete
	n = 0
	s = live.Status{Ready: true, AppliedC: 10, Head: 10, Lag: &n}
	check(200, &n) // includes an empty completed projection
	s.Ready = false
	check(503, &n) // lost registration/pin/assignment
	s.Ready = true
	dbErr = errors.New("secret")
	check(503, nil)
	before := calls
	if w := request(h, "/healthz"); w.Code != 200 || calls != before {
		t.Fatal("liveness probes dependencies")
	}
	dbErr = nil
	check(200, &n)
	h.SetInitialized(false)
	check(503, nil)
}
func TestFailClosedAndDeadline(t *testing.T) {
	for _, storage := range []func(context.Context) error{nil, func(ctx context.Context) error { <-ctx.Done(); return nil }} {
		h := health.New(storage, func() live.Status { return live.Status{Ready: true} }, time.Millisecond)
		h.SetInitialized(true)
		if w := request(h, "/readyz"); w.Code != 503 {
			t.Fatal(w.Code)
		}
	}
	h := health.New(func(context.Context) error { return nil }, nil, 0)
	h.SetInitialized(true)
	if request(h, "/readyz").Code != 503 {
		t.Fatal("nil status ready")
	}
}

func TestHTTPConcurrentAndWithdrawal(t *testing.T) {
	h := health.New(func(context.Context) error { return nil }, func() live.Status { return live.Status{Ready: true} }, 0)
	h.SetInitialized(true)
	for _, tc := range []struct {
		method, path string
		code         int
	}{{"GET", "/readyz", 200}, {"HEAD", "/healthz", 200}, {"POST", "/readyz", 405}, {"GET", "/missing", 404}} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
		if w.Code != tc.code {
			t.Fatal(tc, w.Code)
		}
	}
	w := request(h, "/readyz")
	if w.Header().Get("Content-Type") != "application/json" || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal(w.Header())
	}
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			for range 20 {
				h.SetInitialized(true)
				request(h, "/readyz")
			}
		})
	}
	wg.Wait()
	entered, release := make(chan struct{}), make(chan struct{})
	h = health.New(func(context.Context) error { close(entered); <-release; return nil }, func() live.Status { return live.Status{Ready: true} }, time.Second)
	h.SetInitialized(true)
	done := make(chan int, 1)
	go func() { done <- request(h, "/readyz").Code }()
	<-entered
	h.SetInitialized(false)
	close(release)
	if <-done != 503 {
		t.Fatal("withdrawal ignored")
	}
}
