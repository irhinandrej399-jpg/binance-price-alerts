package health_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/health"
)

func request(h http.Handler, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
	return w
}

func check(t *testing.T, h http.Handler, code int, feed health.FeedState) {
	t.Helper()
	w := request(h, "/readyz")
	var body struct {
		Status string
		Feed   health.FeedState
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	want := "not_ready"
	if code == 200 {
		want = "ready"
	}
	if w.Code != code || body.Status != want || body.Feed != feed {
		t.Fatalf("want %d %s %s; got %d %s", code, want, feed, w.Code, w.Body.String())
	}
}

func TestFeedLifecycleAndIdlePair(t *testing.T) {
	state := health.Unconfirmed
	var storageErr error
	calls := 0
	h := health.New(func(context.Context) error { calls++; return storageErr },
		func(context.Context) (health.FeedState, error) { calls++; return state, nil }, time.Second)
	check(t, h, 503, health.Starting)
	if calls != 0 {
		t.Fatal("startup probes")
	}
	h.SetInitialized(true)
	check(t, h, 503, health.Unconfirmed)
	state = health.Live // Explicit feed confirmation, independent of a trade's age.
	check(t, h, 200, health.Live)
	// A rare pair sends no new trades. Readiness has no clock/quote-age input.
	check(t, h, 200, health.Live)
	state = health.Disconnected
	check(t, h, 503, health.Disconnected)
	before := calls
	if w := request(h, "/healthz"); w.Code != 200 || calls != before {
		t.Fatal("reconnect affected liveness")
	}
	state = health.Unconfirmed // Opening a connection or ACK alone is insufficient.
	check(t, h, 503, health.Unconfirmed)
	state = health.Live
	check(t, h, 200, health.Live)
	storageErr = errors.New("write failed postgres://secret")
	if w := request(h, "/readyz"); w.Code != 503 || strings.Contains(w.Body.String(), "secret") {
		t.Fatal("storage failure hidden/leaked")
	}
	before = calls
	if w := request(h, "/healthz"); w.Code != 200 || calls != before {
		t.Fatal("storage affected liveness")
	}
	storageErr = nil
	check(t, h, 200, health.Live)
	h.SetInitialized(false)
	check(t, h, 503, health.Starting)
}

func TestFailClosed(t *testing.T) {
	for _, tc := range []struct {
		name    string
		storage func(context.Context) error
		feed    func(context.Context) (health.FeedState, error)
	}{
		{"nil storage", nil, func(context.Context) (health.FeedState, error) { return health.Live, nil }},
		{"nil feed", func(context.Context) error { return nil }, nil},
		{"unknown", func(context.Context) error { return nil }, func(context.Context) (health.FeedState, error) { return "secret", nil }},
		{"feed error", func(context.Context) error { return nil }, func(context.Context) (health.FeedState, error) { return health.Live, errors.New("secret") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := health.New(tc.storage, tc.feed, 0)
			h.SetInitialized(true)
			w := request(h, "/readyz")
			if w.Code != 503 || strings.Contains(w.Body.String(), "secret") {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestCancellationAndTimeout(t *testing.T) {
	for _, stage := range []string{"storage", "feed"} {
		for _, canceled := range []bool{false, true} {
			t.Run(stage+map[bool]string{false: " timeout", true: " canceled"}[canceled], func(t *testing.T) {
				wait := func(ctx context.Context) {
					t.Helper()
					if _, ok := ctx.Deadline(); !ok {
						t.Error("no deadline")
					}
					<-ctx.Done()
				}
				h := health.New(func(ctx context.Context) error {
					if stage == "storage" {
						wait(ctx)
					}
					return nil
				}, func(ctx context.Context) (health.FeedState, error) {
					if stage == "feed" {
						wait(ctx)
					}
					return health.Live, nil
				}, 10*time.Millisecond)
				h.SetInitialized(true)
				r := httptest.NewRequest("GET", "/readyz", nil)
				if canceled {
					ctx, cancel := context.WithCancel(r.Context())
					cancel()
					r = r.WithContext(ctx)
				}
				w := httptest.NewRecorder()
				h.ServeHTTP(w, r)
				if w.Code != 503 {
					t.Fatalf("canceled probe returned %d", w.Code)
				}
			})
		}
	}
}

func TestInitializationWithdrawnDuringProbe(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	h := health.New(func(context.Context) error { return nil }, func(context.Context) (health.FeedState, error) { close(started); <-release; return health.Live, nil }, time.Second)
	h.SetInitialized(true)
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() { result <- request(h, "/readyz") }()
	<-started
	h.SetInitialized(false)
	close(release)
	if w := <-result; w.Code != 503 {
		t.Fatalf("withdrawn initialization: %d", w.Code)
	}
}

func TestHTTPAndConcurrentRequests(t *testing.T) {
	h := health.New(func(context.Context) error { return nil }, func(context.Context) (health.FeedState, error) { return health.Live, nil }, 0)
	h.SetInitialized(true)
	for _, path := range []string{"/healthz", "/readyz"} {
		w := request(h, path)
		if w.Code != 200 || w.Header().Get("Content-Type") != "application/json" || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("HTTP contract: %v", w.Result())
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("POST", path, nil))
		if w.Code != 405 {
			t.Fatal(w.Code)
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("HEAD", path, nil))
		if w.Code != 200 {
			t.Fatal(w.Code)
		}
	}
	if w := request(h, "/other"); w.Code != 404 {
		t.Fatal(w.Code)
	}
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			for range 20 {
				h.SetInitialized(true)
				if request(h, "/readyz").Code != 200 {
					t.Error("concurrent readiness")
				}
			}
		})
	}
	wg.Wait()
}
