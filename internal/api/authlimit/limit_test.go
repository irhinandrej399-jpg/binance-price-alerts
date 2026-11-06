package authlimit

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/bodylimit"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/login"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/registration"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/session"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
)

func TestSharedQuota(t *testing.T) {
	p := postgrestest.Open(t)
	q := postgrestest.Open(t)
	ctx := context.Background()
	_, err := p.Exec(ctx, `DELETE FROM app.auth_limits`)
	if err != nil {
		t.Fatal(err)
	}
	var unix atomic.Int64
	start := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	unix.Store(start.UnixNano())
	clock := func() time.Time { return time.Unix(0, unix.Load()) }
	a, b := New(p, clock), New(q, clock)
	var admitted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 60; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := a
			if i%2 == 0 {
				s = b
			}
			ok, _, e := s.Allow(ctx, "192.0.2.1")
			if e != nil {
				t.Error(e)
			}
			if ok {
				admitted.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if admitted.Load() != 20 {
		t.Fatalf("admitted %d, want 20", admitted.Load())
	}
	unix.Store(start.Add(time.Minute - time.Nanosecond).UnixNano())
	if ok, retry, e := b.Allow(ctx, "192.0.2.1"); e != nil || ok || retry != 1 {
		t.Fatalf("last instant: %v %d %v", ok, retry, e)
	}
	if ok, _, e := b.Allow(ctx, "192.0.2.2"); e != nil || !ok {
		t.Fatalf("independent key: %v %v", ok, e)
	}
	unix.Store(start.Add(time.Minute).UnixNano())
	if ok, _, e := New(q, clock).Allow(ctx, "192.0.2.1"); e != nil || !ok {
		t.Fatalf("new window/restart: %v %v", ok, e)
	}
	// A backward clock must not reopen an already used older window.
	unix.Store(start.UnixNano())
	for i := 0; i < 19; i++ {
		if ok, _, e := a.Allow(ctx, "192.0.2.1"); e != nil || !ok {
			t.Fatal("clock regression lost current window")
		}
	}
	if ok, _, e := a.Allow(ctx, "::ffff:192.0.2.1"); e != nil || ok {
		t.Fatalf("mapped IP bypass: %v %v", ok, e)
	}
}

func TestHTTPSharedHandlers(t *testing.T) {
	p := postgrestest.Open(t)
	ctx := context.Background()
	if _, e := p.Exec(ctx, `DELETE FROM app.auth_limits`); e != nil {
		t.Fatal(e)
	}
	now := func() time.Time { return time.Date(2030, 1, 1, 0, 0, 10, 0, time.UTC) }
	calls := 0
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(401) })
	a, b := New(p, now).Handler(next), New(p, now).Handler(next)
	for i := 0; i < 21; i++ {
		path := "/v1/accounts"
		h := a
		if i%2 == 0 {
			path = "/v1/sessions"
			h = b
		}
		r := httptest.NewRequest("POST", path, nil)
		r.RemoteAddr = "[::ffff:192.0.2.10]:1234"
		r.Header.Set("X-Forwarded-For", "198.51.100."+strconv.Itoa(i))
		r.Header.Set("Forwarded", "for=198.51.100."+strconv.Itoa(i))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		want := 401
		if i == 20 {
			want = 429
			if w.Header().Get("Retry-After") != "50" {
				t.Fatal(w.Header())
			}
		}
		if w.Code != want {
			t.Fatalf("request %d: %d %s", i, w.Code, w.Body)
		}
	}
	if calls != 20 {
		t.Fatal(calls)
	}
	// Storage failure must never invoke the credential handler.
	p.Close()
	r := httptest.NewRequest("POST", "/v1/sessions", nil)
	r.RemoteAddr = "192.0.2.10:9"
	w := httptest.NewRecorder()
	b.ServeHTTP(w, r)
	if w.Code != 503 || calls != 20 || w.Body.String() != "{\"error\":{\"code\":\"auth_limit_unavailable\"}}\n" {
		t.Fatalf("unsafe storage error: %d %s", w.Code, w.Body)
	}
}

func TestInvalidPeer(t *testing.T) {
	h := New(nil, nil).Handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("credential handler called") }))
	for _, peer := range []string{"", "invalid", "hostname:80", "[fe80::1%en0]:80", "192.0.2.1/24:80"} {
		r := httptest.NewRequest("POST", "/v1/accounts", nil)
		r.RemoteAddr = peer
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 400 || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("invalid peer: %d", w.Code)
		}
	}
}

func TestCredentialHandlersAndRouting(t *testing.T) {
	p := postgrestest.Open(t)
	ctx := context.Background()
	if _, e := p.Exec(ctx, `DELETE FROM app.auth_limits`); e != nil {
		t.Fatal(e)
	}
	email := "ops-013@example.com"
	if _, e := p.Exec(ctx, `DELETE FROM app.users WHERE email=$1`, email); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _, _ = p.Exec(ctx, `DELETE FROM app.users WHERE email=$1`, email) })
	s := New(p, func() time.Time { return time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC) })
	mux := http.NewServeMux()
	mux.Handle("POST /v1/accounts", s.Handler(registration.New(account.New(p))))
	mux.Handle("POST /v1/sessions", s.Handler(login.New(account.New(p), session.New(p, nil))))
	h := bodylimit.New(bodylimit.PublicLimit, mux)
	request := func(method, path, body string, want int) {
		t.Helper()
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.RemoteAddr = "[2001:db8::13]:9999"
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("%s %s: %d %s", method, path, w.Code, w.Body)
		}
	}
	request("GET", "/v1/accounts", "", 405)
	request("POST", "/v1/absent", "", 404)
	request("POST", "/v1/accounts", strings.Repeat("x", int(bodylimit.PublicLimit)+1), 413)
	body := `{"email":"ops-013@example.com","password":"secret"}`
	request("POST", "/v1/accounts", body, 201)
	request("POST", "/v1/sessions", body, 200)
	for i := 0; i < 18; i++ {
		request("POST", "/v1/sessions", "{", 400)
	}
	request("POST", "/v1/accounts", body, 429)
	request("POST", "/v1/sessions", body, 429)
	var sessions, used int
	if e := p.QueryRow(ctx, `SELECT count(*) FROM app.sessions s JOIN app.users u ON u.id=s.user_id WHERE email=$1`, email).Scan(&sessions); e != nil {
		t.Fatal(e)
	}
	if e := p.QueryRow(ctx, `SELECT used FROM app.auth_limits WHERE ip='2001:db8::13'`).Scan(&used); e != nil {
		t.Fatal(e)
	}
	if sessions != 1 || used != 20 {
		t.Fatalf("sessions=%d attempts=%d", sessions, used)
	}
}

func TestDatabaseClockAndDeadline(t *testing.T) {
	p := postgrestest.Open(t)
	ctx := context.Background()
	if _, e := p.Exec(ctx, `DELETE FROM app.auth_limits`); e != nil {
		t.Fatal(e)
	}
	s := New(p, nil)
	var before, after, start time.Time
	if e := p.QueryRow(ctx, `SELECT date_trunc('minute',clock_timestamp())`).Scan(&before); e != nil {
		t.Fatal(e)
	}
	if ok, _, e := s.Allow(ctx, "192.0.2.99"); e != nil || !ok {
		t.Fatal("database clock admission failed")
	}
	if e := p.QueryRow(ctx, `SELECT window_start,date_trunc('minute',clock_timestamp()) FROM app.auth_limits WHERE ip='192.0.2.99'`).Scan(&start, &after); e != nil {
		t.Fatal(e)
	}
	if start.Before(before) || start.After(after) {
		t.Fatal("window does not match database clock")
	}
	tx, e := p.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, `SELECT 1 FROM app.auth_limits WHERE ip='192.0.2.99' FOR UPDATE`); e != nil {
		t.Fatal(e)
	}
	h := s.Handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("handler called after admission timeout") }))
	r := httptest.NewRequest("POST", "/v1/sessions", nil)
	r.RemoteAddr = "192.0.2.99:9999"
	w := httptest.NewRecorder()
	began := time.Now()
	h.ServeHTTP(w, r)
	if w.Code != 503 || time.Since(began) > 3*time.Second {
		t.Fatalf("deadline: %d %s", w.Code, time.Since(began))
	}
	if e = tx.Rollback(ctx); e != nil {
		t.Fatal(e)
	}
	var used int
	if e = p.QueryRow(ctx, `SELECT used FROM app.auth_limits WHERE ip='192.0.2.99'`).Scan(&used); e != nil || used != 1 {
		t.Fatalf("failed admission changed quota: %d %v", used, e)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if ok, _, e := s.Allow(cancelled, "192.0.2.100"); e == nil || ok {
		t.Fatal("cancelled admission authorized")
	}
}
