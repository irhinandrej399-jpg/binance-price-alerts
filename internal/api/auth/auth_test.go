package auth_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/auth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/session"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
)

func protected(t *testing.T, store *session.Store) http.Handler {
	t.Helper()
	return auth.New(store, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, ok := auth.UserID(r.Context())
		if !ok || id == "" {
			t.Error("missing trusted owner")
		}
		_, _ = w.Write([]byte(id))
	}))
}

func checkRejected(t *testing.T, h http.Handler, headers []string, status int, code string) {
	t.Helper()
	r := httptest.NewRequest("GET", "/protected?user_id=attacker&access_token=forged", nil)
	r.Header.Set("X-User-ID", "attacker")
	for _, v := range headers {
		r.Header.Add("Authorization", v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != status || w.Body.String() != fmt.Sprintf("{\"error\":{\"code\":%q}}\n", code) {
		t.Fatalf("response: %d %s", w.Code, w.Body.String())
	}
	if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Content-Type") != "application/json" {
		t.Fatal("missing response headers")
	}
	if status == 401 && w.Header().Get("WWW-Authenticate") != "Bearer" {
		t.Fatal("missing Bearer challenge")
	}
}

func TestMissingAndMalformedCredentials(t *testing.T) {
	if id, ok := auth.UserID(context.Background()); ok || id != "" {
		t.Fatal("unauthenticated context has an owner")
	}
	// A nil DB exposes any attempt to treat malformed credentials as valid input.
	h := protected(t, session.New(nil, nil))
	for _, headers := range [][]string{nil, {""}, {"Basic abc"}, {"Bearer"}, {"Bearer "}, {"Bearer bad"}, {"Bearer a b"}, {"Bearer abc, Bearer def"}, {"Bearer abc", "Bearer def"}} {
		t.Run(fmt.Sprint(headers), func(t *testing.T) { checkRejected(t, h, headers, 401, "invalid_session") })
	}
}

func TestProtectedEndpointWithPostgres(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	a, err := account.New(pool).Create(ctx, fmt.Sprintf("auth-%d@example.com", time.Now().UnixNano()), "test-only-hash")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(ctx, "DELETE FROM app.users WHERE id=$1", a.ID); err != nil {
			t.Error(err)
		}
	})
	now := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	store := session.New(pool, func() time.Time { return now })
	credentials, err := store.Issue(ctx, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	h := protected(t, store)
	for _, scheme := range []string{"Bearer", "bearer", "BEARER"} {
		r := httptest.NewRequest("POST", "/protected?user_id=attacker", strings.NewReader(`{"user_id":"attacker"}`))
		r.Header.Set("Authorization", scheme+" "+credentials.AccessToken)
		r.Header.Set("X-User-ID", "attacker")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 200 || w.Body.String() != a.ID {
			t.Fatalf("trusted owner lost: %d %s", w.Code, w.Body.String())
		}
	}
	now = credentials.ExpiresAt.Add(-time.Microsecond)
	r := httptest.NewRequest("GET", "/protected", nil)
	r.Header.Set("Authorization", "Bearer "+credentials.AccessToken)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 || w.Body.String() != a.ID {
		t.Fatal("must accept just before expiry")
	}
	for _, offset := range []time.Duration{0, time.Microsecond} {
		now = credentials.ExpiresAt.Add(offset)
		checkRejected(t, h, []string{"Bearer " + credentials.AccessToken}, 401, "invalid_session")
	}
	now = credentials.ExpiresAt.Add(-time.Hour)
	forged := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	if forged == credentials.AccessToken {
		t.Fatal("unexpected random token")
	}
	checkRejected(t, h, []string{"Bearer " + forged}, 401, "invalid_session")
	checkRejected(t, h, []string{"Bearer " + credentials.AccessToken + "="}, 401, "invalid_session")
	unavailable := postgrestest.Open(t)
	unavailable.Close()
	checkRejected(t, protected(t, session.New(unavailable, nil)), []string{"Bearer " + credentials.AccessToken}, 500, "internal_error")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	r = r.WithContext(canceled)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 500 || w.Body.String() != "{\"error\":{\"code\":\"internal_error\"}}\n" {
		t.Fatalf("canceled request accepted or leaked error: %d %s", w.Code, w.Body.String())
	}
}
