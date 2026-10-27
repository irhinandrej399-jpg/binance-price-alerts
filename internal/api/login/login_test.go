package login_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/login"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/password"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/session"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
)

func request(h http.Handler, method, path, media, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", media)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func checkError(t *testing.T, w *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if w.Code != status || w.Body.String() != fmt.Sprintf("{\"error\":{\"code\":%q}}\n", code) || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("unsafe error: %d %q", w.Code, w.Body.String())
	}
}
func TestInvalidRequest(t *testing.T) {
	h := login.New(nil, nil)
	for _, body := range []string{"", `{`, `null`, `[]`, `{}`, `{"email":1,"password":"s"}`, `{"email":"a@b","password":null}`, `{"email":" ","password":"s"}`, `{"email":"a@b","password":""}`, `{"email":"a@b","password":"s","user_id":"x"}`, `{"email":"a@b","password":"s"} {}`} {
		checkError(t, request(h, "POST", "/v1/sessions", "application/json", body), 400, "invalid_request")
	}
	checkError(t, request(h, "POST", "/v1/sessions", "text/plain", `{}`), 415, "unsupported_media_type")
	if w := request(h, "GET", "/v1/sessions", "application/json", ""); w.Code != 405 {
		t.Fatal(w.Code)
	}
	if w := request(h, "POST", "/other", "application/json", `{}`); w.Code != 404 {
		t.Fatal(w.Code)
	}
}
func TestLoginWithPostgres(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	repo := account.New(pool)
	email := fmt.Sprintf("login-%d+tag@example.com", time.Now().UnixNano())
	const plain = "  пароль-秘密  "
	hash, err := password.Hash(plain)
	if err != nil {
		t.Fatal(err)
	}
	a, err := repo.Create(ctx, email, hash)
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
	h := login.New(repo, store)
	send := func(email, pass string) *httptest.ResponseRecorder {
		b, _ := json.Marshal(map[string]string{"email": email, "password": pass})
		return request(h, "POST", "/v1/sessions", "application/json; charset=utf-8", string(b))
	}
	w := send(" \t"+strings.ToUpper(email)+"\n", plain)
	if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("login failed: %d", w.Code)
	}
	var result session.Credentials
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &fields)
	if len(fields) != 4 || strings.Contains(w.Body.String(), hash) || strings.Contains(w.Body.String(), plain) {
		t.Fatal("unexpected credential response")
	}
	if id, err := store.Verify(ctx, result.AccessToken); err != nil || id != a.ID {
		t.Fatal("issued token cannot identify owner", err)
	}
	if result.ExpiresIn != 86400 || !result.ExpiresAt.Equal(now.Add(24*time.Hour)) {
		t.Fatal("wrong TTL")
	}
	for _, input := range [][2]string{{email, "wrong"}, {"missing-" + email, plain}, {"not-an-email", plain}, {email, strings.TrimSpace(plain)}} {
		checkError(t, send(input[0], input[1]), 401, "invalid_credentials")
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM app.sessions WHERE user_id=$1", a.ID).Scan(&count); err != nil || count != 1 {
		t.Fatal("failed login must not issue session", err)
	}
	now = result.ExpiresAt
	if _, err := store.Verify(ctx, result.AccessToken); err == nil {
		t.Fatal("token must expire")
	}
	unavailable := postgrestest.Open(t)
	unavailable.Close()
	body, _ := json.Marshal(map[string]string{"email": email, "password": plain})
	for _, broken := range []http.Handler{login.New(account.New(unavailable), store), login.New(repo, session.New(unavailable, time.Now))} {
		checkError(t, request(broken, "POST", "/v1/sessions", "application/json", string(body)), 500, "internal_error")
	}
	// Corrupted stored hash also fails closed with a uniform authentication error.
	if _, err := pool.Exec(ctx, "UPDATE app.users SET password_hash='corrupt' WHERE id=$1", a.ID); err != nil {
		t.Fatal(err)
	}
	checkError(t, send(email, plain), 401, "invalid_credentials")
}
