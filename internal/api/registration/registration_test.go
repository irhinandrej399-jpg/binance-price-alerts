package registration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/password"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/registration"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
)

func request(h http.Handler, method, path, contentType, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", contentType)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func checkError(t *testing.T, w *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	want := fmt.Sprintf("{\"error\":{\"code\":%q}}\n", code)
	if w.Code != status || w.Body.String() != want || w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("unexpected public error: status %d, body %q", w.Code, w.Body.String())
	}
}

func TestInvalidRequest(t *testing.T) {
	// Invalid input must be rejected before touching storage or hashing.
	h := registration.New(nil)
	for _, body := range []string{
		``, `{`, `null`, `[]`, `{}`, `{"email":1,"password":"secret"}`,
		`{"email":"a@example.com","password":null}`,
		`{"email":"a@example.com","password":""}`,
		`{"email":"a@example.com","password":"secret","extra":true}`,
		`{"email":"a@example.com","password":"secret"} {}`,
		`{"email":"a@example.com","password":"secret"} garbage`,
	} {
		t.Run(body, func(t *testing.T) {
			checkError(t, request(h, "POST", "/v1/accounts", "application/json", body), 400, "invalid_request")
		})
	}
	for _, email := range []string{"", "missing-at", "a@", "@example.com", "Alice <a@example.com>", "a b@example.com", "a@example.com (Alice)", "a@example.com,b@example.com"} {
		body, _ := json.Marshal(map[string]string{"email": email, "password": "secret"})
		checkError(t, request(h, "POST", "/v1/accounts", "application/json", string(body)), 400, "invalid_request")
	}
	checkError(t, request(h, "POST", "/v1/accounts", "text/plain", `{}`), 415, "unsupported_media_type")
	if w := request(h, "GET", "/v1/accounts", "application/json", ""); w.Code != 405 {
		t.Fatalf("method: %d", w.Code)
	}
	if w := request(h, "POST", "/other", "application/json", `{}`); w.Code != 404 {
		t.Fatalf("path: %d", w.Code)
	}
}

func TestRegisterWithPostgres(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	email := fmt.Sprintf("register-%d+tag@example.com", time.Now().UnixNano())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := pool.Exec(ctx, "DELETE FROM app.users WHERE email=$1", email); err != nil {
			t.Error(err)
		}
	})
	repo := account.New(pool)
	h := registration.New(repo)
	invalidBody, _ := json.Marshal(map[string]string{"email": email, "password": ""})
	checkError(t, request(h, "POST", "/v1/accounts", "application/json", string(invalidBody)), 400, "invalid_request")
	if _, err := repo.ByEmail(ctx, email); !errors.Is(err, account.ErrNotFound) {
		t.Fatal("invalid request must not create an account")
	}
	const plain = "  password-秘密  "
	body, _ := json.Marshal(map[string]string{"email": " \t" + strings.ToUpper(email) + "\n", "password": plain})
	w := request(h, "POST", "/v1/accounts", "application/json; charset=utf-8", string(body))
	if w.Code != 201 || w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("registration status: %d", w.Code)
	}
	var result map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result) != 2 || result["id"] == "" || result["email"] != email {
		t.Fatal("response must contain only public id/email")
	}
	saved, err := repo.ByEmail(ctx, email)
	if err != nil {
		t.Fatal(err)
	}
	if saved.ID != result["id"] || saved.PasswordHash == plain {
		t.Fatal("incorrect persisted account")
	}
	if err := password.Verify(saved.PasswordHash, plain); err != nil {
		t.Fatal("stored password does not verify")
	}
	checkError(t, request(h, "POST", "/v1/accounts", "application/json", string(body)), 409, "email_exists")
	after, err := repo.ByEmail(ctx, email)
	if err != nil || after != saved {
		t.Fatal("duplicate changed saved account")
	}
	// Valid request against unavailable real storage must expose no SQL/credentials.
	unavailable := postgrestest.Open(t)
	unavailable.Close()
	checkError(t, request(registration.New(account.New(unavailable)), "POST", "/v1/accounts", "application/json", string(body)), 500, "internal_error")
}
