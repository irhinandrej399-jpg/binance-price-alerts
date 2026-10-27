package telegramcode_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/session"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/telegramcode"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
)

func TestUnauthenticated(t *testing.T) {
	h := telegramcode.Handler(session.New(nil, nil), telegramcode.New(nil, nil))
	r := httptest.NewRequest("POST", "/v1/telegram/link-codes?user_id=foreign", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatalf("status %d", w.Code)
	}
}

func TestIssuanceWithPostgres(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	create := func(label string) string {
		t.Helper()
		a, err := account.New(pool).Create(ctx, fmt.Sprintf("code-%s-%d@example.com", label, time.Now().UnixNano()), "test-only-hash")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if _, err := pool.Exec(ctx, "DELETE FROM app.users WHERE id=$1", a.ID); err != nil {
				t.Error(err)
			}
		})
		return a.ID
	}
	owner, foreign := create("owner"), create("foreign")
	now := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	store := telegramcode.New(pool, func() time.Time { return now })
	sessions := session.New(pool, func() time.Time { return now })
	bearer, err := sessions.Issue(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	foreignCode, err := store.Issue(ctx, foreign)
	if err != nil {
		t.Fatal(err)
	}
	h := telegramcode.Handler(sessions, store)
	issue := func(body string, status int) telegramcode.Code {
		t.Helper()
		r := httptest.NewRequest("POST", "/v1/telegram/link-codes?user_id="+foreign, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+bearer.AccessToken)
		r.Header.Set("X-User-ID", foreign)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != status {
			t.Fatalf("status %d: %s", w.Code, w.Body.String())
		}
		if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Content-Type") != "application/json" {
			t.Fatal("missing headers")
		}
		var result telegramcode.Code
		if status == 201 {
			if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
		} else if w.Body.String() != "{\"error\":{\"code\":\"invalid_request\"}}\n" {
			t.Fatal("unexpected error")
		}
		return result
	}
	first := issue("", 201)
	raw, err := base64.RawURLEncoding.Strict().DecodeString(first.Value)
	if err != nil || len(raw) != 32 || len(first.Value) != 43 || first.ExpiresIn != 600 || !first.ExpiresAt.Equal(now.Add(10*time.Minute)) {
		t.Fatal("incorrect code format/lifetime")
	}
	valid := func(code string, at time.Time) string {
		t.Helper()
		digest := sha256.Sum256([]byte(code))
		var id string
		err := pool.QueryRow(ctx, "SELECT user_id::text FROM app.telegram_link_codes WHERE code_hash=$1 AND expires_at>$2", digest[:], at).Scan(&id)
		if err != nil && err.Error() != "no rows in result set" {
			t.Fatal(err)
		}
		return id
	}
	if valid(first.Value, first.ExpiresAt.Add(-time.Microsecond)) != owner || valid(first.Value, first.ExpiresAt) != "" || valid(first.Value, first.ExpiresAt.Add(time.Second)) != "" {
		t.Fatal("owner or expiry boundary")
	}
	// Only the digest persists; the table has no plaintext credential column.
	var digest []byte
	var expiry time.Time
	if err := pool.QueryRow(ctx, "SELECT code_hash,expires_at FROM app.telegram_link_codes WHERE user_id=$1", owner).Scan(&digest, &expiry); err != nil {
		t.Fatal(err)
	}
	expected := sha256.Sum256([]byte(first.Value))
	if string(digest) != string(expected[:]) || !expiry.Equal(first.ExpiresAt) {
		t.Fatal("stored credential differs")
	}
	issue(`{"user_id":"`+foreign+`"}`, 400)
	if valid(first.Value, now) != owner {
		t.Fatal("rejected input replaced code")
	}
	second := issue("", 201)
	if second.Value == first.Value || valid(first.Value, now) != "" || valid(second.Value, now) != owner || valid(foreignCode.Value, now) != foreign {
		t.Fatal("replacement or isolation failed")
	}
	var wg sync.WaitGroup
	results := make(chan telegramcode.Code, 8)
	for range 8 {
		wg.Go(func() {
			c, err := store.Issue(ctx, owner)
			if err != nil {
				t.Error(err)
				return
			}
			results <- c
		})
	}
	wg.Wait()
	close(results)
	live := 0
	seen := map[string]bool{}
	for c := range results {
		if seen[c.Value] {
			t.Fatal("repeated random code")
		}
		seen[c.Value] = true
		if valid(c.Value, now) == owner {
			live++
		}
	}
	if live != 1 || len(seen) != 8 {
		t.Fatalf("concurrent issuance left %d live codes", live)
	}
	if valid(foreignCode.Value, now) != foreign {
		t.Fatal("other account affected")
	}
	if _, err := store.Issue(ctx, "00000000-0000-0000-0000-000000000000"); !errors.Is(err, telegramcode.ErrStorage) {
		t.Fatal("missing owner accepted", err)
	}
	broken := postgrestest.Open(t)
	broken.Close()
	failed := telegramcode.Handler(sessions, telegramcode.New(broken, nil))
	r := httptest.NewRequest(http.MethodPost, "/v1/telegram/link-codes", nil)
	r.Header.Set("Authorization", "Bearer "+bearer.AccessToken)
	w := httptest.NewRecorder()
	failed.ServeHTTP(w, r)
	if w.Code != 500 || w.Body.String() != "{\"error\":{\"code\":\"internal_error\"}}\n" {
		t.Fatalf("storage error leaked: %d %s", w.Code, w.Body.String())
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if c, err := store.Issue(canceled, owner); !errors.Is(err, context.Canceled) || c.Value != "" {
		t.Fatal("canceled issue accepted", err)
	}
	if _, err := pool.Exec(ctx, "DELETE FROM app.users WHERE id=$1", owner); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM app.telegram_link_codes WHERE user_id=$1", owner).Scan(&count); err != nil || count != 0 {
		t.Fatal("orphan code", err)
	}
}
