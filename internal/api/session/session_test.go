package session_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/session"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
)

func TestSessionsWithPostgres(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	a, err := account.New(pool).Create(ctx, fmt.Sprintf("session-%d@example.com", time.Now().UnixNano()), "test-only-hash")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err := pool.Exec(ctx, "DELETE FROM app.users WHERE id=$1", a.ID)
		if err != nil {
			t.Error(err)
		}
	})
	now := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	store := session.New(pool, func() time.Time { return now })
	first, err := store.Issue(ctx, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Issue(ctx, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(first.AccessToken)
	if err != nil || len(raw) != 32 || first.AccessToken == second.AccessToken {
		t.Fatal("tokens must be independent random 256-bit values")
	}
	if first.TokenType != "Bearer" || first.ExpiresIn != 86400 || !first.ExpiresAt.Equal(now.Add(24*time.Hour)) {
		t.Fatal("incorrect session lifetime")
	}
	digest := sha256.Sum256([]byte(first.AccessToken))
	var userID string
	var expires time.Time
	if err := pool.QueryRow(ctx, "SELECT user_id::text, expires_at FROM app.sessions WHERE token_hash=$1", digest[:]).Scan(&userID, &expires); err != nil || userID != a.ID || !expires.Equal(first.ExpiresAt) {
		t.Fatal("only digest and owner/expiry must persist", err)
	}
	now = first.ExpiresAt.Add(-time.Microsecond)
	if id, err := store.Verify(ctx, first.AccessToken); err != nil || id != a.ID {
		t.Fatal("must be valid just before expiry", err)
	}
	now = first.ExpiresAt
	if _, err := store.Verify(ctx, first.AccessToken); !errors.Is(err, session.ErrInvalid) {
		t.Fatal("must expire at exact boundary", err)
	}
	now = now.Add(time.Hour)
	if _, err := store.Verify(ctx, first.AccessToken); !errors.Is(err, session.ErrInvalid) {
		t.Fatal("expired session accepted", err)
	}
	now = now.Add(-2 * time.Hour)
	for _, token := range []string{"", "bad", first.AccessToken + "=", "\n" + first.AccessToken, base64.RawURLEncoding.EncodeToString(make([]byte, 32))} {
		if _, err := store.Verify(ctx, token); !errors.Is(err, session.ErrInvalid) {
			t.Fatal("invalid token accepted", err)
		}
	}
	if _, err := store.Issue(ctx, "00000000-0000-0000-0000-000000000000"); !errors.Is(err, session.ErrStorage) {
		t.Fatal("missing owner must fail", err)
	}
	// Deleting the owner cascades to all sessions.
	if _, err := pool.Exec(ctx, "DELETE FROM app.users WHERE id=$1", a.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Verify(ctx, second.AccessToken); !errors.Is(err, session.ErrInvalid) {
		t.Fatal("deleted owner's token accepted", err)
	}
	unavailable := postgrestest.Open(t)
	unavailable.Close()
	broken := session.New(unavailable, func() time.Time { return now })
	if _, err := broken.Issue(ctx, a.ID); !errors.Is(err, session.ErrStorage) {
		t.Fatal("must report safe storage error", err)
	}
	if _, err := broken.Verify(ctx, first.AccessToken); !errors.Is(err, session.ErrStorage) {
		t.Fatal("must report safe storage error", err)
	}
}
