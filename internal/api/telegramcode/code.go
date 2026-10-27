// Package telegramcode issues API-owned credentials for Telegram linking.
package telegramcode

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/auth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/session"
)

const TTL = 10 * time.Minute

var ErrStorage = errors.New("Telegram code storage operation failed")

// Code contains a secret returned only at issuance. Never log it.
type Code struct {
	Value     string    `json:"code"`
	ExpiresIn int       `json:"expires_in"`
	ExpiresAt time.Time `json:"expires_at"`
}

type Store struct {
	db  account.DB
	now func() time.Time
}

// New accepts a committing API pool and an optional clock. If db is a
// transaction, the caller must commit before exposing the code.
func New(db account.DB, now func() time.Time) *Store {
	if now == nil {
		now = time.Now
	}
	return &Store{db: db, now: now}
}

// Issue atomically replaces the owner's previous code. The user row lock is
// first in the DEC-006 order; future consumption must use that same lock order.
func (s *Store) Issue(ctx context.Context, userID string) (Code, error) {
	raw := make([]byte, 32)
	rand.Read(raw)
	value := base64.RawURLEncoding.EncodeToString(raw)
	digest := sha256.Sum256([]byte(value))
	expires := s.now().UTC().Truncate(time.Microsecond).Add(TTL)
	var saved time.Time
	err := s.db.QueryRow(ctx, `WITH owner AS (
 SELECT id FROM app.users WHERE id=$1 FOR UPDATE
 ) INSERT INTO app.telegram_link_codes(user_id,code_hash,expires_at)
 SELECT id,$2,$3 FROM owner
 ON CONFLICT(user_id) DO UPDATE SET code_hash=EXCLUDED.code_hash, expires_at=EXCLUDED.expires_at
 RETURNING expires_at`, userID, digest[:], expires).Scan(&saved)
	if err != nil {
		if ctx.Err() != nil {
			return Code{}, ctx.Err()
		}
		return Code{}, ErrStorage
	}
	return Code{Value: value, ExpiresIn: int(TTL / time.Second), ExpiresAt: saved}, nil
}

// Handler serves POST /v1/telegram/link-codes, including Bearer authentication.
// The request has no body; owner selection is exclusively auth.UserID.
func Handler(sessions *session.Store, codes *Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/telegram/link-codes", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		// Read at most one byte; reject any body without buffering client input.
		var input [1]byte
		n, err := io.ReadFull(r.Body, input[:])
		if n != 0 || err != io.EOF {
			writeError(w, 400, "invalid_request")
			return
		}
		id, ok := auth.UserID(r.Context())
		if !ok {
			writeError(w, 401, "invalid_session")
			return
		}
		code, err := codes.Issue(r.Context(), id)
		if err != nil {
			writeError(w, 500, "internal_error")
			return
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(code)
	})
	return auth.New(sessions, mux)
}

func writeError(w http.ResponseWriter, status int, code string) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]map[string]string{"error": {"code": code}})
}
