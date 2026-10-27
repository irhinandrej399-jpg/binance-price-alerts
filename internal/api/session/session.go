// Package session issues and verifies API-owned opaque bearer credentials.
package session

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	"github.com/jackc/pgx/v5"
)

var (
	ErrInvalid = errors.New("invalid session")
	ErrStorage = errors.New("session storage operation failed")
)

const TTL = 24 * time.Hour

// Credentials is returned only upon issuance; do not log it.
type Credentials struct {
	AccessToken string    `json:"access_token"`
	TokenType   string    `json:"token_type"`
	ExpiresIn   int       `json:"expires_in"`
	ExpiresAt   time.Time `json:"expires_at"`
}

type Store struct {
	db  account.DB
	now func() time.Time
}

// New accepts a committing pool and an optional clock (nil means time.Now).
// Passing a transaction makes its commit the caller's responsibility.
func New(db account.DB, now func() time.Time) *Store {
	if now == nil {
		now = time.Now
	}
	return &Store{db: db, now: now}
}

func (s *Store) Issue(ctx context.Context, userID string) (Credentials, error) {
	raw := make([]byte, 32)
	rand.Read(raw)
	token := base64.RawURLEncoding.EncodeToString(raw)
	digest := sha256.Sum256([]byte(token))
	expires := s.now().UTC().Truncate(time.Microsecond).Add(TTL)
	var saved time.Time
	err := s.db.QueryRow(ctx, `INSERT INTO app.sessions(token_hash,user_id,expires_at) VALUES ($1,$2,$3) RETURNING expires_at`, digest[:], userID, expires).Scan(&saved)
	if err != nil {
		return Credentials{}, storageError(ctx)
	}
	return Credentials{AccessToken: token, TokenType: "Bearer", ExpiresIn: int(TTL / time.Second), ExpiresAt: saved}, nil
}

// Verify returns the trusted owner only while now < expires_at. All invalid
// tokens share one error; unavailable storage is distinguished for callers.
func (s *Store) Verify(ctx context.Context, token string) (string, error) {
	if len(token) != 43 {
		return "", ErrInvalid
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil || len(raw) != 32 || base64.RawURLEncoding.EncodeToString(raw) != token {
		return "", ErrInvalid
	}
	digest := sha256.Sum256([]byte(token))
	var userID string
	err = s.db.QueryRow(ctx, `SELECT user_id::text FROM app.sessions WHERE token_hash=$1 AND expires_at > $2`, digest[:], s.now().UTC()).Scan(&userID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrInvalid
	}
	if err != nil {
		return "", storageError(ctx)
	}
	return userID, nil
}

func storageError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return ErrStorage
}
