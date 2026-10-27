// Package telegramlink atomically consumes API-owned Telegram linking codes.
package telegramlink

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrInvalidCode = errors.New("invalid or expired Telegram code")
	ErrChatInUse   = errors.New("Telegram chat already linked")
	ErrInvalidChat = errors.New("private Telegram chat required")
	ErrStorage     = errors.New("Telegram link storage operation failed")
)

type DB interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}
type Store struct {
	db  DB
	now func() time.Time
}

func New(db DB, now func() time.Time) *Store {
	if now == nil {
		now = time.Now
	}
	return &Store{db: db, now: now}
}
func validCode(code string) bool {
	raw, err := base64.RawURLEncoding.Strict().DecodeString(code)
	return err == nil && len(code) == 43 && len(raw) == 32
}

// Consume commits before returning success. Call only after authenticating the
// webhook and verifying that chatID belongs to an ordinary private message.
func (s *Store) Consume(ctx context.Context, code string, chatID int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if chatID <= 0 {
		return ErrInvalidChat
	}
	if !validCode(code) {
		return ErrInvalidCode
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return storageError(ctx, err)
	}
	defer tx.Rollback(context.Background())
	digest := sha256.Sum256([]byte(code))
	var owner string
	// Discovery is not authorization: issuance can replace this row while we wait.
	err = tx.QueryRow(ctx, "SELECT user_id::text FROM app.telegram_link_codes WHERE code_hash=$1", digest[:]).Scan(&owner)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrInvalidCode
	}
	if err != nil {
		return storageError(ctx, err)
	}
	var locked string
	err = tx.QueryRow(ctx, "SELECT id::text FROM app.users WHERE id=$1 FOR UPDATE", owner).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrInvalidCode
	}
	if err != nil {
		return storageError(ctx, err)
	}
	// Recheck the current hash and expiry AFTER acquiring the common owner lock.
	tag, err := tx.Exec(ctx, "DELETE FROM app.telegram_link_codes WHERE user_id=$1 AND code_hash=$2 AND expires_at>$3", owner, digest[:], s.now().UTC())
	if err != nil {
		return storageError(ctx, err)
	}
	if tag.RowsAffected() != 1 {
		return ErrInvalidCode
	}
	_, err = tx.Exec(ctx, "UPDATE app.users SET telegram_chat_id=$2, telegram_version=telegram_version+1 WHERE id=$1", owner, chatID)
	if err != nil {
		return storageError(ctx, err)
	}
	return storageError(ctx, tx.Commit(ctx))
}
func storageError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var pgerr *pgconn.PgError
	if errors.As(err, &pgerr) && pgerr.Code == "23505" && pgerr.ConstraintName == "users_telegram_chat_key" {
		return ErrChatInUse
	}
	return ErrStorage
}
