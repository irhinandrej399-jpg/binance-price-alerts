// Package account stores API-owned user accounts. Password hashing is supplied
// by the caller; this repository expects a precomputed hash, not a password.
package account

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrEmailExists  = errors.New("account email already exists")
	ErrNotFound     = errors.New("account not found")
	ErrInvalidInput = errors.New("account email and password hash must not be empty")
	ErrStorage      = errors.New("account storage operation failed")
)

// Account is an internal credential record, never an HTTP response or event.
// PasswordHash is opaque encoded output of the password adapter (IAM-002).
type Account struct {
	ID           string
	Email        string
	PasswordHash string
}

// DB is implemented by pgxpool.Pool, pgxpool.Conn and pgx.Tx. A caller may use
// its own transaction; success in that case still requires the caller's commit.
type DB interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

type Repository struct{ db DB }

func New(db DB) *Repository { return &Repository{db: db} }

// NormalizeEmail trims surrounding whitespace and lowercases the whole address.
// It does not remove dots/plus suffixes or perform provider-specific rewriting.
func NormalizeEmail(email string) string { return strings.ToLower(strings.TrimSpace(email)) }

// Create inserts one account, relying on PostgreSQL uniqueness rather than a
// check-then-insert race. passwordHash must already be hashed by the caller.
func (r *Repository) Create(ctx context.Context, email, passwordHash string) (Account, error) {
	email = NormalizeEmail(email)
	if email == "" || strings.TrimSpace(passwordHash) == "" {
		return Account{}, ErrInvalidInput
	}
	a, err := scan(r.db.QueryRow(ctx,
		`INSERT INTO app.users(email, password_hash) VALUES ($1, $2) RETURNING id::text, email, password_hash`, email, passwordHash))
	var pgerr *pgconn.PgError
	if errors.As(err, &pgerr) && pgerr.Code == "23505" && pgerr.ConstraintName == "users_email_key" {
		return Account{}, ErrEmailExists
	}
	if err != nil {
		return Account{}, storageError(ctx)
	}
	return a, nil
}

func (r *Repository) ByEmail(ctx context.Context, email string) (Account, error) {
	a, err := scan(r.db.QueryRow(ctx,
		`SELECT id::text, email, password_hash FROM app.users WHERE email=$1`, NormalizeEmail(email)))
	if errors.Is(err, pgx.ErrNoRows) {
		return Account{}, ErrNotFound
	}
	if err != nil {
		return Account{}, storageError(ctx)
	}
	return a, nil
}

func scan(row pgx.Row) (Account, error) {
	var a Account
	err := row.Scan(&a.ID, &a.Email, &a.PasswordHash)
	return a, err
}

// Raw PostgreSQL errors can contain email or credential values in Detail.
func storageError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return ErrStorage
}
