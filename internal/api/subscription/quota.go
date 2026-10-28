package subscription

import (
	"context"
	"errors"

	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/jackc/pgx/v5"
)

var (
	ErrQuotaExceeded     = errors.New("subscription quota exceeded")
	ErrTelegramNotLinked = errors.New("Telegram not linked")
)

// OwnerQuota composes quota writes with the caller's subscription/workflow and
// journal/outbox transaction. It is valid only while that transaction is open.
// On ANY operation error the caller must roll back the transaction. There are
// no internal commits or retries; command identity/retry policy belongs upstream.
type OwnerQuota struct {
	tx    pgx.Tx
	owner string
}

// LockQuota must be the first business row lock in a READ COMMITTED transaction.
// All creation, terminal transitions and binding changes use this same user row.
// Do not perform network I/O while holding the lock.
func LockQuota(ctx context.Context, tx pgx.Tx, owner string) (*OwnerQuota, error) {
	var id string
	err := tx.QueryRow(ctx, `SELECT id::text FROM app.users WHERE id=$1 FOR UPDATE`, owner).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, storageError(ctx, err)
	}
	return &OwnerQuota{tx: tx, owner: id}, nil
}

// Create reserves one slot and inserts an initial active record. Success is
// provisional until the caller commits, including any later journal/outbox.
// Threshold/quote validation is performed by the application before locking.
func (q *OwnerQuota) Create(ctx context.Context, v Record) error {
	if v.OwnerID != q.owner || v.State != domain.Active || v.Version != 1 || v.Workflow != nil || v.Blocked != nil {
		return ErrInvalidInput
	}
	var chat *int64
	var count int
	err := q.tx.QueryRow(ctx, `SELECT telegram_chat_id,active_subscriptions FROM app.users WHERE id=$1`, q.owner).Scan(&chat, &count)
	if err != nil {
		return storageError(ctx, err)
	}
	if chat == nil {
		return ErrTelegramNotLinked
	}
	if count >= 15 {
		return ErrQuotaExceeded
	}
	if _, err = q.tx.Exec(ctx, `UPDATE app.users SET active_subscriptions=active_subscriptions+1 WHERE id=$1`, q.owner); err != nil {
		return storageError(ctx, err)
	}
	if err = New(q.tx).Insert(ctx, v); err != nil {
		return err
	}
	_, err = q.tx.Exec(ctx, `UPDATE app.subscriptions SET quota_reserved=true WHERE owner_id=$1 AND id=$2`, q.owner, v.ID)
	return storageError(ctx, err)
}

// ReleaseTerminal reconciles the slot AFTER a validated terminal after-image
// is saved in this same transaction, with the user lock acquired before the
// subscription lock. Nonterminal states retain the slot; terminal retries and
// late success after cancellation cannot release it twice. This is a persistence
// primitive, not cancellation or delivery authorization: those callers must
// enforce lifecycle, workflow/attempt identity, versions and journal/outbox.
func (q *OwnerQuota) ReleaseTerminal(ctx context.Context, id string) error {
	var state domain.State
	var reserved bool
	err := q.tx.QueryRow(ctx, `SELECT state,quota_reserved FROM app.subscriptions WHERE owner_id=$1 AND id=$2 FOR UPDATE`, q.owner, id).Scan(&state, &reserved)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return storageError(ctx, err)
	}
	if !reserved || (state != domain.Delivered && state != domain.Cancelled) {
		return nil
	}
	if _, err = q.tx.Exec(ctx, `UPDATE app.users SET active_subscriptions=active_subscriptions-1 WHERE id=$1`, q.owner); err != nil {
		return storageError(ctx, err)
	}
	_, err = q.tx.Exec(ctx, `UPDATE app.subscriptions SET quota_reserved=false WHERE owner_id=$1 AND id=$2`, q.owner, id)
	return storageError(ctx, err)
}
