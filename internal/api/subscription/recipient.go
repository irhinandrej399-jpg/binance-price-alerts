package subscription

import (
	"context"
	"errors"

	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/jackc/pgx/v5"
)

// Recipient is a rate-limiter snapshot, never a send permission or address input.
type Recipient struct{ ChatID, BindingVersion int64 }
type RecipientResult struct {
	Outcome   string
	Recipient *Recipient
	Blocked   *Blocked
}

// PrepareRecipient takes the same user -> subscription locks as admission but
// writes nothing, allocates no attempt, and does not read prices. The snapshot
// must be checked again by admission after both rate limiters grant access.
func PrepareRecipient(ctx context.Context, db Beginner, c AdmissionCommand) (RecipientResult, error) {
	if c.OwnerID == "" || c.SubscriptionID == "" || c.WorkflowID == "" {
		return RecipientResult{}, ErrInvalidInput
	}
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return RecipientResult{}, storageError(ctx, err)
	}
	defer tx.Rollback(context.Background())
	if _, err = LockQuota(ctx, tx, c.OwnerID); err != nil {
		return RecipientResult{}, err
	}
	r, err := scanRecord(tx.QueryRow(ctx, `SELECT `+recordColumns+` FROM app.subscriptions WHERE owner_id=$1 AND id=$2 FOR UPDATE`, c.OwnerID, c.SubscriptionID))
	if errors.Is(err, pgx.ErrNoRows) {
		return RecipientResult{}, ErrNotFound
	}
	if err != nil {
		return RecipientResult{}, storageError(ctx, err)
	}
	if r.Workflow == nil || r.Workflow.TriggerID != c.WorkflowID {
		return RecipientResult{}, ErrAdmissionRejected
	}
	if r.State == domain.Cancelled || r.State == domain.Delivered {
		return RecipientResult{Outcome: "terminal"}, nil
	}
	var chat *int64
	var version int64
	if err = tx.QueryRow(ctx, `SELECT telegram_chat_id,telegram_version FROM app.users WHERE id=$1`, c.OwnerID).Scan(&chat, &version); err != nil {
		return RecipientResult{}, storageError(ctx, err)
	}
	if r.Blocked != nil && (r.Blocked.Reason != domain.ChannelUnavailable || chat == nil || version <= r.Blocked.BindingVersion) {
		return RecipientResult{Outcome: "blocked", Blocked: r.Blocked}, nil
	}
	if chat == nil {
		return RecipientResult{Outcome: "missing_channel", Blocked: &Blocked{Reason: domain.ChannelUnavailable, BindingVersion: version}}, nil
	}
	return RecipientResult{Outcome: "recipient_ready", Recipient: &Recipient{ChatID: *chat, BindingVersion: version}}, nil
}
