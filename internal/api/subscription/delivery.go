package subscription

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/journal"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/alerttriggered"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	changed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/subscriptionchanged"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/jackc/pgx/v5"
)

var ErrCandidateRejected = errors.New("delivery candidate does not match canonical state or condition")

// DeliveryCandidate is trusted application input, never a user-supplied quote.
// Quote must be the latest valid value read by API before opening this transaction.
// It remains diagnostic and does not authorize a Telegram attempt. Age and an
// unconfirmed stream are warnings, not a TTL. ExpectedVersion fences new work.
type DeliveryCandidate struct {
	OwnerID         string
	SubscriptionID  string
	ExpectedVersion int64
	Quote           priceobserved.Observed
}

// RequestDelivery commits canonical workflow identity, delivery outbox and the
// full SubscriptionChanged journal after-image atomically. A repeat returns the
// existing workflow and current state even with the original expected version;
// it never resets active/sending/blocked/terminal work or emits another event.
// Rejections without a workflow return the canonical record and
// ErrCandidateRejected. A commit error is ambiguous: recover by repeating the
// subscription command, which returns a committed workflow if it exists.
// No network calls, quota release, attempts or automatic transaction retries.
func RequestDelivery(ctx context.Context, db Beginner, c DeliveryCandidate) (Record, error) {
	if c.OwnerID == "" || c.SubscriptionID == "" || c.ExpectedVersion < 1 || c.Quote.Validate() != nil {
		return Record{}, ErrInvalidInput
	}
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Record{}, storageError(ctx, err)
	}
	defer tx.Rollback(context.Background())
	if _, err = LockQuota(ctx, tx, c.OwnerID); err != nil {
		return Record{}, err
	}
	r, err := scanRecord(tx.QueryRow(ctx, `SELECT `+recordColumns+` FROM app.subscriptions WHERE owner_id=$1 AND id=$2 FOR UPDATE`, c.OwnerID, c.SubscriptionID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Record{}, ErrNotFound
	}
	if err != nil {
		return Record{}, storageError(ctx, err)
	}
	if r.Pair != c.Quote.Pair {
		return Record{}, ErrInvalidInput
	}
	// Identity recovery precedes the version fence: the successful first command
	// already advanced that version. State is returned as-is, never resurrected.
	if r.Workflow != nil {
		return r, nil
	}
	if r.State != domain.Active || r.Version != c.ExpectedVersion || !price.Satisfied(*c.Quote.Price, r.Threshold, r.Direction) {
		return r, ErrCandidateRejected
	}
	if r.Version == math.MaxInt64 {
		return Record{}, ErrInvalidInput
	}
	r.State = domain.PendingDelivery
	r.Version++
	r.Workflow = &Workflow{TriggerID: newUUID(), Version: 1}
	_, err = tx.Exec(ctx, `UPDATE app.subscriptions SET state=$3,version=$4,workflow_id=$5,workflow_version=$6 WHERE owner_id=$1 AND id=$2`, r.OwnerID, r.ID, r.State, r.Version, r.Workflow.TriggerID, r.Workflow.Version)
	if err != nil {
		return Record{}, storageError(ctx, err)
	}
	now := time.Now().UTC()
	workID := newUUID()
	changeID := newUUID()
	body, err := alerttriggered.Encode(event.Envelope{EventID: workID, Type: alerttriggered.Type, Version: event.Version1, OccurredAt: now, CorrelationID: workID, OrderingKey: r.ID},
		alerttriggered.Triggered{SchemaVersion: alerttriggered.Version1, Intent: alerttriggered.DeliveryRequested, TriggerID: r.Workflow.TriggerID, WorkflowVersion: 1,
			UserID: r.OwnerID, SubscriptionID: r.ID, SubscriptionVersion: uint64(r.Version), Pair: r.Pair, Threshold: &r.Threshold, Direction: r.Direction, CandidateQuote: &c.Quote})
	if err != nil {
		return Record{}, ErrInvalidInput
	}
	_, err = tx.Exec(ctx, `INSERT INTO app.delivery_outbox (subscription_id,trigger_id,event_id,body) VALUES ($1,$2,$3,$4)`, r.ID, r.Workflow.TriggerID, workID, body)
	if err != nil {
		return Record{}, storageError(ctx, err)
	}
	_, err = journal.Append(ctx, tx, []journal.Entry{{Metadata: event.Envelope{EventID: changeID, Type: changed.Type, Version: event.Version1, OccurredAt: now, CorrelationID: workID, OrderingKey: r.ID},
		Change: changed.Changed{SchemaVersion: changed.Version2, OwnerID: r.OwnerID, SubscriptionID: r.ID, SubscriptionVersion: uint64(r.Version), State: r.State, Pair: r.Pair,
			Threshold: &r.Threshold, Direction: r.Direction, Activation: r.Activation, CreationSnapshot: &changed.Snapshot{Price: &r.CreationPrice, TradeTime: r.CreationTradeTime},
			Workflow: &changed.Workflow{TriggerID: r.Workflow.TriggerID, Version: uint64(r.Workflow.Version)}}}})
	if err != nil {
		return Record{}, err
	}
	// Journal lock is last: only end the transaction after Append.
	if err = tx.Commit(ctx); err != nil {
		return Record{}, storageError(ctx, err)
	}
	return r, nil
}
