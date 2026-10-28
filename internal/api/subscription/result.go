package subscription

import (
	"context"
	"errors"
	"math"
	"strings"
	"time"

	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/jackc/pgx/v5"
)

var (
	ErrAttemptUnknown = errors.New("delivery attempt not known for workflow")
	ErrResultConflict = errors.New("delivery result contradicts saved observation")
)

type DeliveryOutcome string

const (
	DeliverySucceeded DeliveryOutcome = "succeeded"
	DeliveryRetryable DeliveryOutcome = "retryable"
	DeliveryUncertain DeliveryOutcome = "uncertain"
	DeliveryFailed    DeliveryOutcome = "blocked"
)

// DeliveryResultCommand is trusted notifications input, not user input. Identity
// is a previously admitted attempt. Binding/fence come from its stored permit,
// never from the reporter. Only success accepts a positive Telegram MessageID;
// only blocked accepts a domain Reason. Unknown/expired local leases do not
// invalidate a confirmed success. Retry repeats this operation, never Telegram.
type DeliveryResultCommand struct {
	OwnerID, SubscriptionID, WorkflowID, AttemptID string
	Outcome                                        DeliveryOutcome
	MessageID                                      int64
	Reason                                         domain.BlockReason
}

// SaveDeliveryResult atomically persists audit, canonical state, quota and the
// journal/outbox change. Returns the CURRENT canonical record, also on replay.
// First success wins the canonical receipt. Every known late success is audited,
// including after cancellation; negatives only affect the current fenced attempt.
// Lock order: user -> subscription -> attempt/results -> journal, no network I/O.
func SaveDeliveryResult(ctx context.Context, db Beginner, c DeliveryResultCommand) (Record, error) {
	if strings.TrimSpace(c.OwnerID) == "" || strings.TrimSpace(c.SubscriptionID) == "" || strings.TrimSpace(c.WorkflowID) == "" || strings.TrimSpace(c.AttemptID) == "" || !validDeliveryResult(c) {
		return Record{}, ErrInvalidInput
	}
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Record{}, storageError(ctx, err)
	}
	defer tx.Rollback(context.Background())
	quota, err := LockQuota(ctx, tx, c.OwnerID)
	if err != nil {
		return Record{}, err
	}
	r, err := scanRecord(tx.QueryRow(ctx, `SELECT `+recordColumns+` FROM app.subscriptions WHERE owner_id=$1 AND id=$2 FOR UPDATE`, c.OwnerID, c.SubscriptionID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Record{}, ErrNotFound
	}
	if err != nil {
		return Record{}, storageError(ctx, err)
	}
	if r.Workflow == nil || r.Workflow.TriggerID != c.WorkflowID {
		return Record{}, ErrAttemptUnknown
	}
	var fence, binding int64
	err = tx.QueryRow(ctx, `SELECT fence,binding_version FROM app.delivery_admissions WHERE subscription_id=$1 AND workflow_id=$2 AND attempt_id=$3`, r.ID, c.WorkflowID, c.AttemptID).Scan(&fence, &binding)
	if errors.Is(err, pgx.ErrNoRows) {
		return Record{}, ErrAttemptUnknown
	}
	if err != nil {
		return Record{}, storageError(ctx, err)
	}
	var message *int64
	var reason *string
	err = tx.QueryRow(ctx, `SELECT message_id,block_reason FROM app.delivery_results WHERE attempt_id=$1 AND outcome=$2`, c.AttemptID, c.Outcome).Scan(&message, &reason)
	if err == nil {
		if (message != nil && *message != c.MessageID) || (reason != nil && *reason != string(c.Reason)) {
			return Record{}, ErrResultConflict
		}
		return r, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Record{}, storageError(ctx, err)
	}
	var current *string
	var currentFence int64
	err = tx.QueryRow(ctx, `SELECT current_attempt_id,admission_fence FROM app.subscriptions WHERE id=$1`, r.ID).Scan(&current, &currentFence)
	if err != nil {
		return Record{}, storageError(ctx, err)
	}
	terminal := r.State == domain.Delivered || r.State == domain.Cancelled
	mutate := !terminal && (c.Outcome == DeliverySucceeded || (current != nil && *current == c.AttemptID && currentFence == fence))
	var msg, why any
	if c.Outcome == DeliverySucceeded {
		msg = c.MessageID
	}
	if c.Outcome == DeliveryFailed {
		why = string(c.Reason)
	}
	_, err = tx.Exec(ctx, `INSERT INTO app.delivery_results (subscription_id,workflow_id,attempt_id,outcome,message_id,block_reason,binding_version) VALUES ($1,$2,$3,$4,$5,$6,$7)`, r.ID, c.WorkflowID, c.AttemptID, c.Outcome, msg, why, binding)
	if err != nil {
		return Record{}, storageError(ctx, err)
	}
	if mutate {
		if r.Version == math.MaxInt64 {
			return Record{}, ErrInvalidInput
		}
		r.Version++
		r.Blocked = nil
		switch c.Outcome {
		case DeliverySucceeded:
			r.State = domain.Delivered
		case DeliveryRetryable, DeliveryUncertain:
			r.State = domain.PendingDelivery
		case DeliveryFailed:
			r.State = domain.DeliveryBlocked
			r.Blocked = &Blocked{Reason: c.Reason, BindingVersion: binding}
		}
		var blockVersion, receiptAttempt any
		if r.Blocked != nil {
			blockVersion = binding
		}
		if c.Outcome == DeliverySucceeded {
			receiptAttempt = c.AttemptID
		}
		_, err = tx.Exec(ctx, `UPDATE app.subscriptions SET state=$3,version=$4,block_reason=$5,blocked_binding_version=$6,current_attempt_id=NULL,delivered_attempt_id=$7,delivered_message_id=$8 WHERE owner_id=$1 AND id=$2`, r.OwnerID, r.ID, r.State, r.Version, why, blockVersion, receiptAttempt, msg)
		if err != nil {
			return Record{}, storageError(ctx, err)
		}
		if err = quota.ReleaseTerminal(ctx, r.ID); err != nil {
			return Record{}, err
		}
		if err = appendWorkflowChange(ctx, tx, r, time.Now().UTC()); err != nil {
			return Record{}, err
		}
	}
	// After taking the journal lock, only end the transaction.
	if err = tx.Commit(ctx); err != nil {
		return Record{}, storageError(ctx, err)
	}
	return r, nil
}

func validDeliveryResult(c DeliveryResultCommand) bool {
	switch c.Outcome {
	case DeliverySucceeded:
		return c.MessageID > 0 && c.Reason == ""
	case DeliveryRetryable, DeliveryUncertain:
		return c.MessageID == 0 && c.Reason == ""
	case DeliveryFailed:
		return c.MessageID == 0 && (c.Reason == domain.ChannelUnavailable || c.Reason == domain.AttemptsExhausted || c.Reason == domain.PermanentFailure)
	default:
		return false
	}
}
