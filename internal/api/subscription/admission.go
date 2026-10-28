package subscription

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/journal"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	changed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/subscriptionchanged"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/jackc/pgx/v5"
)

// ErrAdmissionChanged requires another preparation and a fresh price read.
var ErrAdmissionChanged = errors.New("admission binding changed during quote read")

// AdmissionQuoteRequired is a non-mutating preparation result. The transaction
// and all locks are released before the caller performs market-data I/O.
type AdmissionQuoteRequired struct {
	Record         Record
	BindingVersion int64
}

func (*AdmissionQuoteRequired) Error() string { return "latest quote required" }

var ErrAdmissionRejected = errors.New("admission workflow does not match canonical record")

type AdmissionOutcome string

const (
	Admitted          AdmissionOutcome = "admitted"
	WaitCondition     AdmissionOutcome = "wait_condition"
	AttemptBusy       AdmissionOutcome = "attempt_busy"
	AdmissionBlocked  AdmissionOutcome = "blocked"
	AdmissionTerminal AdmissionOutcome = "terminal"
)

// AdmissionCommand is trusted application input. Read the latest valid quote
// before calling, outside this transaction. Each new check needs a new RequestID;
// a retry restores its saved response even without Quote/PermitTTL. RequestIDs
// are scoped to the subscription and must never be reused for a different check.
// PermitTTL is service configuration, not user input: (0, 1 minute], microsecond
// precision. The deadline is calculated by PostgreSQL after acquiring locks.
type AdmissionCommand struct {
	OwnerID, SubscriptionID, WorkflowID, RequestID string
	Quote                                          priceobserved.Observed
	PermitTTL                                      time.Duration
	ExpectedBindingVersion                         *int64 // Optional binding CAS for a prepared price read.
}

// AttemptPermission authorizes one immediate HTTP attempt after rate limiting.
// Never reuse it for another HTTP call or send after ExpiresAt; replay does not
// renew it, revoke an already started request, or guarantee external exactly-once.
// Quote retains source times/revision/warnings. It is not the candidate event.
type AttemptPermission struct {
	ID                     string
	Fence                  int64
	ExpiresAt              time.Time
	ChatID, BindingVersion int64
	Quote                  priceobserved.Observed
}
type AdmissionResult struct {
	Outcome AdmissionOutcome
	Record  Record
	Attempt *AttemptPermission
}

// AdmitDelivery serializes on user -> subscription -> command/attempt -> journal.
// It never performs network I/O or changes the quota. All returned command
// outcomes are durable and immutable. ErrStorage at commit is ambiguous: repeat
// the same identity, never mint a replacement request merely on a timeout.
// A saved response is historical: terminal transitions or lease expiry do not
// turn its replay into a new permission. New commands check current state.
func AdmitDelivery(ctx context.Context, db Beginner, c AdmissionCommand) (AdmissionResult, error) {
	return admitDelivery(ctx, db, c, false)
}

// PrepareAdmission restores a saved response or commits a quote-independent
// outcome (terminal, busy, unchanged/missing channel). Otherwise it returns
// AdmissionQuoteRequired without writing. AdmitDelivery must then receive a new
// latest quote and ExpectedBindingVersion from that preparation.
func PrepareAdmission(ctx context.Context, db Beginner, c AdmissionCommand) (AdmissionResult, error) {
	return admitDelivery(ctx, db, c, true)
}
func admitDelivery(ctx context.Context, db Beginner, c AdmissionCommand, prepare bool) (AdmissionResult, error) {
	if c.OwnerID == "" || c.SubscriptionID == "" || c.WorkflowID == "" || strings.TrimSpace(c.RequestID) == "" || len(c.RequestID) > 200 {
		return AdmissionResult{}, ErrInvalidInput
	}
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return AdmissionResult{}, storageError(ctx, err)
	}
	defer tx.Rollback(context.Background())
	if _, err = LockQuota(ctx, tx, c.OwnerID); err != nil {
		return AdmissionResult{}, err
	}
	r, err := scanRecord(tx.QueryRow(ctx, `SELECT `+recordColumns+` FROM app.subscriptions WHERE owner_id=$1 AND id=$2 FOR UPDATE`, c.OwnerID, c.SubscriptionID))
	if errors.Is(err, pgx.ErrNoRows) {
		return AdmissionResult{}, ErrNotFound
	}
	if err != nil {
		return AdmissionResult{}, storageError(ctx, err)
	}
	if r.Workflow == nil || r.Workflow.TriggerID != c.WorkflowID {
		return AdmissionResult{}, ErrAdmissionRejected
	}
	var body []byte
	err = tx.QueryRow(ctx, `SELECT response FROM app.delivery_admissions WHERE subscription_id=$1 AND request_id=$2`, r.ID, c.RequestID).Scan(&body)
	if err == nil {
		var saved AdmissionResult
		if json.Unmarshal(body, &saved) != nil {
			return AdmissionResult{}, ErrStorage
		}
		return saved, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return AdmissionResult{}, storageError(ctx, err)
	}
	if !prepare && (c.Quote.Validate() != nil || c.Quote.Pair != r.Pair || c.PermitTTL < time.Microsecond || c.PermitTTL > time.Minute || c.PermitTTL%time.Microsecond != 0) {
		return AdmissionResult{}, ErrInvalidInput
	}
	var chat *int64
	var binding, fence int64
	var current *string
	var now time.Time
	err = tx.QueryRow(ctx, `SELECT u.telegram_chat_id,u.telegram_version,s.admission_fence,s.current_attempt_id,clock_timestamp()
 FROM app.users u JOIN app.subscriptions s ON s.owner_id=u.id WHERE s.id=$1`, r.ID).Scan(&chat, &binding, &fence, &current, &now)
	if err != nil {
		return AdmissionResult{}, storageError(ctx, err)
	}
	now = now.UTC()
	result := AdmissionResult{Record: r}
	switch {
	case r.State == domain.Cancelled || r.State == domain.Delivered:
		result.Outcome = AdmissionTerminal
	default:
		// A binding change cannot invalidate a previously admitted in-flight call.
		// Do not permit another call until the old permission expires (or SUB-019
		// closes it and clears current_attempt_id under these same locks).
		busy := false
		if current != nil {
			var expires time.Time
			if err = tx.QueryRow(ctx, `SELECT expires_at FROM app.delivery_admissions WHERE subscription_id=$1 AND attempt_id=$2 AND fence=$3`, r.ID, *current, fence).Scan(&expires); err != nil {
				return AdmissionResult{}, storageError(ctx, err)
			}
			busy = now.Before(expires)
		}
		switch {
		case busy:
			result.Outcome = AttemptBusy
		case r.Blocked != nil && (r.Blocked.Reason != domain.ChannelUnavailable || chat == nil || binding <= r.Blocked.BindingVersion):
			result.Outcome = AdmissionBlocked
		case chat == nil:
			result.Outcome = AdmissionBlocked
			result.Record.State = domain.DeliveryBlocked
			result.Record.Blocked = &Blocked{Reason: domain.ChannelUnavailable, BindingVersion: binding}
		case prepare:
			return AdmissionResult{}, &AdmissionQuoteRequired{Record: r, BindingVersion: binding}
		case c.ExpectedBindingVersion != nil && binding != *c.ExpectedBindingVersion:
			return AdmissionResult{}, ErrAdmissionChanged
		case !price.Satisfied(*c.Quote.Price, r.Threshold, r.Direction):
			result.Outcome = WaitCondition
			result.Record.State = domain.Active
			result.Record.Blocked = nil
		default:
			if fence == math.MaxInt64 {
				return AdmissionResult{}, ErrInvalidInput
			}
			result.Outcome = Admitted
			result.Record.State = domain.Sending
			result.Record.Blocked = nil
			result.Attempt = &AttemptPermission{ID: newUUID(), Fence: fence + 1, ExpiresAt: now.Add(c.PermitTTL), ChatID: *chat, BindingVersion: binding, Quote: c.Quote}
		}
	}
	// Every new permission changes the visible version, including sending ->
	// sending after expiry. Other responses journal only an actual state change.
	mutated := result.Attempt != nil || result.Record.State != r.State || (r.Blocked != nil && result.Record.Blocked == nil)
	if mutated {
		if r.Version == math.MaxInt64 {
			return AdmissionResult{}, ErrInvalidInput
		}
		result.Record.Version++
		var reason, blockedVersion, attempt any
		if result.Record.Blocked != nil {
			reason = string(result.Record.Blocked.Reason)
			blockedVersion = result.Record.Blocked.BindingVersion
		}
		if result.Attempt != nil {
			attempt = result.Attempt.ID
			fence = result.Attempt.Fence
		}
		_, err = tx.Exec(ctx, `UPDATE app.subscriptions SET state=$3,version=$4,block_reason=$5,blocked_binding_version=$6,
    current_attempt_id=$7,admission_fence=$8 WHERE owner_id=$1 AND id=$2`, r.OwnerID, r.ID, result.Record.State, result.Record.Version, reason, blockedVersion, attempt, fence)
		if err != nil {
			return AdmissionResult{}, storageError(ctx, err)
		}
	}
	body, err = json.Marshal(result)
	if err != nil {
		return AdmissionResult{}, ErrInvalidInput
	}
	var attemptID, attemptFence, expires, chatID, bindingVersion any
	if a := result.Attempt; a != nil {
		attemptID = a.ID
		attemptFence = a.Fence
		expires = a.ExpiresAt
		chatID = a.ChatID
		bindingVersion = a.BindingVersion
	}
	_, err = tx.Exec(ctx, `INSERT INTO app.delivery_admissions (subscription_id,request_id,workflow_id,attempt_id,fence,expires_at,chat_id,binding_version,response) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, r.ID, c.RequestID, c.WorkflowID, attemptID, attemptFence, expires, chatID, bindingVersion, body)
	if err != nil {
		return AdmissionResult{}, storageError(ctx, err)
	}
	if mutated {
		if err = appendAdmissionChange(ctx, tx, result.Record, now); err != nil {
			return AdmissionResult{}, err
		}
	}
	// The journal lock is last. Only end the transaction after Append.
	if err = tx.Commit(ctx); err != nil {
		return AdmissionResult{}, storageError(ctx, err)
	}
	return result, nil
}

func appendAdmissionChange(ctx context.Context, tx pgx.Tx, r Record, now time.Time) error {
	id := newUUID()
	_, err := journal.Append(ctx, tx, []journal.Entry{{Metadata: event.Envelope{EventID: id, Type: changed.Type, Version: event.Version1, OccurredAt: now, CorrelationID: id, OrderingKey: r.ID}, Change: changed.Changed{
		SchemaVersion: changed.Version1, SubscriptionID: r.ID, SubscriptionVersion: uint64(r.Version), State: r.State, Pair: r.Pair, Threshold: &r.Threshold, Direction: r.Direction, Activation: r.Activation,
		CreationSnapshot: &changed.Snapshot{Price: &r.CreationPrice, TradeTime: r.CreationTradeTime}, Workflow: &changed.Workflow{TriggerID: r.Workflow.TriggerID, Version: uint64(r.Workflow.Version)},
	}}})
	return err
}
