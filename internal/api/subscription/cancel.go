package subscription

import (
	"context"
	"errors"
	"math"
	"net/http"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/auth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/journal"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	changed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/subscriptionchanged"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type cancelResponse struct {
	ID                   string       `json:"id"`
	State                domain.State `json:"state"`
	Version              int64        `json:"version"`
	MessageMayBeInFlight bool         `json:"message_may_be_in_flight"`
}

// NewCancelHandler mounts DELETE /v1/subscriptions/{id}. Wrap with auth.New.
// User and subscription locks serialize cancellation with future admission and
// result writers. They must check the canonical terminal state and version in
// their own transaction before granting a new attempt. No external I/O is done.
func NewCancelHandler(db Beginner) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /v1/subscriptions/{id}", func(w http.ResponseWriter, r *http.Request) {
		owner, ok := auth.UserID(r.Context())
		if !ok {
			createError(w, 401, "invalid_session")
			return
		}
		var id pgtype.UUID
		if err := id.Scan(r.PathValue("id")); err != nil || !id.Valid {
			createError(w, 400, "invalid_request")
			return
		}
		eventID := newUUID()
		tx, err := db.BeginTx(r.Context(), pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
		if err != nil {
			createError(w, 500, "internal_error")
			return
		}
		defer tx.Rollback(context.Background())
		quota, err := LockQuota(r.Context(), tx, owner)
		if err != nil {
			createError(w, 500, "internal_error")
			return
		}
		var result cancelResponse
		var workflow *string
		err = tx.QueryRow(r.Context(), `SELECT id::text,state,version,workflow_id FROM app.subscriptions
   WHERE owner_id=$1 AND id=$2 FOR UPDATE`, owner, id).Scan(&result.ID, &result.State, &result.Version, &workflow)
		if errors.Is(err, pgx.ErrNoRows) {
			createError(w, 404, "subscription_not_found")
			return
		}
		if err != nil {
			createError(w, 500, "internal_error")
			return
		}
		// A workflow can have a previously admitted ambiguous request, even after
		// returning to active/pending/blocked. Without attempt history we cannot
		// safely exclude a message in flight. Preserve the workflow on cancellation
		// so retries return the same conservative warning, including after restart.
		result.MessageMayBeInFlight = workflow != nil && result.State != domain.Delivered
		transitioned := result.State != domain.Cancelled && result.State != domain.Delivered
		if transitioned {
			if result.Version == math.MaxInt64 {
				createError(w, 500, "internal_error")
				return
			}
			tag, err := tx.Exec(r.Context(), `UPDATE app.subscriptions SET state='cancelled',version=version+1,
    block_reason=NULL,blocked_binding_version=NULL
    WHERE owner_id=$1 AND id=$2 AND version=$3 AND state=$4`, owner, id, result.Version, result.State)
			if err != nil || tag.RowsAffected() != 1 {
				createError(w, 500, "internal_error")
				return
			}
			result.State = domain.Cancelled
			result.Version++
		}
		if err = quota.ReleaseTerminal(r.Context(), result.ID); err != nil {
			createError(w, 500, "internal_error")
			return
		}
		if transitioned {
			// Read the persisted after-image before taking the journal lock.
			// Terminal retries do not allocate a new position or event.
			record, err := New(tx).ByID(r.Context(), owner, result.ID)
			if err != nil {
				createError(w, 500, "internal_error")
				return
			}
			change := changed.Changed{SchemaVersion: changed.Version1, SubscriptionID: record.ID,
				SubscriptionVersion: uint64(record.Version), State: record.State, Pair: record.Pair,
				Threshold: &record.Threshold, Direction: record.Direction, Activation: record.Activation,
				CreationSnapshot: &changed.Snapshot{Price: &record.CreationPrice, TradeTime: record.CreationTradeTime}}
			if record.Workflow != nil {
				change.Workflow = &changed.Workflow{TriggerID: record.Workflow.TriggerID, Version: uint64(record.Workflow.Version)}
			}
			_, err = journal.Append(r.Context(), tx, []journal.Entry{{
				Metadata: event.Envelope{EventID: eventID, Type: changed.Type, Version: event.Version1,
					OccurredAt: time.Now().UTC(), CorrelationID: eventID, OrderingKey: record.ID},
				Change: change,
			}})
			if err != nil {
				createError(w, 500, "internal_error")
				return
			}
		}
		// No business locks or external I/O after Append; only end the transaction.
		if err = tx.Commit(r.Context()); err != nil {
			createError(w, 500, "internal_error")
			return
		}
		createJSON(w, 200, result)
	})
	return mux
}
