// Package subscription persists API-owned canonical subscription records.
// It is a persistence primitive, not the public create/transition operation.
package subscription

import (
	"context"
	"errors"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const LatestPrice = "latest_price"

var (
	ErrNotFound     = errors.New("subscription not found")
	ErrExists       = errors.New("subscription identity already exists")
	ErrInvalidInput = errors.New("invalid subscription record")
	ErrStorage      = errors.New("subscription storage operation failed")
)

type Workflow struct {
	TriggerID string
	Version   int64
}

type Blocked struct {
	Reason         domain.BlockReason
	BindingVersion int64
}

// Record is a full stored after-image, not an HTTP response. Versions range
// from 1 to MaxInt64; they are independent of market revisions and journal C.
// Creation fields and warnings are immutable audit data, never send permission.
// Percent is optional signed decimal text; Threshold is already computed.
type Record struct {
	ID                string
	OwnerID           string
	Pair              string
	Threshold         price.Price
	Direction         price.Direction
	CreationPrice     price.Price
	CreationTradeTime time.Time
	Percent           *string
	Stale             bool
	StreamUnconfirmed bool
	GapDetected       bool
	State             domain.State
	Version           int64
	Activation        string
	Workflow          *Workflow
	Blocked           *Blocked
}

type Repository struct{ db account.DB }

// New accepts a pool, connection or pgx.Tx. Application writers must supply a
// transaction and atomically compose locks/quota/journal/outbox before commit.
func New(db account.DB) *Repository { return &Repository{db: db} }

// Insert never overwrites an existing identity. The caller supplies a stable
// UUID and version; retries report ErrExists, not a second subscription. This
// method alone does not enforce quota, channel policy or command idempotency.
func (r *Repository) Insert(ctx context.Context, v Record) error {
	var workflowID, workflowVersion, reason, binding any
	if v.Workflow != nil {
		workflowID, workflowVersion = v.Workflow.TriggerID, v.Workflow.Version
	}
	if v.Blocked != nil {
		reason, binding = string(v.Blocked.Reason), v.Blocked.BindingVersion
	}
	var id string
	err := r.db.QueryRow(ctx, `INSERT INTO app.subscriptions
 (id,owner_id,pair,threshold,direction,creation_price,creation_trade_time,percent,
 stale,stream_unconfirmed,gap_detected,state,version,activation,workflow_id,workflow_version,block_reason,blocked_binding_version)
 VALUES ($1,$2,$3,$4::numeric,$5,$6::numeric,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18) RETURNING id::text`,
		v.ID, v.OwnerID, v.Pair, v.Threshold.String(), string(v.Direction), v.CreationPrice.String(), v.CreationTradeTime, v.Percent,
		v.Stale, v.StreamUnconfirmed, v.GapDetected, string(v.State), v.Version, v.Activation, workflowID, workflowVersion, reason, binding).Scan(&id)
	return storageError(ctx, err)
}

// ByID filters by both subscription and authenticated owner. Missing and foreign
// records return the same error. Matcher must use API interfaces, never this DB.
func (r *Repository) ByID(ctx context.Context, ownerID, id string) (Record, error) {
	var v Record
	var threshold, base string
	var workflowID, reason *string
	var workflowVersion, binding *int64
	err := r.db.QueryRow(ctx, `SELECT id::text,owner_id::text,pair,threshold::text,direction,creation_price::text,
 creation_trade_time,percent,stale,stream_unconfirmed,gap_detected,state,version,activation,
 workflow_id,workflow_version,block_reason,blocked_binding_version
 FROM app.subscriptions WHERE owner_id=$1 AND id=$2`, ownerID, id).Scan(
		&v.ID, &v.OwnerID, &v.Pair, &threshold, &v.Direction, &base, &v.CreationTradeTime, &v.Percent,
		&v.Stale, &v.StreamUnconfirmed, &v.GapDetected, &v.State, &v.Version, &v.Activation, &workflowID, &workflowVersion, &reason, &binding)
	if errors.Is(err, pgx.ErrNoRows) {
		return Record{}, ErrNotFound
	}
	if err != nil {
		return Record{}, storageError(ctx, err)
	}
	v.Threshold, err = price.Parse(threshold)
	if err != nil {
		return Record{}, ErrStorage
	}
	v.CreationPrice, err = price.Parse(base)
	if err != nil {
		return Record{}, ErrStorage
	}
	v.CreationTradeTime = v.CreationTradeTime.UTC()
	if workflowID != nil {
		v.Workflow = &Workflow{TriggerID: *workflowID, Version: *workflowVersion}
	}
	if reason != nil {
		v.Blocked = &Blocked{Reason: domain.BlockReason(*reason), BindingVersion: *binding}
	}
	return v, nil
}

// Do not expose raw PostgreSQL errors: Detail may contain user data.
func storageError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		switch pe.Code {
		case "23505":
			return ErrExists
		case "23502", "23503", "23514", "22P02", "22003", "22007", "22008":
			return ErrInvalidInput
		}
	}
	return ErrStorage
}
