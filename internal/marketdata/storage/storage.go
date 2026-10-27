// Package storage owns durable market quotes and publication work. It performs
// no network publication; every successful acceptance follows a synchronous commit.
package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/trade"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound = errors.New("market storage: not found")
	ErrStale    = errors.New("market storage: stale generation or claim")
	ErrInvalid  = errors.New("market storage: invalid input or exhausted revision")
	pairPattern = regexp.MustCompile(`^[A-Z0-9]+$`)
)

type Repository struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Repository { return &Repository{pool: pool} }

// BeginGeneration fences earlier ingest workers. Call before each new connection,
// including restart. Historical gaps are sticky; no external replay is implied.
func (r *Repository) BeginGeneration(ctx context.Context, pair string) (uint64, error) {
	if !pairPattern.MatchString(pair) {
		return 0, ErrInvalid
	}
	var gen int64
	err := r.pool.QueryRow(ctx, `INSERT INTO app.market_streams(pair,generation,state) VALUES($1,1,'unconfirmed')
 ON CONFLICT(pair) DO UPDATE SET generation=app.market_streams.generation+1,sequence=0,state='unconfirmed',gap_detected=true
 RETURNING generation`, pair).Scan(&gen)
	return uint64(gen), err
}
func (r *Repository) SetStatus(ctx context.Context, pair string, gen uint64, state priceobserved.StreamState, gap bool) error {
	if gen == 0 || gen > math.MaxInt64 || (state != priceobserved.Live && state != priceobserved.Unconfirmed) {
		return ErrInvalid
	}
	tag, err := r.pool.Exec(ctx, `UPDATE app.market_streams SET state=$3,gap_detected=gap_detected OR $4 WHERE pair=$1 AND generation=$2`, pair, int64(gen), state, gap)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrStale
	}
	return nil
}

// Latest overlays the current durable stream status on the last accepted quote.
// The quote's original revision and source timestamps remain unchanged.
func (r *Repository) Latest(ctx context.Context, pair string) (priceobserved.Observed, error) {
	var raw []byte
	var state priceobserved.StreamState
	var gap bool
	err := r.pool.QueryRow(ctx, `SELECT latest,state,gap_detected FROM app.market_streams WHERE pair=$1 AND latest IS NOT NULL`, pair).Scan(&raw, &state, &gap)
	if errors.Is(err, pgx.ErrNoRows) {
		return priceobserved.Observed{}, ErrNotFound
	}
	if err != nil {
		return priceobserved.Observed{}, err
	}
	var q priceobserved.Observed
	if err = json.Unmarshal(raw, &q); err != nil {
		return q, err
	}
	q.StreamState = state
	q.GapDetected = gap
	return q, nil
}

// Accept assigns durable revisions; candidate revision counters/state are
// provisional decoder metadata. gen is the fencing token from BeginGeneration.
// meta's unique event_id and timestamps are persisted once, unchanged on replay.
func (r *Repository) Accept(ctx context.Context, gen uint64, q priceobserved.Observed, meta event.Envelope) (bool, error) {
	if gen == 0 || gen > math.MaxInt64 {
		return false, ErrInvalid
	}
	if _, err := trade.CheckNext(nil, q); err != nil {
		return false, err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	if _, err = tx.Exec(ctx, "SET LOCAL synchronous_commit=on"); err != nil {
		return false, err
	}
	var raw []byte
	var current, seq int64
	var state priceobserved.StreamState
	var gap bool
	err = tx.QueryRow(ctx, `SELECT latest,generation,sequence,state,gap_detected FROM app.market_streams WHERE pair=$1 FOR UPDATE`, q.Pair).Scan(&raw, &current, &seq, &state, &gap)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrStale
	}
	if err != nil {
		return false, err
	}
	if uint64(current) != gen {
		return false, ErrStale
	}
	var previous *priceobserved.Observed
	if raw != nil {
		previous = new(priceobserved.Observed)
		if err = json.Unmarshal(raw, previous); err != nil {
			return false, err
		}
	}
	decision, err := trade.CheckNext(previous, q)
	if err != nil {
		return false, err
	}
	if !decision.Accept {
		return false, nil
	}
	if seq == math.MaxInt64 {
		return false, ErrInvalid
	}
	q.Revision = priceobserved.Revision{StreamID: trade.StreamID, Generation: gen, Sequence: uint64(seq + 1)}
	q.StreamState = state
	q.GapDetected = gap || decision.GapDetected
	data, err := priceobserved.Encode(meta, q)
	if err != nil {
		return false, err
	}
	raw, err = json.Marshal(q)
	if err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `UPDATE app.market_streams SET latest=$2,sequence=$3,gap_detected=$4 WHERE pair=$1`, q.Pair, raw, seq+1, q.GapDetected); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO app.price_outbox(event_id,pair,data) VALUES($1,$2,$3)`, meta.EventID, q.Pair, data); err != nil {
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

type Work struct {
	EventID string
	Data    []byte
	Token   int64
}

// Claim leases the oldest unpublished event of a pair. Different pairs can
// progress concurrently; no later event can overtake an unacknowledged predecessor.
func (r *Repository) Claim(ctx context.Context, lease time.Duration) (Work, error) {
	if lease < time.Microsecond {
		return Work{}, ErrInvalid
	}
	var w Work
	err := r.pool.QueryRow(ctx, `WITH candidate AS (
 SELECT o.position FROM app.price_outbox o WHERE NOT o.published
 AND (o.lease_until IS NULL OR o.lease_until<=clock_timestamp())
 AND NOT EXISTS(SELECT 1 FROM app.price_outbox p WHERE p.pair=o.pair AND NOT p.published AND p.position<o.position)
 ORDER BY o.position FOR UPDATE OF o SKIP LOCKED LIMIT 1)
 UPDATE app.price_outbox o SET claim_token=o.claim_token+1,lease_until=clock_timestamp()+($1::bigint*interval '1 microsecond')
 FROM candidate c WHERE o.position=c.position RETURNING o.event_id,o.data,o.claim_token`, lease.Microseconds()).Scan(&w.EventID, &w.Data, &w.Token)
	if errors.Is(err, pgx.ErrNoRows) {
		return Work{}, ErrNotFound
	}
	if err != nil {
		return Work{}, fmt.Errorf("claim publication: %w", err)
	}
	return w, nil
}
func (r *Repository) MarkPublished(ctx context.Context, id string, token int64) error {
	tag, err := r.pool.Exec(ctx, `UPDATE app.price_outbox SET published=true,lease_until=NULL WHERE event_id=$1 AND claim_token=$2 AND NOT published AND lease_until>clock_timestamp()`, id, token)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrStale
	}
	return nil
}
