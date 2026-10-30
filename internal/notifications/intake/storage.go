// Package intake durably accepts delivery requests before acknowledging Kafka.
package intake

import (
	"context"
	"errors"
	"math"
	"strings"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/alerttriggered"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrConfig     = errors.New("intake: invalid configuration")
	ErrContract   = errors.New("intake: invalid delivery request; partition stopped")
	ErrCheckpoint = errors.New("intake: stale or invalid checkpoint; restart consumer")
	ErrPosition   = errors.New("intake: Kafka position unavailable; consumer stopped")
	ErrAck        = errors.New("intake: offset commit failed; restart from SQL")
)

type Store struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Checkpoint registers a required partition at zero, never at the broker offset
// or latest. This table belongs to one logical intake per environment topic.
func (s *Store) Checkpoint(ctx context.Context, topic string, partition int32) (int64, error) {
	if s.pool == nil || strings.TrimSpace(topic) == "" || partition < 0 {
		return 0, ErrConfig
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO app.delivery_checkpoints(topic,partition) VALUES($1,$2) ON CONFLICT DO NOTHING`, topic, partition); err != nil {
		return 0, err
	}
	var next int64
	err := s.pool.QueryRow(ctx, `SELECT next_offset FROM app.delivery_checkpoints WHERE topic=$1 AND partition=$2`, topic, partition).Scan(&next)
	return next, err
}

// Accept is called serially in Kafka fetch order. expected is the cursor from
// the preceding accepted delivery (or SQL on assignment). Invisible aborted
// records and control markers may leave gaps before offset. A caller cannot
// skip a delivered record and still claim a continuous prefix.
func (s *Store) Accept(ctx context.Context, topic string, partition int32, expected, offset int64, key, body []byte) error {
	if expected < 0 || offset < expected || offset == math.MaxInt64 {
		return ErrCheckpoint
	}
	meta, req, err := alerttriggered.Decode(body)
	if err != nil || string(key) != req.SubscriptionID {
		return ErrContract
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var next int64
	if err = tx.QueryRow(ctx, `SELECT next_offset FROM app.delivery_checkpoints WHERE topic=$1 AND partition=$2 FOR UPDATE`, topic, partition).Scan(&next); err != nil {
		return err
	}
	if offset < next {
		return tx.Commit(ctx)
	} // already durable; never reset job state
	if next != expected {
		return ErrCheckpoint
	}
	_, err = tx.Exec(ctx, `INSERT INTO app.delivery_jobs(trigger_id,subscription_id,user_id,event_id,body) VALUES($1,$2,$3,$4,$5) ON CONFLICT(trigger_id) DO NOTHING`, req.TriggerID, req.SubscriptionID, req.UserID, meta.EventID, body)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE app.delivery_checkpoints SET next_offset=$3 WHERE topic=$1 AND partition=$2`, topic, partition, offset+1); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
