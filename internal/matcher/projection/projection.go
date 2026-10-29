// Package projection durably consumes complete SubscriptionChanged transactions.
package projection

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	changed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/subscriptionchanged"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrSequence = errors.New("invalid projection sequence or transport")

// Batch contains exactly one complete API journal transaction, in order.
// NextOffset is the next Kafka offset after its final record, not journal C.
// The caller supplies the contiguous read_committed partition prefix; aborted
// Kafka transactions can leave gaps in physical offsets.
type Batch struct {
	Topic      string
	Partition  int32
	NextOffset int64
	Events     [][]byte
}
type Checkpoint struct {
	C          uint64
	NextOffset int64
}
type Store struct {
	pool *pgxpool.Pool
	id   string
}

// New binds storage to a stable projection identity (a future bootstrap generation).
// It neither migrates nor resets an existing projection.
func New(pool *pgxpool.Pool, id string) *Store { return &Store{pool: pool, id: id} }

// Handle commits projection + C + transport checkpoint before invoking ack.
// Any commit uncertainty or ack failure is returned for retry with the same batch.
// Callers must serialize consumption/acks for one partition; auto-commit is forbidden.
func (s *Store) Handle(ctx context.Context, b Batch, ack func(context.Context, int64) error) error {
	if ack == nil || strings.TrimSpace(s.id) == "" || strings.TrimSpace(b.Topic) == "" || b.Partition < 0 || b.NextOffset <= 0 || len(b.Events) == 0 {
		return ErrSequence
	}
	changes := make([]changed.Changed, len(b.Events))
	for i, raw := range b.Events {
		_, c, err := changed.Decode(raw)
		if err != nil {
			return err
		}
		changes[i] = c
		if i > 0 && (changes[i-1].Position == ^uint64(0) || c.Position != changes[i-1].Position+1 || c.TransactionEnd != changes[0].TransactionEnd) {
			return ErrSequence
		}
	}
	first, last := changes[0], changes[len(changes)-1]
	if last.Position != first.TransactionEnd {
		return ErrSequence
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, `INSERT INTO app.matcher_checkpoints(projection_id,topic,partition) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, s.id, b.Topic, b.Partition); err != nil {
		return err
	}
	var ctext, topic string
	var partition int32
	var next int64
	err = tx.QueryRow(ctx, `SELECT journal_c::text,next_offset,topic,partition FROM app.matcher_checkpoints WHERE projection_id=$1 FOR UPDATE`, s.id).Scan(&ctext, &next, &topic, &partition)
	if err != nil {
		return err
	}
	c, err := strconv.ParseUint(ctext, 10, 64)
	if err != nil {
		return err
	}
	if topic != b.Topic || partition != b.Partition {
		return ErrSequence
	}
	if last.Position > c {
		if c == ^uint64(0) || first.Position != c+1 || b.NextOffset <= next {
			return ErrSequence
		}
		for _, change := range changes {
			body, err := json.Marshal(change)
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `INSERT INTO app.matcher_subscriptions(projection_id,subscription_id,version,body) VALUES($1,$2,$3::numeric,$4)
     ON CONFLICT(projection_id,subscription_id) DO UPDATE SET version=EXCLUDED.version,body=EXCLUDED.body
     WHERE app.matcher_subscriptions.version < EXCLUDED.version`, s.id, change.SubscriptionID, strconv.FormatUint(change.SubscriptionVersion, 10), body)
			if err != nil {
				return err
			}
		}
		c = last.Position
	} // An already committed whole transaction is a harmless replay.
	if b.NextOffset > next {
		next = b.NextOffset
	}
	_, err = tx.Exec(ctx, `UPDATE app.matcher_checkpoints SET journal_c=$2::numeric,next_offset=$3 WHERE projection_id=$1`, s.id, strconv.FormatUint(c, 10), next)
	if err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	if err = ack(ctx, next); err != nil {
		return fmt.Errorf("ack durable projection: %w", err)
	}
	return nil
}

// Checkpoint returns zero for a not-yet-created projection; it never initializes one.
func (s *Store) Checkpoint(ctx context.Context) (Checkpoint, error) {
	var cp Checkpoint
	var c string
	err := s.pool.QueryRow(ctx, `SELECT journal_c::text,next_offset FROM app.matcher_checkpoints WHERE projection_id=$1`, s.id).Scan(&c, &cp.NextOffset)
	if errors.Is(err, pgx.ErrNoRows) {
		return cp, nil
	}
	if err != nil {
		return cp, err
	}
	cp.C, err = strconv.ParseUint(c, 10, 64)
	return cp, err
}

// Get returns the full current after-image, including terminal tombstones.
// pgx.ErrNoRows means this projection has no such subscription.
func (s *Store) Get(ctx context.Context, id string) (changed.Changed, error) {
	var body []byte
	var c changed.Changed
	err := s.pool.QueryRow(ctx, `SELECT body FROM app.matcher_subscriptions WHERE projection_id=$1 AND subscription_id=$2`, s.id, id).Scan(&body)
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(body, &c)
	return c, err
}
