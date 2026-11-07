// Package projection durably consumes complete SubscriptionChanged transactions.
package projection

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	changed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/subscriptionchanged"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/fencing"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/sourcebinding"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrSequence = errors.New("invalid projection sequence or transport")

// Batch contains exactly one complete API journal transaction, in order.
// NextOffset is the next Kafka offset after its final record, not journal C.
// The caller supplies the contiguous read_committed partition prefix; aborted
// Kafka transactions can leave gaps in physical offsets.
type Batch struct {
	Source     kafkasource.Identity
	Topic      string
	Partition  int32
	NextOffset int64
	Events     [][]byte
}
type Checkpoint struct {
	Source     *kafkasource.Identity
	Topic      string
	Partition  int32
	C          uint64
	NextOffset int64
}
type Store struct {
	pool   *pgxpool.Pool
	id     string
	source *kafkasource.Identity
	grants []fencing.Grant
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
	if s.source != nil {
		if b.Partition != 0 {
			return ErrSequence
		}
		if err := s.source.Validate(); err != nil {
			return err
		}
		if err := b.Source.Validate(); err != nil {
			return err
		}
		if b.Source != *s.source || b.Topic != b.Source.Topic {
			return kafkasource.ErrMismatch
		}
	}
	changes, err := decodeTransaction(b.Events)
	if err != nil {
		return err
	}
	first, last := changes[0], changes[len(changes)-1]

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if err = s.authorize(ctx, tx); err != nil {
		return err
	}
	if s.source != nil {
		if _, err = tx.Exec(ctx, `SELECT app.matcher_projection_source($1,$2,$3,$4,$5)`, s.id, b.Topic, b.Partition, b.Source.ClusterID, b.Source.TopicID[:]); err != nil {
			return sourcebinding.Error(err)
		}
	} else if _, err = tx.Exec(ctx, `INSERT INTO app.matcher_checkpoints(projection_id,topic,partition) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, s.id, b.Topic, b.Partition); err != nil {
		return err
	}
	var ctext string
	var topic *string
	var partition *int32
	var next int64
	var cluster *string
	var tid []byte
	err = tx.QueryRow(ctx, `SELECT journal_c::text,next_offset,topic,partition,source_cluster,source_topic_id FROM app.matcher_checkpoints WHERE projection_id=$1 FOR UPDATE`, s.id).Scan(&ctext, &next, &topic, &partition, &cluster, &tid)
	if err != nil {
		return err
	}
	if s.source == nil && cluster != nil {
		return sourcebinding.ErrUnbound
	}
	c, err := strconv.ParseUint(ctext, 10, 64)
	if err != nil {
		return err
	}
	if topic != nil && (*topic != b.Topic || partition == nil || *partition != b.Partition) {
		return ErrSequence
	}
	if last.Position > c {
		if c == ^uint64(0) || first.Position != c+1 || b.NextOffset <= next {
			return ErrSequence
		}
		if err := s.applyChanges(ctx, tx, changes); err != nil {
			return err
		}
		c = last.Position
	} // An already committed whole transaction is a harmless replay.
	if b.NextOffset > next {
		next = b.NextOffset
	}
	if s.source != nil {
		_, err = tx.Exec(ctx, `SELECT app.matcher_projection_advance($1,$2,$3,$4,$5,$6::numeric,$7)`, s.id, b.Topic, b.Partition, b.Source.ClusterID, b.Source.TopicID[:], strconv.FormatUint(c, 10), next)
	} else {
		_, err = tx.Exec(ctx, `UPDATE app.matcher_checkpoints SET journal_c=$2::numeric,next_offset=$3,topic=$4,partition=$5 WHERE projection_id=$1`, s.id, strconv.FormatUint(c, 10), next, b.Topic, b.Partition)
	}
	if err != nil {
		return sourcebinding.Error(err)
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
	cp, err := s.readCheckpoint(ctx, s.pool)
	if errors.Is(err, pgx.ErrNoRows) {
		return Checkpoint{}, nil
	}
	return cp, err
}

type checkpointReader interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func (s *Store) readCheckpoint(ctx context.Context, q checkpointReader) (Checkpoint, error) {
	var cp Checkpoint
	var c string
	var cluster, topic *string
	var partition *int32
	var tid []byte
	err := q.QueryRow(ctx, `SELECT journal_c::text,next_offset,topic,partition,source_cluster,source_topic_id FROM app.matcher_checkpoints WHERE projection_id=$1`, s.id).Scan(&c, &cp.NextOffset, &topic, &partition, &cluster, &tid)
	if err != nil {
		return cp, err
	}
	cp.C, err = strconv.ParseUint(c, 10, 64)
	if err != nil {
		return cp, err
	}
	if topic != nil {
		cp.Topic = *topic
	}
	if partition != nil {
		cp.Partition = *partition
	}
	cp.Source, err = sourcebinding.Decode(cluster, tid, topic)
	if err != nil {
		return cp, err
	}
	return cp, sourcebinding.Check(s.source, cp.Source, topic != nil)
}

// NewSourceBound requires provenance on every Kafka batch. Snapshot/replay have
// no transport cursor until the first Handle commits. Grants retain fencing.
func NewSourceBound(pool *pgxpool.Pool, id string, grants []fencing.Grant, source kafkasource.Identity) *Store {
	s := NewFenced(pool, id, grants)
	s.source = &source
	return s
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
	if body == nil {
		return c, ErrSnapshotImage
	}
	im, err := decodeImage(body, nil)
	if err != nil {
		return c, err
	}
	return *im.Change, nil
}

func decodeTransaction(events [][]byte) ([]changed.Changed, error) {
	if len(events) == 0 {
		return nil, ErrSequence
	}
	changes := make([]changed.Changed, len(events))
	for i, raw := range events {
		_, c, err := changed.Decode(raw)
		if err != nil {
			return nil, err
		}
		if _, err := (Image{Change: &c}).OwnerID(); err != nil {
			return nil, err
		}
		changes[i] = c
		if i > 0 && (changes[i-1].Position == ^uint64(0) || c.Position != changes[i-1].Position+1 || c.TransactionEnd != changes[0].TransactionEnd) {
			return nil, ErrSequence
		}
	}
	first, last := changes[0], changes[len(changes)-1]
	if last.Position != first.TransactionEnd {
		return nil, ErrSequence
	}
	return changes, nil
}

// NewFenced requires the complete assignment set of this replica-local view.
func NewFenced(pool *pgxpool.Pool, id string, grants []fencing.Grant) *Store {
	return &Store{pool: pool, id: id, grants: append([]fencing.Grant(nil), grants...)}
}
func (s *Store) authorize(ctx context.Context, tx pgx.Tx) error {
	if err := fencing.Lock(ctx, tx); err != nil {
		return err
	}
	return fencing.Projection(ctx, tx, s.id, s.grants)
}
