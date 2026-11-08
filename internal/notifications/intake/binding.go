package intake

import (
	"context"
	"errors"
	"math"
	"strings"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/alerttriggered"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrUnbound = errors.New("intake: legacy state has no proven provenance")
	ErrMissing = errors.New("intake: bootstrap or partition missing")
)

// Binding identifies the logical intake and its SQL bootstrap, independently
// of replicas, group membership and restarts. It must come from provisioning.
type Binding struct {
	ConsumerID string
	Generation string
	Source     kafkasource.Identity
}

func (b Binding) Validate() error {
	if strings.TrimSpace(b.ConsumerID) == "" || strings.TrimSpace(b.Generation) == "" {
		return ErrConfig
	}
	return b.Source.Validate()
}

// Progress contains only persisted provenance and a next position. An error
// never authorizes cleanup, even when the returned Next has its zero value.
type Progress struct {
	Binding   Binding
	Partition int32
	Next      int64
}

func bindingError(err error) error {
	var e *pgconn.PgError
	if errors.As(err, &e) {
		switch e.Code {
		case "P0640":
			return errors.Join(ErrUnbound, err)
		case "P0641":
			return errors.Join(ErrMissing, err)
		case "P0642":
			return errors.Join(kafkasource.ErrMismatch, err)
		case "P0643":
			return errors.Join(ErrCheckpoint, err)
		}
	}
	return err
}

// Bootstrap is an explicit operator action on an empty intake, never an
// assignment-time fallback. It atomically binds every supplied partition at 0.
func (s *Store) Bootstrap(ctx context.Context, b Binding, partitions []int32) error {
	if s == nil || s.pool == nil {
		return ErrConfig
	}
	if err := b.Validate(); err != nil {
		return err
	}
	if len(partitions) == 0 {
		return ErrConfig
	}
	seen := make(map[int32]bool, len(partitions))
	for _, p := range partitions {
		if p < 0 || seen[p] {
			return ErrConfig
		}
		seen[p] = true
	}
	_, err := s.pool.Exec(ctx, `SELECT app.delivery_bootstrap($1,$2,$3,$4,$5,$6)`, b.ConsumerID, b.Generation, b.Source.ClusterID, b.Source.TopicID[:], b.Source.Topic, partitions)
	return bindingError(err)
}

// ReadProgress uses one MVCC statement. In particular it neither creates an
// absent checkpoint nor derives old provenance from the expected identity.
func (s *Store) ReadProgress(ctx context.Context, b Binding, partition int32) (Progress, error) {
	var result Progress
	if s == nil || s.pool == nil || partition < 0 {
		return result, ErrConfig
	}
	if err := b.Validate(); err != nil {
		return result, err
	}
	var consumer, generation, cluster, topic *string
	var tid []byte
	var next *int64
	var legacy bool
	err := s.pool.QueryRow(ctx, `SELECT s.consumer_id,s.generation,s.source_cluster,s.source_topic_id,s.topic,c.next_offset,
 EXISTS(SELECT FROM app.delivery_checkpoints) OR EXISTS(SELECT FROM app.delivery_jobs)
 FROM (VALUES(1)) anchor(x) LEFT JOIN app.delivery_intake_source s ON true
 LEFT JOIN app.delivery_checkpoints c ON c.topic=s.topic AND c.partition=$1`, partition).Scan(&consumer, &generation, &cluster, &tid, &topic, &next, &legacy)
	if err != nil {
		return result, err
	}
	if consumer == nil {
		if legacy {
			return result, ErrUnbound
		}
		return result, ErrMissing
	}
	if generation == nil || cluster == nil || topic == nil || len(tid) != 16 {
		return result, kafkasource.ErrIdentity
	}
	actual := Binding{ConsumerID: *consumer, Generation: *generation, Source: kafkasource.Identity{ClusterID: *cluster, Topic: *topic}}
	copy(actual.Source.TopicID[:], tid)
	if err = actual.Validate(); err != nil {
		return result, err
	}
	if actual != b {
		return result, kafkasource.ErrMismatch
	}
	if next == nil {
		return result, ErrMissing
	}
	return Progress{Binding: actual, Partition: partition, Next: *next}, nil
}

// AcceptBound persists the original job and prefix with an identity check in
// the same transaction, including duplicate and deferred commit failure paths.
func (s *Store) AcceptBound(ctx context.Context, b Binding, partition int32, expected, offset int64, key, body []byte) error {
	if s == nil || s.pool == nil || partition < 0 {
		return ErrConfig
	}
	if err := b.Validate(); err != nil {
		return err
	}
	if expected < 0 || offset < expected || offset == math.MaxInt64 {
		return ErrCheckpoint
	}
	meta, req, err := alerttriggered.Decode(body)
	if err != nil || string(key) != req.SubscriptionID {
		return ErrContract
	}
	_, err = s.pool.Exec(ctx, `SELECT app.delivery_accept_bound($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		b.ConsumerID, b.Generation, b.Source.ClusterID, b.Source.TopicID[:], b.Source.Topic, partition, expected, offset,
		req.TriggerID, req.SubscriptionID, req.UserID, meta.EventID, body)
	return bindingError(err)
}
