package pricework

import (
	"context"
	"strings"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/sourcebinding"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
)

// ScopeProvenance describes a stable scope in one SQL snapshot. Source == nil
// means that no ledger, work (of any state), or checkpoint exists. A non-nil
// Source without Checkpoint is bound history without a confirmed Kafka offset.
// HasWork includes terminal work and Latest work without a transport position.
type ScopeProvenance struct {
	Source     *kafkasource.Identity
	HasWork    bool
	Checkpoint *TransportCheckpoint
}

// ReadScope checks the whole scope against this source-bound store's expected
// identity and the supplied topic/partition, even before a cursor exists. It
// neither binds nor locks the scope and grants no authority to write or ack.
// Bootstrap in the absence of Checkpoint remains an explicit caller decision;
// Accept/Complete must still atomically validate source and fencing.
func (s *Store) ReadScope(ctx context.Context, topic string, partition int32) (ScopeProvenance, error) {
	var out ScopeProvenance
	if strings.TrimSpace(s.scope) == "" || strings.TrimSpace(topic) == "" || partition < 0 {
		return out, ErrInvalid
	}
	if s.source == nil {
		return out, kafkasource.ErrIdentity
	}
	if err := s.source.Validate(); err != nil {
		return out, err
	}
	if topic != s.source.Topic {
		return out, kafkasource.ErrMismatch
	}
	var cluster, boundTopic *string
	var tid []byte
	var boundPartition *int32
	var next *int64
	var hasWork, hasCheckpoint, otherCheckpoint bool
	// Start from a constant row, not a cursor: missing cursor must not hide the
	// ledger or any historical work. All subqueries share one statement snapshot.
	err := s.pool.QueryRow(ctx, `SELECT s.source_cluster,s.source_topic_id,s.topic,s.partition,
 EXISTS(SELECT FROM app.matcher_price_work WHERE scope=$1),
 EXISTS(SELECT FROM app.matcher_price_checkpoints WHERE scope=$1),
 EXISTS(SELECT FROM app.matcher_price_checkpoints WHERE scope=$1 AND (topic<>$2 OR partition<>$3)),
 (SELECT next_offset FROM app.matcher_price_checkpoints WHERE scope=$1 AND topic=$2 AND partition=$3)
 FROM (VALUES(1)) AS anchor(n) LEFT JOIN app.matcher_price_sources s ON s.scope=$1`, s.scope, topic, partition).Scan(&cluster, &tid, &boundTopic, &boundPartition, &hasWork, &hasCheckpoint, &otherCheckpoint, &next)
	if err != nil {
		return out, err
	}
	source, err := sourcebinding.Decode(cluster, tid, boundTopic)
	if err != nil {
		return out, err
	}
	if source == nil {
		if hasWork || hasCheckpoint {
			return out, sourcebinding.ErrUnbound
		}
		return out, nil
	}
	if err = sourcebinding.Check(s.source, source, true); err != nil {
		return out, err
	}
	if boundPartition == nil || *boundPartition != partition || otherCheckpoint {
		return out, kafkasource.ErrMismatch
	}
	out.Source = source
	out.HasWork = hasWork
	if next != nil {
		out.Checkpoint = &TransportCheckpoint{Source: source, Topic: topic, Partition: partition, NextOffset: *next}
	}
	return out, nil
}
