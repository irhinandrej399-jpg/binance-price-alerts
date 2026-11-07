package projection

import (
	"context"
	"errors"
	"strconv"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/sourcebinding"
	"github.com/jackc/pgx/v5"
)

var ErrRecovery = errors.New("projection: no valid snapshot checkpoint; bootstrap required")

// Recovery is one SQL view of an explicitly selected durable generation. The
// runtime owns the stable projection ID; no process-local active pointer is used.
// Snapshot expiry is not replay-pin expiry: the latter must be checked via API.
type Recovery struct {
	SnapshotID string
	Checkpoint Checkpoint
	Topic      string
	Partition  int32
}

func (s *Store) Recover(ctx context.Context) (Recovery, error) {
	var r Recovery
	var c, originC string
	var topic *string
	var partition *int32
	var complete bool
	var cluster *string
	var tid []byte
	err := s.pool.QueryRow(ctx, `SELECT c.snapshot_id,c.journal_c::text,c.next_offset,c.topic,c.partition,g.boundary_c::text,g.complete,c.source_cluster,c.source_topic_id
 FROM app.matcher_checkpoints c JOIN app.matcher_snapshot_generations g ON g.snapshot_id=c.snapshot_id
 WHERE c.projection_id=$1`, s.id).Scan(&r.SnapshotID, &c, &r.Checkpoint.NextOffset, &topic, &partition, &originC, &complete, &cluster, &tid)
	if errors.Is(err, pgx.ErrNoRows) {
		return Recovery{}, ErrRecovery
	}
	if err != nil {
		return Recovery{}, err
	}
	r.Checkpoint.C, err = strconv.ParseUint(c, 10, 64)
	if err != nil {
		return Recovery{}, errors.Join(ErrRecovery, err)
	}
	boundary, err := strconv.ParseUint(originC, 10, 64)
	if err != nil {
		return Recovery{}, errors.Join(ErrRecovery, err)
	}
	if !complete || r.SnapshotID == "" || r.Checkpoint.C < boundary || r.Checkpoint.NextOffset < 0 {
		return Recovery{}, ErrRecovery
	}
	if topic == nil {
		if partition != nil || r.Checkpoint.NextOffset != 0 {
			return Recovery{}, ErrRecovery
		}
	} else {
		if *topic == "" || partition == nil || *partition != 0 || r.Checkpoint.NextOffset <= 0 {
			return Recovery{}, ErrRecovery
		}
		r.Topic, r.Partition = *topic, *partition
	}
	r.Checkpoint.Topic = r.Topic
	r.Checkpoint.Partition = r.Partition
	r.Checkpoint.Source, err = sourcebinding.Decode(cluster, tid, topic)
	if err != nil {
		return Recovery{}, err
	}
	if err = sourcebinding.Check(s.source, r.Checkpoint.Source, topic != nil); err != nil {
		return Recovery{}, err
	}
	return r, nil
}
