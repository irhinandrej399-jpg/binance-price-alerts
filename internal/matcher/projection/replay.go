package projection

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	changed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/subscriptionchanged"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
)

var (
	ErrOwnerMissing     = errors.New("projection owner unavailable; new snapshot bootstrap required")
	ErrIncomplete       = errors.New("snapshot is incomplete")
	ErrProjectionExists = errors.New("projection already has another origin")
	ErrSnapshotImage    = errors.New("use ReadImage for snapshot after-image")
)

// Image contains exactly one authoritative representation. Snapshot preserves
// every field of MAT-007; Change is the full CON-002 after-image after replay.
// Neither journal positions nor absent audit fields are synthesized.
type Image struct {
	Snapshot *pb.SnapshotSubscription
	Change   *changed.Changed
}

// OwnerID returns the canonical subscription owner from either durable form.
// Missing legacy ownership requires a new snapshot, never a guessed identity.
func (im Image) OwnerID() (string, error) {
	var owner string
	if im.Snapshot != nil && im.Change == nil {
		owner = im.Snapshot.OwnerId
	} else if im.Change != nil && im.Snapshot == nil && im.Change.SchemaVersion == changed.Version2 {
		owner = im.Change.OwnerID
	}
	if strings.TrimSpace(owner) == "" {
		return "", ErrOwnerMissing
	}
	return owner, nil
}

// Initialize binds this isolated projection to a complete immutable generation.
// Retrying the same origin never resets replay. No active-generation pointer or
// Kafka offset is written. Pin validity remains the coordinator's responsibility.
func (s *Store) Initialize(ctx context.Context, snapshotID string) error {
	if strings.TrimSpace(s.id) == "" || snapshotID == "" {
		return ErrSequence
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if err = s.authorize(ctx, tx); err != nil {
		return err
	}
	var c string
	var complete bool
	err = tx.QueryRow(ctx, `SELECT boundary_c::text,complete FROM app.matcher_snapshot_generations WHERE snapshot_id=$1`, snapshotID).Scan(&c, &complete)
	if err != nil {
		return err
	}
	if !complete {
		return ErrIncomplete
	}
	tag, err := tx.Exec(ctx, `INSERT INTO app.matcher_checkpoints(projection_id,journal_c,snapshot_id) VALUES($1,$2::numeric,$3) ON CONFLICT DO NOTHING`, s.id, c, snapshotID)
	if err != nil {
		return err
	}
	var origin *string
	if err = tx.QueryRow(ctx, `SELECT snapshot_id FROM app.matcher_checkpoints WHERE projection_id=$1 FOR UPDATE`, s.id).Scan(&origin); err != nil {
		return err
	}
	if origin == nil || *origin != snapshotID {
		return ErrProjectionExists
	}
	if tag.RowsAffected() == 0 {
		return tx.Commit(ctx)
	}
	// Keyset pages bound client memory while the entire initialization remains
	// one atomic transaction. Completed MAT-007 rows are immutable.
	last := ""
	for {
		rows, err := tx.Query(ctx, `SELECT subscription_id,body FROM app.matcher_snapshot_rows WHERE snapshot_id=$1 AND subscription_id>$2 ORDER BY subscription_id LIMIT 1000`, snapshotID, last)
		if err != nil {
			return err
		}
		batch := new(pgx.Batch)
		count := 0
		for rows.Next() {
			var body []byte
			if err = rows.Scan(&last, &body); err != nil {
				rows.Close()
				return err
			}
			r := new(pb.SnapshotSubscription)
			if err = proto.Unmarshal(body, r); err != nil {
				rows.Close()
				return err
			}
			batch.Queue(`INSERT INTO app.matcher_subscriptions(projection_id,subscription_id,version,snapshot_body) VALUES($1,$2,$3::numeric,$4)`, s.id, r.SubscriptionId, strconv.FormatInt(r.Version, 10), body)
			count++
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if count == 0 {
			break
		}
		if err = tx.SendBatch(ctx, batch).Close(); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// ApplyReplay accepts exactly one whole SQL journal transaction. It requires an
// initialized snapshot projection and changes no Kafka transport checkpoint.
// Commit ambiguity is resolved by repeating the same transaction, even via a
// different pool, then reading Checkpoint before AcknowledgeReplay.
func (s *Store) ApplyReplay(ctx context.Context, events [][]byte) error {
	changes, err := decodeTransaction(events)
	if err != nil {
		return err
	}
	if strings.TrimSpace(s.id) == "" {
		return ErrSequence
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if err = s.authorize(ctx, tx); err != nil {
		return err
	}
	var ctext string
	var origin *string
	if err = tx.QueryRow(ctx, `SELECT journal_c::text,snapshot_id FROM app.matcher_checkpoints WHERE projection_id=$1 FOR UPDATE`, s.id).Scan(&ctext, &origin); err != nil {
		return err
	}
	if origin == nil {
		return ErrSequence
	}
	c, err := strconv.ParseUint(ctext, 10, 64)
	if err != nil {
		return err
	}
	last := changes[len(changes)-1].Position
	if last > c {
		if c == ^uint64(0) || changes[0].Position != c+1 {
			return ErrSequence
		}
		if err = s.applyChanges(ctx, tx, changes); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `UPDATE app.matcher_checkpoints SET journal_c=$2::numeric WHERE projection_id=$1`, s.id, strconv.FormatUint(last, 10)); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) applyChanges(ctx context.Context, tx pgx.Tx, changes []changed.Changed) error {
	for _, change := range changes {
		body, err := json.Marshal(change)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO app.matcher_subscriptions(projection_id,subscription_id,version,body) VALUES($1,$2,$3::numeric,$4)
 ON CONFLICT(projection_id,subscription_id) DO UPDATE SET version=EXCLUDED.version,body=EXCLUDED.body,snapshot_body=NULL
 WHERE app.matcher_subscriptions.version < EXCLUDED.version`, s.id, change.SubscriptionID, strconv.FormatUint(change.SubscriptionVersion, 10), body)
		if err != nil {
			return err
		}
	}
	return nil
}

func decodeImage(body, snapshot []byte) (Image, error) {
	if snapshot != nil {
		r := new(pb.SnapshotSubscription)
		err := proto.Unmarshal(snapshot, r)
		if err != nil {
			return Image{}, err
		}
		im := Image{Snapshot: r}
		_, err = im.OwnerID()
		if err != nil {
			return Image{}, err
		}
		return im, nil
	}
	var c changed.Changed
	err := json.Unmarshal(body, &c)
	if err != nil {
		return Image{}, err
	}
	im := Image{Change: &c}
	_, err = im.OwnerID()
	if err != nil {
		return Image{}, err
	}
	return im, nil
}
func (s *Store) ReadImage(ctx context.Context, id string) (Image, error) {
	var body, snapshot []byte
	err := s.pool.QueryRow(ctx, `SELECT body,snapshot_body FROM app.matcher_subscriptions WHERE projection_id=$1 AND subscription_id=$2`, s.id, id).Scan(&body, &snapshot)
	if err != nil {
		return Image{}, err
	}
	return decodeImage(body, snapshot)
}

// Walk reads one consistent MVCC view of this entire projection, including
// terminal tombstones, ordered by ID. The returned checkpoint belongs to that
// same view. Callback errors abort traversal. Callback must respect ctx and must
// not assume that replay has stopped after this read; activation is MAT-008.
func (s *Store) Walk(ctx context.Context, visit func(Image) error) (Checkpoint, error) {
	var cp Checkpoint
	if visit == nil {
		return cp, ErrSequence
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return cp, err
	}
	defer tx.Rollback(context.Background())
	var c string
	if err = tx.QueryRow(ctx, `SELECT journal_c::text,next_offset FROM app.matcher_checkpoints WHERE projection_id=$1`, s.id).Scan(&c, &cp.NextOffset); err != nil {
		return cp, err
	}
	cp.C, err = strconv.ParseUint(c, 10, 64)
	if err != nil {
		return cp, err
	}
	rows, err := tx.Query(ctx, `SELECT body,snapshot_body FROM app.matcher_subscriptions WHERE projection_id=$1 ORDER BY subscription_id`, s.id)
	if err != nil {
		return cp, err
	}
	defer rows.Close()
	for rows.Next() {
		var body, snapshot []byte
		if err = rows.Scan(&body, &snapshot); err != nil {
			return cp, err
		}
		im, e := decodeImage(body, snapshot)
		if e != nil {
			return cp, e
		}
		if err = visit(im); err != nil {
			return cp, err
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return cp, err
	}
	return cp, tx.Commit(ctx)
}
