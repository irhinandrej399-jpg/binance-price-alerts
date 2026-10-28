package subscription

import (
	"context"
	"errors"
	"math"
	"strings"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/journal"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrSnapshotLost means restart bootstrap, never an empty replay tail.
// Missing, foreign, expired and abandoned resources deliberately share an error.
var ErrSnapshotLost = errors.New("snapshot or replay pin unavailable; restart snapshot")

// RetentionLock is reserved in the API database. A cleaner must acquire the
// exclusive SESSION lock before starting READ COMMITTED and reading current pins.
// Builders acquire the shared SESSION lock before BEGIN REPEATABLE READ.
const RetentionLock int64 = 180006015

type Snapshot struct {
	ID        string
	C         uint64
	ExpiresAt time.Time
}
type SnapshotPage struct {
	Snapshot Snapshot
	Records  []Record
	Next     string
}
type ReplayPin struct {
	ID, Owner, Phase string
	Progress, Issued uint64
	ExpiresAt        time.Time
}
type Snapshots struct{ pool *pgxpool.Pool }

func NewSnapshots(pool *pgxpool.Pool) *Snapshots { return &Snapshots{pool: pool} }

func validResource(owner string, ttl time.Duration) bool {
	return strings.TrimSpace(owner) != "" && len(owner) <= 200 && ttl >= time.Microsecond && ttl <= 24*time.Hour
}

// retentionConn protects both initial publication and lifetime extension. A
// cleaner cannot discard an expired pin while Handoff is still extending it.
func (s *Snapshots) retentionConn(ctx context.Context) (*pgxpool.Conn, func(), error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, nil, storageError(ctx, err)
	}
	release := func() {
		defer conn.Release()
		c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := conn.Exec(c, `SELECT pg_advisory_unlock_shared($1)`, RetentionLock); err != nil {
			conn.Hijack().Close(c)
		}
	}
	// Always unlock even after an ambiguous lock-acquisition response.
	if _, err = conn.Exec(ctx, `SELECT pg_advisory_lock_shared($1)`, RetentionLock); err != nil {
		release()
		return nil, nil, storageError(ctx, err)
	}
	return conn, release, nil
}

// Build returns only after S, C and the initial pin have committed. An ambiguous
// commit may leave an unused resource; expiry releases it. TTL is a resource
// budget, not a subscription/workflow TTL. There are no implicit retries.
func (s *Snapshots) Build(ctx context.Context, owner string, ttl time.Duration) (Snapshot, error) {
	if !validResource(owner, ttl) {
		return Snapshot{}, ErrInvalidInput
	}
	conn, release, err := s.retentionConn(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	defer release()
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return Snapshot{}, storageError(ctx, err)
	}
	defer tx.Rollback(context.Background())
	c, err := journal.Current(ctx, tx)
	if err != nil {
		return Snapshot{}, err
	}
	snap := Snapshot{ID: newUUID(), C: c}
	err = tx.QueryRow(ctx, `INSERT INTO app.subscription_snapshots(id,owner,boundary,expires_at)
 VALUES($1,$2,$3,clock_timestamp()+$4::bigint*interval '1 microsecond') RETURNING expires_at`, snap.ID, owner, int64(c), ttl.Microseconds()).Scan(&snap.ExpiresAt)
	if err != nil {
		return Snapshot{}, storageError(ctx, err)
	}
	// LIKE captures all canonical columns, including workflow/attempt references.
	// Future subscription schema changes must update this frozen projection too.
	_, err = tx.Exec(ctx, `INSERT INTO app.subscription_snapshot_rows SELECT $1::uuid,s.* FROM app.subscriptions s
 WHERE state IN ('active','pending_delivery','sending','delivery_blocked')`, snap.ID)
	if err != nil {
		return Snapshot{}, storageError(ctx, err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO app.subscription_replay_pins(id,owner,progress,issued,phase,expires_at)
 VALUES($1,$2,$3,$3,'snapshot',$4)`, snap.ID, owner, int64(c), snap.ExpiresAt)
	if err != nil {
		return Snapshot{}, storageError(ctx, err)
	}
	var alive bool
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()<$1`, snap.ExpiresAt).Scan(&alive); err != nil {
		return Snapshot{}, storageError(ctx, err)
	}
	if !alive {
		return Snapshot{}, ErrSnapshotLost
	}
	if err = tx.Commit(ctx); err != nil {
		return Snapshot{}, storageError(ctx, err)
	}
	snap.ExpiresAt = snap.ExpiresAt.UTC()
	return snap, nil
}

// Page uses a stable keyset cursor: empty begins, Next is the last returned UUID
// only when another row exists. A nonempty cursor must belong to this snapshot.
func (s *Snapshots) Page(ctx context.Context, owner, id, after string, limit int) (SnapshotPage, error) {
	if limit < 1 || limit > 1000 {
		return SnapshotPage{}, ErrInvalidInput
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return SnapshotPage{}, storageError(ctx, err)
	}
	defer tx.Rollback(context.Background())
	page := SnapshotPage{Records: make([]Record, 0), Snapshot: Snapshot{ID: id}}
	var c int64
	err = tx.QueryRow(ctx, `SELECT s.boundary,s.expires_at FROM app.subscription_snapshots s JOIN app.subscription_replay_pins p ON p.id=s.id
 WHERE s.id::text=$1 AND s.owner=$2 AND p.owner=$2 AND s.expires_at>clock_timestamp()
 AND p.expires_at>clock_timestamp() AND p.phase<>'abandoned'`, id, owner).Scan(&c, &page.Snapshot.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return SnapshotPage{}, ErrSnapshotLost
	}
	if err != nil {
		return SnapshotPage{}, storageError(ctx, err)
	}
	page.Snapshot.C = uint64(c)
	page.Snapshot.ExpiresAt = page.Snapshot.ExpiresAt.UTC()
	if after != "" {
		var exists bool
		err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.subscription_snapshot_rows WHERE snapshot_id=$1 AND id::text=$2)`, id, after).Scan(&exists)
		if err != nil {
			return SnapshotPage{}, storageError(ctx, err)
		}
		if !exists {
			return SnapshotPage{}, ErrInvalidInput
		}
	}
	rows, err := tx.Query(ctx, `SELECT `+recordColumns+` FROM app.subscription_snapshot_rows WHERE snapshot_id=$1 AND ($2='' OR id>NULLIF($2,'')::uuid) ORDER BY id LIMIT $3`, id, after, limit+1)
	if err != nil {
		return SnapshotPage{}, storageError(ctx, err)
	}
	for rows.Next() {
		r, e := scanRecord(rows)
		if e != nil {
			rows.Close()
			return SnapshotPage{}, storageError(ctx, e)
		}
		page.Records = append(page.Records, r)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return SnapshotPage{}, storageError(ctx, err)
	}
	if len(page.Records) > limit {
		page.Records = page.Records[:limit]
		page.Next = page.Records[limit-1].ID
	}
	if err = tx.Commit(ctx); err != nil {
		return SnapshotPage{}, storageError(ctx, err)
	}
	return page, nil
}

func readPin(ctx context.Context, tx pgx.Tx, owner, id string) (ReplayPin, error) {
	var p ReplayPin
	var progress, issued int64
	err := tx.QueryRow(ctx, `SELECT id::text,owner,phase,progress,issued,expires_at FROM app.subscription_replay_pins
 WHERE id::text=$1 AND owner=$2 AND phase<>'abandoned' AND expires_at>clock_timestamp() FOR UPDATE`, id, owner).Scan(&p.ID, &p.Owner, &p.Phase, &progress, &issued, &p.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, ErrSnapshotLost
	}
	if err != nil {
		return p, storageError(ctx, err)
	}
	// Recheck actual wall clock AFTER any wait on a concurrent pin updater.
	var alive bool
	if err = tx.QueryRow(ctx, `SELECT $1>clock_timestamp()`, p.ExpiresAt).Scan(&alive); err != nil {
		return p, storageError(ctx, err)
	}
	if !alive {
		return p, ErrSnapshotLost
	}
	p.Progress, p.Issued = uint64(progress), uint64(issued)
	p.ExpiresAt = p.ExpiresAt.UTC()
	return p, nil
}
func (s *Snapshots) changePin(ctx context.Context, owner, id string, change func(pgx.Tx, *ReplayPin) error) (ReplayPin, error) {
	return changePinOn(ctx, s.pool, owner, id, change)
}
func changePinOn(ctx context.Context, db Beginner, owner, id string, change func(pgx.Tx, *ReplayPin) error) (ReplayPin, error) {
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ReplayPin{}, storageError(ctx, err)
	}
	defer tx.Rollback(context.Background())
	p, err := readPin(ctx, tx, owner, id)
	if err != nil {
		return ReplayPin{}, err
	}
	if change != nil {
		if err = change(tx, &p); err != nil {
			return ReplayPin{}, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return ReplayPin{}, storageError(ctx, err)
	}
	return p, nil
}
func (s *Snapshots) CheckPin(ctx context.Context, owner, id string) (ReplayPin, error) {
	return s.changePin(ctx, owner, id, nil)
}

// Handoff changes the SAME pin atomically. Repeats return its original replay
// deadline rather than extending it. No heartbeat silently renews the resource.
func (s *Snapshots) Handoff(ctx context.Context, owner, id string, ttl time.Duration) (ReplayPin, error) {
	if !validResource(owner, ttl) {
		return ReplayPin{}, ErrInvalidInput
	}
	conn, release, err := s.retentionConn(ctx)
	if err != nil {
		return ReplayPin{}, err
	}
	defer release()
	return changePinOn(ctx, conn, owner, id, func(tx pgx.Tx, p *ReplayPin) error {
		if p.Phase == "replay" {
			return nil
		}
		err := tx.QueryRow(ctx, `UPDATE app.subscription_replay_pins SET phase='replay',expires_at=clock_timestamp()+$2::bigint*interval '1 microsecond' WHERE id=$1 RETURNING expires_at`, id, ttl.Microseconds()).Scan(&p.ExpiresAt)
		p.Phase = "replay"
		p.ExpiresAt = p.ExpiresAt.UTC()
		return storageError(ctx, err)
	})
}

// RecordIssued is trusted API storage input: call only for a whole transaction
// boundary actually being issued by replay. Never accept it directly from a
// consumer. A lost response may leave a larger issued bound; replay is repeatable.
func (s *Snapshots) RecordIssued(ctx context.Context, owner, id string, c uint64) (ReplayPin, error) {
	return s.progress(ctx, owner, id, c, false)
}
func (s *Snapshots) Acknowledge(ctx context.Context, owner, id string, c uint64) (ReplayPin, error) {
	return s.progress(ctx, owner, id, c, true)
}
func (s *Snapshots) progress(ctx context.Context, owner, id string, c uint64, ack bool) (ReplayPin, error) {
	if c > math.MaxInt64 {
		return ReplayPin{}, ErrInvalidInput
	}
	return s.changePin(ctx, owner, id, func(tx pgx.Tx, p *ReplayPin) error {
		if p.Phase != "replay" || c < p.Progress || (ack && c > p.Issued) {
			return ErrInvalidInput
		}
		// Initial C remains valid even after a future cleaner removes its batch.
		if c != p.Progress && c != p.Issued {
			var valid bool
			err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.api_journal_batches WHERE transaction_end=$1)`, int64(c)).Scan(&valid)
			if err != nil {
				return storageError(ctx, err)
			}
			if !valid {
				return ErrInvalidInput
			}
		}
		if ack {
			_, err := tx.Exec(ctx, `UPDATE app.subscription_replay_pins SET progress=$2 WHERE id=$1`, id, int64(c))
			p.Progress = c
			return storageError(ctx, err)
		}
		if c > p.Issued {
			_, err := tx.Exec(ctx, `UPDATE app.subscription_replay_pins SET issued=$2 WHERE id=$1`, id, int64(c))
			p.Issued = c
			return storageError(ctx, err)
		}
		return nil
	})
}
func (s *Snapshots) Abandon(ctx context.Context, owner, id string) error {
	// Idempotent, also safe after expiry. It never deletes snapshot or journal data.
	_, err := s.pool.Exec(ctx, `UPDATE app.subscription_replay_pins SET phase='abandoned' WHERE id::text=$1 AND owner=$2`, id, owner)
	return storageError(ctx, err)
}
