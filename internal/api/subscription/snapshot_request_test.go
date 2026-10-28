package subscription

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
)

func TestSnapshotRequestValidation(t *testing.T) {
	// Invalid input must fail without even acquiring a database connection.
	s := NewSnapshots(nil)
	for _, id := range []string{"", " ", "bad id", strings.Repeat("a", 129), "ключ"} {
		if _, e := s.BuildRequest(context.Background(), "matcher", id, time.Minute); !errors.Is(e, ErrInvalidInput) {
			t.Fatal(id, e)
		}
	}
}

func TestSnapshotRequestReplay(t *testing.T) {
	pool := postgrestest.Open(t)
	other := postgrestest.Open(t) // separate pool / replica
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	owner, key := "request-"+newUUID(), newUUID()
	a, e := NewSnapshots(pool).BuildRequest(ctx, owner, key, time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	page, e := NewSnapshots(pool).Page(ctx, owner, a.ID, "", 1000)
	if e != nil {
		t.Fatal(e)
	}
	snapshotFixture(t, pool, domain.Active)
	b, e := NewSnapshots(other).BuildRequest(ctx, owner, key, time.Minute)
	if e != nil || !reflect.DeepEqual(a, b) {
		t.Fatal(a, b, e)
	}
	again, e := NewSnapshots(other).Page(ctx, owner, b.ID, "", 1000)
	if e != nil || !reflect.DeepEqual(page, again) {
		t.Fatal("snapshot changed", e)
	}
	if _, e = NewSnapshots(other).BuildRequest(ctx, owner, key, time.Hour); !errors.Is(e, ErrSnapshotRequestConflict) {
		t.Fatal("parameters", e)
	}
	foreign, e := NewSnapshots(other).BuildRequest(ctx, owner+"-other", key, time.Minute)
	if e != nil || foreign.ID == a.ID {
		t.Fatal("owner isolation", e)
	}
	if e = NewSnapshots(pool).Abandon(ctx, owner, a.ID); e != nil {
		t.Fatal(e)
	}
	if _, e = NewSnapshots(other).BuildRequest(ctx, owner, key, time.Minute); !errors.Is(e, ErrSnapshotLost) {
		t.Fatal("abandoned", e)
	}
}

func TestSnapshotRequestConcurrent(t *testing.T) {
	pool, other := postgrestest.Open(t), postgrestest.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	owner, key := "concurrent-"+newUUID(), newUUID()
	type result struct {
		s Snapshot
		e error
	}
	ch := make(chan result, 12)
	start := make(chan struct{})
	for i := 0; i < 12; i++ {
		s := NewSnapshots(pool)
		if i%2 == 1 {
			s = NewSnapshots(other)
		}
		go func() { <-start; a, e := s.BuildRequest(ctx, owner, key, time.Minute); ch <- result{a, e} }()
	}
	close(start)
	var want Snapshot
	for i := 0; i < 12; i++ {
		r := <-ch
		if r.e != nil {
			t.Fatal(r.e)
		}
		if i == 0 {
			want = r.s
		}
		if !reflect.DeepEqual(want, r.s) {
			t.Fatal("multiple resources", want, r.s)
		}
	}
	var n int
	if e := pool.QueryRow(ctx, `SELECT count(*) FROM app.subscription_snapshots WHERE owner=$1`, owner).Scan(&n); e != nil || n != 1 {
		t.Fatal(n, e)
	}
}

// Fault wrapper follows the existing delivery commit fixtures: transactions and
// all writes are real; only the acknowledgement of Commit is replaced.
type snapshotCommitDB struct {
	Beginner
	lost bool
}
type snapshotCommitTx struct {
	pgx.Tx
	lost bool
}

func (d snapshotCommitDB) BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	tx, e := d.Beginner.BeginTx(ctx, opts)
	if e != nil {
		return nil, e
	}
	return snapshotCommitTx{tx, d.lost}, nil
}
func (tx snapshotCommitTx) Commit(ctx context.Context) error {
	if tx.lost {
		if e := tx.Tx.Commit(ctx); e != nil {
			return e
		}
	} else {
		if e := tx.Tx.Rollback(ctx); e != nil {
			return e
		}
	}
	return errors.New("injected commit acknowledgement failure")
}
func TestSnapshotRequestCommitRecovery(t *testing.T) {
	pool, other := postgrestest.Open(t), postgrestest.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s := NewSnapshots(pool)
	for _, lost := range []bool{false, true} {
		owner, key := "fault-"+newUUID(), newUUID()
		conn, release, e := s.snapshotConn(ctx, owner, key)
		if e != nil {
			t.Fatal(e)
		}
		_, e = buildSnapshotOn(ctx, snapshotCommitDB{conn, lost}, owner, key, time.Minute)
		release()
		if !errors.Is(e, ErrStorage) {
			t.Fatal("commit error", e)
		}
		var n, keys, pins int
		if e = pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM app.subscription_snapshots WHERE owner=$1), (SELECT count(*) FROM app.subscription_snapshot_requests WHERE owner=$1), (SELECT count(*) FROM app.subscription_replay_pins WHERE owner=$1)`, owner).Scan(&n, &keys, &pins); e != nil {
			t.Fatal(e)
		}
		want := 0
		if lost {
			want = 1
		}
		if n != want || keys != want || pins != want {
			t.Fatal("partial publication", n, keys, pins)
		}
		var previous Snapshot
		if lost {
			var c int64
			if e = pool.QueryRow(ctx, `SELECT id::text,boundary,expires_at FROM app.subscription_snapshots WHERE owner=$1`, owner).Scan(&previous.ID, &c, &previous.ExpiresAt); e != nil {
				t.Fatal(e)
			}
			previous.C, previous.ExpiresAt = uint64(c), previous.ExpiresAt.UTC()
		}
		recovered, e := NewSnapshots(other).BuildRequest(ctx, owner, key, time.Minute)
		if e != nil || lost && !reflect.DeepEqual(previous, recovered) {
			t.Fatal("recovery", previous, recovered, e)
		}
		// Full key is immutable to runtime, including after a lost response.
		for _, sql := range []string{`UPDATE app.subscription_snapshot_requests SET ttl_ns=ttl_ns WHERE owner=$1`, `DELETE FROM app.subscription_snapshot_requests WHERE owner=$1`} {
			if _, e = pool.Exec(ctx, sql, owner); e == nil {
				t.Fatal("runtime can alter request tombstone")
			}
		}
	}
}

func TestSnapshotRequestLostResources(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s := NewSnapshots(pool)
	for _, mode := range []string{"expired", "pin-expired", "missing-pin", "missing-snapshot"} {
		t.Run(mode, func(t *testing.T) {
			owner, key := "lost-"+newUUID(), newUUID()
			ttl := time.Minute
			if mode == "expired" {
				ttl = 250 * time.Millisecond
			}
			snap, e := s.BuildRequest(ctx, owner, key, ttl)
			if e != nil {
				t.Fatal(e)
			}
			switch mode {
			case "expired":
				_, e = pool.Exec(ctx, `SELECT pg_sleep(0.3)`)
			case "pin-expired":
				_, e = pool.Exec(ctx, `UPDATE app.subscription_replay_pins SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, snap.ID)
			default:
				_, e = pool.Exec(ctx, `SELECT app.test_snapshot_remove($1,$2)`, snap.ID, mode == "missing-snapshot")
			}
			if e != nil {
				t.Fatal(e)
			}
			for i := 0; i < 2; i++ {
				if _, e = s.BuildRequest(ctx, owner, key, ttl); !errors.Is(e, ErrSnapshotLost) {
					t.Fatal("recreated lost resource", e)
				}
			}
			var id string
			if e = pool.QueryRow(ctx, `SELECT snapshot_id::text FROM app.subscription_snapshot_requests WHERE owner=$1 AND request_id=$2`, owner, key).Scan(&id); e != nil || id != snap.ID {
				t.Fatal("lost tombstone", id, e)
			}
			fresh, e := s.BuildRequest(ctx, owner, newUUID(), time.Minute)
			if e != nil || fresh.ID == snap.ID {
				t.Fatal("fresh bootstrap", e)
			}
		})
	}
}

func TestSnapshotRequestCancelledWait(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s := NewSnapshots(pool)
	owner, key := "wait-"+newUUID(), newUUID()
	_, release, e := s.snapshotConn(ctx, owner, key)
	if e != nil {
		t.Fatal(e)
	}
	short, stop := context.WithTimeout(ctx, 100*time.Millisecond)
	_, e = s.BuildRequest(short, owner, key, time.Minute)
	stop()
	release()
	if !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal("cancelled wait", e)
	}
	var n int
	if e = pool.QueryRow(ctx, `SELECT count(*) FROM app.subscription_snapshot_requests WHERE owner=$1`, owner).Scan(&n); e != nil || n != 0 {
		t.Fatal("cancelled key", n, e)
	}
	if _, e = s.BuildRequest(ctx, owner, key, time.Minute); e != nil {
		t.Fatal("leaked request lock", e)
	}
}
