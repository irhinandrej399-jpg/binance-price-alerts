package subscription

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/journal"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	changed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/subscriptionchanged"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func snapshotFixture(t *testing.T, db Beginner, state domain.State) Record {
	t.Helper()
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	u, err := account.New(tx).Create(ctx, fmt.Sprintf("snapshot-%s@example.com", newUUID()), "hash")
	if err != nil {
		t.Fatal(err)
	}
	p, _ := price.Parse("999999999999999999.123456789012345678")
	r := Record{ID: newUUID(), OwnerID: u.ID, Pair: "BTCUSDT", Threshold: p, CreationPrice: p, CreationTradeTime: time.Now().UTC().Truncate(time.Microsecond), Direction: price.Up, State: state, Version: 1, Activation: LatestPrice, Stale: true, GapDetected: true, StreamUnconfirmed: true}
	percent := "-1.123456789012345678"
	r.Percent = &percent
	if state != domain.Active && state != domain.Cancelled {
		r.Workflow = &Workflow{TriggerID: newUUID(), Version: 3}
	}
	if state == domain.DeliveryBlocked {
		r.Blocked = &Blocked{Reason: domain.ChannelUnavailable, BindingVersion: 4}
	}
	if err = New(tx).Insert(ctx, r); err != nil {
		t.Fatal(err)
	}
	snapshotAppend(t, tx, r)
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return r
}
func snapshotAppend(t *testing.T, tx pgx.Tx, r Record) uint64 {
	t.Helper()
	id := newUUID()
	c := changed.Changed{SchemaVersion: 1, SubscriptionID: r.ID, SubscriptionVersion: uint64(r.Version), State: r.State, Pair: r.Pair, Threshold: &r.Threshold, Direction: r.Direction, Activation: r.Activation, CreationSnapshot: &changed.Snapshot{Price: &r.CreationPrice, TradeTime: r.CreationTradeTime}}
	if r.Workflow != nil {
		c.Workflow = &changed.Workflow{TriggerID: r.Workflow.TriggerID, Version: uint64(r.Workflow.Version)}
	}
	end, err := journal.Append(context.Background(), tx, []journal.Entry{{Metadata: event.Envelope{EventID: id, Type: changed.Type, Version: 1, OccurredAt: time.Now().UTC(), CorrelationID: id, OrderingKey: r.ID}, Change: c}})
	if err != nil {
		t.Fatal(err)
	}
	return end
}

func TestSnapshotPagesAndPins(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	want := map[string]Record{}
	for _, state := range []domain.State{domain.Active, domain.PendingDelivery, domain.Sending, domain.DeliveryBlocked, domain.Delivered, domain.Cancelled} {
		r := snapshotFixture(t, pool, state)
		if state != domain.Delivered && state != domain.Cancelled {
			want[r.ID] = r
		}
	}
	s := NewSnapshots(pool)
	snap, err := s.Build(ctx, "matcher-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.Page(ctx, "matcher-a", snap.ID, "", 1)
	if err != nil || len(first.Records) != 1 {
		t.Fatalf("first: %+v %v", first, err)
	}
	// A committed change between pages must occur only in replay.
	r := snapshotFixture(t, pool, domain.Active)
	repeat, err := s.Page(ctx, "matcher-a", snap.ID, "", 1)
	if err != nil || !reflect.DeepEqual(first, repeat) {
		t.Fatalf("repeat changed: %v", err)
	}
	cursor := ""
	found := map[string]Record{}
	for {
		page, e := s.Page(ctx, "matcher-a", snap.ID, cursor, 1)
		if e != nil {
			t.Fatal(e)
		}
		for _, v := range page.Records {
			found[v.ID] = v
		}
		if page.Next == "" {
			break
		}
		if page.Next <= cursor {
			t.Fatal("cursor did not advance")
		}
		cursor = page.Next
	}
	for id, v := range want {
		if !reflect.DeepEqual(found[id], v) {
			t.Fatalf("snapshot lost after-image: %+v want %+v", found[id], v)
		}
	}
	if _, ok := found[r.ID]; ok {
		t.Fatal("mixed boundary")
	}
	tail, err := journal.ReadAfter(ctx, pool, snap.C, 1000)
	if err != nil || len(tail) != 1 || tail[0].Change.SubscriptionID != r.ID {
		t.Fatalf("tail: %+v %v", tail, err)
	}
	if _, err = s.Page(ctx, "other", snap.ID, "", 1); !errors.Is(err, ErrSnapshotLost) {
		t.Fatalf("foreign: %v", err)
	}
	pin, err := s.Handoff(ctx, "matcher-a", snap.ID, time.Minute)
	if err != nil || pin.ID != snap.ID || pin.Progress != snap.C || pin.Phase != "replay" {
		t.Fatalf("handoff: %+v %v", pin, err)
	}
	if _, err = s.Acknowledge(ctx, "matcher-a", pin.ID, tail[0].Change.TransactionEnd); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("unissued ack: %v", err)
	}
	end := tail[0].Change.TransactionEnd
	if _, err = s.RecordIssued(ctx, "matcher-a", pin.ID, end); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		p, e := s.Acknowledge(ctx, "matcher-a", pin.ID, end)
		if e != nil || p.Progress != end {
			t.Fatalf("ack: %+v %v", p, e)
		}
	}
	if _, err = s.Acknowledge(ctx, "matcher-a", pin.ID, snap.C); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("regression: %v", err)
	}
	if err = s.Abandon(ctx, "matcher-a", pin.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CheckPin(ctx, "matcher-a", pin.ID); !errors.Is(err, ErrSnapshotLost) {
		t.Fatalf("abandoned: %v", err)
	}
	exp, err := s.Build(ctx, "matcher-a", time.Microsecond)
	if err != nil && !errors.Is(err, ErrSnapshotLost) {
		t.Fatal(err)
	}
	if err == nil {
		if _, err = s.Page(ctx, "matcher-a", exp.ID, "", 1); !errors.Is(err, ErrSnapshotLost) {
			t.Fatalf("expired: %v", err)
		}
	}
}

func waitSnapshotLock(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pid uint32) {
	t.Helper()
	for {
		var waiting bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')`, pid).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("expected PostgreSQL lock wait")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestSnapshotDelayedCommits(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	a := snapshotFixture(t, pool, domain.Active)
	b := snapshotFixture(t, pool, domain.Active)
	before, err := journal.Current(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	t1, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer t1.Rollback(ctx)
	if _, err = t1.Exec(ctx, `UPDATE app.subscriptions SET version=2 WHERE id=$1`, a.ID); err != nil {
		t.Fatal(err)
	}
	a.Version = 2
	end1 := snapshotAppend(t, t1, a)
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	t2, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer t2.Rollback(ctx)
	if _, err = t2.Exec(ctx, `UPDATE app.subscriptions SET version=2 WHERE id=$1`, b.ID); err != nil {
		t.Fatal(err)
	}
	// Obtain valid input outside the goroutine so failures remain on the test goroutine.
	id := newUUID()
	entry := journal.Entry{Metadata: event.Envelope{EventID: id, Type: changed.Type, Version: 1, OccurredAt: time.Now().UTC(), CorrelationID: id, OrderingKey: b.ID}, Change: changed.Changed{SchemaVersion: 1, SubscriptionID: b.ID, SubscriptionVersion: 2, State: b.State, Pair: b.Pair, Threshold: &b.Threshold, Direction: b.Direction, Activation: b.Activation, CreationSnapshot: &changed.Snapshot{Price: &b.CreationPrice, TradeTime: b.CreationTradeTime}}}
	done := make(chan error, 1)
	go func() {
		_, e := journal.Append(ctx, t2, []journal.Entry{entry})
		if e == nil {
			e = t2.Commit(ctx)
		}
		done <- e
	}()
	waitSnapshotLock(t, ctx, pool, conn.Conn().PgConn().PID())
	s := NewSnapshots(pool)
	snap, err := s.BuildRequest(ctx, "delayed", "delayed-request", time.Minute)
	if err != nil || snap.C != before {
		t.Fatalf("snapshot boundary %+v %v", snap, err)
	}
	page, err := s.Page(ctx, "delayed", snap.ID, "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, r := range page.Records {
		if r.ID == a.ID || r.ID == b.ID {
			n++
			if r.Version != 1 {
				t.Fatal("uncommitted after-image")
			}
		}
	}
	if n != 2 {
		t.Fatal("missing old rows")
	}
	if err = t1.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	tail, err := journal.ReadAfter(ctx, pool, snap.C, 1000)
	if err != nil || len(tail) != 2 || tail[0].Change.TransactionEnd != end1 || tail[1].Change.TransactionEnd <= end1 {
		t.Fatalf("late commits lost: %+v %v", tail, err)
	}
	repeat, err := s.Page(ctx, "delayed", snap.ID, "", 1000)
	if err != nil || !reflect.DeepEqual(page, repeat) {
		t.Fatal("materialized snapshot changed after commit")
	}
}

func TestSnapshotRetentionOrdering(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	s := NewSnapshots(pool)
	cleaner, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleaner.Release()
	if _, err = cleaner.Exec(ctx, `SELECT pg_advisory_lock($1)`, RetentionLock); err != nil {
		t.Fatal(err)
	}
	defer cleaner.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, RetentionLock)
	type result struct {
		snap Snapshot
		err  error
	}
	done := make(chan result, 1)
	go func() {
		snap, e := s.BuildRequest(ctx, "after-cleaner", "after-cleaner-request", time.Minute)
		done <- result{snap, e}
	}()
	// Wait until a real builder session is blocked on the advisory lock.
	for {
		var waiting bool
		err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND objid=$1 AND NOT granted)`, RetentionLock).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("builder did not wait")
		case <-time.After(5 * time.Millisecond):
		}
	}
	r := snapshotFixture(t, pool, domain.Active)
	latest, err := journal.Current(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = cleaner.Exec(ctx, `SELECT pg_advisory_unlock($1)`, RetentionLock); err != nil {
		t.Fatal(err)
	}
	res := <-done
	if res.err != nil || res.snap.C != latest {
		t.Fatalf("premature MVCC snapshot: %+v", res)
	}
	page, err := s.Page(ctx, "after-cleaner", res.snap.ID, "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, v := range page.Records {
		if v.ID == r.ID {
			found = true
		}
	}
	if !found {
		t.Fatal("builder used snapshot from before retention wait")
	}

	// Hold publication: builder already has shared lock and its MVCC snapshot;
	// cleaner cannot enter until the pin commits.
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(ctx)
	if _, err = blocker.Exec(ctx, `SELECT app.test_snapshot_lock(false)`); err != nil {
		t.Fatal(err)
	}
	go func() {
		snap, e := s.BuildRequest(ctx, "before-cleaner", "before-cleaner-request", time.Minute)
		done <- result{snap, e}
	}()
	for {
		var waiting bool
		err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE query LIKE 'INSERT INTO app.subscription_snapshots%' AND wait_event_type='Lock')`).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("builder did not reach publication")
		case <-time.After(5 * time.Millisecond):
		}
	}
	var acquired bool
	if err = cleaner.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, RetentionLock).Scan(&acquired); err != nil || acquired {
		t.Fatalf("cleaner passed uncommitted pin: %v %v", acquired, err)
	}
	if err = blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	res = <-done
	if res.err != nil {
		t.Fatal(res.err)
	}
	if _, err = cleaner.Exec(ctx, `SELECT pg_advisory_lock($1)`, RetentionLock); err != nil {
		t.Fatal(err)
	}
	// Cleaner starts its transaction AFTER obtaining the session lock.
	tx, err := cleaner.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var progress int64
	if err = tx.QueryRow(ctx, `SELECT progress FROM app.subscription_replay_pins WHERE id=$1 AND phase='snapshot' AND expires_at>clock_timestamp()`, res.snap.ID).Scan(&progress); err != nil || uint64(progress) != res.snap.C {
		t.Fatalf("cleaner missed published pin: %d %v", progress, err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotPinBoundariesAndFailure(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	r := snapshotFixture(t, pool, domain.Active)
	s := NewSnapshots(pool)
	snap, err := s.Build(ctx, "pin-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Acknowledge(ctx, "pin-owner", snap.ID, snap.C); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("ack before handoff: %v", err)
	}
	pin, err := s.Handoff(ctx, "pin-owner", snap.ID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.Handoff(ctx, "pin-owner", snap.ID, time.Hour)
	if err != nil || !reflect.DeepEqual(pin, again) {
		t.Fatalf("handoff retry renewed pin: %v", err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	id := newUUID()
	e := journal.Entry{Metadata: event.Envelope{EventID: id, Type: changed.Type, Version: 1, OccurredAt: time.Now().UTC(), CorrelationID: id, OrderingKey: r.ID}, Change: changed.Changed{SchemaVersion: 1, SubscriptionID: r.ID, SubscriptionVersion: 1, State: r.State, Pair: r.Pair, Threshold: &r.Threshold, Direction: r.Direction, Activation: r.Activation, CreationSnapshot: &changed.Snapshot{Price: &r.CreationPrice, TradeTime: r.CreationTradeTime}}}
	e2 := e
	e2.Metadata.EventID = newUUID()
	end, err := journal.Append(ctx, tx, []journal.Entry{e, e2})
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	for _, c := range []uint64{end - 1, end + 1, ^uint64(0)} {
		if _, err = s.RecordIssued(ctx, "pin-owner", pin.ID, c); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("bad issued %d: %v", c, err)
		}
	}
	if _, err = s.RecordIssued(ctx, "other-owner", pin.ID, end); !errors.Is(err, ErrSnapshotLost) {
		t.Fatalf("foreign progress: %v", err)
	}
	if _, err = s.RecordIssued(ctx, "pin-owner", pin.ID, end); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Acknowledge(ctx, "pin-owner", pin.ID, end-1); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("partial ack: %v", err)
	}
	for _, sql := range []string{`UPDATE app.subscription_snapshot_rows SET version=version WHERE snapshot_id=$1`, `DELETE FROM app.subscription_snapshot_rows WHERE snapshot_id=$1`, `UPDATE app.subscription_snapshots SET boundary=boundary WHERE id=$1`} {
		if _, err = pool.Exec(ctx, sql, snap.ID); err == nil {
			t.Fatal("runtime can mutate snapshot")
		}
	}
	if _, err = s.Page(ctx, "pin-owner", snap.ID, newUUID(), 1); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("foreign cursor: %v", err)
	}
	if _, err = s.Page(ctx, "pin-owner", "not-a-uuid", "", 1); !errors.Is(err, ErrSnapshotLost) {
		t.Fatalf("unknown resource: %v", err)
	}

	// Cancellation while waiting for retention must not leak a pooled session lock.
	lock, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	if _, err = lock.Exec(ctx, `SELECT pg_advisory_lock($1)`, RetentionLock); err != nil {
		t.Fatal(err)
	}
	short, stop := context.WithTimeout(ctx, 50*time.Millisecond)
	_, err = s.Build(short, "cancelled-builder", time.Minute)
	stop()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled builder: %v", err)
	}
	if _, err = lock.Exec(ctx, `SELECT pg_advisory_unlock($1)`, RetentionLock); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM app.subscription_snapshots WHERE owner='cancelled-builder'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("cancelled publication: %d %v", count, err)
	}
	if _, err = s.Build(ctx, "after-cancel", time.Minute); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotExpiryAndBuilderRollback(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	s := NewSnapshots(pool)
	snap, err := s.Build(ctx, "expires", 300*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `SELECT pg_sleep(0.35)`); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Page(ctx, "expires", snap.ID, "", 1); !errors.Is(err, ErrSnapshotLost) {
		t.Fatalf("expired page: %v", err)
	}
	if _, err = s.CheckPin(ctx, "expires", snap.ID); !errors.Is(err, ErrSnapshotLost) {
		t.Fatalf("expired pin: %v", err)
	}
	if _, err = s.Handoff(ctx, "expires", snap.ID, time.Minute); !errors.Is(err, ErrSnapshotLost) {
		t.Fatalf("expired handoff: %v", err)
	}
	if _, err = s.Acknowledge(ctx, "expires", snap.ID, snap.C); !errors.Is(err, ErrSnapshotLost) {
		t.Fatalf("expired ack: %v", err)
	}

	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(ctx)
	if _, err = blocker.Exec(ctx, `SELECT app.test_snapshot_lock(true)`); err != nil {
		t.Fatal(err)
	}
	buildCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() {
		_, e := s.BuildRequest(buildCtx, "rollback-builder", "rollback-builder-request", time.Minute)
		done <- e
	}()
	for {
		var waiting bool
		err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE query LIKE 'INSERT INTO app.subscription_snapshot_rows%' AND wait_event_type='Lock')`).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("builder did not materialize")
		case <-time.After(5 * time.Millisecond):
		}
	}
	stop()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("builder cancellation: %v", err)
	}
	var n int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM app.subscription_snapshots WHERE owner='rollback-builder'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("partially published snapshot: %d %v", n, err)
	}
	cleaner, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleaner.Release()
	// PostgreSQL may still be finishing backend cleanup after a cancelled socket.
	// A bounded blocking acquisition proves eventual release, without assuming
	// network disconnect and backend lock cleanup happen in the same instant.
	if _, err = cleaner.Exec(ctx, `SELECT pg_advisory_lock($1)`, RetentionLock); err != nil {
		t.Fatalf("builder leaked retention lock: %v", err)
	}
	if _, err = cleaner.Exec(ctx, `SELECT pg_advisory_unlock($1)`, RetentionLock); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotHandoffRetention(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	s := NewSnapshots(pool)
	snap, err := s.Build(ctx, "handoff-lock", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(ctx)
	if _, err = blocker.Exec(ctx, `SELECT id FROM app.subscription_replay_pins WHERE id=$1 FOR UPDATE`, snap.ID); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, e := s.Handoff(ctx, "handoff-lock", snap.ID, time.Minute); done <- e }()
	for {
		var waiting bool
		err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE query LIKE 'SELECT id::text,owner,phase,progress,issued,expires_at%' AND wait_event_type='Lock')`).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("handoff did not wait")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cleaner, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleaner.Release()
	var acquired bool
	if err = cleaner.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, RetentionLock).Scan(&acquired); err != nil {
		t.Fatal(err)
	}
	if acquired {
		cleaner.Exec(ctx, `SELECT pg_advisory_unlock($1)`, RetentionLock)
	}
	if err = blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if acquired {
		t.Fatal("cleaner entered while handoff could extend an expiring pin")
	}
}
