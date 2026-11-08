package snapshotrpc_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSnapshotClockPrecondition(t *testing.T) {
	before := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	after := before.Add(2 * time.Millisecond)
	for _, offset := range []time.Duration{-2 * time.Minute, 0, 2 * time.Minute} {
		err := snapshotClockError(before, before.Add(time.Millisecond+offset), after)
		if (err != nil) != (offset != 0) {
			t.Fatalf("offset=%s: %v", offset, err)
		}
	}
	if err := snapshotClockError(before, before.Add(time.Second), before.Add(2*time.Second)); err == nil {
		t.Fatal("wide sample interval cannot establish clock alignment")
	}
	if err := snapshotClockError(after, before, before); err == nil {
		t.Fatal("host clock moved backwards")
	}
}

// The shortest real-expiry scenario uses 100 ms. Its environment must resolve
// the two clock domains to within half that interval. This is a test
// prerequisite, not production clock tolerance or a change to snapshot TTL.
const expiryTestTTL = 100 * time.Millisecond

func snapshotClockError(before, database, after time.Time) error {
	margin := expiryTestTTL / 2
	if after.Before(before) || database.Before(after.Add(-margin)) || database.After(before.Add(margin)) {
		return fmt.Errorf("snapshot clock prerequisite: host=[%s,%s] sql=%s; clock offset/measurement uncertainty exceeds %s; check host/Docker VM clock synchronization and sample latency before SQL/RPC expiry tests", before.Format(time.RFC3339Nano), after.Format(time.RFC3339Nano), database.UTC().Format(time.RFC3339Nano), margin)
	}
	return nil
}

func requireSnapshotClocks(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	checkSnapshotClocks(t, pool)
	t.Cleanup(func() { checkSnapshotClocks(t, pool) })
}

func checkSnapshotClocks(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	before := time.Now().UTC()
	var database time.Time
	if err := pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&database); err != nil {
		t.Fatal("snapshot SQL clock sample failed")
	}
	after := time.Now().UTC()
	t.Logf("snapshot clocks host=[%s,%s] sql=%s", before.Format(time.RFC3339Nano), after.Format(time.RFC3339Nano), database.UTC().Format(time.RFC3339Nano))
	if err := snapshotClockError(before, database, after); err != nil {
		t.Fatal(err)
	}
}
