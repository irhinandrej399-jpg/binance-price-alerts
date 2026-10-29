package projection_test

import (
	"context"
	"errors"
	"testing"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/projection"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
)

func TestRecoveryRestoresOriginAndAtomicCheckpoint(t *testing.T) {
	pool := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	other := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx := context.Background()
	s := projection.New(pool, t.Name())
	reader := projection.New(other, t.Name())
	if _, err := reader.Recover(ctx); !errors.Is(err, projection.ErrRecovery) {
		t.Fatal(err)
	}
	id := "a9876543-9999-4999-8999-999999999999"
	loadGeneration(t, pool, id, highC, snapshotRow(1, "active"), snapshotRow(2, "pending_delivery"))
	if err := s.Initialize(ctx, id); err != nil {
		t.Fatal(err)
	}
	r, err := reader.Recover(ctx)
	if err != nil || r.SnapshotID != id || r.Checkpoint.C != highC || r.Topic != "" {
		t.Fatal(r, err)
	}
	events := [][]byte{replayWire(highC+1, highC+1, 8, "cancelled")}
	if err = s.Handle(ctx, projection.Batch{Topic: "recovery", Partition: 0, NextOffset: 8, Events: events}, func(context.Context, int64) error { return errors.New("lost ack") }); err == nil {
		t.Fatal("lost ack succeeded")
	}
	r, err = reader.Recover(ctx)
	if err != nil || r.Checkpoint.C != highC+1 || r.Checkpoint.NextOffset != 8 || r.Topic != "recovery" || r.SnapshotID != id {
		t.Fatal(r, err)
	}
	if err = reader.Handle(ctx, projection.Batch{Topic: "recovery", NextOffset: 8, Events: events}, func(_ context.Context, n int64) error {
		if n != 8 {
			t.Fatal(n)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	im, err := reader.ReadImage(ctx, replaySub)
	if err != nil || im.Change == nil || string(im.Change.State) != "cancelled" {
		t.Fatal(im, err)
	}
	im, err = reader.ReadImage(ctx, snapshotRow(2, "pending_delivery").SubscriptionId)
	if err != nil || im.Snapshot.Workflow == nil {
		t.Fatal(im, err)
	}
	// A checkpoint below its immutable origin is unusable, even if the row exists.
	if _, err = pool.Exec(ctx, `UPDATE app.matcher_checkpoints SET journal_c=0 WHERE projection_id=$1`, t.Name()); err != nil {
		t.Fatal(err)
	}
	if _, err = reader.Recover(ctx); !errors.Is(err, projection.ErrRecovery) {
		t.Fatal("invalid cursor accepted", err)
	}

}
