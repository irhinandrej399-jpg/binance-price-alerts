package projection_test

import (
	"context"
	"errors"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/fencing"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/projection"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/sourcebinding"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5"
	"testing"
)

func TestSourceCheckpoint(t *testing.T) {
	p := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx := context.Background()
	src := kafkasource.Identity{ClusterID: "cluster", Topic: "changes", TopicID: [16]byte{1}}
	s := projection.NewSourceBound(p, t.Name(), nil, src)
	b := projection.Batch{Source: src, Topic: src.Topic, NextOffset: 8, Events: [][]byte{wire(1, 1, 1, "active")}}
	if err := s.Handle(ctx, b, func(context.Context, int64) error { return nil }); err != nil {
		t.Fatal(err)
	}
	cp, err := s.Checkpoint(ctx)
	if err != nil || cp.Source == nil || *cp.Source != src || cp.NextOffset != 8 || cp.C != 1 {
		t.Fatal(cp, err)
	}
}

func TestSourceProjectionRejectionAndOldSQL(t *testing.T) {
	p := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	other := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx := context.Background()
	src := kafkasource.Identity{ClusterID: "cluster", Topic: "changes", TopicID: [16]byte{1}}
	s := projection.NewSourceBound(p, t.Name(), nil, src)
	b := projection.Batch{Source: src, Topic: src.Topic, NextOffset: 8, Events: [][]byte{wire(1, 1, 1, "active")}}
	ack := func(context.Context, int64) error { return nil }
	if err := s.Handle(ctx, b, ack); err != nil {
		t.Fatal(err)
	}
	bad := src
	bad.TopicID[0] = 2
	called := false
	noack := func(context.Context, int64) error { called = true; return nil }
	b.Source = bad
	if err := s.Handle(ctx, b, noack); !errors.Is(err, kafkasource.ErrMismatch) {
		t.Fatal(err)
	}
	b.Source = kafkasource.Identity{}
	if err := s.Handle(ctx, b, noack); !errors.Is(err, kafkasource.ErrIdentity) {
		t.Fatal(err)
	}
	if _, err := projection.NewSourceBound(other, t.Name(), nil, bad).Checkpoint(ctx); !errors.Is(err, kafkasource.ErrMismatch) {
		t.Fatal(err)
	}
	b.Source = src
	if err := projection.New(other, t.Name()).Handle(ctx, b, noack); !errors.Is(err, sourcebinding.ErrUnbound) {
		t.Fatal(err)
	}
	if called {
		t.Fatal("ack on unproven source")
	}
	for _, sql := range []string{
		`UPDATE app.matcher_checkpoints SET next_offset=9 WHERE projection_id=$1`,
		`UPDATE app.matcher_checkpoints SET next_offset=next_offset,journal_c=100 WHERE projection_id=$1`,
		`UPDATE app.matcher_checkpoints SET source_cluster=NULL,source_topic_id=NULL WHERE projection_id=$1`,
		`UPDATE app.matcher_checkpoints SET source_cluster='other' WHERE projection_id=$1`,
		`UPDATE app.matcher_checkpoints SET topic='other' WHERE projection_id=$1`,
		`DELETE FROM app.matcher_checkpoints WHERE projection_id=$1`,
		`INSERT INTO app.matcher_checkpoints(projection_id,topic,partition,journal_c,next_offset) VALUES($1,'changes',0,2,9) ON CONFLICT(projection_id) DO UPDATE SET next_offset=EXCLUDED.next_offset`,
	} {
		if _, err := other.Exec(ctx, sql, t.Name()); err == nil {
			t.Fatalf("old runtime SQL succeeded: %s", sql)
		}
	}
	// Race the literal old UPDATE against a real bound transaction on another pool.
	done := make(chan error, 1)
	go func() {
		_, err := other.Exec(ctx, `UPDATE app.matcher_checkpoints SET next_offset=100 WHERE projection_id=$1`, t.Name())
		done <- err
	}()
	b.NextOffset = 20
	b.Events = [][]byte{wire(2, 2, 2, "cancelled")}
	if err := s.Handle(ctx, b, ack); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil {
		t.Fatal("old writer advanced bound row")
	}
	cp, err := s.Checkpoint(ctx)
	if err != nil || cp.NextOffset != 20 || cp.C != 2 || cp.Source == nil || *cp.Source != src {
		t.Fatal(cp, err)
	}
	// Same identity, new process retries the exact committed batch.
	if err := projection.NewSourceBound(other, t.Name(), nil, src).Handle(ctx, b, ack); err != nil {
		t.Fatal(err)
	}
}

func TestSourceSnapshotReplayAndLegacy(t *testing.T) {
	p := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx := context.Background()
	src := kafkasource.Identity{ClusterID: "cluster", Topic: "snapshot-changes", TopicID: [16]byte{3}}
	snap := "a9876543-9999-4999-8999-999999999961"
	loadGeneration(t, p, snap, highC, snapshotRow(1, "active"))
	s := projection.NewSourceBound(p, t.Name(), nil, src)
	if err := s.Initialize(ctx, snap); err != nil {
		t.Fatal(err)
	}
	r, err := s.Recover(ctx)
	if err != nil || r.Checkpoint.Source != nil || r.Checkpoint.NextOffset != 0 {
		t.Fatal(r, err)
	}
	ev := [][]byte{replayWire(highC+1, highC+1, 2, "cancelled")}
	if err = s.ApplyReplay(ctx, ev); err != nil {
		t.Fatal(err)
	}
	b := projection.Batch{Source: src, Topic: src.Topic, NextOffset: 9, Events: ev}
	if err = s.Handle(ctx, b, func(context.Context, int64) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err = s.Initialize(ctx, snap); err != nil {
		t.Fatal(err)
	}
	if err = s.ApplyReplay(ctx, [][]byte{replayWire(highC+2, highC+2, 3, "cancelled")}); err != nil {
		t.Fatal(err)
	}
	r, err = s.Recover(ctx)
	if err != nil || r.Checkpoint.Source == nil || *r.Checkpoint.Source != src || r.Checkpoint.C != highC+2 || r.Checkpoint.NextOffset != 9 {
		t.Fatal(r, err)
	}
	cp, err := s.Walk(ctx, func(projection.Image) error { return nil })
	if err != nil || cp.Source == nil || *cp.Source != src || cp.C != r.Checkpoint.C || cp.NextOffset != r.Checkpoint.NextOffset {
		t.Fatal(cp, r, err)
	}
	// A new SQL generation from the same immutable snapshot has no Kafka cursor.
	fresh := projection.NewSourceBound(p, t.Name()+"-new", nil, src)
	if err = fresh.Initialize(ctx, snap); err != nil {
		t.Fatal(err)
	}
	r, err = fresh.Recover(ctx)
	if err != nil || r.Checkpoint.Source != nil || r.Checkpoint.NextOffset != 0 {
		t.Fatal(r, err)
	}
	legacy := projection.New(p, t.Name()+"-legacy")
	b.Topic = "legacy"
	b.Source = kafkasource.Identity{}
	b.Events = [][]byte{wire(1, 1, 1, "active")}
	if err = legacy.Handle(ctx, b, func(context.Context, int64) error { return nil }); err != nil {
		t.Fatal(err)
	}
	legacySource := src
	legacySource.Topic = "legacy"
	bound := projection.NewSourceBound(p, t.Name()+"-legacy", nil, legacySource)
	if _, err = bound.Checkpoint(ctx); !errors.Is(err, sourcebinding.ErrUnbound) {
		t.Fatal(err)
	}
	b.Source = legacySource
	if err = bound.Handle(ctx, b, func(context.Context, int64) error { t.Fatal("legacy ack"); return nil }); !errors.Is(err, sourcebinding.ErrUnbound) {
		t.Fatal(err)
	}
}

func sourceProjectionRollback(t *testing.T) {
	p := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx := context.Background()
	src := kafkasource.Identity{ClusterID: "cluster", Topic: "rollback", TopicID: [16]byte{4}}
	s := projection.NewSourceBound(p, t.Name(), nil, src)
	b := projection.Batch{Source: src, Topic: src.Topic, NextOffset: 10, Events: [][]byte{wire(1, 1, 1, "active")}}
	if err := s.Handle(ctx, b, func(context.Context, int64) error { t.Fatal("ack after SQL fault"); return nil }); err == nil {
		t.Fatal("missing injected fault")
	}
	cp, err := s.Checkpoint(ctx)
	if err != nil || cp.Source != nil || cp.C != 0 || cp.NextOffset != 0 {
		t.Fatal(cp, err)
	}
	if _, err = s.Get(ctx, "sub"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal(err)
	}
}

func TestSourcePreMigrationProjection(t *testing.T) {
	p := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx := context.Background()
	old := projection.New(p, "source-pre-migration")
	cp, err := old.Checkpoint(ctx)
	if err != nil || cp.Source != nil || cp.C != 7 || cp.NextOffset != 41 {
		t.Fatal(cp, err)
	}
	src := kafkasource.Identity{ClusterID: "cluster", Topic: "changes", TopicID: [16]byte{1}}
	bound := projection.NewSourceBound(p, "source-pre-migration", nil, src)
	if _, err = bound.Checkpoint(ctx); !errors.Is(err, sourcebinding.ErrUnbound) {
		t.Fatal(err)
	}
	b := projection.Batch{Source: src, Topic: src.Topic, NextOffset: 42, Events: [][]byte{wire(8, 8, 8, "active")}}
	if err = bound.Handle(ctx, b, func(context.Context, int64) error { t.Fatal("pre-migration ack"); return nil }); !errors.Is(err, sourcebinding.ErrUnbound) {
		t.Fatal(err)
	}
	cp, err = old.Checkpoint(ctx)
	if err != nil || cp.Source != nil || cp.C != 7 || cp.NextOffset != 41 {
		t.Fatal(cp, err)
	}
}

func TestSourceProjectionFencing(t *testing.T) {
	p := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx := context.Background()
	src := kafkasource.Identity{ClusterID: "cluster", Topic: "changes", TopicID: [16]byte{6}}
	req := fencing.Request{Identity: fencing.Identity{Environment: t.Name(), Group: "matcher", Topic: "prices", Partition: 0}, Scope: t.Name(), ProjectionID: t.Name(), RequestID: "first"}
	g, err := fencing.New(p).Assign(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	old := projection.NewSourceBound(p, t.Name(), []fencing.Grant{g}, src)
	b := projection.Batch{Source: src, Topic: src.Topic, NextOffset: 1, Events: [][]byte{wire(1, 1, 1, "active")}}
	if err = old.Handle(ctx, b, func(context.Context, int64) error { return nil }); err != nil {
		t.Fatal(err)
	}
	req.ExpectedToken = g.Token
	req.RequestID = "second"
	next, err := fencing.New(p).Assign(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	b.NextOffset = 2
	b.Events = [][]byte{wire(2, 2, 2, "cancelled")}
	if err = old.Handle(ctx, b, func(context.Context, int64) error { t.Fatal("stale grant ack"); return nil }); !errors.Is(err, fencing.ErrAuthority) {
		t.Fatal(err)
	}
	cp, err := old.Checkpoint(ctx)
	if err != nil || cp.NextOffset != 1 || cp.Source == nil || *cp.Source != src {
		t.Fatal(cp, err)
	}
	fresh := projection.NewSourceBound(p, t.Name(), []fencing.Grant{next}, src)
	if err = fresh.Handle(ctx, b, func(context.Context, int64) error { return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestSourceProjectionRollback(t *testing.T)       { sourceProjectionRollback(t) }
func TestSourceProjectionCommitRollback(t *testing.T) { sourceProjectionRollback(t) }
