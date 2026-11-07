package pricework_test

import (
	"context"
	"errors"
	"testing"

	f "github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/fencing"
	w "github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricework"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/sourcebinding"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func sourceGrant(t *testing.T, p *pgxpool.Pool, scope string) f.Grant {
	t.Helper()
	g, err := f.New(p).Assign(context.Background(), f.Request{Identity: f.Identity{Environment: scope, Group: "matcher", Topic: "prices", Partition: 2}, Scope: scope, ProjectionID: scope + "-view", RequestID: "first"})
	if err != nil {
		t.Fatal(err)
	}
	return g
}
func priceSource() kafkasource.Identity {
	return kafkasource.Identity{ClusterID: "cluster", Topic: "prices", TopicID: [16]byte{1}}
}
func sourceInput(g f.Grant, src kafkasource.Identity, key string, n int) w.Input {
	in := input(key, n)
	in.ProjectionID = g.ProjectionID
	in.Position.Source = &src
	return in
}
func sourcePrefix(src kafkasource.Identity) *w.Prefix {
	return &w.Prefix{Source: src, Topic: "prices", Partition: 2, From: 40, Next: 41}
}

func TestSourcePriceTakeover(t *testing.T) {
	p := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	p2 := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx := context.Background()
	g := sourceGrant(t, p, t.Name())
	src := priceSource()
	s := w.NewSourceBound(p, g, src)
	in := sourceInput(g, src, "pending", 2)
	if err := s.Accept(ctx, in); err != nil {
		t.Fatal(err)
	}
	canonical := w.Result{Outcome: "workflow", TriggerID: "canonical", WorkflowVersion: 9}
	if err := s.Record(ctx, in.Key, "sub-000", canonical); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadCheckpoint(ctx, "prices", 2); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("synthetic checkpoint", err)
	}
	g2, err := f.New(p2).Assign(ctx, f.Request{Identity: g.Identity, Scope: g.Scope, ProjectionID: t.Name() + "-new", RequestID: "second", ExpectedToken: g.Token})
	if err != nil {
		t.Fatal(err)
	}
	fresh := w.NewSourceBound(p2, g2, src)
	keys, err := fresh.Pending(ctx)
	if err != nil || len(keys) != 1 || keys[0] != in.Key {
		t.Fatal(keys, err)
	}
	got, err := fresh.Get(ctx, in.Key)
	if err != nil || got.Completed != 1 || got.Entries[0].Result.TriggerID != "canonical" || got.Input.Position.Source == nil || *got.Input.Position.Source != src {
		t.Fatal(got, err)
	}
	called := false
	ack := func(context.Context) error { called = true; return nil }
	if err = s.Complete(ctx, in.Key, nil, sourcePrefix(src), ack); !errors.Is(err, f.ErrAuthority) {
		t.Fatal("late token", err)
	}
	bad := src
	bad.TopicID[0] = 2
	wrong := w.NewSourceBound(p2, g2, bad)
	for name, call := range map[string]func() error{
		"accept":   func() error { return wrong.Accept(ctx, in) },
		"get":      func() error { _, e := wrong.Get(ctx, in.Key); return e },
		"record":   func() error { return wrong.Record(ctx, in.Key, "sub-001", w.Result{Outcome: "condition_false"}) },
		"complete": func() error { return wrong.Complete(ctx, in.Key, nil, sourcePrefix(bad), ack) },
	} {
		if err = call(); !errors.Is(err, kafkasource.ErrMismatch) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if called {
		t.Fatal("ack on mismatch/stale")
	}
	if err = fresh.Accept(ctx, in); err != nil {
		t.Fatal("exact acceptance retry old view", err)
	}
	if err = fresh.Record(ctx, in.Key, "sub-001", w.Result{Outcome: "condition_false"}); err != nil {
		t.Fatal(err)
	}
	if err = fresh.Complete(ctx, in.Key, nil, sourcePrefix(bad), ack); !errors.Is(err, kafkasource.ErrMismatch) {
		t.Fatal(err)
	}
	if err = fresh.Complete(ctx, in.Key, nil, sourcePrefix(src), func(context.Context) error {
		cp, e := fresh.ReadCheckpoint(ctx, "prices", 2)
		if e != nil || cp.Source == nil || *cp.Source != src || cp.NextOffset != 41 {
			t.Fatal(cp, e)
		}
		return errors.New("lost ack")
	}); err == nil {
		t.Fatal("expected lost ack")
	}
	if err = fresh.Complete(ctx, in.Key, nil, sourcePrefix(src), ack); err != nil || !called {
		t.Fatal(err)
	}
	got, err = fresh.Get(ctx, in.Key)
	if err != nil || got.Completed != 2 || got.State != "completed" || got.Entries[0].Result.TriggerID != "canonical" {
		t.Fatal(got, err)
	}
	// A new view at the same scope cannot re-label the durable price prefix.
	if _, err = wrong.ReadCheckpoint(ctx, "prices", 2); !errors.Is(err, kafkasource.ErrMismatch) {
		t.Fatal(err)
	}
	legacy := w.NewFenced(p2, g2)
	if err = legacy.Complete(ctx, in.Key, nil, &w.Prefix{Topic: "prices", Partition: 2, From: 41, Next: 42}, ack); !errors.Is(err, sourcebinding.ErrUnbound) {
		t.Fatal(err)
	}
	for _, sql := range []string{
		`UPDATE app.matcher_price_checkpoints SET next_offset=99 WHERE scope=$1`,
		`INSERT INTO app.matcher_price_checkpoints(scope,topic,partition,next_offset) VALUES($1,'prices',2,99) ON CONFLICT DO NOTHING`,
		`UPDATE app.matcher_price_sources SET source_cluster='other' WHERE scope=$1`,
		`DELETE FROM app.matcher_price_sources WHERE scope=$1`,
		`INSERT INTO app.matcher_price_sources VALUES($1,'other',0,'other',decode(repeat('01',16),'hex')) ON CONFLICT(scope) DO UPDATE SET source_cluster='other'`,
	} {
		if _, err = p.Exec(ctx, sql, g.Scope); err == nil {
			t.Fatalf("old SQL succeeded: %s", sql)
		}
	}
	// CAS gap leaves the new work pending and doesn't ack.
	gap := sourceInput(g2, src, "gap", 0)
	gap.Position.Offset = 43
	if err = fresh.Accept(ctx, gap); err != nil {
		t.Fatal(err)
	}
	prefix := sourcePrefix(src)
	prefix.From = 42
	prefix.Next = 44
	called = false
	if err = fresh.Complete(ctx, gap.Key, nil, prefix, ack); !errors.Is(err, w.ErrConflict) || called {
		t.Fatal("prefix gap", err)
	}
	got, err = fresh.Get(ctx, gap.Key)
	if err != nil || got.State != "pending" {
		t.Fatal(got, err)
	}
	// Old literal writer races a valid bound completion; it still cannot advance.
	prefix.From = 41
	done := make(chan error, 1)
	go func() {
		_, e := p.Exec(ctx, `UPDATE app.matcher_price_checkpoints SET next_offset=100 WHERE scope=$1`, g.Scope)
		done <- e
	}()
	if err = fresh.Complete(ctx, gap.Key, nil, prefix, ack); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err == nil {
		t.Fatal("old writer advanced")
	}
	cp, err := fresh.ReadCheckpoint(ctx, "prices", 2)
	if err != nil || cp.NextOffset != 44 {
		t.Fatal(cp, err)
	}
}

func TestSourceLegacyPricesAndLatest(t *testing.T) {
	p := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx := context.Background()
	src := priceSource()
	scope := t.Name() + "-legacy"
	old := w.New(p, scope)
	in := input("pending", 1)
	if err := old.Accept(ctx, in); err != nil {
		t.Fatal(err)
	}
	if err := old.Record(ctx, in.Key, "sub-000", w.Result{Outcome: "workflow", TriggerID: "preserved", WorkflowVersion: 1}); err != nil {
		t.Fatal(err)
	}
	g := sourceGrant(t, p, scope)
	bound := w.NewSourceBound(p, g, src)
	if err := bound.Accept(ctx, in); !errors.Is(err, sourcebinding.ErrUnbound) {
		t.Fatal(err)
	}
	if err := bound.Accept(ctx, sourceInput(g, src, "new", 0)); !errors.Is(err, sourcebinding.ErrUnbound) {
		t.Fatal(err)
	}
	if err := bound.Complete(ctx, in.Key, nil, sourcePrefix(src), func(context.Context) error { t.Fatal("legacy ack"); return nil }); !errors.Is(err, sourcebinding.ErrUnbound) {
		t.Fatal(err)
	}
	got, err := w.New(p, scope).Get(ctx, in.Key)
	if err != nil || got.State != "pending" || got.Entries[0].Result.TriggerID != "preserved" || got.Input.Position.Source != nil {
		t.Fatal(got, err)
	}
	// SQL Latest work has no Kafka cursor; its completion must not fabricate one.
	g = sourceGrant(t, p, t.Name()+"-latest")
	s := w.NewSourceBound(p, g, src)
	latest := sourceInput(g, src, "latest", 0)
	latest.Position = nil
	if err = s.Accept(ctx, latest); err != nil {
		t.Fatal(err)
	}
	if err = s.Complete(ctx, latest.Key, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReadCheckpoint(ctx, "prices", 2); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal(err)
	}
	if err = s.Complete(ctx, latest.Key, nil, sourcePrefix(src), nil); err == nil {
		t.Fatal("Latest acquired transport prefix")
	}
	// Legacy state with only a checkpoint is unbound as well.
	scope = t.Name() + "-cursor"
	if _, err = p.Exec(ctx, `INSERT INTO app.matcher_price_checkpoints VALUES($1,'prices',2,41)`, scope); err != nil {
		t.Fatal(err)
	}
	g = sourceGrant(t, p, scope)
	s = w.NewSourceBound(p, g, src)
	if _, err = s.ReadCheckpoint(ctx, "prices", 2); !errors.Is(err, sourcebinding.ErrUnbound) {
		t.Fatal(err)
	}
	if err = s.Accept(ctx, sourceInput(g, src, "new", 0)); !errors.Is(err, sourcebinding.ErrUnbound) {
		t.Fatal(err)
	}
}
func TestSourceAcceptRollback(t *testing.T) {
	p := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx := context.Background()
	g := sourceGrant(t, p, t.Name())
	src := priceSource()
	s := w.NewSourceBound(p, g, src)
	if err := s.Accept(ctx, sourceInput(g, src, "fail", 2)); err == nil {
		t.Fatal("expected accept failure")
	}
	var n int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM app.matcher_price_sources WHERE scope=$1`, g.Scope).Scan(&n); err != nil || n != 0 {
		t.Fatal("orphan binding", n, err)
	}
	if _, err := s.Get(ctx, "fail"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal(err)
	}
	src.TopicID[0] = 2
	s = w.NewSourceBound(p, g, src)
	if err := s.Accept(ctx, sourceInput(g, src, "ok", 0)); err != nil {
		t.Fatal(err)
	}
}
func sourcePriceRollback(t *testing.T) {
	p := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx := context.Background()
	g := sourceGrant(t, p, t.Name())
	src := priceSource()
	s := w.NewSourceBound(p, g, src)
	in := sourceInput(g, src, "work", 0)
	if err := s.Accept(ctx, in); err != nil {
		t.Fatal(err)
	}
	if err := s.Complete(ctx, in.Key, nil, sourcePrefix(src), func(context.Context) error { t.Fatal("fault ack"); return nil }); err == nil {
		t.Fatal("expected checkpoint/commit fault")
	}
	got, err := s.Get(ctx, in.Key)
	if err != nil || got.State != "pending" {
		t.Fatal(got, err)
	}
	if _, err = s.ReadCheckpoint(ctx, "prices", 2); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("orphan offset", err)
	}
}
func TestSourcePriceRollback(t *testing.T)       { sourcePriceRollback(t) }
func TestSourcePriceCommitRollback(t *testing.T) { sourcePriceRollback(t) }

func TestSourcePreMigrationPending(t *testing.T) {
	p := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx := context.Background()
	scope := "source-pre-migration"
	old := w.New(p, scope)
	got, err := old.Get(ctx, "pending")
	if err != nil || got.Completed != 1 || got.State != "pending" || len(got.Entries) != 2 || got.Entries[0].Result.TriggerID != "pre-migration-canonical" || got.Input.Position.Source != nil {
		t.Fatal(got, err)
	}
	var same bool
	if err = p.QueryRow(ctx, `SELECT w.input=e.input FROM app.matcher_price_work w CROSS JOIN app.source_upgrade_expected e WHERE scope=$1`, scope).Scan(&same); err != nil || !same {
		t.Fatal("migration changed Input bytes", err)
	}
	cp, err := old.ReadCheckpoint(ctx, "prices", 2)
	if err != nil || cp.NextOffset != 41 || cp.Source != nil {
		t.Fatal(cp, err)
	}
	g := sourceGrant(t, p, scope)
	bound := w.NewSourceBound(p, g, priceSource())
	if err = bound.Accept(ctx, sourceInput(g, priceSource(), "new", 0)); !errors.Is(err, sourcebinding.ErrUnbound) {
		t.Fatal(err)
	}
	if err = bound.Record(ctx, "pending", "sub-001", w.Result{Outcome: "condition_false"}); !errors.Is(err, sourcebinding.ErrUnbound) {
		t.Fatal(err)
	}
	got, err = old.Get(ctx, "pending")
	if err != nil || got.Completed != 1 || got.Entries[0].Result.TriggerID != "pre-migration-canonical" {
		t.Fatal(got, err)
	}
}
