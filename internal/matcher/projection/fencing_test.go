package projection_test

import (
	"context"
	"errors"
	f "github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/fencing"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/projection"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"testing"
)

func TestFullProjectionFencedByAnyPartition(t *testing.T) {
	ctx := context.Background()
	p := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	p2 := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	const snapshot = "b4444444-4444-4444-8444-444444444444"
	loadGeneration(t, p, snapshot, 10, snapshotRow(1, "active"))
	a := f.New(p)
	id := t.Name()
	req := f.Request{Identity: f.Identity{Environment: t.Name(), Group: "matcher", Topic: "prices", Partition: 2}, Scope: t.Name() + "-2", ProjectionID: id, RequestID: "first"}
	g2, err := a.Assign(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	req.Scope = t.Name() + "-3"
	req.Identity.Partition = 3
	g3, err := a.Assign(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	full := projection.NewFenced(p, id, []f.Grant{g2, g3})
	if err = projection.New(p2, id).Initialize(ctx, snapshot); !errors.Is(err, f.ErrAuthority) {
		t.Fatalf("legacy Initialize %v", err)
	}
	if err = projection.NewFenced(p2, id, []f.Grant{g2}).Initialize(ctx, snapshot); !errors.Is(err, f.ErrAuthority) {
		t.Fatalf("incomplete authority %v", err)
	}
	if err = full.Initialize(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	if err = full.ApplyReplay(ctx, [][]byte{replayWire(11, 11, 8, "active")}); err != nil {
		t.Fatal(err)
	}
	batch := projection.Batch{Topic: "changes", NextOffset: 100, Events: [][]byte{replayWire(12, 12, 9, "active")}}
	acked := false
	ack := func(context.Context, int64) error { acked = true; return nil }
	if err = full.Handle(ctx, batch, ack); err != nil || !acked {
		t.Fatalf("handle %v", err)
	}
	// Membership is copied by the constructor and remains a full-view guard.
	supplied := []f.Grant{g2, g3}
	copied := projection.NewFenced(p2, id, supplied)
	supplied[0].Token++
	if err = copied.ApplyReplay(ctx, batch.Events); err != nil {
		t.Fatal(err)
	}
	if err = a.Revoke(ctx, g3); err != nil {
		t.Fatal(err)
	}
	acked = false
	for name, call := range map[string]func() error{
		"initialize retry": func() error { return full.Initialize(ctx, snapshot) },
		"replay retry":     func() error { return full.ApplyReplay(ctx, batch.Events) },
		"replay new":       func() error { return full.ApplyReplay(ctx, [][]byte{replayWire(13, 13, 10, "cancelled")}) },
		"handle retry":     func() error { return full.Handle(ctx, batch, ack) },
		"legacy replay":    func() error { return projection.New(p2, id).ApplyReplay(ctx, batch.Events) },
		"legacy handle":    func() error { return projection.New(p2, id).Handle(ctx, batch, ack) },
		"subset":           func() error { return projection.NewFenced(p2, id, []f.Grant{g2}).ApplyReplay(ctx, batch.Events) },
		"wrong projection": func() error { return projection.NewFenced(p2, "other", []f.Grant{g2, g3}).Initialize(ctx, snapshot) },
	} {
		if err = call(); !errors.Is(err, f.ErrAuthority) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	cp, err := full.Checkpoint(ctx)
	if err != nil || cp.C != 12 || cp.NextOffset != 100 || acked {
		t.Fatalf("changed after revoke %+v %v ack=%v", cp, err, acked)
	}
	im, err := full.ReadImage(ctx, replaySub)
	if err != nil || im.Change.SubscriptionVersion != 9 {
		t.Fatalf("changed image %+v %v", im, err)
	}
	// A fresh generation may reuse immutable snapshot, without inventing prices C.
	req.RequestID = "next"
	req.ExpectedToken = g3.Token
	req.ProjectionID = id + "-new"
	next, err := f.New(p2).Assign(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	fresh := projection.NewFenced(p2, next.ProjectionID, []f.Grant{next})
	if err = fresh.Initialize(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	cp, err = fresh.Checkpoint(ctx)
	if err != nil || cp.C != 10 || cp.NextOffset != 0 {
		t.Fatalf("snapshot offset %+v %v", cp, err)
	}
	// Same projection ID can be rebound; the old complete token set stays stale.
	req.RequestID = "rebind"
	req.ExpectedToken = next.Token
	req.ProjectionID = id
	rebound, err := a.Assign(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if err = full.ApplyReplay(ctx, batch.Events); !errors.Is(err, f.ErrAuthority) {
		t.Fatalf("old membership after rebind %v", err)
	}
	if err = projection.NewFenced(p2, id, []f.Grant{g2, rebound}).ApplyReplay(ctx, batch.Events); err != nil {
		t.Fatal(err)
	}
}
