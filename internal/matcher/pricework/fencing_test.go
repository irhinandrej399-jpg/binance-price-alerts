package pricework_test

import (
	"context"
	"errors"
	"testing"

	f "github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/fencing"
	w "github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricework"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
)

func TestProtectedScopeRejectsLegacyConstructor(t *testing.T) {
	ctx := context.Background()
	p := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	in := input("work", 0)
	in.ProjectionID = t.Name()
	_, err := f.New(p).Assign(ctx, f.Request{Identity: f.Identity{Environment: t.Name(), Group: "matcher", Topic: "prices", Partition: 2}, Scope: t.Name(), ProjectionID: in.ProjectionID, RequestID: "first"})
	if err != nil {
		t.Fatal(err)
	}
	if err = w.New(p, t.Name()).Accept(ctx, in); !errors.Is(err, f.ErrAuthority) {
		t.Fatalf("unfenced Accept: %v", err)
	}
}

func TestFencedHandoverPreservesWork(t *testing.T) {
	ctx := context.Background()
	p := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	p2 := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	a := f.New(p)
	req := f.Request{Identity: f.Identity{Environment: t.Name(), Group: "matcher", Topic: "prices", Partition: 2}, Scope: t.Name(), ProjectionID: t.Name() + "-old", RequestID: "old"}
	g, err := a.Assign(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	old := w.NewFenced(p, g)
	in := input("work", 2)
	in.ProjectionID = g.ProjectionID
	if err = old.Accept(ctx, in); err != nil {
		t.Fatal(err)
	}
	canonical := w.Result{Outcome: "workflow", TriggerID: "canonical", WorkflowVersion: 99}
	if err = old.Record(ctx, in.Key, "sub-000", canonical); err != nil {
		t.Fatal(err)
	}
	empty := input("terminal", 0)
	empty.ProjectionID = g.ProjectionID
	if err = old.Accept(ctx, empty); err != nil {
		t.Fatal(err)
	}
	prefix := &w.Prefix{Topic: "prices", Partition: 2, From: 40, Next: 41}
	if err = old.Complete(ctx, empty.Key, nil, prefix, nil); err != nil {
		t.Fatal(err)
	}
	req.RequestID = "new"
	req.ExpectedToken = g.Token
	req.ProjectionID = t.Name() + "-new"
	next, err := f.New(p2).Assign(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	acked := false
	ack := func(context.Context) error { acked = true; return nil }
	for name, call := range map[string]func() error{
		"accept retry":    func() error { return old.Accept(ctx, in) },
		"record retry":    func() error { return old.Record(ctx, in.Key, "sub-000", canonical) },
		"record new":      func() error { return old.Record(ctx, in.Key, "sub-001", w.Result{Outcome: "condition_false"}) },
		"complete":        func() error { return old.Complete(ctx, in.Key, nil, prefix, ack) },
		"terminal prefix": func() error { return old.Complete(ctx, empty.Key, nil, prefix, ack) },
		"legacy record":   func() error { return w.New(p2, g.Scope).Record(ctx, in.Key, "sub-000", canonical) },
		"legacy complete": func() error { return w.New(p2, g.Scope).Complete(ctx, empty.Key, nil, prefix, ack) },
	} {
		if err = call(); !errors.Is(err, f.ErrAuthority) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if acked {
		t.Fatal("ack after fencing")
	}
	fresh := w.NewFenced(p2, next)
	keys, err := fresh.Pending(ctx)
	if err != nil || len(keys) != 1 || keys[0] != in.Key {
		t.Fatalf("pending %v %v", keys, err)
	}
	got, err := fresh.Get(ctx, in.Key)
	if err != nil || got.Completed != 1 || got.Entries[0].Result.TriggerID != "canonical" {
		t.Fatalf("lost canonical %+v %v", got, err)
	}
	// Exact retry and Record/Complete resume the original immutable generation.
	if err = fresh.Accept(ctx, in); err != nil {
		t.Fatal(err)
	}
	if err = fresh.Record(ctx, in.Key, "sub-000", canonical); err != nil {
		t.Fatal(err)
	}
	if err = fresh.Record(ctx, in.Key, "sub-001", w.Result{Outcome: "not_active"}); err != nil {
		t.Fatal(err)
	}
	if err = fresh.Complete(ctx, in.Key, nil, prefix, ack); err != nil || !acked {
		t.Fatalf("resume %v", err)
	}
	if err = fresh.Complete(ctx, empty.Key, nil, prefix, nil); err != nil {
		t.Fatal(err)
	}
	// New work must come from the new assignment's projection and fixed position.
	bad := input("new", 0)
	bad.ProjectionID = g.ProjectionID
	if err = fresh.Accept(ctx, bad); !errors.Is(err, f.ErrAuthority) {
		t.Fatalf("wrong projection %v", err)
	}
	bad.ProjectionID = next.ProjectionID
	bad.Position.Partition = 3
	if err = fresh.Accept(ctx, bad); !errors.Is(err, f.ErrAuthority) {
		t.Fatalf("wrong partition %v", err)
	}
	bad.Position.Partition = 2
	bad.Position.Topic = "other"
	if err = fresh.Accept(ctx, bad); !errors.Is(err, f.ErrAuthority) {
		t.Fatalf("wrong topic %v", err)
	}
	bad.Position = nil
	if err = fresh.Accept(ctx, bad); err != nil {
		t.Fatalf("Latest without offset %v", err)
	}
	// A different legacy scope cannot write using an enrolled projection identity.
	if err = w.New(p, t.Name()+"-bypass").Accept(ctx, bad); !errors.Is(err, f.ErrAuthority) {
		t.Fatalf("protected projection bypass %v", err)
	}
	for _, mutate := range []func(*f.Grant){func(g *f.Grant) { g.Scope = "other" }, func(g *f.Grant) { g.Identity.Partition++ }, func(g *f.Grant) { g.ProjectionID = "other" }} {
		forged := next
		mutate(&forged)
		if err = w.NewFenced(p, forged).Accept(ctx, bad); !errors.Is(err, f.ErrAuthority) {
			t.Fatalf("forged grant %v", err)
		}
	}
	if err = a.Revoke(ctx, next); err != nil {
		t.Fatal(err)
	}
	acked = false
	if err = fresh.Complete(ctx, empty.Key, nil, prefix, ack); !errors.Is(err, f.ErrAuthority) || acked {
		t.Fatalf("revoked terminal %v %v", err, acked)
	}
}
