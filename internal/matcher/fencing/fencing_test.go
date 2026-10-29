package fencing_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	f "github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/fencing"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5"
)

func TestDurableAssignment(t *testing.T) {
	ctx := context.Background()
	p := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	p2 := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	a, b := f.New(p), f.New(p2)
	r := f.Request{Identity: f.Identity{Environment: t.Name(), Group: "matcher", Topic: "prices", Partition: 2}, Scope: t.Name(), ProjectionID: t.Name(), RequestID: "first"}
	g, err := a.Assign(ctx, r)
	if err != nil || g.Token != 1 {
		t.Fatalf("first %+v %v", g, err)
	}
	retry, err := b.Assign(ctx, r)
	if err != nil || retry != g {
		t.Fatalf("retry %+v %v", retry, err)
	}
	changed := r
	changed.ProjectionID = "other"
	if _, err = b.Assign(ctx, changed); !errors.Is(err, f.ErrAuthority) {
		t.Fatalf("request reuse %v", err)
	}
	r.RequestID = "next"
	r.ExpectedToken = g.Token
	next, err := b.Assign(ctx, r)
	if err != nil || next.Token != 2 {
		t.Fatalf("next %+v %v", next, err)
	}
	retry, err = a.Assign(ctx, f.Request{Identity: g.Identity, Scope: g.Scope, ProjectionID: g.ProjectionID, RequestID: "first"})
	if err != nil || retry != g {
		t.Fatalf("late retry %+v %v", retry, err)
	}
	if err = a.Revoke(ctx, g); !errors.Is(err, f.ErrAuthority) {
		t.Fatalf("stale revoke %v", err)
	}
	if err = b.Revoke(ctx, next); err != nil {
		t.Fatal(err)
	}
	if err = a.Revoke(ctx, next); err != nil {
		t.Fatal(err)
	}
	r.RequestID = "third"
	r.ExpectedToken = next.Token
	third, err := a.Assign(ctx, r)
	if err != nil || third.Token != 3 {
		t.Fatalf("after revoke %+v %v", third, err)
	}
	r.RequestID = "stale"
	if _, err = b.Assign(ctx, r); !errors.Is(err, f.ErrAuthority) {
		t.Fatalf("stale CAS %v", err)
	}
}

func TestAssignmentWaitsForSQLCommit(t *testing.T) {
	ctx := context.Background()
	p := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	p2 := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	r := f.Request{Identity: f.Identity{Environment: t.Name(), Group: "matcher", Topic: "prices", Partition: 0}, Scope: t.Name(), ProjectionID: t.Name(), RequestID: "one"}
	a := f.New(p)
	g, err := a.Assign(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err = f.Lock(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if err = f.Price(ctx, tx, g.Scope, &g); err != nil {
		t.Fatal(err)
	}
	// The successor must wait until this already-authorized transaction ends.
	r.RequestID = "two"
	r.ExpectedToken = g.Token
	timeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	_, err = f.New(p2).Assign(timeout, r)
	cancel()
	if err == nil {
		t.Fatal("assignment passed an in-flight guarded transaction")
	}
	if _, err = tx.Exec(ctx, `INSERT INTO app.matcher_connection_probe VALUES (17001)`); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	next, err := f.New(p2).Assign(ctx, r)
	if err != nil || next.Token != 2 {
		t.Fatalf("retry after rollback %+v %v", next, err)
	}
	tx, err = p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err = f.Lock(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if err = f.Price(ctx, tx, g.Scope, &g); !errors.Is(err, f.ErrAuthority) {
		t.Fatalf("old grant after successor commit %v", err)
	}
}

func TestAssignmentExhaustion(t *testing.T) {
	ctx := context.Background()
	p := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	r := f.Request{Identity: f.Identity{Environment: t.Name(), Group: "matcher", Topic: "prices", Partition: 0}, Scope: t.Name(), ProjectionID: t.Name(), RequestID: "one"}
	a := f.New(p)
	g, err := a.Assign(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	g.Token = math.MaxInt64
	gb, _ := json.Marshal(g)
	kb, _ := json.Marshal(g.Identity)
	if _, err = p.Exec(ctx, `UPDATE app.matcher_assignments SET token=$2,grant_body=$3 WHERE assignment_key=$1`, string(kb), g.Token, gb); err != nil {
		t.Fatal(err)
	}
	r.RequestID = "exhausted"
	r.ExpectedToken = g.Token
	if _, err = a.Assign(ctx, r); !errors.Is(err, f.ErrExhausted) {
		t.Fatalf("overflow %v", err)
	}
	r.ExpectedToken = 0
	if _, err = a.Assign(ctx, r); !errors.Is(err, f.ErrAuthority) {
		t.Fatalf("reset %v", err)
	}
}

func TestAssignmentRollback(t *testing.T) {
	ctx := context.Background()
	p := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	p2 := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	r := f.Request{Identity: f.Identity{Environment: t.Name(), Group: "matcher", Topic: "prices", Partition: 0}, Scope: t.Name(), ProjectionID: t.Name(), RequestID: "write-failure"}
	for _, failure := range []string{"write-failure", "commit-failure"} {
		r.RequestID = failure
		if _, err := f.New(p).Assign(ctx, r); err == nil {
			t.Fatalf("missing %s", failure)
		}
		var n int
		for _, table := range []string{"matcher_assignments", "matcher_assignment_requests", "matcher_projection_grants"} {
			// Test-only known table names, no caller input in SQL identifiers.
			if err := p2.QueryRow(ctx, `SELECT count(*) FROM app.`+table+` WHERE assignment_key=$1`, identityKey(r.Identity)).Scan(&n); err != nil || n != 0 {
				t.Fatalf("partial %s %d %v", table, n, err)
			}
		}
	}
	r.RequestID = "response-lost"
	if _, err := f.New(p).Assign(ctx, r); err != nil {
		t.Fatal(err)
	} // discard response
	g, err := f.New(p2).Assign(ctx, r)
	if err != nil || g.Token != 1 {
		t.Fatalf("recover response %+v %v", g, err)
	}
}
func identityKey(i f.Identity) string { b, _ := json.Marshal(i); return string(b) }

func TestConcurrentAssignments(t *testing.T) {
	ctx := context.Background()
	p := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	p2 := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	a, b := f.New(p), f.New(p2)
	r := f.Request{Identity: f.Identity{Environment: t.Name(), Group: "matcher", Topic: "prices", Partition: 0}, Scope: t.Name(), ProjectionID: t.Name(), RequestID: "same"}
	if _, _, err := a.Current(ctx, r.Identity); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("absent %v", err)
	}
	type result struct {
		g   f.Grant
		err error
	}
	out := make(chan result, 2)
	for _, s := range []*f.Store{a, b} {
		go func() { g, err := s.Assign(ctx, r); out <- result{g, err} }()
	}
	x, y := <-out, <-out
	if x.err != nil || y.err != nil || x.g != y.g || x.g.Token != 1 {
		t.Fatalf("concurrent retry %+v %+v", x, y)
	}
	current, active, err := b.Current(ctx, r.Identity)
	if err != nil || !active || current != x.g {
		t.Fatalf("current %+v %v %v", current, active, err)
	}
	r.ExpectedToken = current.Token
	for i, s := range []*f.Store{a, b} {
		req := r
		req.RequestID = fmt.Sprintf("competitor-%d", i)
		go func() { g, err := s.Assign(ctx, req); out <- result{g, err} }()
	}
	x, y = <-out, <-out
	if x.err != nil {
		x, y = y, x
	}
	if x.err != nil || x.g.Token != 2 || !errors.Is(y.err, f.ErrAuthority) {
		t.Fatalf("CAS race %+v %+v", x, y)
	}
	if err = a.Revoke(ctx, x.g); err != nil {
		t.Fatal(err)
	}
	current, active, err = b.Current(ctx, r.Identity)
	if err != nil || active || current != x.g {
		t.Fatalf("revoked current %+v %v %v", current, active, err)
	}
	// Scope cannot drift between partitions or change on the same identity.
	req := r
	req.ExpectedToken = 2
	req.RequestID = "drift"
	req.Scope = "different"
	if _, err = a.Assign(ctx, req); !errors.Is(err, f.ErrAuthority) {
		t.Fatalf("scope drift %v", err)
	}
	req = r
	req.Identity.Partition++
	req.ExpectedToken = 0
	req.RequestID = "other"
	if _, err = a.Assign(ctx, req); !errors.Is(err, f.ErrAuthority) {
		t.Fatalf("partition drift %v", err)
	}
}
