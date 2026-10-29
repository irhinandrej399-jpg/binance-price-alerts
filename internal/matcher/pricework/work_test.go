package pricework_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	observed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	w "github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricework"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5"
)

func input(key string, n int) w.Input {
	p, _ := price.Parse("999999999999999999.123456789012345678")
	q := observed.Observed{SchemaVersion: 1, Pair: "BTCUSDT", Price: &p, SourceID: "trade-1", Revision: observed.Revision{StreamID: "stream", Generation: ^uint64(0), Sequence: 1}, TradeTime: time.Date(2026, 10, 29, 10, 0, 0, 0, time.UTC), ReceivedAt: time.Date(2026, 10, 29, 10, 0, 1, 0, time.UTC), StreamState: observed.Live}
	in := w.Input{Key: key, ProjectionID: "projection", C: ^uint64(0), Quote: q, Position: &w.Position{Topic: "prices", Partition: 2, Offset: 40}}
	for i := 0; i < n; i++ {
		in.Candidates = append(in.Candidates, w.Candidate{OwnerID: "owner", SubscriptionID: fmt.Sprintf("sub-%03d", i), Version: ^uint64(0)})
	}
	return in
}
func TestRecoveryAndIdempotency(t *testing.T) {
	ctx := context.Background()
	p := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	p2 := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	s, r := w.New(p, t.Name()), w.New(p2, t.Name())
	in := input("work", 100)
	var wg sync.WaitGroup
	for _, store := range []*w.Store{s, r} {
		wg.Go(func() {
			if err := store.Accept(ctx, in); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	for i := 0; i < 37; i++ {
		if err := s.Record(ctx, in.Key, in.Candidates[i].SubscriptionID, w.Result{Outcome: "workflow", TriggerID: fmt.Sprintf("trigger-%d", i), WorkflowVersion: 1}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := r.Get(ctx, in.Key)
	if err != nil || got.Completed != 37 || len(got.Entries) != 100 || got.Input.C != in.C || got.Input.Quote.Price.String() != in.Quote.Price.String() {
		t.Fatalf("recovered %+v %v", got, err)
	}
	remaining := 0
	for _, e := range got.Entries {
		if e.Result == nil {
			remaining++
		}
	}
	if remaining != 63 {
		t.Fatal(remaining)
	}
	acked := false
	ack := func(context.Context) error { acked = true; return nil }
	if err = s.Complete(ctx, in.Key, nil, nil, ack); !errors.Is(err, w.ErrPending) || acked {
		t.Fatalf("premature completion %v", err)
	}
	// Repeating acceptance after partial processing must not reset results.
	if err = r.Accept(ctx, in); err != nil {
		t.Fatal(err)
	}
	if err = r.Record(ctx, in.Key, "sub-000", w.Result{Outcome: "workflow", TriggerID: "different", WorkflowVersion: 1}); !errors.Is(err, w.ErrConflict) {
		t.Fatalf("replaced trigger: %v", err)
	}
	for i := 37; i < 100; i++ {
		if err = r.Record(ctx, in.Key, in.Candidates[i].SubscriptionID, w.Result{Outcome: "condition_false"}); err != nil {
			t.Fatal(err)
		}
	}
	lost := errors.New("lost broker ack")
	prefix := &w.Prefix{Topic: "prices", Partition: 2, From: 40, Next: 41}
	err = s.Complete(ctx, in.Key, nil, prefix, func(context.Context) error {
		g, e := r.Get(ctx, in.Key)
		cp, ce := r.Checkpoint(ctx, "prices", 2)
		if e != nil || ce != nil || g.State != "completed" || cp != 41 {
			t.Fatalf("ack before durable commit: %+v %d %v %v", g, cp, e, ce)
		}
		return lost
	})
	if !errors.Is(err, lost) {
		t.Fatal(err)
	}
	if err = r.Complete(ctx, in.Key, nil, prefix, ack); err != nil || !acked {
		t.Fatalf("retry ack %v", err)
	}
	if err = r.Accept(ctx, in); err != nil {
		t.Fatal(err)
	}
	g, err := r.Get(ctx, in.Key)
	if err != nil || g.Completed != 100 || g.Entries[0].Result.TriggerID != "trigger-0" {
		t.Fatalf("lost result %+v %v", g, err)
	}
	keys, err := r.Pending(ctx)
	if err != nil || len(keys) != 0 {
		t.Fatalf("pending %v %v", keys, err)
	}
	in.Candidates[0].Version = 1
	if err = s.Accept(ctx, in); !errors.Is(err, w.ErrConflict) {
		t.Fatalf("mutated input %v", err)
	}
}
func TestSupersededLatestAndIndependentScales(t *testing.T) {
	ctx := context.Background()
	p := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	s := w.New(p, t.Name())
	in := input("old", 2)
	if err := s.Accept(ctx, in); err != nil {
		t.Fatal(err)
	}
	result := w.Result{Outcome: "workflow", TriggerID: "canonical", WorkflowVersion: ^uint64(0)}
	if err := s.Record(ctx, in.Key, "sub-000", result); err != nil {
		t.Fatal(err)
	}
	newer := in.Quote
	newer.Revision.Sequence++
	for _, q := range []observed.Observed{in.Quote, func() observed.Observed { q := newer; q.Pair = "ETHUSDT"; return q }()} {
		if err := s.Complete(ctx, in.Key, &q, nil, nil); !errors.Is(err, w.ErrInvalid) {
			t.Fatalf("bad supersession %v", err)
		}
	}
	if err := s.Complete(ctx, in.Key, &newer, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Record(ctx, in.Key, "sub-000", result); err != nil {
		t.Fatal(err)
	}
	if err := s.Record(ctx, in.Key, "sub-001", w.Result{Outcome: "condition_false"}); !errors.Is(err, w.ErrConflict) {
		t.Fatal(err)
	}
	g, err := s.Get(ctx, in.Key)
	if err != nil || g.State != "superseded" || g.SupersededBy.Revision.Sequence != 2 || g.Entries[0].Result.TriggerID != "canonical" || g.Completed != 1 {
		t.Fatalf("superseded %+v %v", g, err)
	}
	// Same quote, a newer subscription/view must remain eligible; Latest has no offset.
	latest := in
	latest.Key = "latest"
	latest.C = 1
	latest.Position = nil
	latest.Candidates = []w.Candidate{{OwnerID: "owner", SubscriptionID: "new-sub", Version: 2}}
	if err = s.Accept(ctx, latest); err != nil {
		t.Fatal(err)
	}
	keys, err := s.Pending(ctx)
	if err != nil || len(keys) != 1 || keys[0] != "latest" {
		t.Fatalf("pending %v %v", keys, err)
	}
	if err = s.Complete(ctx, latest.Key, &newer, &w.Prefix{Topic: "prices", Partition: 2, From: 0, Next: 42}, nil); !errors.Is(err, w.ErrInvalid) {
		t.Fatalf("invented Latest offset %v", err)
	}
	g, err = s.Get(ctx, latest.Key)
	if err != nil || g.Input.Position != nil {
		t.Fatal(g, err)
	}
	// Separate pair and partition can finish independently, without deriving a prefix from C.
	other := input("eth", 0)
	other.Quote.Pair = "ETHUSDT"
	other.Position = &w.Position{Topic: "prices", Partition: 3, Offset: 7}
	other.C = 12
	if err = s.Accept(ctx, other); err != nil {
		t.Fatal(err)
	}
	if err = s.Complete(ctx, other.Key, nil, &w.Prefix{Topic: "prices", Partition: 3, From: 7, Next: 8}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Checkpoint(ctx, "prices", 2); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("inferred prefix %v", err)
	}
	if _, err = w.New(p, t.Name()+"isolated").Get(ctx, in.Key); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal(err)
	}
}
func TestTransactionalFailures(t *testing.T) {
	ctx := context.Background()
	p := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	s := w.New(p, t.Name())
	bad := input("fail-accept", 2)
	if err := s.Accept(ctx, bad); err == nil || !strings.Contains(err.Error(), "injected candidate insert failure") {
		t.Fatal("expected candidate insert failure")
	}
	if _, err := s.Get(ctx, bad.Key); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("accept rollback %v", err)
	}
	in := input("fail-record", 1)
	if err := s.Accept(ctx, in); err != nil {
		t.Fatal(err)
	}
	if err := s.Record(ctx, in.Key, "sub-000", w.Result{Outcome: "condition_false"}); err == nil || !strings.Contains(err.Error(), "injected local progress failure") {
		t.Fatal("expected progress failure")
	}
	g, err := s.Get(ctx, in.Key)
	if err != nil || g.Completed != 0 || g.Entries[0].Result != nil {
		t.Fatalf("result rollback %+v %v", g, err)
	}
	empty := input("fail-prefix", 0)
	if err = s.Accept(ctx, empty); err != nil {
		t.Fatal(err)
	}
	called := false
	err = s.Complete(ctx, empty.Key, nil, &w.Prefix{Topic: "prices", Partition: 2, From: 40, Next: 41}, func(context.Context) error { called = true; return nil })
	if err == nil || !strings.Contains(err.Error(), "injected price checkpoint failure") || called {
		t.Fatalf("checkpoint failure/ack %v %v", err, called)
	}
	g, err = s.Get(ctx, empty.Key)
	if err != nil || g.State != "pending" {
		t.Fatalf("finish rollback %+v %v", g, err)
	}
	if _, err = s.Checkpoint(ctx, "prices", 2); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("checkpoint leaked %v", err)
	}
}

func TestCommitFailure(t *testing.T) {
	ctx := context.Background()
	p := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	s := w.New(p, t.Name())
	in := input("commit", 0)
	if err := s.Accept(ctx, in); err != nil {
		t.Fatal(err)
	}
	called := false
	err := s.Complete(ctx, in.Key, nil, &w.Prefix{Topic: "prices", Partition: 2, From: 40, Next: 41}, func(context.Context) error { called = true; return nil })
	if err == nil || !strings.Contains(err.Error(), "injected deferred commit failure") || called {
		t.Fatalf("commit failure ack %v %v", err, called)
	}
	g, err := s.Get(ctx, in.Key)
	if err != nil || g.State != "pending" {
		t.Fatalf("commit rollback %+v %v", g, err)
	}
	if _, err = s.Checkpoint(ctx, "prices", 2); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal(err)
	}
}

func TestConcurrentResultsAndPrefixCAS(t *testing.T) {
	ctx := context.Background()
	p := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	p2 := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	s, r := w.New(p, t.Name()), w.New(p2, t.Name())
	in := input("cas", 1)
	if err := s.Accept(ctx, in); err != nil {
		t.Fatal(err)
	}
	result := w.Result{Outcome: "workflow", TriggerID: "same", WorkflowVersion: 1}
	var wg sync.WaitGroup
	for _, store := range []*w.Store{s, r} {
		wg.Go(func() {
			for range 10 {
				if err := store.Record(ctx, in.Key, "sub-000", result); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	g, err := r.Get(ctx, in.Key)
	if err != nil || g.Completed != 1 {
		t.Fatalf("double progress %+v %v", g, err)
	}
	if err = s.Complete(ctx, in.Key, nil, &w.Prefix{Topic: "prices", Partition: 2, From: 40, Next: 41}, nil); err != nil {
		t.Fatal(err)
	}
	// Same price/subscription with a new expected version and view is new work.
	next := in
	next.Key = "new-view"
	next.C = 1
	next.Candidates = []w.Candidate{{OwnerID: "owner", SubscriptionID: "sub-000", Version: 2}}
	next.Position = &w.Position{Topic: "prices", Partition: 2, Offset: 45}
	if err = s.Accept(ctx, next); err != nil {
		t.Fatal(err)
	}
	if err = s.Record(ctx, next.Key, "sub-000", w.Result{Outcome: "version_changed"}); err != nil {
		t.Fatal(err)
	}
	acked := false
	if err = s.Complete(ctx, next.Key, nil, &w.Prefix{Topic: "prices", Partition: 2, From: 42, Next: 46}, func(context.Context) error { acked = true; return nil }); !errors.Is(err, w.ErrConflict) || acked {
		t.Fatalf("prefix gap %v", err)
	}
	g, err = r.Get(ctx, next.Key)
	if err != nil || g.State != "pending" || g.Completed != 1 {
		t.Fatalf("CAS rollback %+v %v", g, err)
	}
	if cp, err := r.Checkpoint(ctx, "prices", 2); err != nil || cp != 41 {
		t.Fatal(cp, err)
	}
	// Caller attests continuous read_committed prefix (including physical gaps).
	if err = s.Complete(ctx, next.Key, nil, &w.Prefix{Topic: "prices", Partition: 2, From: 41, Next: 46}, nil); err != nil {
		t.Fatal(err)
	}
	// Old retry cannot regress the prefix.
	if err = r.Complete(ctx, in.Key, nil, &w.Prefix{Topic: "prices", Partition: 2, From: 40, Next: 41}, nil); err != nil {
		t.Fatal(err)
	}
	if cp, err := r.Checkpoint(ctx, "prices", 2); err != nil || cp != 46 {
		t.Fatal(cp, err)
	}
	// Projection checkpoints belong to a different protocol and remain untouched.
	var count int
	if err = p.QueryRow(ctx, `SELECT count(*) FROM app.matcher_checkpoints WHERE projection_id=$1`, t.Name()).Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
}

func TestInvalidBeforeSQL(t *testing.T) {
	ctx := context.Background()
	s := w.New(nil, "validation")
	for _, mutate := range []func(*w.Input){
		func(in *w.Input) { in.Key = "" }, func(in *w.Input) { in.ProjectionID = "" }, func(in *w.Input) { in.Quote.Revision.Sequence = 0 },
		func(in *w.Input) { in.Position.Offset = -1 }, func(in *w.Input) { in.Candidates[0].Version = 0 }, func(in *w.Input) { in.Candidates[0].OwnerID = "" },
		func(in *w.Input) { in.Candidates = append(in.Candidates, in.Candidates[0]) },
	} {
		in := input("invalid", 1)
		mutate(&in)
		if err := s.Accept(ctx, in); err == nil {
			t.Fatal("invalid accepted")
		}
	}
	for _, result := range []w.Result{{}, {Outcome: "workflow"}, {Outcome: "condition_false", TriggerID: "invented"}, {Outcome: "timeout"}} {
		if err := s.Record(ctx, "key", "sub", result); !errors.Is(err, w.ErrInvalid) {
			t.Fatal(err)
		}
	}
}
