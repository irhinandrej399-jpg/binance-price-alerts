package pricework_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	f "github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/fencing"
	w "github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricework"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/sourcebinding"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Include every work state: Pending alone cannot establish scope provenance.
func TestReadScopeHistory(t *testing.T) {
	p := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx := context.Background()
	for _, mode := range []string{"empty", "ledger", "pending", "latest", "superseded", "checkpoint", "legacy-pending", "legacy-latest", "legacy-superseded", "legacy-checkpoint", "legacy-other-checkpoint"} {
		t.Run(mode, func(t *testing.T) {
			scope := t.Name()
			src := priceSource()
			legacy := len(mode) > 7 && mode[:7] == "legacy-"
			var in w.Input
			var g f.Grant
			var writer *w.Store
			if legacy {
				writer = w.New(p, scope)
			} else {
				g = sourceGrant(t, p, scope)
				writer = w.NewSourceBound(p, g, src)
			}
			if mode == "ledger" {
				// Existing SQL binding function is the only authorized writer of the ledger.
				if _, err := p.Exec(ctx, `SELECT app.matcher_prices_source($1,'prices',2,$2,$3)`, scope, src.ClusterID, src.TopicID[:]); err != nil {
					t.Fatal(err)
				}
			} else if mode == "legacy-checkpoint" || mode == "legacy-other-checkpoint" {
				part := int32(2)
				if mode == "legacy-other-checkpoint" {
					part = 3
				}
				if _, err := p.Exec(ctx, `INSERT INTO app.matcher_price_checkpoints VALUES($1,'prices',$2,41)`, scope, part); err != nil {
					t.Fatal(err)
				}
			} else if mode != "empty" {
				if legacy {
					in = input("work", 0)
				} else {
					in = sourceInput(g, src, "work", 0)
				}
				if mode == "latest" || mode == "legacy-latest" {
					in.Position = nil
				}
				if err := writer.Accept(ctx, in); err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "latest", "legacy-latest":
					if err := writer.Complete(ctx, in.Key, nil, nil, nil); err != nil {
						t.Fatal(err)
					}
				case "superseded", "legacy-superseded":
					replacement := in.Quote
					replacement.Revision.Sequence++
					if err := writer.Complete(ctx, in.Key, &replacement, nil, nil); err != nil {
						t.Fatal(err)
					}
				case "checkpoint":
					if err := writer.Complete(ctx, in.Key, nil, sourcePrefix(src), nil); err != nil {
						t.Fatal(err)
					}
				}
			}
			if legacy {
				g = sourceGrant(t, p, scope)
			}
			store := w.NewSourceBound(p, g, src)
			before := scopeSnapshot(t, p, scope)
			got, err := store.ReadScope(ctx, "prices", 2)
			if legacy {
				if !errors.Is(err, sourcebinding.ErrUnbound) {
					t.Fatalf("got %+v, %v", got, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if (got.Source == nil) != (mode == "empty") || (got.Source != nil && *got.Source != src) {
					t.Fatal(got)
				}
				wantWork := mode != "empty" && mode != "ledger"
				if got.HasWork != wantWork || (got.Checkpoint != nil) != (mode == "checkpoint") {
					t.Fatal(got)
				}
				if got.Checkpoint != nil && (got.Checkpoint.NextOffset != 41 || got.Checkpoint.Topic != "prices" || got.Checkpoint.Partition != 2 || *got.Checkpoint.Source != src) {
					t.Fatal(got.Checkpoint)
				}
				for _, change := range []string{"cluster", "id", "topic", "partition"} {
					bad := src
					topic := "prices"
					part := int32(2)
					switch change {
					case "cluster":
						bad.ClusterID = "other"
					case "id":
						bad.TopicID[0] = 2
					case "topic":
						bad.Topic = "other"
						topic = "other"
					case "partition":
						part = 3
					}
					other := w.NewSourceBound(p, g, bad)
					_, err = other.ReadScope(ctx, topic, part)
					if mode != "empty" && !errors.Is(err, kafkasource.ErrMismatch) {
						t.Fatalf("%s: %v", change, err)
					}
					if mode == "empty" && err != nil {
						t.Fatal(err)
					}
				}
			}
			if mode != "checkpoint" && mode != "legacy-checkpoint" {
				_, err = store.ReadCheckpoint(ctx, "prices", 2)
				if !errors.Is(err, pgx.ErrNoRows) {
					t.Fatal("fabricated cursor", err)
				}
			}
			if after := scopeSnapshot(t, p, scope); before != after {
				t.Fatal("read mutated scope", before, after)
			}
		})
	}
}

// Compare bytes, outcomes, state, ledger, cursor and grant, including legacy data.
func scopeSnapshot(t *testing.T, p *pgxpool.Pool, scope string) string {
	t.Helper()
	var out string
	err := p.QueryRow(context.Background(), `SELECT jsonb_build_array(
 (SELECT jsonb_agg(to_jsonb(w) ORDER BY work_key) FROM app.matcher_price_work w WHERE scope=$1),
 (SELECT jsonb_agg(to_jsonb(c) ORDER BY work_key,subscription_id) FROM app.matcher_price_candidates c WHERE scope=$1),
 (SELECT jsonb_agg(to_jsonb(s)) FROM app.matcher_price_sources s WHERE scope=$1),
 (SELECT jsonb_agg(to_jsonb(c) ORDER BY topic,partition) FROM app.matcher_price_checkpoints c WHERE scope=$1),
 (SELECT jsonb_agg(to_jsonb(g)) FROM app.matcher_assignments g WHERE scope=$1))::text`, scope).Scan(&out)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestReadScopeTakeoverAndGuards(t *testing.T) {
	p := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	p2 := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx := context.Background()
	src := priceSource()
	g := sourceGrant(t, p, t.Name())
	old := w.NewSourceBound(p, g, src)
	empty, err := old.ReadScope(ctx, "prices", 2)
	if err != nil || empty.Source != nil {
		t.Fatal(empty, err)
	}
	// Read does not reserve a binding: a legacy writer can still win after it.
	legacy := w.NewFenced(p2, g)
	in := input("legacy", 0)
	in.ProjectionID = g.ProjectionID
	if err = legacy.Accept(ctx, in); err != nil {
		t.Fatal(err)
	}
	if err = old.Accept(ctx, sourceInput(g, src, "bound", 0)); !errors.Is(err, sourcebinding.ErrUnbound) {
		t.Fatal(err)
	}
	if _, err = old.ReadScope(ctx, "prices", 2); !errors.Is(err, sourcebinding.ErrUnbound) {
		t.Fatal(err)
	}

	g = sourceGrant(t, p, t.Name()+"-bound")
	old = w.NewSourceBound(p, g, src)
	in = sourceInput(g, src, "work", 1)
	if err = old.Accept(ctx, in); err != nil {
		t.Fatal(err)
	}
	if err = old.Record(ctx, in.Key, "sub-000", w.Result{Outcome: "workflow", TriggerID: "canonical", WorkflowVersion: 1}); err != nil {
		t.Fatal(err)
	}
	g2, err := f.New(p2).Assign(ctx, f.Request{Identity: g.Identity, Scope: g.Scope, ProjectionID: g.Scope + "-new", RequestID: "takeover", ExpectedToken: g.Token})
	if err != nil {
		t.Fatal(err)
	}
	fresh := w.NewSourceBound(p2, g2, src)
	before := scopeSnapshot(t, p, g.Scope)
	a, err := old.ReadScope(ctx, "prices", 2)
	if err != nil {
		t.Fatal(err)
	}
	b, err := fresh.ReadScope(ctx, "prices", 2)
	if err != nil || !reflect.DeepEqual(a, b) {
		t.Fatal(a, b, err)
	}
	if before != scopeSnapshot(t, p, g.Scope) {
		t.Fatal("read changed durable state")
	}
	if err = old.Complete(ctx, in.Key, nil, sourcePrefix(src), nil); !errors.Is(err, f.ErrAuthority) {
		t.Fatal(err)
	}
	if err = fresh.Complete(ctx, in.Key, nil, sourcePrefix(src), nil); err != nil {
		t.Fatal(err)
	}
	b, err = fresh.ReadScope(ctx, "prices", 2)
	if err != nil || b.Checkpoint == nil || b.Checkpoint.NextOffset != 41 {
		t.Fatal(b, err)
	}
	got, err := fresh.Get(ctx, in.Key)
	if err != nil || got.Entries[0].Result.TriggerID != "canonical" {
		t.Fatal(got, err)
	}
	if _, err = p.Exec(ctx, `UPDATE app.matcher_price_checkpoints SET next_offset=99 WHERE scope=$1`, g.Scope); err == nil {
		t.Fatal("legacy writer advanced")
	}
	// Bound mismatch remains checked at writes after a successful read.
	bad := src
	bad.TopicID[0] = 2
	wrong := w.NewSourceBound(p2, g2, bad)
	if err = wrong.Accept(ctx, sourceInput(g2, bad, "wrong", 0)); !errors.Is(err, kafkasource.ErrMismatch) {
		t.Fatal(err)
	}
}

func TestReadScopePreMigrationAndFault(t *testing.T) {
	p := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx := context.Background()
	g := sourceGrant(t, p, "source-pre-migration")
	s := w.NewSourceBound(p, g, priceSource())
	before := scopeSnapshot(t, p, g.Scope)
	if _, err := s.ReadScope(ctx, "prices", 2); !errors.Is(err, sourcebinding.ErrUnbound) {
		t.Fatal(err)
	}
	if before != scopeSnapshot(t, p, g.Scope) {
		t.Fatal("changed pre-migration data")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.ReadScope(canceled, "prices", 2); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	tx, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `ALTER TABLE app.matcher_price_sources RENAME TO matcher_price_sources_unavailable`); err == nil {
		t.Fatal("runtime unexpectedly owns ledger")
	}
	// A closed pool yields an explicit SQL access error, never empty/bootstrap.
	closed := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	closed.Close()
	if _, err = w.NewSourceBound(closed, g, priceSource()).ReadScope(ctx, "prices", 2); err == nil {
		t.Fatal("SQL fault allowed bootstrap")
	}
}
