package pricehandler_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/live"
	h "github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricehandler"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricework"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/projection"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestRecoverPartialPrice(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprint(changed), func(t *testing.T) {
			ctx := context.Background()
			first := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
			other := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
			s := pricework.New(first, t.Name())
			v := fixture()
			base := v.selection.Candidates[0]
			v.selection.Candidates = nil
			for i := 0; i < 100; i++ {
				c := base
				c.ID = fmt.Sprintf("sub-%03d", i)
				v.selection.Candidates = append(v.selection.Candidates, c)
			}
			images := imageFunc(func(_ context.Context, id string) (projection.Image, error) {
				im := proto.Clone(v.image.Snapshot).(*pb.SnapshotSubscription)
				im.SubscriptionId = id
				return projection.Image{Snapshot: im}, nil
			})
			workflows := map[string]string{}
			calls := 0
			fail := true
			api := rpc(func(_ context.Context, owner, id string, version int64) (*pb.RequestDeliveryResponse, error) {
				calls++
				if owner != "owner" || version != 1 {
					t.Fatal(owner, id, version)
				}
				trigger := workflows[id]
				if trigger == "" {
					trigger = "canonical-" + id
					workflows[id] = trigger
				}
				// API committed candidate 38, but matcher only recorded the first 37.
				if fail && calls == 38 {
					return nil, status.Error(codes.Unavailable, "lost response after commit")
				}
				return &pb.RequestDeliveryResponse{SubscriptionId: id, SubscriptionVersion: 2, Workflow: &pb.Workflow{TriggerId: trigger, Version: 1}}, nil
			})
			handler := h.New(s, v, images, api, "old-generation")
			pos := &pricework.Position{Topic: "prices", Partition: 0, Offset: 40}
			if err := handler.Handle(ctx, h.Request{Key: "stable-key", Pair: "BTCUSDT", Position: pos}, nil); status.Code(err) != codes.Unavailable {
				t.Fatal(err)
			}
			w, err := s.Get(ctx, "stable-key")
			if err != nil || w.Completed != 37 || w.State != "pending" {
				t.Fatal(w, err)
			}
			first.Close()
			s = pricework.New(other, t.Name())
			// Restart must wait for a current view, even if its stored quote was suitable.
			v.err = live.ErrNotReady
			handler = h.New(s, v, images, api, "new-generation")
			if err = handler.Recover(ctx); !errors.Is(err, live.ErrNotReady) || calls != 38 {
				t.Fatal(err, calls)
			}
			v.err = nil
			if changed {
				p, _ := price.Parse("99")
				v.selection.Quote.Price = &p
				v.selection.Quote.Revision.Generation++
				v.selection.Candidates = nil
			}
			fail = false
			if err = handler.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			w, err = s.Get(ctx, "stable-key")
			if err != nil {
				t.Fatal(err)
			}
			if changed {
				if w.State != "superseded" || w.Completed != 37 || calls != 38 || len(workflows) != 38 || w.SupersededBy.Price.String() != "99" {
					t.Fatal(w, calls, len(workflows))
				}
			} else {
				if w.State != "completed" || w.Completed != 100 || calls != 101 || len(workflows) != 100 {
					t.Fatal(w, calls, len(workflows))
				}
				for _, e := range w.Entries {
					if e.Result == nil || e.Result.TriggerID != workflows[e.Candidate.SubscriptionID] {
						t.Fatal(e)
					}
				}
			}
			keys, err := s.Pending(ctx)
			if err != nil || len(keys) != 0 {
				t.Fatal(keys, err)
			}
			// Recovery never guesses a prefix, even for a single stored Kafka record.
			if _, err = s.Checkpoint(ctx, "prices", 0); !errors.Is(err, pgx.ErrNoRows) {
				t.Fatal(err)
			}
			if err = handler.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			before := calls
			lost := errors.New("ack response lost")
			prefix := &pricework.Prefix{Topic: "prices", Partition: 0, From: 40, Next: 41}
			acks := 0
			ack := func(context.Context) error {
				cp, e := s.Checkpoint(ctx, "prices", 0)
				if e != nil || cp != 41 {
					t.Fatal(cp, e)
				}
				acks++
				if acks == 1 {
					return lost
				}
				return nil
			}
			// Terminal work is absent from Pending but remains routable after lost ack.
			if err = handler.Resume(ctx, "stable-key", prefix, ack); !errors.Is(err, lost) {
				t.Fatal(err)
			}
			if err = handler.Resume(ctx, "stable-key", prefix, ack); err != nil || calls != before || acks != 2 {
				t.Fatal(err, calls, acks)
			}
			if err = handler.Resume(ctx, "unknown", nil, nil); !errors.Is(err, pgx.ErrNoRows) {
				t.Fatal(err)
			}
		})
	}
}

type pairViews map[string]live.Selection

func (v pairViews) Current(pair string) (live.Selection, error) { return v[pair], nil }

func TestRecoverMultipleInputs(t *testing.T) {
	ctx := context.Background()
	s := pricework.New(postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL"), t.Name())
	v := fixture()
	views := pairViews{}
	for i, pair := range []string{"BTCUSDT", "ETHUSDT"} {
		q := v.selection.Quote
		q.Pair = pair
		views[pair] = live.Selection{C: 70, Quote: q}
		var pos *pricework.Position
		if i == 0 {
			pos = &pricework.Position{Topic: "prices", Partition: 3, Offset: 90}
		}
		if err := s.Accept(ctx, pricework.Input{Key: pair, ProjectionID: "old", C: 7, Quote: q, Position: pos}); err != nil {
			t.Fatal(err)
		}
	}
	handler := h.New(s, views, v, rpc(func(context.Context, string, string, int64) (*pb.RequestDeliveryResponse, error) {
		t.Fatal("empty input has no candidates")
		return nil, nil
	}), "new")
	if err := handler.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	for pair := range views {
		w, err := s.Get(ctx, pair)
		if err != nil || w.State != "completed" || w.Input.C != 7 || w.Input.ProjectionID != "old" || w.Input.Quote.Pair != pair {
			t.Fatal(w, err)
		}
	}
	if _, err := s.Checkpoint(ctx, "prices", 3); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal(err)
	}
}
