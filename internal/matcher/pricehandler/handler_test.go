package pricehandler_test

import (
	"context"
	"errors"
	observed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/live"
	h "github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricehandler"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricework"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/projection"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/thresholdindex"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"strings"
	"testing"
	"time"
)

type view struct {
	selection live.Selection
	image     projection.Image
	err       error
}

func (v *view) Current(string) (live.Selection, error)                      { return v.selection, v.err }
func (v *view) ReadImage(context.Context, string) (projection.Image, error) { return v.image, nil }

type rpc func(context.Context, string, string, int64) (*pb.RequestDeliveryResponse, error)

func (f rpc) RequestDelivery(c context.Context, o, i string, v int64) (*pb.RequestDeliveryResponse, error) {
	return f(c, o, i, v)
}
func fixture() *view {
	p, _ := price.Parse("101")
	threshold, _ := price.Parse("100")
	q := observed.Observed{SchemaVersion: 1, Pair: "BTCUSDT", Price: &p, SourceID: "trade", Revision: observed.Revision{StreamID: "stream", Generation: 1, Sequence: 1}, TradeTime: time.Date(2026, 10, 29, 1, 0, 0, 0, time.UTC), ReceivedAt: time.Date(2026, 10, 29, 1, 0, 1, 0, time.UTC), StreamState: observed.Live}
	return &view{selection: live.Selection{C: 7, Quote: q, Candidates: []thresholdindex.Entry{{ID: "sub", Version: 1, Pair: q.Pair, Threshold: threshold, Direction: price.Up}}}, image: projection.Image{Snapshot: &pb.SnapshotSubscription{OwnerId: "owner", SubscriptionId: "sub", Version: 1, Pair: q.Pair, Threshold: "100", Direction: "up", State: "active"}}}
}
func success() *pb.RequestDeliveryResponse {
	return &pb.RequestDeliveryResponse{SubscriptionId: "sub", SubscriptionVersion: 2, State: "pending_delivery", Workflow: &pb.Workflow{TriggerId: "canonical", Version: 1}}
}
func TestDurableRetryAndAck(t *testing.T) {
	ctx := context.Background()
	pool := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	other := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	s, check := pricework.New(pool, t.Name()), pricework.New(other, t.Name())
	v := fixture()
	calls := 0
	client := rpc(func(_ context.Context, o, i string, version int64) (*pb.RequestDeliveryResponse, error) {
		calls++
		if o != "owner" || i != "sub" || version != 1 {
			t.Fatal(o, i, version)
		}
		w, e := check.Get(ctx, "key")
		if e != nil || len(w.Entries) != 1 {
			t.Fatal("RPC before durable accept", w, e)
		}
		if calls == 1 {
			return nil, status.Error(codes.Unavailable, "lost response")
		}
		return success(), nil
	})
	handler := h.New(s, v, v, client, "projection")
	acked := 0
	req := h.Request{Key: "key", Pair: "BTCUSDT", Position: &pricework.Position{Topic: "prices", Partition: 0, Offset: 40}, Prefix: &pricework.Prefix{Topic: "prices", Partition: 0, From: 40, Next: 41}}
	lost := errors.New("lost ack")
	ack := func(context.Context) error {
		w, e := check.Get(ctx, "key")
		cp, ce := check.Checkpoint(ctx, "prices", 0)
		if e != nil || ce != nil || w.State != "completed" || cp != 41 || w.Entries[0].Result.TriggerID != "canonical" {
			t.Fatal(w, e, cp, ce)
		}
		acked++
		if acked == 1 {
			return lost
		}
		return nil
	}
	if e := handler.Handle(ctx, req, ack); status.Code(e) != codes.Unavailable || acked != 0 {
		t.Fatal(e, acked)
	}
	// A new handler/store must recover the immutable command after a lost response.
	handler = h.New(check, v, v, client, "projection")
	if e := handler.Handle(ctx, req, ack); !errors.Is(e, lost) {
		t.Fatal(e)
	}
	if e := handler.Handle(ctx, req, ack); e != nil || calls != 2 || acked != 2 {
		t.Fatal(e, calls, acked)
	}
}
func TestLatestSupersedesHistorical(t *testing.T) {
	ctx := context.Background()
	s := pricework.New(postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL"), t.Name())
	v := fixture()
	old := v.selection.Quote
	p, _ := price.Parse("99")
	v.selection.Quote.Price = &p
	v.selection.Quote.Revision.Sequence++
	v.selection.Candidates = nil
	handler := h.New(s, v, v, rpc(func(context.Context, string, string, int64) (*pb.RequestDeliveryResponse, error) {
		t.Fatal("historical candidate requested")
		return nil, nil
	}), "projection")
	acked := false
	if e := handler.Handle(ctx, h.Request{Key: "old", Pair: "BTCUSDT", Event: &old}, func(context.Context) error { acked = true; return nil }); e != nil || !acked {
		t.Fatal(e)
	}
	w, e := s.Get(ctx, "old")
	if e != nil || w.State != "superseded" || w.SupersededBy.Price.String() != "99" {
		t.Fatal(w, e)
	}
}
func TestRejectionRefreshAndTransient(t *testing.T) {
	for _, code := range []codes.Code{codes.Aborted, codes.FailedPrecondition, codes.DeadlineExceeded, codes.Unavailable} {
		t.Run(code.String(), func(t *testing.T) {
			ctx := context.Background()
			s := pricework.New(postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL"), t.Name())
			v := fixture()
			calls := 0
			handler := h.New(s, v, v, rpc(func(context.Context, string, string, int64) (*pb.RequestDeliveryResponse, error) {
				calls++
				v.image.Snapshot.Version = 2
				v.image.Snapshot.State = "cancelled"
				v.err = live.ErrNotReady
				v.selection.C++
				v.selection.Candidates = nil
				return nil, status.Error(code, "opaque reason")
			}), "projection")
			acked := false
			e := handler.Handle(ctx, h.Request{Key: "key", Pair: "BTCUSDT"}, func(context.Context) error { acked = true; return nil })
			w, we := s.Get(ctx, "key")
			if we != nil {
				t.Fatal(we)
			}
			if code == codes.Aborted || code == codes.FailedPrecondition {
				if e != nil || !acked || w.Entries[0].Result.Outcome != "version_changed" {
					t.Fatal(w, e, acked)
				}
			} else {
				if status.Code(e) != code || acked || w.State != "pending" || w.Entries[0].Result != nil {
					t.Fatal(w, e, acked)
				}
			}
			if calls != 1 {
				t.Fatal(calls)
			}
		})
	}
}
func TestOpaqueRejectionStaysPending(t *testing.T) {
	ctx := context.Background()
	s := pricework.New(postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL"), t.Name())
	v := fixture()
	handler := h.New(s, v, v, rpc(func(context.Context, string, string, int64) (*pb.RequestDeliveryResponse, error) {
		return nil, status.Error(codes.FailedPrecondition, "no parsing")
	}), "projection")
	if e := handler.Handle(ctx, h.Request{Key: "key", Pair: "BTCUSDT"}, func(context.Context) error { t.Fatal("ack pending"); return nil }); status.Code(e) != codes.FailedPrecondition {
		t.Fatal(e)
	}
	w, e := s.Get(ctx, "key")
	if e != nil || w.State != "pending" || w.Entries[0].Result != nil {
		t.Fatal(w, e)
	}
}
func TestNoAckWithoutProvenPrefix(t *testing.T) {
	ctx := context.Background()
	s := pricework.New(postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL"), t.Name())
	v := fixture()
	handler := h.New(s, v, v, rpc(func(context.Context, string, string, int64) (*pb.RequestDeliveryResponse, error) {
		return success(), nil
	}), "projection")
	err := handler.Handle(ctx, h.Request{Key: "key", Pair: "BTCUSDT", Position: &pricework.Position{Topic: "prices", Partition: 0, Offset: 40}}, func(context.Context) error { t.Error("unproven prefix acknowledged"); return nil })
	if !errors.Is(err, h.ErrProtocol) {
		t.Fatal(err)
	}
}
func TestSafetyBoundaries(t *testing.T) {
	for _, mode := range []string{"owner", "notready", "version", "response", "imagechanged"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			s := pricework.New(postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL"), t.Name())
			v := fixture()
			calls := 0
			switch mode {
			case "owner":
				v.image.Snapshot.OwnerId = ""
			case "notready":
				v.err = live.ErrNotReady
			case "version":
				v.selection.Candidates[0].Version = 1 << 63
			case "imagechanged":
				v.image.Snapshot.Version = 2
			}
			handler := h.New(s, v, v, rpc(func(context.Context, string, string, int64) (*pb.RequestDeliveryResponse, error) {
				calls++
				return nil, nil
			}), "projection")
			err := handler.Handle(ctx, h.Request{Key: "key", Pair: "BTCUSDT"}, func(context.Context) error { t.Fatal("unsafe ack"); return nil })
			if err == nil {
				t.Fatal("accepted unsafe input")
			}
			if mode != "response" && calls != 0 {
				t.Fatal(calls)
			}
		})
	}
}
func TestNewViewAtSamePrice(t *testing.T) {
	ctx := context.Background()
	s := pricework.New(postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL"), t.Name())
	v := fixture()
	v.selection.Candidates = nil
	calls := 0
	handler := h.New(s, v, v, rpc(func(context.Context, string, string, int64) (*pb.RequestDeliveryResponse, error) {
		calls++
		return success(), nil
	}), "projection")
	if err := handler.Handle(ctx, h.Request{Key: "empty", Pair: "BTCUSDT"}, nil); err != nil {
		t.Fatal(err)
	}
	v.selection.C++
	v.selection.Candidates = fixture().selection.Candidates
	if err := handler.Handle(ctx, h.Request{Key: "new-sub", Pair: "BTCUSDT"}, nil); err != nil || calls != 1 {
		t.Fatal(err, calls)
	}
}
func TestSupersedeBetweenCandidates(t *testing.T) {
	ctx := context.Background()
	s := pricework.New(postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL"), t.Name())
	v := fixture()
	second := v.selection.Candidates[0]
	second.ID = "sub-2"
	v.selection.Candidates = append(v.selection.Candidates, second)
	images := imageFunc(func(_ context.Context, id string) (projection.Image, error) {
		im := proto.Clone(v.image.Snapshot).(*pb.SnapshotSubscription)
		im.SubscriptionId = id
		return projection.Image{Snapshot: im}, nil
	})
	calls := 0
	handler := h.New(s, v, images, rpc(func(context.Context, string, string, int64) (*pb.RequestDeliveryResponse, error) {
		calls++
		v.selection.Quote.Revision.Sequence++
		p, _ := price.Parse("99")
		v.selection.Quote.Price = &p
		v.selection.Candidates = nil
		return success(), nil
	}), "projection")
	if err := handler.Handle(ctx, h.Request{Key: "key", Pair: "BTCUSDT"}, nil); err != nil {
		t.Fatal(err)
	}
	w, err := s.Get(ctx, "key")
	if err != nil || w.State != "superseded" || w.Completed != 1 || calls != 1 || w.Entries[0].Result.TriggerID != "canonical" || w.Entries[1].Result != nil {
		t.Fatal(w, err, calls)
	}
}

type imageFunc func(context.Context, string) (projection.Image, error)

func (f imageFunc) ReadImage(ctx context.Context, id string) (projection.Image, error) {
	return f(ctx, id)
}
func TestHandlerCommitFailure(t *testing.T) {
	ctx := context.Background()
	s := pricework.New(postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL"), "TestCommitFailure")
	v := fixture()
	handler := h.New(s, v, v, rpc(func(context.Context, string, string, int64) (*pb.RequestDeliveryResponse, error) {
		return success(), nil
	}), "projection")
	err := handler.Handle(ctx, h.Request{Key: "handler", Pair: "BTCUSDT"}, func(context.Context) error { t.Fatal("ack failed commit"); return nil })
	if err == nil || !strings.Contains(err.Error(), "injected deferred commit failure") {
		t.Fatal(err)
	}
	w, e := s.Get(ctx, "handler")
	if e != nil || w.State != "pending" || w.Completed != 1 {
		t.Fatal(w, e)
	}
}
func TestWorkflowRecoveryAfterPairPruned(t *testing.T) {
	ctx := context.Background()
	s := pricework.New(postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL"), t.Name())
	v := fixture()
	calls := 0
	handler := h.New(s, v, v, rpc(func(context.Context, string, string, int64) (*pb.RequestDeliveryResponse, error) {
		calls++
		v.err = live.ErrNotReady
		v.image.Snapshot.State = "pending_delivery"
		v.image.Snapshot.Version = 2
		v.image.Snapshot.Workflow = &pb.SnapshotWorkflow{TriggerId: "canonical", Version: 1}
		if calls == 1 {
			return nil, status.Error(codes.Unavailable, "response lost after workflow")
		}
		return success(), nil
	}), "projection")
	req := h.Request{Key: "key", Pair: "BTCUSDT"}
	if err := handler.Handle(ctx, req, nil); status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
	if err := handler.Handle(ctx, req, nil); err != nil {
		t.Fatal("cannot recover workflow after last active pair pruned", err)
	}
	w, err := s.Get(ctx, "key")
	if err != nil || w.State != "completed" || w.Entries[0].Result.TriggerID != "canonical" || calls != 2 {
		t.Fatal(w, err, calls)
	}
}
