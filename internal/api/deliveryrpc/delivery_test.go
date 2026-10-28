package deliveryrpc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/deliveryrpc"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	market "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const ownerID = "00000000-0000-4000-8000-000000000001"
const subID = "00000000-0000-4000-8000-000000000002"

var now = time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)

func request() *pb.RequestDeliveryRequest {
	return &pb.RequestDeliveryRequest{OwnerId: ownerID, SubscriptionId: subID, ExpectedVersion: 1}
}
func record() sub.Record {
	p, _ := price.Parse("100")
	return sub.Record{ID: subID, OwnerID: ownerID, Pair: "BTCUSDT", Threshold: p, Direction: price.Up, State: domain.Active, Version: 1}
}
func quote(value string) *market.LatestResponse {
	return &market.LatestResponse{Freshness: market.Freshness_FRESHNESS_STALE, Quote: &market.Quote{Pair: "BTCUSDT", Price: value, SourceId: "42", Revision: &market.Revision{StreamId: "durable", Generation: 1, Sequence: 42}, TradeTime: now.Add(-time.Hour).Format(time.RFC3339Nano), ReceivedAt: now.Add(-time.Minute).Format(time.RFC3339Nano), StreamState: "unconfirmed", GapDetected: true}}
}

type quotes func(context.Context, string) (*market.LatestResponse, error)

func (f quotes) Latest(ctx context.Context, pair string) (*market.LatestResponse, error) {
	return f(ctx, pair)
}

type store struct {
	value             sub.Record
	readErr, writeErr error
	reads, writes     int
	candidate         sub.DeliveryCandidate
}

func (s *store) ByID(ctx context.Context, owner, id string) (sub.Record, error) {
	s.reads++
	if owner != ownerID || id != subID {
		return sub.Record{}, sub.ErrNotFound
	}
	return s.value, s.readErr
}
func (s *store) RequestDelivery(ctx context.Context, c sub.DeliveryCandidate) (sub.Record, error) {
	s.writes++
	s.candidate = c
	if s.writeErr != nil {
		return s.value, s.writeErr
	}
	if s.value.Workflow == nil {
		s.value.Version++
		s.value.State = domain.PendingDelivery
		s.value.Workflow = &sub.Workflow{TriggerID: "stable", Version: 1}
	}
	return s.value, nil
}
func TestLatestConditionAndRepeat(t *testing.T) {
	s := &store{value: record()}
	calls := 0
	value := "99"
	server := deliveryrpc.NewServer(s, quotes(func(ctx context.Context, pair string) (*market.LatestResponse, error) {
		calls++
		if pair != "BTCUSDT" {
			t.Fatal(pair)
		}
		return quote(value), nil
	}), func() time.Time { return now })
	if r, e := server.RequestDelivery(context.Background(), request()); r != nil || status.Code(e) != codes.FailedPrecondition || s.writes != 0 {
		t.Fatal(r, e, s.writes)
	}
	value = "101"
	r, e := server.RequestDelivery(context.Background(), request())
	if e != nil || r.Workflow.TriggerId != "stable" || r.SubscriptionVersion != 2 || s.writes != 1 || s.candidate.Quote.Price.String() != "101" || s.candidate.Quote.TradeTime != now.Add(-time.Hour) || !s.candidate.Quote.GapDetected {
		t.Fatal(r, e, s.candidate)
	}
	// Recover even during a quote outage, using the original expected version.
	value = "invalid"
	again, e := server.RequestDelivery(context.Background(), request())
	if e != nil || again.Workflow.TriggerId != r.Workflow.TriggerId || s.writes != 1 || calls != 2 {
		t.Fatal(again, e, s.writes, calls)
	}
}
func TestRejections(t *testing.T) {
	for _, mode := range []string{"stale_version", "cancelled", "missing", "invalid_time", "future", "pair", "bad_revision", "bad_price", "bad_received", "quote_error", "read_error", "not_found", "cas_version", "cas_condition", "write_error", "invalid_request"} {
		t.Run(mode, func(t *testing.T) {
			s := &store{value: record()}
			q := quote("101")
			var qe error
			req := request()
			want := codes.Unavailable
			switch mode {
			case "stale_version":
				req.ExpectedVersion = 2
				want = codes.Aborted
			case "cancelled":
				s.value.State = domain.Cancelled
				want = codes.FailedPrecondition
			case "missing":
				q = &market.LatestResponse{Freshness: market.Freshness_FRESHNESS_MISSING}
			case "invalid_time":
				q.Freshness = market.Freshness_FRESHNESS_INVALID_TIME
			case "future":
				q.Quote.TradeTime = now.Add(time.Second).Format(time.RFC3339Nano)
			case "pair":
				q.Quote.Pair = "ETHUSDT"
			case "bad_revision":
				q.Quote.Revision = nil
			case "bad_price":
				q.Quote.Price = "0"
			case "bad_received":
				q.Quote.ReceivedAt = "garbage"
			case "quote_error":
				qe = errors.New("secret dependency details")
			case "read_error":
				s.readErr = errors.New("secret database details")
			case "not_found":
				s.readErr = sub.ErrNotFound
				want = codes.NotFound
			case "cas_version":
				s.value.Version = 1
				s.writeErr = sub.ErrCandidateRejected
				want = codes.Aborted
			case "cas_condition":
				s.writeErr = sub.ErrCandidateRejected
				want = codes.FailedPrecondition
			case "write_error":
				s.writeErr = sub.ErrStorage
			case "invalid_request":
				req.OwnerId = "bad"
				want = codes.InvalidArgument
			}
			// A concurrent version change becomes visible only at the atomic operation.
			source := quotes(func(context.Context, string) (*market.LatestResponse, error) {
				if mode == "cas_version" {
					s.value.Version = 2
				}
				return q, qe
			})
			r, e := deliveryrpc.NewServer(s, source, func() time.Time { return now }).RequestDelivery(context.Background(), req)
			if r != nil || status.Code(e) != want {
				t.Fatal(r, e, "want", want)
			}
			if status.Convert(e).Message() == "secret dependency details" || status.Convert(e).Message() == "secret database details" {
				t.Fatal(e)
			}
			if mode != "cas_version" && mode != "cas_condition" && mode != "write_error" && s.writes != 0 {
				t.Fatal("unexpected write")
			}
		})
	}
}

func TestRecoverWorkflowStates(t *testing.T) {
	for _, state := range []domain.State{domain.Active, domain.PendingDelivery, domain.Sending, domain.DeliveryBlocked, domain.Delivered, domain.Cancelled} {
		t.Run(string(state), func(t *testing.T) {
			s := &store{value: record()}
			s.value.State = state
			s.value.Version = 9
			s.value.Workflow = &sub.Workflow{TriggerID: "existing", Version: 3}
			server := deliveryrpc.NewServer(s, quotes(func(context.Context, string) (*market.LatestResponse, error) {
				t.Fatal("recovery reads quote")
				return nil, nil
			}), nil)
			r, e := server.RequestDelivery(context.Background(), request())
			if e != nil || r.State != string(state) || r.SubscriptionVersion != 9 || r.Workflow.TriggerId != "existing" || r.Workflow.Version != 3 || s.writes != 0 {
				t.Fatal(r, e)
			}
		})
	}
}
