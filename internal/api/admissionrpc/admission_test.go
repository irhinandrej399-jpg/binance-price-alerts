package admissionrpc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/admissionrpc"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	market "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const ownerID = "00000000-0000-4000-8000-000000000001"
const subID = "00000000-0000-4000-8000-000000000002"
const workflowID = "00000000-0000-4000-8000-000000000003"

var now = time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)

func request() *pb.AdmitDeliveryRequest {
	return &pb.AdmitDeliveryRequest{OwnerId: ownerID, SubscriptionId: subID, WorkflowId: workflowID, RequestId: "check"}
}
func record() sub.Record {
	p, _ := price.Parse("100")
	return sub.Record{ID: subID, OwnerID: ownerID, Pair: "BTCUSDT", Threshold: p, Direction: price.Up, State: domain.PendingDelivery, Version: 2, Workflow: &sub.Workflow{TriggerID: workflowID, Version: 2}}
}
func quote(value string) *market.LatestResponse {
	return &market.LatestResponse{Freshness: market.Freshness_FRESHNESS_STALE, Quote: &market.Quote{Pair: "BTCUSDT", Price: value, SourceId: "42", Revision: &market.Revision{StreamId: "durable", Generation: 1, Sequence: 42}, TradeTime: now.Add(-time.Hour).Format(time.RFC3339Nano), ReceivedAt: now.Add(-time.Minute).Format(time.RFC3339Nano), StreamState: "unconfirmed", GapDetected: true}}
}

type quotes func(context.Context, string) (*market.LatestResponse, error)

func (f quotes) Latest(ctx context.Context, pair string) (*market.LatestResponse, error) {
	return f(ctx, pair)
}
func newServer(s admissionrpc.Store, q sub.QuoteSource, clock func() time.Time) *admissionrpc.Server {
	server, e := admissionrpc.NewServer(s, q, time.Minute, clock)
	if e != nil {
		panic(e)
	}
	return server
}

type store struct {
	value             sub.Record
	saved             *sub.AdmissionResult
	binding           int64
	change            bool
	writes            int
	readErr, writeErr error
}

func (s *store) PrepareAdmission(ctx context.Context, c sub.AdmissionCommand) (sub.AdmissionResult, error) {
	if s.readErr != nil {
		return sub.AdmissionResult{}, s.readErr
	}
	if s.saved != nil {
		return *s.saved, nil
	}
	if s.binding == 0 {
		s.binding = 1
	}
	return sub.AdmissionResult{}, &sub.AdmissionQuoteRequired{Record: s.value, BindingVersion: s.binding}
}
func (s *store) AdmitDelivery(ctx context.Context, c sub.AdmissionCommand) (sub.AdmissionResult, error) {
	if s.writeErr != nil {
		return sub.AdmissionResult{}, s.writeErr
	}
	if s.change {
		s.binding++
		s.change = false
		return sub.AdmissionResult{}, sub.ErrAdmissionChanged
	}
	if c.ExpectedBindingVersion == nil || *c.ExpectedBindingVersion != s.binding {
		return sub.AdmissionResult{}, sub.ErrAdmissionChanged
	}
	s.writes++
	r := sub.AdmissionResult{Record: s.value, Outcome: sub.WaitCondition}
	if price.Satisfied(*c.Quote.Price, s.value.Threshold, s.value.Direction) {
		r.Outcome = sub.Admitted
		r.Record.State = domain.Sending
		r.Attempt = &sub.AttemptPermission{ID: "attempt", Fence: 1, ExpiresAt: now.Add(c.PermitTTL), ChatID: 123, BindingVersion: s.binding, Quote: c.Quote}
	}
	s.saved = &r
	return r, nil
}
func TestLatestAndReplay(t *testing.T) {
	for _, value := range []string{"99", "101.000000000000000001"} {
		t.Run(value, func(t *testing.T) {
			s := &store{value: record()}
			calls := 0
			server := newServer(s, quotes(func(ctx context.Context, pair string) (*market.LatestResponse, error) {
				calls++
				if calls > 1 {
					t.Error("replay reads price")
				}
				if pair != "BTCUSDT" {
					t.Error(pair)
				}
				return quote(value), nil
			}), func() time.Time { return now })
			r, e := server.AdmitDelivery(context.Background(), request())
			if e != nil {
				t.Fatal(e)
			}
			if value == "99" {
				if r.Outcome != "wait_condition" || r.Attempt != nil {
					t.Fatal(r)
				}
			} else if r.Outcome != "admitted" || !proto.Equal(r.Attempt.Quote, quote(value).Quote) || r.Attempt.ChatId != 123 || r.Attempt.ExpiresAt != now.Add(time.Minute).Format(time.RFC3339Nano) {
				t.Fatal(r)
			}
			again, e := server.AdmitDelivery(context.Background(), request())
			if e != nil || !proto.Equal(again, r) || s.writes != 1 || calls != 1 {
				t.Fatal(again, e, s.writes, calls)
			}
		})
	}
}
func TestRebindingReadsPriceAgain(t *testing.T) {
	s := &store{value: record(), change: true}
	calls := 0
	server := newServer(s, quotes(func(context.Context, string) (*market.LatestResponse, error) {
		calls++
		if calls == 1 {
			return quote("101"), nil
		}
		return quote("99"), nil
	}), func() time.Time { return now })
	r, e := server.AdmitDelivery(context.Background(), request())
	if e != nil || r.Outcome != "wait_condition" || r.Attempt != nil || calls != 2 || s.writes != 1 {
		t.Fatal(r, e, calls)
	}
}
func TestInvalidQuoteAndDependencies(t *testing.T) {
	for _, mode := range []string{"missing", "future", "pair", "revision", "price", "received", "quote_error", "read_error", "write_error", "not_found", "workflow"} {
		t.Run(mode, func(t *testing.T) {
			s := &store{value: record()}
			q := quote("101")
			var qe error
			want := codes.Unavailable
			switch mode {
			case "missing":
				q.Quote = nil
			case "future":
				q.Quote.TradeTime = now.Add(time.Second).Format(time.RFC3339Nano)
			case "pair":
				q.Quote.Pair = "ETHUSDT"
			case "revision":
				q.Quote.Revision = nil
			case "price":
				q.Quote.Price = "0"
			case "received":
				q.Quote.ReceivedAt = "bad"
			case "quote_error":
				qe = errors.New("secret")
			case "read_error":
				s.readErr = errors.New("secret")
			case "write_error":
				s.writeErr = errors.New("secret")
			case "not_found":
				s.readErr = sub.ErrNotFound
				want = codes.NotFound
			case "workflow":
				s.readErr = sub.ErrAdmissionRejected
				want = codes.FailedPrecondition
			}
			r, e := newServer(s, quotes(func(context.Context, string) (*market.LatestResponse, error) { return q, qe }), func() time.Time { return now }).AdmitDelivery(context.Background(), request())
			if r != nil || status.Code(e) != want || status.Convert(e).Message() == "secret" || s.writes != 0 {
				t.Fatal(r, e, s.writes)
			}
		})
	}
}
func TestInvalidRequestsAndConfig(t *testing.T) {
	s := newServer(&store{readErr: errors.New("must not read")}, nil, nil)
	for _, req := range []*pb.AdmitDeliveryRequest{nil, {}, {OwnerId: ownerID, SubscriptionId: subID, WorkflowId: "bad", RequestId: "check"}, {OwnerId: ownerID, SubscriptionId: subID, WorkflowId: workflowID, RequestId: " "}} {
		if _, e := s.AdmitDelivery(context.Background(), req); status.Code(e) != codes.InvalidArgument {
			t.Fatal(e)
		}
	}
	for _, ttl := range []time.Duration{0, time.Hour, time.Nanosecond} {
		if _, e := admissionrpc.NewServer(nil, nil, ttl, nil); e == nil {
			t.Fatal(ttl)
		}
	}
}
