package resultrpc_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/resultrpc"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const ownerID = "00000000-0000-4000-8000-000000000001"
const subID = "00000000-0000-4000-8000-000000000002"
const workflowID = "00000000-0000-4000-8000-000000000003"
const attemptID = "00000000-0000-4000-8000-000000000004"

func request() *pb.SaveDeliveryResultRequest {
	return &pb.SaveDeliveryResultRequest{OwnerId: ownerID, SubscriptionId: subID, WorkflowId: workflowID, AttemptId: attemptID, Outcome: "succeeded", MessageId: 123}
}

type storeFunc func(context.Context, sub.DeliveryResultCommand) (sub.Record, error)

func (f storeFunc) SaveDeliveryResult(ctx context.Context, c sub.DeliveryResultCommand) (sub.Record, error) {
	return f(ctx, c)
}
func record() sub.Record {
	return sub.Record{ID: subID, OwnerID: ownerID, Version: 4, State: domain.Delivered, Workflow: &sub.Workflow{TriggerID: workflowID, Version: 2}}
}
func TestSaveResult(t *testing.T) {
	for _, outcome := range []string{"succeeded", "retryable", "uncertain", "blocked"} {
		t.Run(outcome, func(t *testing.T) {
			req := request()
			req.Outcome = outcome
			if outcome != "succeeded" {
				req.MessageId = 0
			}
			if outcome == "blocked" {
				req.BlockReason = string(domain.PermanentFailure)
			}
			calls := 0
			r := record()
			if outcome == "blocked" {
				r.State = domain.DeliveryBlocked
				r.Blocked = &sub.Blocked{Reason: domain.PermanentFailure, BindingVersion: 7}
			}
			server := resultrpc.NewServer(storeFunc(func(ctx context.Context, c sub.DeliveryResultCommand) (sub.Record, error) {
				calls++
				want := sub.DeliveryResultCommand{OwnerID: ownerID, SubscriptionID: subID, WorkflowID: workflowID, AttemptID: attemptID, Outcome: sub.DeliveryOutcome(outcome), MessageID: req.MessageId, Reason: domain.BlockReason(req.BlockReason)}
				if !reflect.DeepEqual(c, want) {
					t.Fatalf("command: %+v", c)
				}
				return r, nil
			}))
			got, e := server.SaveDeliveryResult(context.Background(), req)
			if e != nil || calls != 1 {
				t.Fatal(got, e, calls)
			}
			want := &pb.SaveDeliveryResultResponse{SubscriptionId: subID, SubscriptionVersion: 4, State: string(r.State), WorkflowId: workflowID}
			if r.Blocked != nil {
				want.BlockReason = string(r.Blocked.Reason)
				want.BlockedBindingVersion = 7
			}
			if !proto.Equal(got, want) {
				t.Fatal(got, want)
			}
		})
	}
}
func TestInvalidInput(t *testing.T) {
	server := resultrpc.NewServer(storeFunc(func(context.Context, sub.DeliveryResultCommand) (sub.Record, error) {
		t.Error("invalid reached storage")
		return sub.Record{}, nil
	}))
	mutations := []func(*pb.SaveDeliveryResultRequest){
		func(r *pb.SaveDeliveryResultRequest) { r.OwnerId = "" }, func(r *pb.SaveDeliveryResultRequest) { r.SubscriptionId = "bad" }, func(r *pb.SaveDeliveryResultRequest) { r.WorkflowId = "bad" }, func(r *pb.SaveDeliveryResultRequest) { r.AttemptId = "bad" },
		func(r *pb.SaveDeliveryResultRequest) { r.Outcome = "" }, func(r *pb.SaveDeliveryResultRequest) { r.Outcome = "invalid" }, func(r *pb.SaveDeliveryResultRequest) { r.MessageId = 0 }, func(r *pb.SaveDeliveryResultRequest) { r.MessageId = -1 }, func(r *pb.SaveDeliveryResultRequest) { r.BlockReason = "permanent_failure" },
		func(r *pb.SaveDeliveryResultRequest) { r.Outcome = "retryable" }, func(r *pb.SaveDeliveryResultRequest) {
			r.Outcome = "uncertain"
			r.MessageId = 0
			r.BlockReason = "channel_unavailable"
		}, func(r *pb.SaveDeliveryResultRequest) {
			r.Outcome = "blocked"
			r.MessageId = 0
			r.BlockReason = "invalid"
		}, func(r *pb.SaveDeliveryResultRequest) { r.Outcome = "blocked"; r.MessageId = 0 },
	}
	for _, mutate := range mutations {
		r := request()
		mutate(r)
		if _, e := server.SaveDeliveryResult(context.Background(), r); status.Code(e) != codes.InvalidArgument {
			t.Fatal(r, e)
		}
	}
	if _, e := server.SaveDeliveryResult(context.Background(), nil); status.Code(e) != codes.InvalidArgument {
		t.Fatal(e)
	}
	for _, reason := range []domain.BlockReason{domain.ChannelUnavailable, domain.AttemptsExhausted, domain.PermanentFailure} {
		r := request()
		r.Outcome = "blocked"
		r.MessageId = 0
		r.BlockReason = string(reason)
		s := resultrpc.NewServer(storeFunc(func(context.Context, sub.DeliveryResultCommand) (sub.Record, error) { return record(), nil }))
		if _, e := s.SaveDeliveryResult(context.Background(), r); e != nil {
			t.Fatal(e)
		}
	}
}
func TestErrors(t *testing.T) {
	for _, tc := range []struct {
		e    error
		code codes.Code
	}{{sub.ErrNotFound, codes.NotFound}, {sub.ErrAttemptUnknown, codes.FailedPrecondition}, {sub.ErrResultConflict, codes.AlreadyExists}, {sub.ErrInvalidInput, codes.InvalidArgument}, {errors.New("secret SQL"), codes.Unavailable}, {context.Canceled, codes.Canceled}, {context.DeadlineExceeded, codes.DeadlineExceeded}} {
		s := resultrpc.NewServer(storeFunc(func(context.Context, sub.DeliveryResultCommand) (sub.Record, error) { return sub.Record{}, tc.e }))
		if _, e := s.SaveDeliveryResult(context.Background(), request()); status.Code(e) != tc.code || strings.Contains(e.Error(), "secret") {
			t.Fatal(e, tc.code)
		}
	}
}
