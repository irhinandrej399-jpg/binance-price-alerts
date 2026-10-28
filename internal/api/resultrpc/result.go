// Package resultrpc exposes durable delivery observations to notifications.
package resultrpc

import (
	"context"
	"errors"
	"time"

	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Budget bounds one storage transaction, not a measured latency SLA.
const Budget = 3 * time.Second

type Store interface {
	SaveDeliveryResult(context.Context, sub.DeliveryResultCommand) (sub.Record, error)
}
type Postgres struct{ db sub.Beginner }

func NewPostgres(db sub.Beginner) *Postgres { return &Postgres{db} }
func (p *Postgres) SaveDeliveryResult(ctx context.Context, c sub.DeliveryResultCommand) (sub.Record, error) {
	return sub.SaveDeliveryResult(ctx, p.db, c)
}

type Server struct {
	pb.UnimplementedDeliveryResultsServer
	store Store
}

func NewServer(store Store) *Server { return &Server{store: store} }

// Install credentials and method policy together with RPC registration.
func ServerOptions(cert, key, clientCA []byte, notificationsURI string) ([]grpc.ServerOption, error) {
	return serviceauth.ServerOptions(cert, key, clientCA, map[string][]string{notificationsURI: {pb.DeliveryResults_SaveDeliveryResult_FullMethodName}})
}
func (s *Server) SaveDeliveryResult(ctx context.Context, req *pb.SaveDeliveryResultRequest) (*pb.SaveDeliveryResultResponse, error) {
	if !validRequest(req) {
		return nil, status.Error(codes.InvalidArgument, "valid identities and delivery observation required")
	}
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	r, err := s.store.SaveDeliveryResult(ctx, sub.DeliveryResultCommand{OwnerID: req.OwnerId, SubscriptionID: req.SubscriptionId, WorkflowID: req.WorkflowId, AttemptID: req.AttemptId, Outcome: sub.DeliveryOutcome(req.Outcome), MessageID: req.MessageId, Reason: domain.BlockReason(req.BlockReason)})
	if err != nil || ctx.Err() != nil {
		return nil, rpcError(ctx, err)
	}
	out := &pb.SaveDeliveryResultResponse{SubscriptionId: r.ID, SubscriptionVersion: r.Version, State: string(r.State)}
	if r.Workflow != nil {
		out.WorkflowId = r.Workflow.TriggerID
	}
	if r.Blocked != nil {
		out.BlockReason = string(r.Blocked.Reason)
		out.BlockedBindingVersion = r.Blocked.BindingVersion
	}
	return out, nil
}
func validID(s string) bool { var id pgtype.UUID; return id.Scan(s) == nil && id.Valid }
func validRequest(r *pb.SaveDeliveryResultRequest) bool {
	if r == nil || !validID(r.OwnerId) || !validID(r.SubscriptionId) || !validID(r.WorkflowId) || !validID(r.AttemptId) {
		return false
	}
	switch sub.DeliveryOutcome(r.Outcome) {
	case sub.DeliverySucceeded:
		return r.MessageId > 0 && r.BlockReason == ""
	case sub.DeliveryRetryable, sub.DeliveryUncertain:
		return r.MessageId == 0 && r.BlockReason == ""
	case sub.DeliveryFailed:
		return r.MessageId == 0 && (r.BlockReason == string(domain.ChannelUnavailable) || r.BlockReason == string(domain.AttemptsExhausted) || r.BlockReason == string(domain.PermanentFailure))
	default:
		return false
	}
}
func rpcError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	switch {
	case errors.Is(err, sub.ErrNotFound):
		return status.Error(codes.NotFound, "subscription not found")
	case errors.Is(err, sub.ErrAttemptUnknown):
		return status.Error(codes.FailedPrecondition, "attempt not known for workflow")
	case errors.Is(err, sub.ErrResultConflict):
		return status.Error(codes.AlreadyExists, "conflicting delivery observation")
	case errors.Is(err, sub.ErrInvalidInput):
		return status.Error(codes.InvalidArgument, "invalid delivery observation")
	default:
		return status.Error(codes.Unavailable, "result storage unavailable")
	}
}

type Client struct{ rpc pb.DeliveryResultsClient }

// NewClient uses caller-owned mTLS. No automatic retries. After any ambiguous
// error, repeat the saved observation with the same attempt; never resend Telegram.
func NewClient(conn grpc.ClientConnInterface) *Client {
	return &Client{pb.NewDeliveryResultsClient(conn)}
}
func (c *Client) SaveDeliveryResult(ctx context.Context, req *pb.SaveDeliveryResultRequest) (*pb.SaveDeliveryResultResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	return c.rpc.SaveDeliveryResult(ctx, req)
}
