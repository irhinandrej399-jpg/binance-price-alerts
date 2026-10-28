// Package admissionrpc exposes bounded API-owned attempt admission to notifications.
package admissionrpc

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/quotecheck"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Budget covers preparation, nested Latest (1s), binding retries and commit.
// It is an initial operation limit, not a measured latency SLA.
const Budget = 3 * time.Second

type Store interface {
	PrepareAdmission(context.Context, sub.AdmissionCommand) (sub.AdmissionResult, error)
	AdmitDelivery(context.Context, sub.AdmissionCommand) (sub.AdmissionResult, error)
}
type Postgres struct{ db sub.Beginner }

func NewPostgres(db sub.Beginner) *Postgres { return &Postgres{db} }
func (p *Postgres) PrepareAdmission(ctx context.Context, c sub.AdmissionCommand) (sub.AdmissionResult, error) {
	return sub.PrepareAdmission(ctx, p.db, c)
}
func (p *Postgres) AdmitDelivery(ctx context.Context, c sub.AdmissionCommand) (sub.AdmissionResult, error) {
	return sub.AdmitDelivery(ctx, p.db, c)
}

type Server struct {
	pb.UnimplementedAdmissionServer
	store  Store
	quotes sub.QuoteSource
	ttl    time.Duration
	now    func() time.Time
}

// TTL is trusted service configuration, never caller input. No implicit retries
// of ambiguous commits are made; notifications must keep its original request ID.
func NewServer(store Store, quotes sub.QuoteSource, ttl time.Duration, now func() time.Time) (*Server, error) {
	if ttl < time.Microsecond || ttl > time.Minute || ttl%time.Microsecond != 0 {
		return nil, errors.New("invalid admission permit TTL")
	}
	if now == nil {
		now = time.Now
	}
	return &Server{store: store, quotes: quotes, ttl: ttl, now: now}, nil
}

// Install these credentials and policy together with the RPC registration.
func ServerOptions(cert, key, clientCA []byte, notificationsURI string) ([]grpc.ServerOption, error) {
	return serviceauth.ServerOptions(cert, key, clientCA, map[string][]string{notificationsURI: {pb.Admission_AdmitDelivery_FullMethodName}})
}
func (s *Server) AdmitDelivery(ctx context.Context, req *pb.AdmitDeliveryRequest) (*pb.AdmitDeliveryResponse, error) {
	if req == nil || !validID(req.OwnerId) || !validID(req.SubscriptionId) || !validID(req.WorkflowId) || strings.TrimSpace(req.RequestId) == "" || len(req.RequestId) > 200 {
		return nil, status.Error(codes.InvalidArgument, "owner, subscription, workflow and bounded request ID required")
	}
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	c := sub.AdmissionCommand{OwnerID: req.OwnerId, SubscriptionID: req.SubscriptionId, WorkflowID: req.WorkflowId, RequestID: req.RequestId, PermitTTL: s.ttl}
	for {
		r, err := s.store.PrepareAdmission(ctx, c)
		if ctx.Err() != nil {
			return nil, rpcError(ctx, ctx.Err())
		}
		if err == nil {
			return response(r), nil
		}
		var need *sub.AdmissionQuoteRequired
		if !errors.As(err, &need) {
			return nil, rpcError(ctx, err)
		}
		latest, err := s.quotes.Latest(ctx, need.Record.Pair)
		if err != nil || ctx.Err() != nil {
			return nil, rpcError(ctx, err)
		}
		q, ok := quotecheck.Observation(latest, need.Record.Pair, s.now())
		if !ok {
			return nil, status.Error(codes.Unavailable, "quote unavailable")
		}
		c.Quote = q
		c.ExpectedBindingVersion = &need.BindingVersion
		r, err = s.store.AdmitDelivery(ctx, c)
		if errors.Is(err, sub.ErrAdmissionChanged) && ctx.Err() == nil {
			continue
		}
		if err != nil || ctx.Err() != nil {
			return nil, rpcError(ctx, err)
		}
		return response(r), nil
	}
}
func validID(s string) bool { var id pgtype.UUID; return id.Scan(s) == nil && id.Valid }
func rpcError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	if status.Code(err) == codes.Canceled || status.Code(err) == codes.DeadlineExceeded {
		return status.Error(status.Code(err), "dependency interrupted")
	}
	if errors.Is(err, sub.ErrNotFound) {
		return status.Error(codes.NotFound, "subscription not found")
	}
	if errors.Is(err, sub.ErrAdmissionRejected) {
		return status.Error(codes.FailedPrecondition, "workflow does not match subscription")
	}
	return status.Error(codes.Unavailable, "admission dependency unavailable")
}
func response(r sub.AdmissionResult) *pb.AdmitDeliveryResponse {
	out := &pb.AdmitDeliveryResponse{Outcome: string(r.Outcome), SubscriptionId: r.Record.ID, SubscriptionVersion: r.Record.Version, State: string(r.Record.State)}
	if r.Record.Workflow != nil {
		out.WorkflowId = r.Record.Workflow.TriggerID
	}
	if r.Record.Blocked != nil {
		out.BlockReason = string(r.Record.Blocked.Reason)
		out.BlockedBindingVersion = r.Record.Blocked.BindingVersion
	}
	if a := r.Attempt; a != nil {
		out.Attempt = &pb.AttemptPermission{AttemptId: a.ID, Fence: a.Fence, ExpiresAt: a.ExpiresAt.UTC().Format(time.RFC3339Nano), ChatId: a.ChatID, BindingVersion: a.BindingVersion, Quote: quotecheck.Proto(a.Quote)}
	}
	return out
}

type Client struct{ rpc pb.AdmissionClient }

// NewClient uses a caller-owned mTLS connection, caps deadlines and adds no retry.
func NewClient(conn grpc.ClientConnInterface) *Client { return &Client{pb.NewAdmissionClient(conn)} }
func (c *Client) AdmitDelivery(ctx context.Context, owner, id, workflow, request string) (*pb.AdmitDeliveryResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	return c.rpc.AdmitDelivery(ctx, &pb.AdmitDeliveryRequest{OwnerId: owner, SubscriptionId: id, WorkflowId: workflow, RequestId: request})
}
