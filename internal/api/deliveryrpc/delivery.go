// Package deliveryrpc exposes API-owned delivery work to authenticated matcher.
package deliveryrpc

import (
	"context"
	"errors"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/quotecheck"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Budget includes canonical read, latest-price RPC (itself capped at 1s), and
// atomic commit. This initial operation budget is not a measured latency SLA.
const Budget = 3 * time.Second

// Store belongs exclusively to API. Atomic RequestDelivery must recheck the
// version, state and condition, and recover an existing workflow before CAS.
type Store interface {
	ByID(context.Context, string, string) (sub.Record, error)
	RequestDelivery(context.Context, sub.DeliveryCandidate) (sub.Record, error)
}
type Database interface {
	account.DB
	sub.Beginner
}
type Postgres struct{ db Database }

func NewPostgres(db Database) *Postgres { return &Postgres{db: db} }
func (p *Postgres) ByID(ctx context.Context, owner, id string) (sub.Record, error) {
	return sub.New(p.db).ByID(ctx, owner, id)
}
func (p *Postgres) RequestDelivery(ctx context.Context, c sub.DeliveryCandidate) (sub.Record, error) {
	return sub.RequestDelivery(ctx, p.db, c)
}

type Server struct {
	pb.UnimplementedDeliveryServer
	store  Store
	quotes sub.QuoteSource
	now    func() time.Time
}

func NewServer(store Store, quotes sub.QuoteSource, now func() time.Time) *Server {
	if now == nil {
		now = time.Now
	}
	return &Server{store: store, quotes: quotes, now: now}
}

// Install all options together. Credentials and identity come from trusted
// configuration; metadata and public Bearer sessions never authorize this RPC.
func ServerOptions(cert, key, clientCA []byte, matcherURI string) ([]grpc.ServerOption, error) {
	return serviceauth.ServerOptions(cert, key, clientCA, map[string][]string{matcherURI: {pb.Delivery_RequestDelivery_FullMethodName}})
}
func (s *Server) RequestDelivery(ctx context.Context, req *pb.RequestDeliveryRequest) (*pb.RequestDeliveryResponse, error) {
	if req == nil || !validID(req.OwnerId) || !validID(req.SubscriptionId) || req.ExpectedVersion < 1 {
		return nil, status.Error(codes.InvalidArgument, "owner, subscription and positive version required")
	}
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	r, err := s.store.ByID(ctx, req.OwnerId, req.SubscriptionId)
	if err != nil || ctx.Err() != nil {
		return nil, rpcError(ctx, err)
	}
	// Recovery must not depend on price availability or the now advanced version.
	if r.Workflow != nil {
		return response(r), nil
	}
	if r.Version != req.ExpectedVersion {
		return nil, status.Error(codes.Aborted, "subscription version changed")
	}
	if r.State != domain.Active {
		return nil, status.Error(codes.FailedPrecondition, "subscription is not active")
	}
	latest, err := s.quotes.Latest(ctx, r.Pair)
	if err != nil || ctx.Err() != nil {
		return nil, rpcError(ctx, err)
	}
	q, ok := quotecheck.Observation(latest, r.Pair, s.now())
	if !ok {
		return nil, status.Error(codes.Unavailable, "quote unavailable")
	}
	if !price.Satisfied(*q.Price, r.Threshold, r.Direction) {
		return nil, status.Error(codes.FailedPrecondition, "condition not satisfied")
	}
	// No external I/O is performed under the transaction's owner/row locks.
	r, err = s.store.RequestDelivery(ctx, sub.DeliveryCandidate{OwnerID: req.OwnerId, SubscriptionID: req.SubscriptionId, ExpectedVersion: req.ExpectedVersion, Quote: q})
	if errors.Is(err, sub.ErrCandidateRejected) && ctx.Err() == nil {
		if r.Version != req.ExpectedVersion {
			return nil, status.Error(codes.Aborted, "subscription version changed")
		}
		return nil, status.Error(codes.FailedPrecondition, "candidate rejected")
	}
	if err != nil || ctx.Err() != nil {
		return nil, rpcError(ctx, err)
	}
	return response(r), nil
}
func validID(s string) bool {
	var id pgtype.UUID
	return id.Scan(s) == nil && id.Valid
}
func response(r sub.Record) *pb.RequestDeliveryResponse {
	out := &pb.RequestDeliveryResponse{SubscriptionId: r.ID, SubscriptionVersion: r.Version, State: string(r.State)}
	if r.Workflow != nil {
		out.Workflow = &pb.Workflow{TriggerId: r.Workflow.TriggerID, Version: r.Workflow.Version}
	}
	return out
}
func rpcError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	if status.Code(err) == codes.Canceled || status.Code(err) == codes.DeadlineExceeded {
		return status.Error(status.Code(err), "dependency request interrupted")
	}
	if errors.Is(err, sub.ErrNotFound) {
		return status.Error(codes.NotFound, "subscription not found")
	}
	return status.Error(codes.Unavailable, "delivery dependency unavailable")
}

type Client struct{ rpc pb.DeliveryClient }

// NewClient accepts a caller-owned mTLS connection; it does not add retries.
func NewClient(conn grpc.ClientConnInterface) *Client {
	return &Client{rpc: pb.NewDeliveryClient(conn)}
}
func (c *Client) RequestDelivery(ctx context.Context, owner, id string, version int64) (*pb.RequestDeliveryResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	return c.rpc.RequestDelivery(ctx, &pb.RequestDeliveryRequest{OwnerId: owner, SubscriptionId: id, ExpectedVersion: version})
}
