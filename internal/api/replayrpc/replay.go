// Package replayrpc exposes API-owned SQL replay to authenticated matcher consumers.
package replayrpc

import (
	"context"
	"errors"
	"math"
	"regexp"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/journal"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	changed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/subscriptionchanged"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const Budget = 3 * time.Second

// MaxPageBytes bounds encoded event payload plus per-event protobuf overhead.
// Whole transactions larger than this must be handled by an explicit policy;
// the adapter never splits or silently skips them.
const MaxPageBytes = 3 << 20

var uuid = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

type Pins interface {
	Handoff(context.Context, string, string, time.Duration) (sub.ReplayPin, error)
	CheckPin(context.Context, string, string) (sub.ReplayPin, error)
	RecordIssued(context.Context, string, string, uint64) (sub.ReplayPin, error)
	Acknowledge(context.Context, string, string, uint64) (sub.ReplayPin, error)
	Abandon(context.Context, string, string) error
}
type Server struct {
	pb.UnimplementedSubscriptionReplayServer
	pins    Pins
	journal journal.Reader
}

func NewServer(pins Pins, db journal.Reader) *Server { return &Server{pins: pins, journal: db} }
func ServerOptions(cert, key, ca []byte, matcherURI string) ([]grpc.ServerOption, error) {
	return serviceauth.ServerOptions(cert, key, ca, map[string][]string{matcherURI: {pb.SubscriptionReplay_BeginReplay_FullMethodName, pb.SubscriptionReplay_ReadReplayPage_FullMethodName, pb.SubscriptionReplay_AcknowledgeReplay_FullMethodName, pb.SubscriptionReplay_AbandonReplay_FullMethodName}})
}
func owner(ctx context.Context) (string, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "service identity required")
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || !info.State.HandshakeComplete || len(info.State.VerifiedChains) == 0 || len(info.State.VerifiedChains[0]) == 0 {
		return "", status.Error(codes.Unauthenticated, "service identity required")
	}
	uris := info.State.VerifiedChains[0][0].URIs
	if len(uris) != 1 || len(uris[0].String()) > 200 {
		return "", status.Error(codes.Unauthenticated, "service identity required")
	}
	return uris[0].String(), nil
}

func rpcError(ctx context.Context, e error) error {
	if ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}
	if errors.Is(e, context.Canceled) || errors.Is(e, context.DeadlineExceeded) {
		return status.FromContextError(e).Err()
	}
	if errors.Is(e, sub.ErrSnapshotLost) {
		return status.Error(codes.FailedPrecondition, "replay resource lost; start a new snapshot")
	}
	if errors.Is(e, sub.ErrInvalidInput) || errors.Is(e, journal.ErrInvalidInput) {
		return status.Error(codes.InvalidArgument, "invalid replay boundary or resource request")
	}
	return status.Error(codes.Unavailable, "replay storage unavailable")
}
func invalid() error {
	return status.Error(codes.InvalidArgument, "valid resource ID and replay parameters required")
}
func state(p sub.ReplayPin, h uint64) *pb.ReplayState {
	// A concurrent read may have issued a newer committed whole boundary.
	h = max(h, p.Issued)
	return &pb.ReplayState{ResourceId: p.ID, Progress: p.Progress, Issued: p.Issued, Head: h, ExpiresAt: p.ExpiresAt.UTC().Format(time.RFC3339Nano)}
}
func (s *Server) response(ctx context.Context, o, id string, h uint64) (*pb.ReplayState, error) {
	p, e := s.pins.CheckPin(ctx, o, id)
	if e != nil || ctx.Err() != nil {
		return nil, rpcError(ctx, e)
	}
	if p.Phase != "replay" {
		return nil, invalid()
	}
	if !time.Now().Before(p.ExpiresAt) {
		return nil, rpcError(ctx, sub.ErrSnapshotLost)
	}
	return state(p, h), nil
}
func (s *Server) BeginReplay(ctx context.Context, r *pb.BeginReplayRequest) (*pb.ReplayState, error) {
	o, e := owner(ctx)
	if e != nil {
		return nil, e
	}
	if r == nil || !uuid.MatchString(r.SnapshotId) || r.TtlMillis < 1 || r.TtlMillis > 86400000 {
		return nil, invalid()
	}
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	if _, e = s.pins.Handoff(ctx, o, r.SnapshotId, time.Duration(r.TtlMillis)*time.Millisecond); e != nil {
		return nil, rpcError(ctx, e)
	}
	h, e := journal.Current(ctx, s.journal)
	if e != nil {
		return nil, rpcError(ctx, e)
	}
	return s.response(ctx, o, r.SnapshotId, h)
}
func (s *Server) ReadReplayPage(ctx context.Context, r *pb.ReadReplayPageRequest) (*pb.ReplayPage, error) {
	o, e := owner(ctx)
	if e != nil {
		return nil, e
	}
	if r == nil || !uuid.MatchString(r.ResourceId) || r.After > math.MaxInt64 || r.MaxTransactions < 1 || r.MaxTransactions > 1000 {
		return nil, invalid()
	}
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	p, e := s.pins.CheckPin(ctx, o, r.ResourceId)
	if e != nil {
		return nil, rpcError(ctx, e)
	}
	if p.Phase != "replay" || r.After < p.Progress || r.After > p.Issued {
		return nil, invalid()
	}
	// Pin retains everything strictly after progress. Observe H before reading;
	// commits arriving during ReadAfter are left for the next call.
	h, e := journal.Current(ctx, s.journal)
	if e != nil {
		return nil, rpcError(ctx, e)
	}
	entries, e := journal.ReadAfter(ctx, s.journal, r.After, int(r.MaxTransactions))
	if e != nil {
		return nil, rpcError(ctx, e)
	}
	out := &pb.ReplayPage{Next: r.After}
	used := 0
	for i := 0; i < len(entries); {
		end := entries[i].Change.TransactionEnd
		if end > h {
			break
		}
		batch := make([][]byte, 0)
		size := 0
		for i < len(entries) && entries[i].Change.TransactionEnd == end {
			if ctx.Err() != nil {
				return nil, rpcError(ctx, ctx.Err())
			}
			body, e := changed.Encode(entries[i].Metadata, entries[i].Change)
			if e != nil {
				return nil, rpcError(ctx, e)
			}
			size += len(body) + 5 // tag and length prefix, at most four bytes below 3 MiB.
			if size > MaxPageBytes-used {
				break
			}
			batch = append(batch, body)
			i++
		}
		if size > MaxPageBytes-used {
			if len(out.Events) == 0 {
				return nil, status.Error(codes.ResourceExhausted, "whole replay transaction exceeds page byte budget")
			}
			break
		}
		out.Events = append(out.Events, batch...)
		used += size
		out.Next = end
	}
	// Never trust a client-supplied issued boundary. Only the actual returned
	// complete prefix advances issued; a lost response is safely repeatable.
	if _, e = s.pins.RecordIssued(ctx, o, r.ResourceId, out.Next); e != nil {
		return nil, rpcError(ctx, e)
	}
	out.State, e = s.response(ctx, o, r.ResourceId, h)
	if e != nil {
		return nil, e
	}
	// Concurrent durable acknowledgement can release the old prefix to cleaner
	// while ReadAfter is reading it. Reject a stale page instead of returning a
	// potentially incomplete prefix. The consumer resumes at durable progress.
	if out.State.Progress > r.After {
		return nil, invalid()
	}
	return out, nil
}
func (s *Server) AcknowledgeReplay(ctx context.Context, r *pb.AcknowledgeReplayRequest) (*pb.ReplayState, error) {
	o, e := owner(ctx)
	if e != nil {
		return nil, e
	}
	if r == nil || !uuid.MatchString(r.ResourceId) || r.Boundary > math.MaxInt64 {
		return nil, invalid()
	}
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	if _, e = s.pins.Acknowledge(ctx, o, r.ResourceId, r.Boundary); e != nil {
		return nil, rpcError(ctx, e)
	}
	h, e := journal.Current(ctx, s.journal)
	if e != nil {
		return nil, rpcError(ctx, e)
	}
	return s.response(ctx, o, r.ResourceId, h)
}
func (s *Server) AbandonReplay(ctx context.Context, r *pb.AbandonReplayRequest) (*pb.AbandonReplayResponse, error) {
	o, e := owner(ctx)
	if e != nil {
		return nil, e
	}
	if r == nil || !uuid.MatchString(r.ResourceId) {
		return nil, invalid()
	}
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	if e = s.pins.Abandon(ctx, o, r.ResourceId); e != nil || ctx.Err() != nil {
		return nil, rpcError(ctx, e)
	}
	return &pb.AbandonReplayResponse{}, nil
}

type Client struct{ rpc pb.SubscriptionReplayClient }

// NewClient uses caller-owned mTLS and does not add automatic retries.
func NewClient(conn grpc.ClientConnInterface) *Client {
	return &Client{pb.NewSubscriptionReplayClient(conn)}
}
func (c *Client) BeginReplay(ctx context.Context, r *pb.BeginReplayRequest) (*pb.ReplayState, error) {
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	return c.rpc.BeginReplay(ctx, r)
}
func (c *Client) ReadReplayPage(ctx context.Context, r *pb.ReadReplayPageRequest) (*pb.ReplayPage, error) {
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	return c.rpc.ReadReplayPage(ctx, r)
}
func (c *Client) AcknowledgeReplay(ctx context.Context, r *pb.AcknowledgeReplayRequest) (*pb.ReplayState, error) {
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	return c.rpc.AcknowledgeReplay(ctx, r)
}
func (c *Client) AbandonReplay(ctx context.Context, r *pb.AbandonReplayRequest) (*pb.AbandonReplayResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	return c.rpc.AbandonReplay(ctx, r)
}
