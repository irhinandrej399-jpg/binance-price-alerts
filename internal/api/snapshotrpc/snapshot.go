// Package snapshotrpc exposes frozen API-owned subscription pages to matcher.
package snapshotrpc

import (
	"context"
	"encoding/base64"
	"errors"
	"regexp"
	"strings"
	"time"

	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Budget bounds materialization and page reading together, not a latency SLA.
const Budget = 3 * time.Second
const cursorPrefix = "subscription-snapshot-v1:"

var uuid = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
var requestID = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)

type Store interface {
	BuildRequest(context.Context, string, string, time.Duration) (sub.Snapshot, error)
	Page(context.Context, string, string, string, int) (sub.SnapshotPage, error)
}
type Server struct {
	pb.UnimplementedSubscriptionSnapshotsServer
	store Store
}

func NewServer(store Store) *Server { return &Server{store: store} }

// Install all options with registration. Only this certificate identity may call.
func ServerOptions(cert, key, clientCA []byte, matcherURI string) ([]grpc.ServerOption, error) {
	return serviceauth.ServerOptions(cert, key, clientCA, map[string][]string{matcherURI: {pb.SubscriptionSnapshots_BuildSnapshot_FullMethodName, pb.SubscriptionSnapshots_ReadSnapshotPage_FullMethodName}})
}

// owner is the certificate URI already authorized by serviceauth, never metadata.
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
func validSize(n int32) bool { return n >= 1 && n <= 1000 }
func (s *Server) BuildSnapshot(ctx context.Context, r *pb.BuildSnapshotRequest) (*pb.SnapshotPageResponse, error) {
	o, e := owner(ctx)
	if e != nil {
		return nil, e
	}
	if r == nil || !requestID.MatchString(r.RequestId) || r.TtlMillis < 1 || r.TtlMillis > 86400000 || !validSize(r.PageSize) {
		return nil, status.Error(codes.InvalidArgument, "valid request ID, TTL and page size required")
	}
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	snap, e := s.store.BuildRequest(ctx, o, r.RequestId, time.Duration(r.TtlMillis)*time.Millisecond)
	if e != nil || ctx.Err() != nil {
		return nil, rpcError(ctx, e)
	}
	return s.page(ctx, o, snap.ID, "", int(r.PageSize))
}
func (s *Server) ReadSnapshotPage(ctx context.Context, r *pb.ReadSnapshotPageRequest) (*pb.SnapshotPageResponse, error) {
	o, e := owner(ctx)
	if e != nil {
		return nil, e
	}
	if r == nil || !uuid.MatchString(r.SnapshotId) || !validSize(r.PageSize) {
		return nil, status.Error(codes.InvalidArgument, "valid snapshot ID and page size required")
	}
	after := ""
	if r.Cursor != "" {
		var valid bool
		after, valid = decodeCursor(r.SnapshotId, r.Cursor)
		if !valid {
			return nil, status.Error(codes.InvalidArgument, "invalid subscription snapshot cursor")
		}
	}
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	return s.page(ctx, o, r.SnapshotId, after, int(r.PageSize))
}
func decodeCursor(id, cursor string) (string, bool) {
	if len(cursor) > 160 || !strings.HasPrefix(cursor, cursorPrefix) {
		return "", false
	}
	raw, e := base64.RawURLEncoding.Strict().DecodeString(strings.TrimPrefix(cursor, cursorPrefix))
	if e != nil {
		return "", false
	}
	parts := strings.Split(string(raw), "/")
	if len(parts) != 2 || parts[0] != id || !uuid.MatchString(parts[1]) {
		return "", false
	}
	return parts[1], true
}
func (s *Server) page(ctx context.Context, o, id, after string, n int) (*pb.SnapshotPageResponse, error) {
	p, e := s.store.Page(ctx, o, id, after, n)
	if e != nil || ctx.Err() != nil {
		return nil, rpcError(ctx, e)
	}
	// Storage may have started reading just before expiry. Never report an
	// expired resource as a successful page, even when that page is empty.
	if !time.Now().Before(p.Snapshot.ExpiresAt) {
		return nil, rpcError(ctx, sub.ErrSnapshotLost)
	}
	out := &pb.SnapshotPageResponse{SnapshotId: p.Snapshot.ID, BoundaryC: p.Snapshot.C, ExpiresAt: p.Snapshot.ExpiresAt.UTC().Format(time.RFC3339Nano)}
	if p.Next != "" {
		out.NextCursor = cursorPrefix + base64.RawURLEncoding.EncodeToString([]byte(p.Snapshot.ID+"/"+p.Next))
	}
	for _, r := range p.Records {
		v := &pb.SnapshotSubscription{SubscriptionId: r.ID, OwnerId: r.OwnerID, Pair: r.Pair, Threshold: r.Threshold.String(), Direction: string(r.Direction), CreationPrice: r.CreationPrice.String(), CreationTradeTime: r.CreationTradeTime.UTC().Format(time.RFC3339Nano), Percent: r.Percent, Stale: r.Stale, StreamUnconfirmed: r.StreamUnconfirmed, GapDetected: r.GapDetected, State: string(r.State), Version: r.Version, Activation: r.Activation}
		if r.Workflow != nil {
			v.Workflow = &pb.SnapshotWorkflow{TriggerId: r.Workflow.TriggerID, Version: r.Workflow.Version}
		}
		if r.Blocked != nil {
			v.Blocked = &pb.SnapshotBlocked{Reason: string(r.Blocked.Reason), BindingVersion: r.Blocked.BindingVersion}
		}
		out.Subscriptions = append(out.Subscriptions, v)
	}
	return out, nil
}
func rpcError(ctx context.Context, e error) error {
	if ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}
	if errors.Is(e, context.Canceled) || errors.Is(e, context.DeadlineExceeded) {
		return status.FromContextError(e).Err()
	}
	switch {
	case errors.Is(e, sub.ErrSnapshotLost):
		return status.Error(codes.FailedPrecondition, "snapshot unavailable; restart bootstrap with new request ID")
	case errors.Is(e, sub.ErrSnapshotRequestConflict):
		return status.Error(codes.AlreadyExists, "snapshot request parameters differ")
	case errors.Is(e, sub.ErrInvalidInput):
		return status.Error(codes.InvalidArgument, "invalid snapshot request or cursor")
	default:
		return status.Error(codes.Unavailable, "snapshot storage unavailable")
	}
}

type Client struct {
	rpc pb.SubscriptionSnapshotsClient
}

// NewClient uses caller-owned mTLS. No automatic retries; after an ambiguous
// BuildSnapshot error repeat the same request ID and TTL, including on reconnect.
func NewClient(conn grpc.ClientConnInterface) *Client {
	return &Client{pb.NewSubscriptionSnapshotsClient(conn)}
}
func (c *Client) BuildSnapshot(ctx context.Context, r *pb.BuildSnapshotRequest) (*pb.SnapshotPageResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	return c.rpc.BuildSnapshot(ctx, r)
}
func (c *Client) ReadSnapshotPage(ctx context.Context, r *pb.ReadSnapshotPageRequest) (*pb.SnapshotPageResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	return c.rpc.ReadSnapshotPage(ctx, r)
}
