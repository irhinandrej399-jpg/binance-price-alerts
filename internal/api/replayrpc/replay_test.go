package replayrpc_test

import (
	"context"
	"testing"

	rpc "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/replayrpc"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/grpctest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const matcherURI = "spiffe://binance.test/matcher"
const resourceID = "00000000-0000-4000-8000-000000000001"

func connect(t *testing.T, s *rpc.Server, id string, extra ...grpc.ServerOption) *grpc.ClientConn {
	t.Helper()
	ca := newAuthority(t)
	server := ca.issue(t, nil, true, false)
	client := ca.issue(t, []string{id}, false, false)
	opts, e := rpc.ServerOptions(server.cert, server.key, ca.pem, matcherURI)
	if e != nil {
		t.Fatal(e)
	}
	return grpctest.StartWithClientOptions(t, func(g *grpc.Server) { pb.RegisterSubscriptionReplayServer(g, s) }, []grpc.DialOption{grpc.WithTransportCredentials(clientCreds(t, client, ca.pem)), grpc.WithAuthority("market.test")}, append(opts, extra...)...)
}

// Unauthorized and malformed calls must be rejected before touching dependencies.
func TestRightsAndValidation(t *testing.T) {
	s := rpc.NewServer(nil, nil)
	c := rpc.NewClient(connect(t, s, "spiffe://binance.test/notifications"))
	if _, e := c.BeginReplay(context.Background(), &pb.BeginReplayRequest{SnapshotId: resourceID, TtlMillis: 1000}); status.Code(e) != codes.PermissionDenied {
		t.Fatal(e)
	}
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer user-session", "consumer-id", matcherURI))
	if _, e := c.ReadReplayPage(ctx, &pb.ReadReplayPageRequest{ResourceId: resourceID, MaxTransactions: 1}); status.Code(e) != codes.PermissionDenied {
		t.Fatal(e)
	}
	if _, e := c.AcknowledgeReplay(ctx, &pb.AcknowledgeReplayRequest{ResourceId: resourceID}); status.Code(e) != codes.PermissionDenied {
		t.Fatal(e)
	}
	if _, e := c.AbandonReplay(ctx, &pb.AbandonReplayRequest{ResourceId: resourceID}); status.Code(e) != codes.PermissionDenied {
		t.Fatal(e)
	}
	c = rpc.NewClient(connect(t, s, matcherURI))
	if _, e := c.BeginReplay(context.Background(), &pb.BeginReplayRequest{SnapshotId: resourceID}); status.Code(e) != codes.InvalidArgument {
		t.Fatal(e)
	}
	if _, e := c.ReadReplayPage(context.Background(), &pb.ReadReplayPageRequest{ResourceId: resourceID, MaxTransactions: 1001}); status.Code(e) != codes.InvalidArgument {
		t.Fatal(e)
	}
	if _, e := c.AcknowledgeReplay(context.Background(), &pb.AcknowledgeReplayRequest{ResourceId: "bad"}); status.Code(e) != codes.InvalidArgument {
		t.Fatal(e)
	}
}
