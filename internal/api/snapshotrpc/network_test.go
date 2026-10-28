package snapshotrpc_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"testing"
	"time"

	rpc "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/snapshotrpc"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/grpctest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestRejectPageExpiredDuringRead(t *testing.T) {
	expired := snap
	expired.ExpiresAt = time.Now().Add(-time.Second)
	s := store{page: func(context.Context, string, string, string, int) (sub.SnapshotPage, error) {
		return sub.SnapshotPage{Snapshot: expired}, nil
	}}
	c := rpc.NewClient(connect(t, rpc.NewServer(s), matcherURI))
	if p, e := c.ReadSnapshotPage(context.Background(), &pb.ReadSnapshotPageRequest{SnapshotId: snapshotID, PageSize: 1}); status.Code(e) != codes.FailedPrecondition {
		t.Fatal("expired page returned", p, e)
	}
}
func TestMissingCertificateAndUnavailableAPI(t *testing.T) {
	ca := newAuthority(t)
	cert := ca.issue(t, nil, true, false)
	opts, e := rpc.ServerOptions(cert.cert, cert.key, ca.pem, matcherURI)
	if e != nil {
		t.Fatal(e)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	noCert := credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots})
	conn := grpctest.StartWithClientOptions(t, func(g *grpc.Server) { pb.RegisterSubscriptionSnapshotsServer(g, rpc.NewServer(store{})) }, []grpc.DialOption{grpc.WithTransportCredentials(noCert), grpc.WithAuthority("market.test")}, opts...)
	if _, e := rpc.NewClient(conn).BuildSnapshot(context.Background(), request()); status.Code(e) != codes.Unavailable {
		t.Fatal(e)
	}
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	address := listener.Addr().String()
	listener.Close()
	clientCert := ca.issue(t, []string{matcherURI}, false, false)
	conn, e = grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(clientCreds(t, clientCert, ca.pem)), grpc.WithAuthority("market.test"))
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	c := rpc.NewClient(conn)
	if _, e = c.BuildSnapshot(context.Background(), request()); status.Code(e) != codes.Unavailable {
		t.Fatal(e)
	}
	if _, e = c.ReadSnapshotPage(context.Background(), &pb.ReadSnapshotPageRequest{SnapshotId: snapshotID, PageSize: 1}); status.Code(e) != codes.Unavailable {
		t.Fatal(e)
	}
}
func TestInvalidBuildBeforeStorage(t *testing.T) {
	c := rpc.NewClient(connect(t, rpc.NewServer(store{}), matcherURI)) // Any storage call would panic.
	for _, r := range []*pb.BuildSnapshotRequest{{RequestId: "", TtlMillis: 1, PageSize: 1}, {RequestId: "bad id", TtlMillis: 1, PageSize: 1}, {RequestId: "id", TtlMillis: 0, PageSize: 1}, {RequestId: "id", TtlMillis: 86400001, PageSize: 1}, {RequestId: "id", TtlMillis: 1, PageSize: 1001}} {
		if _, e := c.BuildSnapshot(context.Background(), r); status.Code(e) != codes.InvalidArgument {
			t.Fatal(e)
		}
	}
}
func TestServerBudgetWithoutClientWrapper(t *testing.T) {
	for _, method := range []string{"build", "page"} {
		t.Run(method, func(t *testing.T) {
			wait := func(c context.Context) {
				if d, ok := c.Deadline(); !ok || time.Until(d) > rpc.Budget {
					t.Error("unbounded server")
				}
				<-c.Done()
			}
			s := store{build: func(c context.Context, _, _ string, _ time.Duration) (sub.Snapshot, error) {
				wait(c)
				return sub.Snapshot{}, c.Err()
			}, page: func(c context.Context, _, _, _ string, _ int) (sub.SnapshotPage, error) {
				wait(c)
				return sub.SnapshotPage{}, c.Err()
			}}
			c := pb.NewSubscriptionSnapshotsClient(connect(t, rpc.NewServer(s), matcherURI))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var e error
			if method == "build" {
				_, e = c.BuildSnapshot(ctx, request())
			} else {
				_, e = c.ReadSnapshotPage(ctx, &pb.ReadSnapshotPageRequest{SnapshotId: snapshotID, PageSize: 1})
			}
			if status.Code(e) != codes.DeadlineExceeded || ctx.Err() != nil {
				t.Fatal(e, ctx.Err())
			}
		})
	}
}
