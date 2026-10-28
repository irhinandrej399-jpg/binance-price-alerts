package replayrpc_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"testing"
	"time"

	rpc "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/replayrpc"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/grpctest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type blockedPins struct{ rpc.Pins }

func wait(c context.Context) (sub.ReplayPin, error) { <-c.Done(); return sub.ReplayPin{}, c.Err() }
func (blockedPins) Handoff(c context.Context, _, _ string, _ time.Duration) (sub.ReplayPin, error) {
	return wait(c)
}
func (blockedPins) CheckPin(c context.Context, _, _ string) (sub.ReplayPin, error) { return wait(c) }
func (blockedPins) Acknowledge(c context.Context, _, _ string, _ uint64) (sub.ReplayPin, error) {
	return wait(c)
}
func (blockedPins) Abandon(c context.Context, _, _ string) error { _, e := wait(c); return e }
func TestServerBudgetsAndCallerDeadline(t *testing.T) {
	for _, method := range []string{"begin", "read", "ack", "abandon"} {
		t.Run(method, func(t *testing.T) {
			t.Parallel()
			c := pb.NewSubscriptionReplayClient(connect(t, rpc.NewServer(blockedPins{}, nil), matcherURI))
			for _, duration := range []time.Duration{30 * time.Millisecond, 5 * time.Second} {
				ctx, cancel := context.WithTimeout(context.Background(), duration)
				start := time.Now()
				var e error
				switch method {
				case "begin":
					_, e = c.BeginReplay(ctx, &pb.BeginReplayRequest{SnapshotId: resourceID, TtlMillis: 1000})
				case "read":
					_, e = c.ReadReplayPage(ctx, &pb.ReadReplayPageRequest{ResourceId: resourceID, MaxTransactions: 1})
				case "ack":
					_, e = c.AcknowledgeReplay(ctx, &pb.AcknowledgeReplayRequest{ResourceId: resourceID})
				case "abandon":
					_, e = c.AbandonReplay(ctx, &pb.AbandonReplayRequest{ResourceId: resourceID})
				}
				if status.Code(e) != codes.DeadlineExceeded || time.Since(start) > rpc.Budget+time.Second {
					t.Error(e, time.Since(start))
				}
				if duration == 5*time.Second && ctx.Err() != nil {
					t.Error("server ignored its own budget")
				}
				cancel()
			}
		})
	}
}
func TestNoCertificateAndUnavailableDependency(t *testing.T) {
	ca := newAuthority(t)
	cert := ca.issue(t, nil, true, false)
	opts, e := rpc.ServerOptions(cert.cert, cert.key, ca.pem, matcherURI)
	if e != nil {
		t.Fatal(e)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	conn := grpctest.StartWithClientOptions(t, func(g *grpc.Server) { pb.RegisterSubscriptionReplayServer(g, rpc.NewServer(nil, nil)) }, []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots})), grpc.WithAuthority("market.test")}, opts...)
	c := rpc.NewClient(conn)
	if _, e = c.BeginReplay(context.Background(), &pb.BeginReplayRequest{SnapshotId: resourceID, TtlMillis: 1000}); status.Code(e) != codes.Unavailable {
		t.Fatal(e)
	}
}
