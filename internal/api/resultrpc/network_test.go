package resultrpc_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/resultrpc"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/grpctest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const notificationsURI = "spiffe://binance.test/notifications"
const matcherURI = "spiffe://binance.test/matcher"
const apiURI = "spiffe://binance.test/api"

func connect(t *testing.T, server *resultrpc.Server, identity string) *grpc.ClientConn {
	t.Helper()
	ca := newAuthority(t)
	cert := ca.issue(t, nil, true, false)
	client := ca.issue(t, []string{identity}, false, false)
	opts, e := resultrpc.ServerOptions(cert.cert, cert.key, ca.pem, notificationsURI)
	if e != nil {
		t.Fatal(e)
	}
	creds := clientCreds(t, client, ca.pem)
	if identity == "" {
		roots := x509.NewCertPool()
		roots.AppendCertsFromPEM(ca.pem)
		creds = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots})
	}
	return grpctest.StartWithClientOptions(t, func(s *grpc.Server) { pb.RegisterDeliveryResultsServer(s, server) }, []grpc.DialOption{grpc.WithTransportCredentials(creds), grpc.WithAuthority("market.test"), grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if d, ok := ctx.Deadline(); !ok || time.Until(d) > resultrpc.Budget {
			t.Error("unbounded client")
		}
		return invoke(ctx, method, req, reply, cc, opts...)
	})}, opts...)
}
func TestAuthorizationOverTLS(t *testing.T) {
	for _, id := range []string{notificationsURI, matcherURI, apiURI, ""} {
		t.Run(id, func(t *testing.T) {
			var calls atomic.Int32
			server := resultrpc.NewServer(storeFunc(func(context.Context, sub.DeliveryResultCommand) (sub.Record, error) {
				calls.Add(1)
				return record(), nil
			}))
			client := resultrpc.NewClient(connect(t, server, id))
			ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer public-token", "service-identity", notificationsURI))
			_, e := client.SaveDeliveryResult(ctx, request())
			want := codes.PermissionDenied
			wantCalls := int32(0)
			if id == notificationsURI {
				want = codes.OK
				wantCalls = 1
			}
			if id == "" {
				want = codes.Unavailable
			}
			if status.Code(e) != want || calls.Load() != wantCalls {
				t.Fatal(e, calls.Load())
			}
		})
	}
}
func TestDeadlineAndCancellation(t *testing.T) {
	for _, mode := range []string{"budget", "earlier", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			entered := make(chan struct{})
			finished := make(chan error, 1)
			server := resultrpc.NewServer(storeFunc(func(ctx context.Context, _ sub.DeliveryResultCommand) (sub.Record, error) {
				if d, ok := ctx.Deadline(); !ok || time.Until(d) > resultrpc.Budget {
					t.Error("unbounded server")
				}
				close(entered)
				<-ctx.Done()
				finished <- ctx.Err()
				return sub.Record{}, ctx.Err()
			}))
			client := resultrpc.NewClient(connect(t, server, notificationsURI))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if mode == "earlier" {
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(ctx, 150*time.Millisecond)
				defer stop()
			}
			result := make(chan error, 1)
			go func() { _, e := client.SaveDeliveryResult(ctx, request()); result <- e }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("not entered")
			}
			want := codes.DeadlineExceeded
			if mode == "cancel" {
				cancel()
				want = codes.Canceled
			}
			if e := <-result; status.Code(e) != want {
				t.Fatal(e)
			}
			select {
			case e := <-finished:
				if !errors.Is(e, context.Canceled) && !errors.Is(e, context.DeadlineExceeded) {
					t.Fatal(e)
				}
			case <-time.After(time.Second):
				t.Fatal("storage not canceled")
			}
		})
	}
}
func TestServerBudget(t *testing.T) {
	server := resultrpc.NewServer(storeFunc(func(ctx context.Context, _ sub.DeliveryResultCommand) (sub.Record, error) {
		<-ctx.Done()
		return sub.Record{}, ctx.Err()
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, e := server.SaveDeliveryResult(ctx, request()); status.Code(e) != codes.DeadlineExceeded || ctx.Err() != nil {
		t.Fatal(e, ctx.Err())
	}
}
func TestUnavailableAPI(t *testing.T) {
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	address := listener.Addr().String()
	listener.Close()
	ca := newAuthority(t)
	cert := ca.issue(t, []string{notificationsURI}, false, false)
	conn, e := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(clientCreds(t, cert, ca.pem)), grpc.WithAuthority("market.test"))
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	if _, e = resultrpc.NewClient(conn).SaveDeliveryResult(context.Background(), request()); status.Code(e) != codes.Unavailable {
		t.Fatal(e)
	}
}
func TestStorageErrorsOverTLS(t *testing.T) {
	var calls atomic.Int32
	server := resultrpc.NewServer(storeFunc(func(context.Context, sub.DeliveryResultCommand) (sub.Record, error) {
		calls.Add(1)
		return sub.Record{}, errors.New("secret SQL")
	}))
	client := resultrpc.NewClient(connect(t, server, notificationsURI))
	if _, e := client.SaveDeliveryResult(context.Background(), request()); status.Code(e) != codes.Unavailable || calls.Load() != 1 {
		t.Fatal(e, calls.Load())
	}
	r := request()
	r.AttemptId = "bad"
	if _, e := client.SaveDeliveryResult(context.Background(), r); status.Code(e) != codes.InvalidArgument || calls.Load() != 1 {
		t.Fatal(e, calls.Load())
	}
}
