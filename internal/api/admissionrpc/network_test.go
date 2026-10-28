package admissionrpc_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/admissionrpc"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	market "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/latest"
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

func connect(t *testing.T, server *admissionrpc.Server, identity string) *grpc.ClientConn {
	t.Helper()
	ca := newAuthority(t)
	cert := ca.issue(t, nil, true, false)
	client := ca.issue(t, []string{identity}, false, false)
	opts, err := admissionrpc.ServerOptions(cert.cert, cert.key, ca.pem, notificationsURI)
	if err != nil {
		t.Fatal(err)
	}
	creds := clientCreds(t, client, ca.pem)
	if identity == "" {
		roots := x509.NewCertPool()
		roots.AppendCertsFromPEM(ca.pem)
		creds = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots})
	}
	return grpctest.StartWithClientOptions(t, func(s *grpc.Server) { pb.RegisterAdmissionServer(s, server) }, []grpc.DialOption{grpc.WithTransportCredentials(creds), grpc.WithAuthority("market.test"), grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if d, ok := ctx.Deadline(); !ok || time.Until(d) > admissionrpc.Budget {
			t.Error("unbounded client")
		}
		return invoke(ctx, method, req, reply, cc, opts...)
	})}, opts...)
}
func TestAuthorization(t *testing.T) {
	for _, id := range []string{notificationsURI, matcherURI, apiURI, ""} {
		t.Run(id, func(t *testing.T) {
			s := &store{value: record()}
			var calls atomic.Int32
			server := newServer(s, quotes(func(context.Context, string) (*market.LatestResponse, error) { calls.Add(1); return quote("101"), nil }), func() time.Time { return now })
			conn := connect(t, server, id)
			ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer public-token", "service-identity", notificationsURI))
			r, err := admissionrpc.NewClient(conn).AdmitDelivery(ctx, ownerID, subID, workflowID, "check")
			want := codes.PermissionDenied
			if id == notificationsURI {
				want = codes.OK
			}
			if id == "" {
				want = codes.Unavailable
			}
			if status.Code(err) != want {
				t.Fatal(r, err)
			}
			if id == notificationsURI {
				if calls.Load() != 1 || r.Attempt == nil {
					t.Fatal(r, calls.Load())
				}
			} else if calls.Load() != 0 {
				t.Fatal("unauthorized quote read")
			}
		})
	}
}
func TestDeadlineCancellation(t *testing.T) {
	for _, mode := range []string{"budget", "earlier", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			entered := make(chan struct{})
			finished := make(chan error, 1)
			server := newServer(&store{value: record()}, quotes(func(ctx context.Context, _ string) (*market.LatestResponse, error) {
				if d, ok := ctx.Deadline(); !ok || time.Until(d) > admissionrpc.Budget {
					t.Error("unbounded server")
				}
				close(entered)
				<-ctx.Done()
				finished <- ctx.Err()
				return nil, ctx.Err()
			}), nil)
			conn := connect(t, server, notificationsURI)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if mode == "earlier" {
				var c context.CancelFunc
				ctx, c = context.WithTimeout(ctx, 150*time.Millisecond)
				defer c()
			}
			result := make(chan error, 1)
			go func() {
				_, e := admissionrpc.NewClient(conn).AdmitDelivery(ctx, ownerID, subID, workflowID, "check")
				result <- e
			}()
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
				if e != context.Canceled && e != context.DeadlineExceeded {
					t.Fatal(e)
				}
			case <-time.After(time.Second):
				t.Fatal("dependency not canceled")
			}
		})
	}
}
func TestServerBudget(t *testing.T) {
	server := newServer(&store{value: record()}, quotes(func(ctx context.Context, _ string) (*market.LatestResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, e := server.AdmitDelivery(ctx, request()); status.Code(e) != codes.DeadlineExceeded || ctx.Err() != nil {
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
	client := ca.issue(t, []string{notificationsURI}, false, false)
	conn, e := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(clientCreds(t, client, ca.pem)), grpc.WithAuthority("market.test"))
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	if r, e := admissionrpc.NewClient(conn).AdmitDelivery(context.Background(), ownerID, subID, workflowID, "check"); r != nil || status.Code(e) != codes.Unavailable {
		t.Fatal(r, e)
	}
}

type reader func(context.Context, string) (priceobserved.Observed, error)

func (f reader) Latest(ctx context.Context, pair string) (priceobserved.Observed, error) {
	return f(ctx, pair)
}

// Exercise the actual latest.Client over a second authenticated connection.
func marketConnection(t *testing.T, r latest.Reader) *grpc.ClientConn {
	t.Helper()
	ca := newAuthority(t)
	server := ca.issue(t, nil, true, false)
	client := ca.issue(t, []string{apiURI}, false, false)
	opts, e := latest.ServerOptions(server.cert, server.key, ca.pem, apiURI, matcherURI)
	if e != nil {
		t.Fatal(e)
	}
	return grpctest.StartWithClientOptions(t, func(s *grpc.Server) {
		market.RegisterQuotesServer(s, latest.NewServer(r, func() time.Time { return now }))
	}, []grpc.DialOption{grpc.WithTransportCredentials(clientCreds(t, client, ca.pem)), grpc.WithAuthority("market.test")}, opts...)
}
func TestUnavailableQuoteOverTLS(t *testing.T) {
	conn := marketConnection(t, reader(func(ctx context.Context, _ string) (priceobserved.Observed, error) {
		<-ctx.Done()
		return priceobserved.Observed{}, ctx.Err()
	}))
	s := &store{value: record()}
	api := connect(t, newServer(s, latest.NewClient(conn), func() time.Time { return now }), notificationsURI)
	_, e := admissionrpc.NewClient(api).AdmitDelivery(context.Background(), ownerID, subID, workflowID, "check")
	if status.Code(e) != codes.DeadlineExceeded {
		t.Fatal(e)
	}
	// Access store only after server-side work has stopped (no write on timeout).
}
func TestInvalidInputOverTLS(t *testing.T) {
	server := newServer(&store{value: record()}, quotes(func(context.Context, string) (*market.LatestResponse, error) { t.Error("quote read"); return nil, nil }), nil)
	conn := connect(t, server, notificationsURI)
	_, e := admissionrpc.NewClient(conn).AdmitDelivery(context.Background(), "invalid", subID, workflowID, "check")
	if status.Code(e) != codes.InvalidArgument || strings.Contains(e.Error(), "secret") {
		t.Fatal(e)
	}
}

var _ sub.QuoteSource = (*latest.Client)(nil)
