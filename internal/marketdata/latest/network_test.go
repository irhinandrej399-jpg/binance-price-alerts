package latest_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/latest"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/grpctest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const apiURI = "spiffe://binance.test/api"

type authority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}
type pair struct{ cert, key []byte }

func newAuthority(t *testing.T) authority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ephemeral test CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return authority{parsed, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}
func (ca authority) issue(t *testing.T, ids []string, server, expired bool) pair {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: serial, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	if server {
		cert.DNSNames = []string{"market.test"}
		cert.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	}
	if expired {
		cert.NotAfter = time.Now().Add(-time.Minute)
	}
	for _, id := range ids {
		u, err := url.Parse(id)
		if err != nil {
			t.Fatal(err)
		}
		cert.URIs = append(cert.URIs, u)
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pair{pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})}
}
func clientCreds(t *testing.T, p pair, ca []byte) credentials.TransportCredentials {
	t.Helper()
	creds, err := serviceauth.ClientCredentials(p.cert, p.key, ca)
	if err != nil {
		t.Fatal(err)
	}
	return creds
}

func connect(t *testing.T, r latest.Reader, identity string) *grpc.ClientConn {
	t.Helper()
	ca := newAuthority(t)
	server := ca.issue(t, nil, true, false)
	client := ca.issue(t, []string{identity}, false, false)
	opts, err := latest.ServerOptions(server.cert, server.key, ca.pem, apiURI, "spiffe://binance.test/matcher")
	if err != nil {
		t.Fatal(err)
	}
	return grpctest.StartWithClientOptions(t, func(s *grpc.Server) { pb.RegisterQuotesServer(s, latest.NewServer(r, func() time.Time { return now })) }, []grpc.DialOption{grpc.WithTransportCredentials(clientCreds(t, client, ca.pem)), grpc.WithAuthority("market.test"), grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > latest.Budget {
			t.Error("unbounded client call")
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	})}, opts...)
}
func TestTLSQuoteAndAuthorization(t *testing.T) {
	for _, id := range []string{apiURI, "spiffe://binance.test/notifications", "spiffe://binance.test/matcher"} {
		t.Run(id, func(t *testing.T) {
			var calls atomic.Int32
			conn := connect(t, reader(func(context.Context, string) (priceobserved.Observed, error) { calls.Add(1); return quote(), nil }), id)
			ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer public-token", "service-identity", apiURI))
			r, err := latest.NewClient(conn).Latest(ctx, "BTCUSDT")
			if id == apiURI || id == "spiffe://binance.test/matcher" {
				if err != nil || r.Quote.SourceId != "42" || calls.Load() != 1 {
					t.Fatalf("%v %v", r, err)
				}
			} else if status.Code(err) != codes.PermissionDenied || calls.Load() != 0 {
				t.Fatalf("%v calls=%d", err, calls.Load())
			}
		})
	}
}
func TestTLSDeadlineAndCancellation(t *testing.T) {
	for _, mode := range []string{"default budget", "earlier deadline", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			entered := make(chan struct{})
			finished := make(chan error, 1)
			conn := connect(t, reader(func(ctx context.Context, _ string) (priceobserved.Observed, error) {
				d, ok := ctx.Deadline()
				if !ok || time.Until(d) > latest.Budget {
					t.Error("missing server bound")
				}
				close(entered)
				<-ctx.Done()
				finished <- ctx.Err()
				return priceobserved.Observed{}, ctx.Err()
			}), apiURI)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if mode == "earlier deadline" {
				var c context.CancelFunc
				ctx, c = context.WithTimeout(ctx, 100*time.Millisecond)
				defer c()
			}
			result := make(chan error, 1)
			go func() { _, err := latest.NewClient(conn).Latest(ctx, "BTCUSDT"); result <- err }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("handler not entered")
			}
			want := codes.DeadlineExceeded
			serverErr := context.DeadlineExceeded
			if mode == "cancel" {
				cancel()
				want = codes.Canceled
				serverErr = context.Canceled
			}
			select {
			case err := <-result:
				if status.Code(err) != want {
					t.Fatal(err)
				}
			case <-time.After(4 * time.Second):
				t.Fatal("client hung")
			}
			select {
			case err := <-finished:
				// A client deadline may cancel the HTTP/2 stream before the server timer fires.
				if err != serverErr && !(mode != "cancel" && err == context.Canceled) {
					t.Fatalf("server: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("storage not canceled")
			}
		})
	}
}
func TestTLSStorageUnavailable(t *testing.T) {
	conn := connect(t, reader(func(context.Context, string) (priceobserved.Observed, error) {
		return priceobserved.Observed{}, status.Error(codes.Internal, "private database error")
	}), apiURI)
	_, err := latest.NewClient(conn).Latest(context.Background(), "BTCUSDT")
	if status.Code(err) != codes.Unavailable || status.Convert(err).Message() != "quote storage unavailable" {
		t.Fatal(err)
	}
}
func TestUnavailableServer(t *testing.T) {
	// Keep the port bound to avoid port reuse races; reject every real TCP connection.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	defer func() { _ = lis.Close(); <-done }()
	ca := newAuthority(t)
	client := ca.issue(t, []string{apiURI}, false, false)
	conn, err := grpc.NewClient("passthrough:///"+lis.Addr().String(), grpc.WithTransportCredentials(clientCreds(t, client, ca.pem)), grpc.WithAuthority("market.test"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, err = latest.NewClient(conn).Latest(context.Background(), "BTCUSDT")
	if status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
}
