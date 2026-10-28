package catalogrpc_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/catalog"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/catalogrpc"
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

type source func(context.Context) (catalog.Snapshot, error)

func (f source) Fetch(ctx context.Context) (catalog.Snapshot, error) { return f(ctx) }

func connect(t *testing.T, r catalogrpc.Source, identity string) *grpc.ClientConn {
	t.Helper()
	ca := newAuthority(t)
	server := ca.issue(t, nil, true, false)
	client := ca.issue(t, []string{identity}, false, false)
	opts, err := catalogrpc.ServerOptions(server.cert, server.key, ca.pem, apiURI)
	if err != nil {
		t.Fatal(err)
	}
	creds := clientCreds(t, client, ca.pem)
	if identity == "" {
		roots := x509.NewCertPool()
		roots.AppendCertsFromPEM(ca.pem)
		creds = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots})
	}
	return grpctest.StartWithClientOptions(t, func(s *grpc.Server) {
		pb.RegisterCatalogServer(s, catalogrpc.NewServer(r))
	}, []grpc.DialOption{grpc.WithTransportCredentials(creds), grpc.WithAuthority("market.test"), grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		d, ok := ctx.Deadline()
		if !ok || time.Until(d) > catalogrpc.Budget {
			t.Error("unbounded client")
		}
		return invoke(ctx, method, req, reply, cc, opts...)
	})}, opts...)
}

var updated = time.Date(2026, 10, 28, 9, 0, 0, 123456789, time.UTC)

func TestSnapshot(t *testing.T) {
	for _, pairs := range [][]catalog.Pair{
		{{Symbol: "币USDT", BaseAsset: "币", QuoteAsset: "USDT"}, {Symbol: "BTCUSDT", BaseAsset: "BTC", QuoteAsset: "USDT"}},
		{},
	} {
		t.Run(string(rune(len(pairs)+'0')), func(t *testing.T) {
			var calls atomic.Int32
			want := catalog.Snapshot{Pairs: pairs, UpdatedAt: updated}
			conn := connect(t, source(func(context.Context) (catalog.Snapshot, error) { calls.Add(1); return want, nil }), apiURI)
			got, err := catalogrpc.NewClient(conn).Fetch(context.Background())
			if err != nil || !reflect.DeepEqual(got, want) || calls.Load() != 1 {
				t.Fatalf("got=%+v err=%v calls=%d", got, err, calls.Load())
			}
		})
	}
}

func TestAuthorization(t *testing.T) {
	for _, id := range []string{apiURI, "spiffe://binance.test/matcher", "spiffe://binance.test/notifications", ""} {
		t.Run(id, func(t *testing.T) {
			var calls atomic.Int32
			conn := connect(t, source(func(context.Context) (catalog.Snapshot, error) {
				calls.Add(1)
				return catalog.Snapshot{UpdatedAt: updated}, nil
			}), id)
			ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer public-token", "service-identity", apiURI))
			_, err := catalogrpc.NewClient(conn).Fetch(ctx)
			switch {
			case id == apiURI:
				if err != nil || calls.Load() != 1 {
					t.Fatalf("%v calls=%d", err, calls.Load())
				}
			case id == "":
				// TLS handshake rejects missing client credentials before the handler.
				if status.Code(err) != codes.Unavailable || calls.Load() != 0 {
					t.Fatalf("%v calls=%d", err, calls.Load())
				}
			default:
				if status.Code(err) != codes.PermissionDenied || calls.Load() != 0 {
					t.Fatalf("%v calls=%d", err, calls.Load())
				}
			}
		})
	}
}

func TestSourceError(t *testing.T) {
	var calls atomic.Int32
	conn := connect(t, source(func(context.Context) (catalog.Snapshot, error) {
		calls.Add(1)
		return catalog.Snapshot{Pairs: []catalog.Pair{{Symbol: "partial"}}, UpdatedAt: updated}, errors.New("private source credentials")
	}), apiURI)
	got, err := catalogrpc.NewClient(conn).Fetch(context.Background())
	if status.Code(err) != codes.Unavailable || status.Convert(err).Message() != "catalog source unavailable" || !reflect.DeepEqual(got, catalog.Snapshot{}) || calls.Load() != 1 {
		t.Fatalf("%+v %v calls=%d", got, err, calls.Load())
	}
}

func TestDeadlineAndCancellation(t *testing.T) {
	for _, mode := range []string{"budget", "earlier deadline", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			entered := make(chan struct{})
			finished := make(chan error, 1)
			conn := connect(t, source(func(ctx context.Context) (catalog.Snapshot, error) {
				d, ok := ctx.Deadline()
				limit := catalogrpc.Budget
				if mode == "earlier deadline" {
					limit = 150 * time.Millisecond
				}
				if !ok || time.Until(d) > limit {
					t.Error("server did not preserve deadline")
				}
				close(entered)
				<-ctx.Done()
				finished <- ctx.Err()
				return catalog.Snapshot{}, ctx.Err()
			}), apiURI)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if mode == "earlier deadline" {
				var c context.CancelFunc
				ctx, c = context.WithTimeout(ctx, 150*time.Millisecond)
				defer c()
			}
			result := make(chan error, 1)
			go func() { _, err := catalogrpc.NewClient(conn).Fetch(ctx); result <- err }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("source not entered")
			}
			want := codes.DeadlineExceeded
			if mode == "cancel" {
				cancel()
				want = codes.Canceled
			}
			select {
			case err := <-result:
				if status.Code(err) != want {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("client hung")
			}
			select {
			case err := <-finished:
				if (mode == "cancel" && err != context.Canceled) || (err != context.Canceled && err != context.DeadlineExceeded) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("source not canceled")
			}
		})
	}
}

func TestUnavailableServer(t *testing.T) {
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
	got, err := catalogrpc.NewClient(conn).Fetch(context.Background())
	if status.Code(err) != codes.Unavailable || !reflect.DeepEqual(got, catalog.Snapshot{}) {
		t.Fatalf("%+v %v", got, err)
	}
}

func TestBinanceFixtureComposition(t *testing.T) {
	body, err := os.ReadFile("../catalog/testdata/exchange_info.json")
	if err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int32
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != "/api/v3/exchangeInfo" {
			t.Error(r.URL.Path)
		}
		_, _ = w.Write(body)
	}))
	defer httpServer.Close()
	fetcher, err := catalog.New(httpServer.Client(), httpServer.URL, func() time.Time { return updated })
	if err != nil {
		t.Fatal(err)
	}
	// Filtering stays in MKT-001; the RPC preserves its complete result.
	want, err := fetcher.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	conn := connect(t, fetcher, apiURI)
	got, err := catalogrpc.NewClient(conn).Fetch(context.Background())
	if err != nil || !reflect.DeepEqual(got, want) || requests.Load() != 2 {
		t.Fatalf("%+v %v requests=%d", got, err, requests.Load())
	}
}

// Bypass the bounded client to prove the server caps callers with longer budgets.
func TestServerBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	server := catalogrpc.NewServer(source(func(ctx context.Context) (catalog.Snapshot, error) {
		d, ok := ctx.Deadline()
		if !ok || time.Until(d) > catalogrpc.Budget {
			t.Error("server did not cap the deadline")
		}
		<-ctx.Done()
		return catalog.Snapshot{UpdatedAt: updated}, nil
	}))
	response, err := server.Fetch(ctx, &pb.CatalogRequest{})
	if response != nil || status.Code(err) != codes.DeadlineExceeded || ctx.Err() != nil {
		t.Fatalf("%v %v caller=%v", response, err, ctx.Err())
	}
}
