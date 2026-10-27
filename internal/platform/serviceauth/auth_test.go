package serviceauth_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/probe/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/grpctest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const identity = "spiffe://binance.test/api"

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

type probe struct {
	pb.UnimplementedProbeServer
	call func(context.Context, *pb.CallRequest) (*pb.CallResponse, error)
}

func (p probe) Call(ctx context.Context, in *pb.CallRequest) (*pb.CallResponse, error) {
	return p.call(ctx, in)
}
func connect(t *testing.T, server pair, clientCA []byte, creds credentials.TransportCredentials, name string, grants map[string][]string, call func(context.Context, *pb.CallRequest) (*pb.CallResponse, error)) *grpc.ClientConn {
	t.Helper()
	opts, err := serviceauth.ServerOptions(server.cert, server.key, clientCA, grants)
	if err != nil {
		t.Fatal(err)
	}
	return grpctest.StartWithClientOptions(t, func(s *grpc.Server) { pb.RegisterProbeServer(s, probe{call: call}) }, []grpc.DialOption{grpc.WithTransportCredentials(creds), grpc.WithAuthority(name)}, opts...)
}
func TestAuthorizationOverTLS(t *testing.T) {
	ca, foreign := newAuthority(t), newAuthority(t)
	server := ca.issue(t, nil, true, false)
	valid := ca.issue(t, []string{identity}, false, false)
	cases := []struct {
		name               string
		client             pair
		roots              []byte
		serverName, method string
		grants             map[string][]string
		want               codes.Code
		raw                bool
	}{
		{name: "allowed", client: valid, want: codes.OK},
		{name: "missing certificate and public bearer", raw: true, want: codes.Unavailable},
		{name: "untrusted client", client: foreign.issue(t, []string{identity}, false, false), want: codes.Unavailable},
		{name: "expired client", client: ca.issue(t, []string{identity}, false, true), want: codes.Unavailable},
		{name: "no URI", client: ca.issue(t, nil, false, false), want: codes.Unauthenticated},
		{name: "ambiguous URI", client: ca.issue(t, []string{identity, "spiffe://binance.test/matcher"}, false, false), want: codes.Unauthenticated},
		{name: "other identity cannot spoof metadata", client: ca.issue(t, []string{"spiffe://binance.test/notifications"}, false, false), want: codes.PermissionDenied},
		{name: "no method permission", client: valid, grants: map[string][]string{identity: {"/binance.probe.v1.Probe/Other"}}, want: codes.PermissionDenied},
		{name: "empty policy", client: valid, grants: map[string][]string{}, want: codes.PermissionDenied},
		{name: "unknown method", client: valid, method: "/binance.probe.v1.Probe/Unknown", want: codes.PermissionDenied},
		{name: "unknown service even if granted", client: valid, method: "/unknown.Service/Call", grants: map[string][]string{identity: {"/unknown.Service/Call"}}, want: codes.PermissionDenied},
		{name: "untrusted server", client: valid, roots: foreign.pem, want: codes.Unavailable},
		{name: "wrong server name", client: valid, serverName: "impostor.test", want: codes.Unavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			roots := tc.roots
			if roots == nil {
				roots = ca.pem
			}
			name := tc.serverName
			if name == "" {
				name = "market.test"
			}
			grants := tc.grants
			if grants == nil {
				grants = map[string][]string{identity: {pb.Probe_Call_FullMethodName}}
			}
			var creds credentials.TransportCredentials
			if tc.raw {
				pool := x509.NewCertPool()
				pool.AppendCertsFromPEM(roots)
				creds = credentials.NewTLS(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS13})
			} else {
				creds = clientCreds(t, tc.client, roots)
			}
			var calls atomic.Int32
			conn := connect(t, server, ca.pem, creds, name, grants, func(_ context.Context, in *pb.CallRequest) (*pb.CallResponse, error) {
				calls.Add(1)
				return &pb.CallResponse{Value: in.Value}, nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer public-user-session", "x-service-id", identity))
			method := tc.method
			if method == "" {
				method = pb.Probe_Call_FullMethodName
			}
			out := new(pb.CallResponse)
			err := conn.Invoke(ctx, method, &pb.CallRequest{Value: "secure"}, out)
			if status.Code(err) != tc.want {
				t.Fatalf("status=%v want=%v", status.Code(err), tc.want)
			}
			wantCalls := int32(0)
			if tc.want == codes.OK {
				wantCalls = 1
				if out.Value != "secure" {
					t.Fatal("response mismatch")
				}
			}
			if calls.Load() != wantCalls {
				t.Fatalf("handler calls=%d want=%d", calls.Load(), wantCalls)
			}
		})
	}
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(4 * time.Second):
		t.Fatal("server did not signal")
		var zero T
		return zero
	}
}
func TestContextOverTLS(t *testing.T) {
	for _, cancellation := range []bool{false, true} {
		name := "deadline"
		if cancellation {
			name = "cancellation"
		}
		t.Run(name, func(t *testing.T) {
			ca := newAuthority(t)
			server := ca.issue(t, nil, true, false)
			client := ca.issue(t, []string{identity}, false, false)
			entered := make(chan time.Time, 1)
			stopped := make(chan error, 1)
			conn := connect(t, server, ca.pem, clientCreds(t, client, ca.pem), "market.test", map[string][]string{identity: {pb.Probe_Call_FullMethodName}}, func(ctx context.Context, _ *pb.CallRequest) (*pb.CallResponse, error) {
				d, _ := ctx.Deadline()
				entered <- d
				<-ctx.Done()
				stopped <- ctx.Err()
				return nil, status.FromContextError(ctx.Err()).Err()
			})
			budget := time.Second
			if cancellation {
				budget = 3 * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			result := make(chan error, 1)
			go func() { _, err := pb.NewProbeClient(conn).Call(ctx, &pb.CallRequest{}); result <- err }()
			d := receive(t, entered)
			want, _ := ctx.Deadline()
			if d.IsZero() || d.Sub(want).Abs() > 250*time.Millisecond {
				t.Fatal("deadline was not forwarded")
			}
			code := codes.DeadlineExceeded
			if cancellation {
				cancel()
				code = codes.Canceled
			}
			if status.Code(receive(t, result)) != code {
				t.Fatal("unexpected client context result")
			}
			err := receive(t, stopped)
			if cancellation && err != context.Canceled {
				t.Fatal("cancellation did not reach server context")
			}
			if err != context.Canceled && err != context.DeadlineExceeded {
				t.Fatal("server context was not stopped")
			}
		})
	}
}

func TestConfigurationErrorsAreRedacted(t *testing.T) {
	ca := newAuthority(t)
	server := ca.issue(t, nil, true, false)
	other := ca.issue(t, nil, true, false)
	for _, tc := range []struct {
		name             string
		cert, key, roots []byte
		grants           map[string][]string
		want             string
	}{
		{"bad certificate", []byte("SECRET-CERT"), server.key, ca.pem, nil, "serviceauth: invalid certificate or private key"},
		{"bad key", server.cert, []byte("SECRET-KEY"), ca.pem, nil, "serviceauth: invalid certificate or private key"},
		{"mismatched key", server.cert, other.key, ca.pem, nil, "serviceauth: invalid certificate or private key"},
		{"no roots", server.cert, server.key, nil, nil, "serviceauth: invalid trust roots"},
		{"bad roots", server.cert, server.key, []byte("SECRET-ROOTS"), nil, "serviceauth: invalid trust roots"},
		{"bad identity", server.cert, server.key, ca.pem, map[string][]string{"SECRET-IDENTITY": {pb.Probe_Call_FullMethodName}}, "serviceauth: invalid policy identity"},
		{"wildcard", server.cert, server.key, ca.pem, map[string][]string{identity: {"/*"}}, "serviceauth: invalid full method name"},
		{"bad method", server.cert, server.key, ca.pem, map[string][]string{identity: {"SECRET-METHOD"}}, "serviceauth: invalid full method name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options, err := serviceauth.ServerOptions(tc.cert, tc.key, tc.roots, tc.grants)
			if err == nil || err.Error() != tc.want || options != nil {
				t.Fatal("expected redacted configuration error")
			}
			if tc.grants == nil {
				creds, err := serviceauth.ClientCredentials(tc.cert, tc.key, tc.roots)
				if err == nil || err.Error() != tc.want || creds != nil {
					t.Fatal("expected redacted client error")
				}
			}
		})
	}
}

func TestPolicyIsSnapshot(t *testing.T) {
	ca := newAuthority(t)
	server := ca.issue(t, nil, true, false)
	client := ca.issue(t, []string{identity}, false, false)
	methods := []string{pb.Probe_Call_FullMethodName}
	grants := map[string][]string{identity: methods}
	var calls atomic.Int32
	conn := connect(t, server, ca.pem, clientCreds(t, client, ca.pem), "market.test", grants, func(_ context.Context, _ *pb.CallRequest) (*pb.CallResponse, error) {
		calls.Add(1)
		return &pb.CallResponse{}, nil
	})
	methods[0] = "/unknown.Service/Other"
	delete(grants, identity)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := pb.NewProbeClient(conn).Call(ctx, &pb.CallRequest{})
	if err != nil || calls.Load() != 1 {
		t.Fatal("caller mutation changed captured policy")
	}
}

func TestRegisteredStreamingDenied(t *testing.T) {
	ca := newAuthority(t)
	server := ca.issue(t, nil, true, false)
	client := ca.issue(t, []string{identity}, false, false)
	const method = "/test.Stream/Call"
	options, err := serviceauth.ServerOptions(server.cert, server.key, ca.pem, map[string][]string{identity: {method}})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	conn := grpctest.StartWithClientOptions(t, func(s *grpc.Server) {
		s.RegisterService(&grpc.ServiceDesc{ServiceName: "test.Stream", HandlerType: (*interface{})(nil), Streams: []grpc.StreamDesc{{StreamName: "Call", Handler: func(any, grpc.ServerStream) error { calls.Add(1); return nil }, ServerStreams: true}}}, struct{}{})
	}, []grpc.DialOption{grpc.WithTransportCredentials(clientCreds(t, client, ca.pem)), grpc.WithAuthority("market.test")}, options...)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, method)
	if err != nil {
		t.Fatal(err)
	}
	err = stream.RecvMsg(new(pb.CallResponse))
	if status.Code(err) != codes.PermissionDenied || calls.Load() != 0 {
		t.Fatal("stream handler was not blocked")
	}
}
