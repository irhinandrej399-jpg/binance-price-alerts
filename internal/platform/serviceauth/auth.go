// Package serviceauth authenticates internal unary RPCs with mutual TLS and
// authorizes exact certificate URI identities against exact full method names.
package serviceauth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/url"
	"regexp"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

var fullMethod = regexp.MustCompile(`^/[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*/[A-Za-z_][A-Za-z0-9_]*$`)

// material parses explicitly supplied PEM, never system roots. Errors deliberately
// omit PEM contents, private keys, identities, paths and underlying parser errors.
func material(certPEM, keyPEM, caPEM []byte) (tls.Certificate, *x509.CertPool, error) {
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return tls.Certificate{}, nil, errors.New("serviceauth: invalid certificate or private key")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return tls.Certificate{}, nil, errors.New("serviceauth: invalid trust roots")
	}
	return cert, roots, nil
}

// ClientCredentials requires a client certificate and explicit server trust roots.
// The target authority must match the server DNS SAN (grpc.WithAuthority when
// connecting to an IP or a resolver alias). No insecure verification is exposed.
func ClientCredentials(certPEM, keyPEM, serverCAPEM []byte) (credentials.TransportCredentials, error) {
	cert, roots, err := material(certPEM, keyPEM, serverCAPEM)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, RootCAs: roots}), nil
}

func validIdentity(id string) bool {
	u, err := url.Parse(id)
	return err == nil && u.Scheme != "" && u.Host != "" && u.User == nil && u.RawQuery == "" && u.Fragment == "" && u.String() == id
}

type policy map[string]map[string]bool

func (p policy) authorize(ctx context.Context, method string) error {
	remote, ok := peer.FromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "service identity required")
	}
	info, ok := remote.AuthInfo.(credentials.TLSInfo)
	if !ok || !info.State.HandshakeComplete || info.State.Version < tls.VersionTLS13 || len(info.State.VerifiedChains) == 0 || len(info.State.VerifiedChains[0]) == 0 {
		return status.Error(codes.Unauthenticated, "service identity required")
	}
	leaf := info.State.VerifiedChains[0][0]
	if len(leaf.URIs) != 1 || !validIdentity(leaf.URIs[0].String()) {
		return status.Error(codes.Unauthenticated, "service identity required")
	}
	if !p[leaf.URIs[0].String()][method] {
		return status.Error(codes.PermissionDenied, "service method forbidden")
	}
	return nil
}

// ServerOptions returns mutual TLS credentials and deny-by-default authorization.
// Grants are copied at construction and cannot contain wildcard methods. An empty
// policy denies all calls. Install the returned options together and do not
// override their TLS, interceptors or unknown-service handler. Streaming is denied.
func ServerOptions(certPEM, keyPEM, clientCAPEM []byte, grants map[string][]string) ([]grpc.ServerOption, error) {
	cert, roots, err := material(certPEM, keyPEM, clientCAPEM)
	if err != nil {
		return nil, err
	}
	p := make(policy, len(grants))
	for id, methods := range grants {
		if !validIdentity(id) {
			return nil, errors.New("serviceauth: invalid policy identity")
		}
		p[id] = make(map[string]bool, len(methods))
		for _, method := range methods {
			if !fullMethod.MatchString(method) {
				return nil, errors.New("serviceauth: invalid full method name")
			}
			p[id][method] = true
		}
	}
	denyStream := func(_ any, stream grpc.ServerStream) error {
		// Never authorize an unknown or streaming method, even if misconfigured in
		// grants. Unknown RPCs bypass gRPC's unary interceptor.
		// Empty method cannot appear in the validated grants.
		return p.authorize(stream.Context(), "")
	}
	return []grpc.ServerOption{
		grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert})),
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if err := p.authorize(ctx, info.FullMethod); err != nil {
				return nil, err
			}
			return handler(ctx, req)
		}),
		grpc.ChainStreamInterceptor(func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, _ grpc.StreamHandler) error {
			return denyStream(srv, stream)
		}),
		grpc.UnknownServiceHandler(denyStream),
	}, nil
}
