// Package grpctest provides a real loopback gRPC transport for tests.
package grpctest

import (
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Start registers services on a real TCP loopback server and returns a lazy
// client connection. Each RPC must set its own finite context deadline.
// Transport credentials are insecure and restricted to this test helper.
// Server options allow subsequent adapters to exercise their interceptors.
func Start(t testing.TB, register func(*grpc.Server), options ...grpc.ServerOption) *grpc.ClientConn {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for test gRPC: %v", err)
	}
	server := grpc.NewServer(options...)
	done := make(chan error, 1)
	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
		select {
		case err := <-done:
			if err != nil && err != grpc.ErrServerStopped {
				t.Errorf("serve test gRPC: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("test gRPC server did not stop")
		}
	})
	// gRPC requires registration before Serve.
	register(server)
	go func() { done <- server.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///"+lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("create test gRPC client: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}
