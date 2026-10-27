package grpctest_test

import (
	"context"
	"net"
	"testing"
	"time"

	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/probe/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/grpctest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type probe struct {
	pb.UnimplementedProbeServer
	call func(context.Context, *pb.CallRequest) (*pb.CallResponse, error)
}

func (s probe) Call(ctx context.Context, in *pb.CallRequest) (*pb.CallResponse, error) {
	return s.call(ctx, in)
}

func client(t *testing.T, call func(context.Context, *pb.CallRequest) (*pb.CallResponse, error)) pb.ProbeClient {
	t.Helper()
	conn := grpctest.Start(t, func(s *grpc.Server) {
		pb.RegisterProbeServer(s, probe{call: call})
	})
	return pb.NewProbeClient(conn)
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for server")
		var zero T
		return zero
	}
}

func TestUnaryRoundTrip(t *testing.T) {
	c := client(t, func(ctx context.Context, in *pb.CallRequest) (*pb.CallResponse, error) {
		return &pb.CallResponse{Value: in.GetValue()}, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	out, err := c.Call(ctx, &pb.CallRequest{Value: "loopback"})
	if err != nil || out.GetValue() != "loopback" {
		t.Fatalf("Call = %v, %v", out, err)
	}
}

func TestDeadlineReachesServer(t *testing.T) {
	deadline := make(chan time.Time, 1)
	stopped := make(chan error, 1)
	c := client(t, func(ctx context.Context, _ *pb.CallRequest) (*pb.CallResponse, error) {
		d, _ := ctx.Deadline()
		deadline <- d
		<-ctx.Done()
		stopped <- ctx.Err()
		return nil, status.FromContextError(ctx.Err()).Err()
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := c.Call(ctx, &pb.CallRequest{}, grpc.WaitForReady(true))
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("Call error = %v; want DeadlineExceeded", err)
	}
	d := receive(t, deadline)
	want, _ := ctx.Deadline()
	// gRPC forwards a relative timeout, so allow scheduling/clock rounding.
	if d.IsZero() || d.Sub(want).Abs() > 250*time.Millisecond {
		t.Fatalf("server deadline = %v; client deadline = %v", d, want)
	}
	if err := receive(t, stopped); err != context.DeadlineExceeded && err != context.Canceled {
		t.Fatalf("server context error = %v", err)
	}
}

func TestCancellationReachesServer(t *testing.T) {
	entered := make(chan struct{}, 1)
	stopped := make(chan error, 1)
	c := client(t, func(ctx context.Context, _ *pb.CallRequest) (*pb.CallResponse, error) {
		entered <- struct{}{}
		<-ctx.Done()
		stopped <- ctx.Err()
		return nil, status.FromContextError(ctx.Err()).Err()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := c.Call(ctx, &pb.CallRequest{})
		result <- err
	}()
	receive(t, entered)
	cancel()
	if err := receive(t, result); status.Code(err) != codes.Canceled {
		t.Fatalf("Call error = %v; want Canceled", err)
	}
	if err := receive(t, stopped); err != context.Canceled {
		t.Fatalf("server context error = %v; want Canceled", err)
	}
}

func TestUnavailableServer(t *testing.T) {
	// Reserve an unserved loopback endpoint until cleanup; no port-reuse race.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			connection, err := lis.Accept()
			if err != nil {
				return
			}
			_ = connection.Close()
		}
	}()
	t.Cleanup(func() {
		_ = lis.Close()
		receive(t, done)
	})
	conn, err := grpc.NewClient("passthrough:///"+lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = pb.NewProbeClient(conn).Call(ctx, &pb.CallRequest{})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Call error = %v; want Unavailable", err)
	}
}
