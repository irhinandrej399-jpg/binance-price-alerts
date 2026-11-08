package cleanerprocess

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGateBoundaries(t *testing.T) {
	for _, after := range []bool{false, true} {
		g := &gates{}
		p := g.pause(after)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		called := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			_, e := g.intercept(ctx, nil, &grpc.UnaryServerInfo{}, func(context.Context, any) (any, error) { close(called); return "real", nil })
			done <- e
		}()
		select {
		case <-p.Entered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		select {
		case <-called:
			if !after {
				t.Fatal("request escaped before gate")
			}
		default:
			if after {
				t.Fatal("response gate before handler")
			}
		}
		p.Fail()
		if status.Code(<-done) != codes.Unavailable {
			t.Fatal("lost response delivered")
		}
		p.Release()
		p.Release()
		_, e := g.intercept(ctx, nil, &grpc.UnaryServerInfo{}, func(context.Context, any) (any, error) { return "retry", nil })
		if e != nil {
			t.Fatal(e)
		}
		cancel()
	}
}

func TestBarrierCancellation(t *testing.T) {
	p := NewBarrier()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if p.Arrive(ctx) == nil {
		t.Fatal("canceled barrier passed")
	}
	p.Release()
	p.Release()
}
