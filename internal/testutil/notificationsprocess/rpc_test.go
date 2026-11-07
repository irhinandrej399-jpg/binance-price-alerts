package notificationsprocess

import (
	"context"
	"google.golang.org/grpc"
	"testing"
	"time"
)

func TestRPCPauseCancellation(t *testing.T) {
	for _, after := range []bool{false, true} {
		f := &Fixture{gates: &rpcGates{}}
		p := f.PauseRPC("/test/Call", after)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		calls := make(chan struct{}, 1)
		go func() {
			_, e := f.gates.intercept(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/test/Call"}, func(context.Context, any) (any, error) { calls <- struct{}{}; return nil, nil })
			done <- e
		}()
		select {
		case <-p.Entered:
		case <-time.After(time.Second):
			t.Fatal("gate not reached")
		}
		if !after {
			select {
			case <-calls:
				t.Fatal("handler entered before release")
			default:
			}
		}
		cancel()
		select {
		case e := <-done:
			if e != context.Canceled {
				t.Fatal("cancellation lost")
			}
		case <-time.After(time.Second):
			t.Fatal("gate leaked")
		}
		p.Release()
		f.gates.release()
	}
}
