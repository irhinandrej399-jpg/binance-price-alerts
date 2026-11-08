package progressrpc_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/notifications/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/intake"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/progressrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/grpctest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type reader struct {
	cp    intake.Progress
	err   error
	calls int
	wait  bool
}

func (r *reader) ReadProgress(ctx context.Context, b intake.Binding, partition int32) (intake.Progress, error) {
	r.calls++
	if d, ok := ctx.Deadline(); !ok || time.Until(d) > progressrpc.Budget {
		panic("missing SQL budget")
	}
	if r.wait {
		<-ctx.Done()
		return intake.Progress{}, ctx.Err()
	}
	return r.cp, r.err
}
func request() *pb.ReadProgressRequest {
	return &pb.ReadProgressRequest{Generation: "bootstrap-1", ConsumerId: "stable-intake", ClusterId: "cluster", TopicId: append([]byte{1}, make([]byte, 15)...), Topic: "prices", Partition: 2}
}
func checkpoint() intake.Progress {
	return intake.Progress{Binding: intake.Binding{ConsumerID: "stable-intake", Generation: "bootstrap-1", Source: kafkasource.Identity{ClusterID: "cluster", Topic: "prices", TopicID: [16]byte{1}}}, Partition: 2, Next: 41}
}
func TestProofAndFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*reader)
		code codes.Code
	}{
		{"confirmed", func(r *reader) {}, codes.OK},
		{"unbound", func(r *reader) { r.cp.Binding.Generation = "other" }, codes.FailedPrecondition},
		{"consumer", func(r *reader) { r.cp.Binding.ConsumerID = "other" }, codes.FailedPrecondition},
		{"missing", func(r *reader) { r.err = intake.ErrMissing }, codes.FailedPrecondition},
		{"cluster", func(r *reader) { r.cp.Binding.Source.ClusterID = "other" }, codes.FailedPrecondition},
		{"source", func(r *reader) { r.cp.Binding.Source.TopicID[0] = 2 }, codes.FailedPrecondition},
		{"partition", func(r *reader) { r.cp.Partition = 1 }, codes.FailedPrecondition},
		{"topic", func(r *reader) { r.cp.Binding.Source.Topic = "other" }, codes.FailedPrecondition},
		{"negative", func(r *reader) { r.cp.Next = -1 }, codes.FailedPrecondition},
		{"legacy", func(r *reader) { r.err = intake.ErrUnbound }, codes.FailedPrecondition},
		{"sql", func(r *reader) { r.err = errors.New("private DSN") }, codes.Unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &reader{cp: checkpoint()}
			tc.edit(r)
			out, err := progressrpc.NewServer(r).ReadProgress(context.Background(), request())
			if status.Code(err) != tc.code {
				t.Fatal(out, err)
			}
			if tc.code == codes.OK {
				if out.NextOffset == nil || *out.NextOffset != 41 || out.ConsumerId != "stable-intake" || out.TopicId[0] != 1 {
					t.Fatal(out)
				}
			} else if out != nil || strings.Contains(err.Error(), "DSN") {
				t.Fatal(out, err)
			}
		})
	}
	r := &reader{}
	for _, edit := range []func(*pb.ReadProgressRequest){func(q *pb.ReadProgressRequest) { q.ConsumerId = "" }, func(q *pb.ReadProgressRequest) { q.ConsumerId = strings.Repeat("a", 257) }, func(q *pb.ReadProgressRequest) { q.Generation = "" }, func(q *pb.ReadProgressRequest) { q.Generation = strings.Repeat("g", 257) }, func(q *pb.ReadProgressRequest) { q.ClusterId = " " }, func(q *pb.ReadProgressRequest) { q.Topic = "" }, func(q *pb.ReadProgressRequest) { q.TopicId = make([]byte, 16) }, func(q *pb.ReadProgressRequest) { q.TopicId = nil }, func(q *pb.ReadProgressRequest) { q.Partition = -1 }} {
		q := request()
		edit(q)
		if _, err := progressrpc.NewServer(r).ReadProgress(context.Background(), q); status.Code(err) != codes.InvalidArgument {
			t.Fatal(err)
		}
	}
	if r.calls != 0 {
		t.Fatal("invalid request reached storage")
	}
}
func TestDeadlineAndMTLS(t *testing.T) {
	ca := newAuthority(t)
	server := ca.issue(t, nil, true, false)
	for _, allowed := range []bool{true, false} {
		uri := "spiffe://test/cleaner"
		if !allowed {
			uri = "spiffe://test/matcher"
		}
		client := ca.issue(t, []string{uri}, false, false)
		r := &reader{cp: checkpoint()}
		opts, err := progressrpc.ServerOptions(server.cert, server.key, ca.pem, "spiffe://test/cleaner")
		if err != nil {
			t.Fatal(err)
		}
		conn := grpctest.StartWithClientOptions(t, func(g *grpc.Server) { pb.RegisterNotificationsProgressServer(g, progressrpc.NewServer(r)) }, []grpc.DialOption{grpc.WithTransportCredentials(clientCreds(t, client, ca.pem)), grpc.WithAuthority("market.test")}, opts...)
		c := progressrpc.NewClient(conn)
		_, err = c.ReadProgress(context.Background(), request())
		if !allowed {
			if status.Code(err) != codes.PermissionDenied || r.calls != 0 {
				t.Fatal(err, r.calls)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		oversized := request()
		oversized.ConsumerId = strings.Repeat("a", progressrpc.MaxMessageBytes+1)
		if _, err = c.ReadProgress(context.Background(), oversized); status.Code(err) != codes.ResourceExhausted {
			t.Fatal(err)
		}
		r.wait = true
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		_, err = c.ReadProgress(ctx, request())
		cancel()
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatal(err)
		}
	}
}

func TestServerBudget(t *testing.T) {
	r := &reader{wait: true}
	start := time.Now()
	out, err := progressrpc.NewServer(r).ReadProgress(context.Background(), request())
	if out != nil || status.Code(err) != codes.DeadlineExceeded || time.Since(start) > 2*progressrpc.Budget {
		t.Fatal(out, err)
	}
}

type cancelReader struct{ started, finished chan struct{} }

func (r *cancelReader) ReadProgress(ctx context.Context, _ intake.Binding, _ int32) (intake.Progress, error) {
	close(r.started)
	<-ctx.Done()
	close(r.finished)
	return intake.Progress{}, ctx.Err()
}
func TestStopCancelsAndJoinsRead(t *testing.T) {
	ca := newAuthority(t)
	server := ca.issue(t, nil, true, false)
	client := ca.issue(t, []string{"spiffe://test/cleaner"}, false, false)
	opts, err := progressrpc.ServerOptions(server.cert, server.key, ca.pem, "spiffe://test/cleaner")
	if err != nil {
		t.Fatal(err)
	}
	r := &cancelReader{make(chan struct{}), make(chan struct{})}
	var srv *grpc.Server
	conn := grpctest.StartWithClientOptions(t, func(g *grpc.Server) { srv = g; pb.RegisterNotificationsProgressServer(g, progressrpc.NewServer(r)) }, []grpc.DialOption{grpc.WithTransportCredentials(clientCreds(t, client, ca.pem)), grpc.WithAuthority("market.test")}, opts...)
	done := make(chan error, 1)
	go func() {
		_, err := progressrpc.NewClient(conn).ReadProgress(context.Background(), request())
		done <- err
	}()
	select {
	case <-r.started:
	case <-time.After(2 * time.Second):
		t.Fatal("read did not start")
	}
	srv.Stop()
	select {
	case <-r.finished:
	default:
		t.Fatal("Stop returned before SQL reader joined")
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("stopped RPC confirmed progress")
		}
	case <-time.After(time.Second):
		t.Fatal("RPC not cancelled")
	}
}
