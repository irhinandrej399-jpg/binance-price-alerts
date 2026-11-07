package progressrpc_test

import (
	"context"
	"errors"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/matcher/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/progressrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/sourcebinding"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/grpctest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
	"testing"
	"time"
)

type reader struct {
	cp    progressrpc.Checkpoint
	err   error
	calls int
	wait  bool
}

func (r *reader) Read(ctx context.Context, kind pb.ConsumerKind, id, topic string, partition int32) (progressrpc.Checkpoint, error) {
	r.calls++
	if d, ok := ctx.Deadline(); !ok || time.Until(d) > progressrpc.Budget {
		panic("missing SQL budget")
	}
	if r.wait {
		<-ctx.Done()
		return progressrpc.Checkpoint{}, ctx.Err()
	}
	return r.cp, r.err
}
func request() *pb.ReadProgressRequest {
	return &pb.ReadProgressRequest{Kind: pb.ConsumerKind_CONSUMER_KIND_PRICES, ConsumerId: "stable-scope", ClusterId: "cluster", TopicId: append([]byte{1}, make([]byte, 15)...), Topic: "prices", Partition: 2}
}
func checkpoint() progressrpc.Checkpoint {
	return progressrpc.Checkpoint{Source: &kafkasource.Identity{ClusterID: "cluster", Topic: "prices", TopicID: [16]byte{1}}, Topic: "prices", Partition: 2, NextOffset: 41}
}
func TestProofAndFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*reader)
		code codes.Code
	}{
		{"confirmed", func(r *reader) {}, codes.OK},
		{"unbound", func(r *reader) { r.cp.Source = nil }, codes.FailedPrecondition},
		{"source", func(r *reader) { r.cp.Source.TopicID[0] = 2 }, codes.FailedPrecondition},
		{"partition", func(r *reader) { r.cp.Partition = 1 }, codes.FailedPrecondition},
		{"topic", func(r *reader) { r.cp.Topic = "other" }, codes.FailedPrecondition},
		{"negative", func(r *reader) { r.cp.NextOffset = -1 }, codes.FailedPrecondition},
		{"legacy", func(r *reader) { r.err = sourcebinding.ErrUnbound }, codes.FailedPrecondition},
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
				if out.NextOffset == nil || *out.NextOffset != 41 || out.ConsumerId != "stable-scope" || out.TopicId[0] != 1 {
					t.Fatal(out)
				}
			} else if out != nil || strings.Contains(err.Error(), "DSN") {
				t.Fatal(out, err)
			}
		})
	}
	r := &reader{}
	for _, edit := range []func(*pb.ReadProgressRequest){func(q *pb.ReadProgressRequest) { q.ConsumerId = "" }, func(q *pb.ReadProgressRequest) { q.ConsumerId = strings.Repeat("a", 257) }, func(q *pb.ReadProgressRequest) { q.Kind = 99 }, func(q *pb.ReadProgressRequest) { q.TopicId = nil }, func(q *pb.ReadProgressRequest) { q.Partition = -1 }} {
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
		conn := grpctest.StartWithClientOptions(t, func(g *grpc.Server) { pb.RegisterMatcherProgressServer(g, progressrpc.NewServer(r)) }, []grpc.DialOption{grpc.WithTransportCredentials(clientCreds(t, client, ca.pem)), grpc.WithAuthority("market.test")}, opts...)
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

func (r *cancelReader) Read(ctx context.Context, _ pb.ConsumerKind, _ string, _ string, _ int32) (progressrpc.Checkpoint, error) {
	close(r.started)
	<-ctx.Done()
	close(r.finished)
	return progressrpc.Checkpoint{}, ctx.Err()
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
	conn := grpctest.StartWithClientOptions(t, func(g *grpc.Server) { srv = g; pb.RegisterMatcherProgressServer(g, progressrpc.NewServer(r)) }, []grpc.DialOption{grpc.WithTransportCredentials(clientCreds(t, client, ca.pem)), grpc.WithAuthority("market.test")}, opts...)
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
