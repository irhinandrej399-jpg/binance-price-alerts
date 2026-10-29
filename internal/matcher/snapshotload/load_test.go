package snapshotload_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/snapshotrpc"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/projection"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/snapshotload"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/grpctest"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const sid = "11111111-1111-4111-8111-111111111111"

func row(n int, state string) *pb.SnapshotSubscription {
	percent := "1.5"
	r := &pb.SnapshotSubscription{SubscriptionId: fmt.Sprintf("00000000-0000-4000-8000-%012d", n), OwnerId: sid, Pair: "BTCUSDT", Threshold: "999999999999999999.123456789012345678", Direction: "up", CreationPrice: "0.000000000000000001", CreationTradeTime: "2026-10-29T09:00:00.123456789Z", Percent: &percent, Stale: true, StreamUnconfirmed: true, GapDetected: true, State: state, Version: 7, Activation: "latest_price", Workflow: &pb.SnapshotWorkflow{TriggerId: sid, Version: 3}}
	if state == "delivery_blocked" {
		r.Blocked = &pb.SnapshotBlocked{Reason: "channel_unavailable", BindingVersion: 0}
	}
	return r
}

type source struct {
	first *pb.SnapshotPageResponse
	next  func(context.Context, *pb.ReadSnapshotPageRequest) (*pb.SnapshotPageResponse, error)
}

func (s *source) BuildSnapshot(_ context.Context, r *pb.BuildSnapshotRequest) (*pb.SnapshotPageResponse, error) {
	if r.RequestId != "stable-request" || r.TtlMillis != 60000 {
		return nil, errors.New("changed request identity")
	}
	return proto.Clone(s.first).(*pb.SnapshotPageResponse), nil
}
func (s *source) ReadSnapshotPage(c context.Context, r *pb.ReadSnapshotPageRequest) (*pb.SnapshotPageResponse, error) {
	return s.next(c, r)
}
func first() *pb.SnapshotPageResponse {
	return &pb.SnapshotPageResponse{SnapshotId: sid, BoundaryC: 9007199254740993, ExpiresAt: time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano), Subscriptions: []*pb.SnapshotSubscription{row(1, "active"), row(2, "pending_delivery")}, NextCursor: "opaque-cursor"}
}
func req() *pb.BuildSnapshotRequest {
	return &pb.BuildSnapshotRequest{RequestId: "stable-request", TtlMillis: 60000, PageSize: 2}
}

func TestDurablePagesInterruptionRetryAndIsolation(t *testing.T) {
	pool := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	other := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx := context.Background()
	live := projection.New(pool, t.Name())
	// An unrelated existing working generation and checkpoint must survive bootstrap.
	if _, err := pool.Exec(ctx, `INSERT INTO app.matcher_checkpoints(projection_id,topic,partition,journal_c,next_offset) VALUES($1,'changes',0,42,73)`, t.Name()); err != nil {
		t.Fatal(err)
	}
	p := first()
	interrupted := errors.New("connection lost")
	src := &source{first: p}
	loader := snapshotload.New(pool, src)
	reader := snapshotload.New(other, src)
	src.next = func(_ context.Context, r *pb.ReadSnapshotPageRequest) (*pb.SnapshotPageResponse, error) {
		if r.SnapshotId != sid || r.Cursor != "opaque-cursor" || r.PageSize != 2 {
			t.Fatal(r)
		}
		info, err := reader.Info(ctx, sid)
		if err != nil || info.Complete || info.Count != 2 {
			t.Fatalf("partial: %+v %v", info, err)
		}
		return nil, interrupted
	}
	if _, err := loader.Load(ctx, req()); !errors.Is(err, interrupted) {
		t.Fatal(err)
	}
	if _, err := reader.Get(ctx, sid, row(1, "active").SubscriptionId); !errors.Is(err, snapshotload.ErrIncomplete) {
		t.Fatalf("partial exposed: %v", err)
	}
	src.next = func(_ context.Context, _ *pb.ReadSnapshotPageRequest) (*pb.SnapshotPageResponse, error) {
		last := proto.Clone(p).(*pb.SnapshotPageResponse)
		last.NextCursor = ""
		last.Subscriptions = []*pb.SnapshotSubscription{row(3, "sending"), row(4, "delivery_blocked")}
		return last, nil
	}
	for range 2 {
		generation, err := loader.Load(ctx, req())
		if err != nil || generation.SnapshotID != sid || generation.C != p.BoundaryC || generation.Count != 4 || !generation.Complete {
			t.Fatalf("complete: %+v %v", generation, err)
		}
	}
	// A retry can use new page boundaries without duplicating or dropping rows.
	src.first = proto.Clone(p).(*pb.SnapshotPageResponse)
	src.first.Subscriptions = append(src.first.Subscriptions, row(3, "sending"))
	src.next = func(context.Context, *pb.ReadSnapshotPageRequest) (*pb.SnapshotPageResponse, error) {
		last := proto.Clone(p).(*pb.SnapshotPageResponse)
		last.NextCursor = ""
		last.Subscriptions = []*pb.SnapshotSubscription{row(4, "delivery_blocked")}
		return last, nil
	}
	resized := req()
	resized.PageSize = 3
	g, err := reader.Load(ctx, resized)
	if err != nil || g.Count != 4 || g.ExpiresAt.Format(time.RFC3339Nano) != p.ExpiresAt {
		t.Fatalf("resized retry: %+v %v", g, err)
	}
	for i, state := range []string{"active", "pending_delivery", "sending", "delivery_blocked"} {
		want := row(i+1, state)
		got, err := reader.Get(ctx, sid, want.SubscriptionId)
		if err != nil || !proto.Equal(got, want) {
			t.Fatalf("after-image %d: %+v %v", i, got, err)
		}
	}
	cp, err := live.Checkpoint(ctx)
	if err != nil || cp.C != 42 || cp.NextOffset != 73 {
		t.Fatalf("live checkpoint: %+v %v", cp, err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM app.matcher_checkpoints WHERE projection_id=$1`, sid).Scan(&count); err != nil || count != 0 {
		t.Fatalf("created live checkpoint: %d %v", count, err)
	}
}
func TestEmptySnapshot(t *testing.T) {
	pool := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	p := first()
	p.SnapshotId = "22222222-2222-4222-8222-222222222222"
	p.Subscriptions = nil
	p.NextCursor = ""
	p.BoundaryC = 0
	l := snapshotload.New(pool, &source{first: p})
	for range 2 {
		g, err := l.Load(context.Background(), req())
		if err != nil || !g.Complete || g.Count != 0 || g.C != 0 {
			t.Fatalf("empty: %+v %v", g, err)
		}
	}
}
func TestRejectInvalidFirstPageBeforeStorage(t *testing.T) {
	for _, mutate := range []func(*pb.SnapshotPageResponse){
		func(p *pb.SnapshotPageResponse) { p.SnapshotId = "bad" },
		func(p *pb.SnapshotPageResponse) { p.ExpiresAt = time.Now().Add(-time.Second).Format(time.RFC3339Nano) },
		func(p *pb.SnapshotPageResponse) { p.Subscriptions[0].Threshold = "1e9" },
		func(p *pb.SnapshotPageResponse) { p.Subscriptions[0].Version = 0 },
		func(p *pb.SnapshotPageResponse) { p.Subscriptions[0].State = "delivered" },
		func(p *pb.SnapshotPageResponse) { p.Subscriptions[1].Workflow = nil },
		func(p *pb.SnapshotPageResponse) { p.Subscriptions[1] = p.Subscriptions[0] },
		func(p *pb.SnapshotPageResponse) { p.Subscriptions = nil },
	} {
		p := first()
		mutate(p)
		_, err := snapshotload.New(nil, &source{first: p}).Load(context.Background(), req())
		if !errors.Is(err, snapshotload.ErrInvalidSnapshot) {
			t.Fatalf("invalid page: %v", err)
		}
	}
}

func TestRejectInconsistentLaterPage(t *testing.T) {
	pool := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx := context.Background()
	for i, mutate := range []func(*pb.SnapshotPageResponse){
		func(p *pb.SnapshotPageResponse) { p.BoundaryC++ },
		func(p *pb.SnapshotPageResponse) { p.SnapshotId = sid },
		func(p *pb.SnapshotPageResponse) { p.ExpiresAt = time.Now().Add(2 * time.Hour).Format(time.RFC3339Nano) },
		func(p *pb.SnapshotPageResponse) { p.ExpiresAt = time.Now().Add(-time.Second).Format(time.RFC3339Nano) },
		func(p *pb.SnapshotPageResponse) { p.Subscriptions = []*pb.SnapshotSubscription{row(2, "active")} },
		func(p *pb.SnapshotPageResponse) { p.NextCursor = "opaque-cursor" },
	} {
		p := first()
		p.SnapshotId = fmt.Sprintf("44444444-4444-4444-8444-%012d", i)
		src := &source{first: p}
		src.next = func(context.Context, *pb.ReadSnapshotPageRequest) (*pb.SnapshotPageResponse, error) {
			next := proto.Clone(p).(*pb.SnapshotPageResponse)
			next.Subscriptions = []*pb.SnapshotSubscription{row(3, "active")}
			next.NextCursor = ""
			mutate(next)
			return next, nil
		}
		l := snapshotload.New(pool, src)
		if _, err := l.Load(ctx, req()); !errors.Is(err, snapshotload.ErrInvalidSnapshot) {
			t.Fatal(err)
		}
		g, err := l.Info(ctx, p.SnapshotId)
		if err != nil || g.Complete || g.Count != 2 {
			t.Fatalf("invalid later page committed: %+v %v", g, err)
		}
	}
}
func TestPageRollback(t *testing.T) {
	pool := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx := context.Background()
	p := first()
	p.SnapshotId = "33333333-3333-4333-8333-333333333333"
	p.NextCursor = ""
	l := snapshotload.New(pool, &source{first: p})
	if _, err := l.Load(ctx, req()); err == nil {
		t.Fatal("missing injected UPDATE failure")
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM app.matcher_snapshot_rows WHERE snapshot_id=$1`, p.SnapshotId).Scan(&count); err != nil || count != 0 {
		t.Fatalf("page rollback: %d %v", count, err)
	}
	if _, err := l.Info(ctx, p.SnapshotId); err == nil {
		t.Fatal("metadata escaped rollback")
	}
}
func TestConcurrentFullRetry(t *testing.T) {
	pool := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	other := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	p := first()
	p.SnapshotId = "55555555-5555-4555-8555-555555555555"
	p.NextCursor = ""
	done := make(chan error, 2)
	for _, l := range []*snapshotload.Loader{snapshotload.New(pool, &source{first: p}), snapshotload.New(other, &source{first: p})} {
		go func() { _, err := l.Load(context.Background(), req()); done <- err }()
	}
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	l := snapshotload.New(other, nil)
	g, err := l.Info(context.Background(), p.SnapshotId)
	if err != nil || !g.Complete || g.Count != 2 {
		t.Fatalf("concurrent: %+v %v", g, err)
	}
	// A conflicting after-image must not overwrite durable data on retry.
	changed := proto.Clone(p).(*pb.SnapshotPageResponse)
	changed.Subscriptions[1].Version++
	if _, err := snapshotload.New(pool, &source{first: changed}).Load(context.Background(), req()); !errors.Is(err, snapshotload.ErrInvalidSnapshot) {
		t.Fatal(err)
	}
	got, err := l.Get(context.Background(), p.SnapshotId, p.Subscriptions[1].SubscriptionId)
	if err != nil || !proto.Equal(got, p.Subscriptions[1]) {
		t.Fatalf("conflicting repeat changed row: %v %v", got, err)
	}
}

type rpcSource struct {
	pb.UnimplementedSubscriptionSnapshotsServer
	source *source
}

func (s rpcSource) BuildSnapshot(ctx context.Context, r *pb.BuildSnapshotRequest) (*pb.SnapshotPageResponse, error) {
	return s.source.BuildSnapshot(ctx, r)
}
func (s rpcSource) ReadSnapshotPage(ctx context.Context, r *pb.ReadSnapshotPageRequest) (*pb.SnapshotPageResponse, error) {
	return s.source.ReadSnapshotPage(ctx, r)
}

func TestRPCClientPages(t *testing.T) {
	pool := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	p := first()
	p.SnapshotId = "66666666-6666-4666-8666-666666666666"
	src := &source{first: p, next: func(ctx context.Context, r *pb.ReadSnapshotPageRequest) (*pb.SnapshotPageResponse, error) {
		if r.Cursor != p.NextCursor || r.SnapshotId != p.SnapshotId {
			return nil, status.Error(codes.InvalidArgument, "wrong cursor")
		}
		last := proto.Clone(p).(*pb.SnapshotPageResponse)
		last.NextCursor = ""
		last.Subscriptions = []*pb.SnapshotSubscription{row(3, "active")}
		last.Subscriptions[0].Percent = nil
		last.Subscriptions[0].Workflow = nil
		return last, nil
	}}
	conn := grpctest.Start(t, func(s *grpc.Server) { pb.RegisterSubscriptionSnapshotsServer(s, rpcSource{source: src}) })
	loader := snapshotload.New(pool, snapshotrpc.NewClient(conn))
	g, err := loader.Load(context.Background(), req())
	if err != nil || g.Count != 3 || !g.Complete {
		t.Fatalf("RPC pages: %+v %v", g, err)
	}
	got, err := loader.Get(context.Background(), p.SnapshotId, row(3, "active").SubscriptionId)
	if err != nil || got.Percent != nil || got.Workflow != nil {
		t.Fatalf("optional fields: %+v %v", got, err)
	}

}
func TestCanceledLoadStaysIncomplete(t *testing.T) {
	pool := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	p := first()
	p.SnapshotId = "77777777-7777-4777-8777-777777777777"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := &source{first: p, next: func(ctx context.Context, _ *pb.ReadSnapshotPageRequest) (*pb.SnapshotPageResponse, error) {
		cancel()
		return nil, ctx.Err()
	}}
	loader := snapshotload.New(pool, src)
	if _, err := loader.Load(ctx, req()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	g, err := loader.Info(context.Background(), p.SnapshotId)
	if err != nil || g.Complete || g.Count != 2 {
		t.Fatalf("cancel: %+v %v", g, err)
	}
}
