package projection_test

import (
	"context"
	"errors"
	"fmt"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/projection"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/snapshotload"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestReplayInvalidBeforeStorage(t *testing.T) {
	s := projection.New(nil, "replay")
	for _, events := range [][][]byte{nil, {wire(1, 2, 1, "active")}, {wire(1, 2, 1, "active"), wire(3, 3, 2, "active")}, {[]byte(`{}`)}} {
		if err := s.ApplyReplay(context.Background(), events); err == nil {
			t.Fatal("accepted invalid transaction")
		}
	}
}

const replaySnapshot = "a4444444-4444-4444-8444-444444444444"
const replaySub = "00000000-0000-4000-8000-000000000001"
const highC uint64 = 18446744073709551600

type snapshotSource struct {
	page *pb.SnapshotPageResponse
	next *pb.SnapshotPageResponse
}

func (s snapshotSource) BuildSnapshot(context.Context, *pb.BuildSnapshotRequest) (*pb.SnapshotPageResponse, error) {
	return proto.Clone(s.page).(*pb.SnapshotPageResponse), nil
}
func (s snapshotSource) ReadSnapshotPage(context.Context, *pb.ReadSnapshotPageRequest) (*pb.SnapshotPageResponse, error) {
	if s.next == nil {
		return nil, errors.New("interrupted")
	}
	return proto.Clone(s.next).(*pb.SnapshotPageResponse), nil
}
func snapshotRow(n int, state string) *pb.SnapshotSubscription {
	p := "-1.500000000000000001"
	r := &pb.SnapshotSubscription{SubscriptionId: fmt.Sprintf("00000000-0000-4000-8000-%012d", n), OwnerId: replaySnapshot, Pair: "BTCUSDT", Threshold: "999999999999999999.123456789012345678", Direction: "up", CreationPrice: "0.000000000000000001", CreationTradeTime: "2026-10-29T09:00:00.123456789Z", Percent: &p, Stale: true, StreamUnconfirmed: true, GapDetected: true, State: state, Version: 7, Activation: "latest_price", Workflow: &pb.SnapshotWorkflow{TriggerId: replaySnapshot, Version: 3}}
	if state == "delivery_blocked" {
		r.Blocked = &pb.SnapshotBlocked{Reason: "channel_unavailable", BindingVersion: 13}
	}
	return r
}
func loadGeneration(t *testing.T, pool *pgxpool.Pool, id string, c uint64, rs ...*pb.SnapshotSubscription) {
	t.Helper()
	page := &pb.SnapshotPageResponse{SnapshotId: id, BoundaryC: c, ExpiresAt: time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano), Subscriptions: rs}
	_, err := snapshotload.New(pool, snapshotSource{page: page}).Load(context.Background(), &pb.BuildSnapshotRequest{RequestId: t.Name(), TtlMillis: 60000, PageSize: 1000})
	if err != nil {
		t.Fatal(err)
	}
}
func replayWire(pos, end, version uint64, state string) []byte {
	return []byte(strings.ReplaceAll(string(wire(pos, end, version, state)), `"sub"`, `"`+replaySub+`"`))
}
func TestSnapshotReplayAndKafkaIndependence(t *testing.T) {
	pool := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	other := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx := context.Background()
	s := projection.New(pool, t.Name())
	reader := projection.New(other, t.Name())
	// Real MAT-007 persists multiple pages. A partial generation cannot initialize.
	page := &pb.SnapshotPageResponse{SnapshotId: replaySnapshot, BoundaryC: highC, ExpiresAt: time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano), Subscriptions: []*pb.SnapshotSubscription{snapshotRow(1, "active"), snapshotRow(2, "pending_delivery")}, NextCursor: "next"}
	src := snapshotSource{page: page}
	req := &pb.BuildSnapshotRequest{RequestId: t.Name(), TtlMillis: 60000, PageSize: 2}
	if _, err := snapshotload.New(pool, src).Load(ctx, req); err == nil {
		t.Fatal("missing interruption")
	}
	if err := s.Initialize(ctx, replaySnapshot); !errors.Is(err, projection.ErrIncomplete) {
		t.Fatal(err)
	}
	cp, err := s.Checkpoint(ctx)
	if err != nil || cp.C != 0 {
		t.Fatal(cp, err)
	}
	src.next = proto.Clone(page).(*pb.SnapshotPageResponse)
	src.next.NextCursor = ""
	src.next.Subscriptions = []*pb.SnapshotSubscription{snapshotRow(3, "sending"), snapshotRow(4, "delivery_blocked")}
	if _, err := snapshotload.New(pool, src).Load(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := s.Initialize(ctx, replaySnapshot); err != nil {
		t.Fatal(err)
	}
	count := 0
	cp, err = reader.Walk(ctx, func(im projection.Image) error {
		count++
		want := snapshotRow(count, []string{"active", "pending_delivery", "sending", "delivery_blocked"}[count-1])
		if im.Change != nil || !proto.Equal(im.Snapshot, want) {
			t.Fatalf("lost snapshot fields: %v", im)
		}
		return nil
	})
	if err != nil || count != 4 || cp.C != highC || cp.NextOffset != 0 {
		t.Fatal(cp, count, err)
	}
	if _, err := reader.Get(ctx, replaySub); !errors.Is(err, projection.ErrSnapshotImage) {
		t.Fatal(err)
	}
	// Old/equal subscription versions advance C without changing the snapshot.
	for i, v := range []uint64{6, 7} {
		pos := highC + uint64(i) + 1
		if err := s.ApplyReplay(ctx, [][]byte{replayWire(pos, pos, v, "active")}); err != nil {
			t.Fatal(err)
		}
	}
	im, err := reader.ReadImage(ctx, replaySub)
	if err != nil || !proto.Equal(im.Snapshot, snapshotRow(1, "active")) {
		t.Fatal(im, err)
	}
	events := [][]byte{replayWire(highC+3, highC+4, 8, "pending_delivery"), replayWire(highC+4, highC+4, 9, "cancelled")}
	// Concurrent retries from two independent pools simulate lost commit responses.
	var wg sync.WaitGroup
	for _, st := range []*projection.Store{s, reader} {
		wg.Go(func() {
			if err := st.ApplyReplay(ctx, events); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if err := reader.Initialize(ctx, replaySnapshot); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyReplay(ctx, events); err != nil {
		t.Fatal(err)
	}
	im, err = reader.ReadImage(ctx, replaySub)
	if err != nil || im.Snapshot != nil || im.Change.State != "cancelled" || im.Change.SubscriptionVersion != 9 {
		t.Fatal(im, err)
	}
	cp, err = reader.Checkpoint(ctx)
	if err != nil || cp.C != highC+4 || cp.NextOffset != 0 {
		t.Fatal(cp, err)
	}
	var topic *string
	if err := pool.QueryRow(ctx, `SELECT topic FROM app.matcher_checkpoints WHERE projection_id=$1`, t.Name()).Scan(&topic); err != nil || topic != nil {
		t.Fatal(topic, err)
	}
	// Reject gap, overlapping and partial whole-transaction inputs, preserving C.
	for _, events := range [][][]byte{
		{replayWire(highC+6, highC+6, 10, "active")},
		{replayWire(highC+4, highC+5, 10, "active"), replayWire(highC+5, highC+5, 10, "active")},
		{replayWire(highC+5, highC+6, 10, "active")},
	} {
		if err := s.ApplyReplay(ctx, events); err == nil {
			t.Fatal("accepted gap/partial")
		}
	}
	// Kafka duplicate only advances the real transport prefix; never restores active.
	batch := projection.Batch{Topic: "changes", NextOffset: 42, Events: [][]byte{replayWire(highC+1, highC+1, 6, "active")}}
	ack := func(_ context.Context, n int64) error {
		if n != 42 {
			t.Error(n)
		}
		return nil
	}
	if err := reader.Handle(ctx, batch, ack); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyReplay(ctx, [][]byte{replayWire(highC+5, highC+5, 8, "active")}); err != nil {
		t.Fatal(err)
	}
	cp, err = s.Checkpoint(ctx)
	if err != nil || cp.C != highC+5 || cp.NextOffset != 42 {
		t.Fatal(cp, err)
	}
	im, err = s.ReadImage(ctx, replaySub)
	if err != nil || im.Change.State != "cancelled" {
		t.Fatal(im, err)
	}
	// A new subscription after C is included, and newer workflow state survives.
	create := []byte(strings.ReplaceAll(string(replayWire(highC+6, highC+6, 1, "sending")), replaySub, "00000000-0000-4000-8000-000000000005"))
	if err := s.ApplyReplay(ctx, [][]byte{create}); err != nil {
		t.Fatal(err)
	}
	count = 0
	cp, err = s.Walk(ctx, func(im projection.Image) error { count++; return nil })
	if err != nil || count != 5 || cp.C != highC+6 {
		t.Fatal(count, cp, err)
	}
	// An unrelated live projection cannot be silently replaced.
	live := projection.New(pool, t.Name()+"-live")
	if err := live.Handle(ctx, projection.Batch{Topic: "live", NextOffset: 1, Events: [][]byte{wire(1, 1, 1, "active")}}, func(context.Context, int64) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := live.Initialize(ctx, replaySnapshot); !errors.Is(err, projection.ErrProjectionExists) {
		t.Fatal(err)
	}
	isolated := projection.New(pool, t.Name()+"-other")
	if _, err := isolated.ReadImage(ctx, replaySub); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal(err)
	}
}

func TestReplayRollback(t *testing.T) {
	pool := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx := context.Background()
	id := "a5555555-5555-4555-8555-555555555555"
	loadGeneration(t, pool, id, 0, snapshotRow(1, "delivery_blocked"))
	s := projection.New(pool, t.Name())
	if err := s.Initialize(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyReplay(ctx, [][]byte{replayWire(1, 1, 8, "cancelled")}); err == nil {
		t.Fatal("missing injected rollback")
	}
	recovered := projection.New(postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL"), t.Name())
	recovery, err := recovered.Recover(ctx)
	if err != nil || recovery.SnapshotID != id || recovery.Checkpoint.C != 0 || recovery.Checkpoint.NextOffset != 0 {
		t.Fatal(recovery, err)
	}
	im, err := recovered.ReadImage(ctx, replaySub)
	if err != nil || !proto.Equal(im.Snapshot, snapshotRow(1, "delivery_blocked")) {
		t.Fatal(im, err)
	}
}

func TestEmptyInitializationAndConsistentWalk(t *testing.T) {
	pool := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	other := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx := context.Background()
	id := "a6666666-6666-4666-8666-666666666666"
	loadGeneration(t, pool, id, 0)
	s := projection.New(pool, t.Name())
	reader := projection.New(other, t.Name())
	var wg sync.WaitGroup
	for _, st := range []*projection.Store{s, reader} {
		wg.Go(func() {
			if err := st.Initialize(ctx, id); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	cp, err := s.Walk(ctx, func(projection.Image) error { t.Fatal("empty snapshot had row"); return nil })
	if err != nil || cp.C != 0 {
		t.Fatal(cp, err)
	}
	for pos := uint64(1); pos <= 2; pos++ {
		events := [][]byte{[]byte(strings.ReplaceAll(string(replayWire(pos, pos, 1, "active")), replaySub, fmt.Sprintf("sub-%d", pos)))}
		if err := s.ApplyReplay(ctx, events); err != nil {
			t.Fatal(err)
		}
	}
	n := 0
	cp, err = s.Walk(ctx, func(im projection.Image) error {
		n++
		if n == 1 {
			return reader.ApplyReplay(ctx, [][]byte{[]byte(strings.ReplaceAll(string(replayWire(3, 3, 1, "active")), replaySub, "sub-3"))})
		}
		return nil
	})
	if err != nil || cp.C != 2 || n != 2 {
		t.Fatal(cp, n, err)
	}
	lost := errors.New("callback stopped")
	if _, err = s.Walk(ctx, func(projection.Image) error { return lost }); !errors.Is(err, lost) {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.Initialize(cancelled, id); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := s.ApplyReplay(cancelled, [][]byte{replayWire(4, 4, 1, "active")}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := s.Initialize(ctx, "missing"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal(err)
	}
	loadGeneration(t, pool, "a8888888-8888-4888-8888-888888888888", 0)
	if err := s.Initialize(ctx, "a8888888-8888-4888-8888-888888888888"); !errors.Is(err, projection.ErrProjectionExists) {
		t.Fatal(err)
	}
}

func TestInitializeKeysetPages(t *testing.T) {
	pool := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx := context.Background()
	id := "a7777777-7777-4777-8777-777777777777"
	rows := make([]*pb.SnapshotSubscription, 1001)
	for i := range rows {
		rows[i] = snapshotRow(i+1, "active")
	}
	page := &pb.SnapshotPageResponse{SnapshotId: id, BoundaryC: 42, ExpiresAt: time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano), Subscriptions: rows[:1000], NextCursor: "last"}
	next := proto.Clone(page).(*pb.SnapshotPageResponse)
	next.Subscriptions = rows[1000:]
	next.NextCursor = ""
	_, err := snapshotload.New(pool, snapshotSource{page: page, next: next}).Load(ctx, &pb.BuildSnapshotRequest{RequestId: t.Name(), TtlMillis: 60000, PageSize: 1000})
	if err != nil {
		t.Fatal(err)
	}
	s := projection.New(pool, t.Name())
	if err := s.Initialize(ctx, id); err != nil {
		t.Fatal(err)
	}
	n := 0
	cp, err := s.Walk(ctx, func(im projection.Image) error {
		if !proto.Equal(im.Snapshot, rows[n]) {
			t.Fatal("page boundary lost row")
		}
		n++
		return nil
	})
	if err != nil || cp.C != 42 || n != 1001 {
		t.Fatal(cp, n, err)
	}
}

func TestInitializeRollback(t *testing.T) {
	pool := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx := context.Background()
	id := "a9999999-9999-4999-8999-999999999999"
	loadGeneration(t, pool, id, 19, snapshotRow(1, "active"), snapshotRow(2, "sending"))
	s := projection.New(pool, t.Name())
	if err := s.Initialize(ctx, id); err == nil {
		t.Fatal("missing initialization failure")
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM app.matcher_checkpoints WHERE projection_id=$1`, t.Name()).Scan(&n); err != nil || n != 0 {
		t.Fatal(n, err)
	}
	if _, err := s.ReadImage(ctx, replaySub); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal(err)
	}
}
