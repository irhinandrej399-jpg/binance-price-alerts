package snapshotrpc_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	rpc "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/snapshotrpc"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/grpctest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const matcherURI = "spiffe://binance.test/matcher"
const snapshotID = "00000000-0000-4000-8000-000000000001"
const rowID = "00000000-0000-4000-8000-000000000002"
const otherID = "00000000-0000-4000-8000-000000000003"

var expiry = time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
var snap = sub.Snapshot{ID: snapshotID, C: 9007199254740993, ExpiresAt: expiry}

type store struct {
	build func(context.Context, string, string, time.Duration) (sub.Snapshot, error)
	page  func(context.Context, string, string, string, int) (sub.SnapshotPage, error)
}

func (s store) BuildRequest(c context.Context, o, r string, d time.Duration) (sub.Snapshot, error) {
	return s.build(c, o, r, d)
}
func (s store) Page(c context.Context, o, id, a string, n int) (sub.SnapshotPage, error) {
	return s.page(c, o, id, a, n)
}
func connect(t *testing.T, s *rpc.Server, identity string, extra ...grpc.ServerOption) *grpc.ClientConn {
	t.Helper()
	ca := newAuthority(t)
	server := ca.issue(t, nil, true, false)
	client := ca.issue(t, []string{identity}, false, false)
	opts, e := rpc.ServerOptions(server.cert, server.key, ca.pem, matcherURI)
	if e != nil {
		t.Fatal(e)
	}
	return grpctest.StartWithClientOptions(t, func(g *grpc.Server) { pb.RegisterSubscriptionSnapshotsServer(g, s) }, []grpc.DialOption{grpc.WithTransportCredentials(clientCreds(t, client, ca.pem)), grpc.WithAuthority("market.test")}, append(opts, extra...)...)
}
func request() *pb.BuildSnapshotRequest {
	return &pb.BuildSnapshotRequest{RequestId: "bootstrap-1", TtlMillis: 60000, PageSize: 1}
}
func row() sub.Record {
	p, _ := price.Parse("999999999999999999.123456789012345678")
	percent := "-1.123456789012345678"
	return sub.Record{ID: rowID, OwnerID: otherID, Pair: "BTCUSDT", Threshold: p, CreationPrice: p, CreationTradeTime: expiry, Direction: price.Up, Percent: &percent, Stale: true, StreamUnconfirmed: true, GapDetected: true, State: domain.DeliveryBlocked, Version: 7, Activation: sub.LatestPrice, Workflow: &sub.Workflow{TriggerID: otherID, Version: 3}, Blocked: &sub.Blocked{Reason: domain.ChannelUnavailable, BindingVersion: 4}}
}
func TestPagesOverTLS(t *testing.T) {
	s := store{build: func(c context.Context, o, r string, d time.Duration) (sub.Snapshot, error) {
		if o != matcherURI || r != "bootstrap-1" || d != time.Minute {
			t.Error(o, r, d)
		}
		return snap, nil
	}, page: func(c context.Context, o, id, a string, n int) (sub.SnapshotPage, error) {
		if o != matcherURI || id != snapshotID || n != 1 {
			t.Error(o, id, n)
		}
		if a == "" {
			return sub.SnapshotPage{Snapshot: snap, Records: []sub.Record{row()}, Next: rowID}, nil
		}
		if a != rowID {
			t.Error(a)
		}
		return sub.SnapshotPage{Snapshot: snap}, nil
	}}
	client := rpc.NewClient(connect(t, rpc.NewServer(s), matcherURI))
	first, e := client.BuildSnapshot(context.Background(), request())
	if e != nil {
		t.Fatal(e)
	}
	want := &pb.SnapshotSubscription{SubscriptionId: rowID, OwnerId: otherID, Pair: "BTCUSDT", Threshold: row().Threshold.String(), Direction: "up", CreationPrice: row().CreationPrice.String(), CreationTradeTime: expiry.Format(time.RFC3339Nano), Percent: row().Percent, Stale: true, StreamUnconfirmed: true, GapDetected: true, State: "delivery_blocked", Version: 7, Activation: sub.LatestPrice, Workflow: &pb.SnapshotWorkflow{TriggerId: otherID, Version: 3}, Blocked: &pb.SnapshotBlocked{Reason: "channel_unavailable", BindingVersion: 4}}
	if first.SnapshotId != snapshotID || first.BoundaryC != snap.C || first.ExpiresAt != expiry.Format(time.RFC3339Nano) || first.NextCursor == "" || len(first.Subscriptions) != 1 || !proto.Equal(first.Subscriptions[0], want) {
		t.Fatal(first)
	}
	again, e := client.ReadSnapshotPage(context.Background(), &pb.ReadSnapshotPageRequest{SnapshotId: snapshotID, PageSize: 1})
	if e != nil || !proto.Equal(first, again) {
		t.Fatal(again, e)
	}
	last, e := client.ReadSnapshotPage(context.Background(), &pb.ReadSnapshotPageRequest{SnapshotId: snapshotID, Cursor: first.NextCursor, PageSize: 1})
	if e != nil || last.SnapshotId != snapshotID || last.BoundaryC != snap.C || last.ExpiresAt != first.ExpiresAt || last.NextCursor != "" || len(last.Subscriptions) != 0 {
		t.Fatal(last, e)
	}
	for _, r := range []*pb.ReadSnapshotPageRequest{{SnapshotId: snapshotID, Cursor: "price:7:42", PageSize: 1}, {SnapshotId: otherID, Cursor: first.NextCursor, PageSize: 1}, {SnapshotId: snapshotID, Cursor: strings.Repeat("a", 1000), PageSize: 1}, {SnapshotId: snapshotID, PageSize: 0}, {SnapshotId: "bad", PageSize: 1}} {
		if _, e := client.ReadSnapshotPage(context.Background(), r); status.Code(e) != codes.InvalidArgument {
			t.Fatal(e)
		}
	}
}
func TestErrorsAndAuthorization(t *testing.T) {
	for _, id := range []string{matcherURI, "spiffe://binance.test/notifications", "spiffe://binance.test/api"} {
		t.Run(id, func(t *testing.T) {
			var calls atomic.Int32
			s := store{build: func(context.Context, string, string, time.Duration) (sub.Snapshot, error) {
				calls.Add(1)
				return snap, nil
			}, page: func(context.Context, string, string, string, int) (sub.SnapshotPage, error) {
				calls.Add(1)
				return sub.SnapshotPage{Snapshot: snap}, nil
			}}
			client := rpc.NewClient(connect(t, rpc.NewServer(s), id))
			ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("service-identity", matcherURI, "authorization", "Bearer public"))
			_, e := client.BuildSnapshot(ctx, request())
			_, p := client.ReadSnapshotPage(ctx, &pb.ReadSnapshotPageRequest{SnapshotId: snapshotID, PageSize: 1})
			if id == matcherURI {
				if e != nil || p != nil || calls.Load() != 3 {
					t.Fatal(e, p, calls.Load())
				}
			} else if status.Code(e) != codes.PermissionDenied || status.Code(p) != codes.PermissionDenied || calls.Load() != 0 {
				t.Fatal(e, p, calls.Load())
			}
		})
	}
	for _, tc := range []struct {
		err  error
		code codes.Code
	}{{sub.ErrSnapshotLost, codes.FailedPrecondition}, {sub.ErrSnapshotRequestConflict, codes.AlreadyExists}, {sub.ErrInvalidInput, codes.InvalidArgument}, {errors.New("secret SQL"), codes.Unavailable}} {
		t.Run(tc.code.String(), func(t *testing.T) {
			s := store{build: func(context.Context, string, string, time.Duration) (sub.Snapshot, error) {
				return sub.Snapshot{}, tc.err
			}, page: func(context.Context, string, string, string, int) (sub.SnapshotPage, error) {
				return sub.SnapshotPage{}, tc.err
			}}
			c := rpc.NewClient(connect(t, rpc.NewServer(s), matcherURI))
			_, e := c.BuildSnapshot(context.Background(), request())
			_, p := c.ReadSnapshotPage(context.Background(), &pb.ReadSnapshotPageRequest{SnapshotId: snapshotID, PageSize: 1})
			if status.Code(e) != tc.code || status.Code(p) != tc.code || strings.Contains(e.Error(), "secret") {
				t.Fatal(e, p)
			}
		})
	}
}
func TestDeadlineCancellation(t *testing.T) {
	for _, method := range []string{"build", "page"} {
		for _, mode := range []string{"budget", "earlier", "cancel"} {
			t.Run(method+mode, func(t *testing.T) {
				entered := make(chan struct{})
				finished := make(chan error, 1)
				wait := func(c context.Context) {
					if d, ok := c.Deadline(); !ok || time.Until(d) > rpc.Budget {
						t.Error("unbounded")
					}
					close(entered)
					<-c.Done()
					finished <- c.Err()
				}
				s := store{build: func(c context.Context, _, _ string, _ time.Duration) (sub.Snapshot, error) {
					wait(c)
					return sub.Snapshot{}, c.Err()
				}, page: func(c context.Context, _, _, _ string, _ int) (sub.SnapshotPage, error) {
					wait(c)
					return sub.SnapshotPage{}, c.Err()
				}}
				client := rpc.NewClient(connect(t, rpc.NewServer(s), matcherURI))
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if mode == "earlier" {
					var stop context.CancelFunc
					ctx, stop = context.WithTimeout(ctx, 150*time.Millisecond)
					defer stop()
				}
				result := make(chan error, 1)
				go func() {
					var e error
					if method == "build" {
						_, e = client.BuildSnapshot(ctx, request())
					} else {
						_, e = client.ReadSnapshotPage(ctx, &pb.ReadSnapshotPageRequest{SnapshotId: snapshotID, PageSize: 1})
					}
					result <- e
				}()
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal("not entered")
				}
				want := codes.DeadlineExceeded
				if mode == "cancel" {
					cancel()
					want = codes.Canceled
				}
				if e := <-result; status.Code(e) != want {
					t.Fatal(e)
				}
				select {
				case <-finished:
				case <-time.After(time.Second):
					t.Fatal("storage not cancelled")
				}
			})
		}
	}
}
