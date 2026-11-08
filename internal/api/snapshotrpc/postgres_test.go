package snapshotrpc_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	rpc "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/snapshotrpc"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// timedStore records only clocks and operation outcomes, never connection data.
// It separates SQL rejection from the RPC's final host-clock expiry check.
type timedStore struct {
	rpc.Store
	t *testing.T
}

func (s timedStore) clocks(stage string, expiry time.Time, err error) {
	s.t.Helper()
	s.t.Logf("%s host=%s expires=%s outcome=%v", stage, time.Now().UTC().Format(time.RFC3339Nano), expiry.UTC().Format(time.RFC3339Nano), err)
}

func (s timedStore) BuildRequest(ctx context.Context, owner, request string, ttl time.Duration) (sub.Snapshot, error) {
	s.clocks("before SQL BuildRequest", time.Time{}, nil)
	out, err := s.Store.BuildRequest(ctx, owner, request, ttl)
	s.clocks("after SQL BuildRequest", out.ExpiresAt, err)
	return out, err
}
func (s timedStore) Page(ctx context.Context, owner, id, after string, n int) (sub.SnapshotPage, error) {
	out, err := s.Store.Page(ctx, owner, id, after, n)
	s.clocks("after SQL Page / before RPC expiry", out.Snapshot.ExpiresAt, err)
	return out, err
}

func fixture(t *testing.T, pool *pgxpool.Pool, state domain.State) sub.Record {
	t.Helper()
	ctx := context.Background()
	a, e := account.New(pool).Create(ctx, fmt.Sprintf("snapshot-rpc-%d@example.com", time.Now().UnixNano()), "hash")
	if e != nil {
		t.Fatal(e)
	}
	r := row()
	r.ID = a.ID
	r.OwnerID = a.ID
	r.State = state
	if r.Workflow != nil {
		r.Workflow.TriggerID = a.ID
	}
	if state == domain.Active {
		r.Workflow = nil
		r.Blocked = nil
	} else if state != domain.DeliveryBlocked {
		r.Blocked = nil
	}
	if e = sub.New(pool).Insert(ctx, r); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if _, e := pool.Exec(ctx, `DELETE FROM app.subscriptions WHERE owner_id=$1`, a.ID); e != nil {
			t.Error(e)
		}
		if _, e := pool.Exec(ctx, `DELETE FROM app.users WHERE id=$1`, a.ID); e != nil {
			t.Error(e)
		}
	})
	return r
}
func TestPostgresSnapshotRecoveryOverTLS(t *testing.T) {
	pool := postgrestest.Open(t)
	requireSnapshotClocks(t, pool)
	pool2 := postgrestest.Open(t)
	ctx := context.Background()
	storage := sub.NewSnapshots(pool)
	want := map[string]sub.Record{}
	for _, state := range []domain.State{domain.Active, domain.PendingDelivery, domain.Sending, domain.DeliveryBlocked} {
		r := fixture(t, pool, state)
		want[r.ID] = r
	}
	var lost atomic.Bool
	committed := make(chan *pb.SnapshotPageResponse, 1)
	firstClient := rpc.NewClient(connect(t, rpc.NewServer(timedStore{storage, t}), matcherURI, grpc.ChainUnaryInterceptor(func(c context.Context, r any, i *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		out, e := h(c, r)
		if e == nil && i.FullMethod == pb.SubscriptionSnapshots_BuildSnapshot_FullMethodName && lost.CompareAndSwap(false, true) {
			committed <- proto.Clone(out.(*pb.SnapshotPageResponse)).(*pb.SnapshotPageResponse)
			return nil, status.Error(codes.Unavailable, "injected lost response after commit")
		}
		return out, e
	})))
	req := request()
	req.RequestId = fmt.Sprintf("lost-%d", time.Now().UnixNano())
	if _, e := firstClient.BuildSnapshot(ctx, req); status.Code(e) != codes.Unavailable {
		t.Fatal(e)
	}
	late := fixture(t, pool, domain.Active) // New commit must not alter recovered frozen pages.
	client := rpc.NewClient(connect(t, rpc.NewServer(timedStore{sub.NewSnapshots(pool2), t}), matcherURI))
	first, e := client.BuildSnapshot(ctx, req)
	if e != nil || !proto.Equal(first, <-committed) {
		t.Fatal(first, e)
	}
	t.Cleanup(func() {
		if e := storage.Abandon(ctx, matcherURI, first.SnapshotId); e != nil {
			t.Error(e)
		}
	})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, e := client.BuildSnapshot(ctx, req)
			if e != nil || !proto.Equal(first, v) {
				t.Error(v, e)
			}
		}()
	}
	wg.Wait()
	var count int
	if e = pool.QueryRow(ctx, `SELECT count(*) FROM app.subscription_snapshot_requests WHERE owner=$1 AND request_id=$2`, matcherURI, req.RequestId).Scan(&count); e != nil || count != 1 {
		t.Fatal(count, e)
	}
	cursor := ""
	found := map[string]bool{}
	for {
		p, e := client.ReadSnapshotPage(ctx, &pb.ReadSnapshotPageRequest{SnapshotId: first.SnapshotId, Cursor: cursor, PageSize: 1})
		if e != nil || p.SnapshotId != first.SnapshotId || p.BoundaryC != first.BoundaryC || p.ExpiresAt != first.ExpiresAt {
			t.Fatal(p, e)
		}
		for _, r := range p.Subscriptions {
			if r.SubscriptionId == late.ID {
				t.Fatal("mixed snapshot boundary", r)
			}
			if expected, ok := want[r.SubscriptionId]; ok {
				if r.OwnerId != expected.OwnerID || r.Threshold != expected.Threshold.String() || r.State != string(expected.State) || r.Version != expected.Version || r.CreationPrice != expected.CreationPrice.String() || r.Percent == nil || *r.Percent != *expected.Percent {
					t.Fatal(r)
				}
				if (r.Workflow == nil) != (expected.Workflow == nil) || (r.Blocked == nil) != (expected.Blocked == nil) {
					t.Fatal(r)
				}
				found[r.SubscriptionId] = true
			}
		}
		if p.NextCursor == "" {
			break
		}
		cursor = p.NextCursor
	}
	if len(found) != len(want) {
		t.Fatal(found)
	}
	changed := proto.Clone(req).(*pb.BuildSnapshotRequest)
	changed.TtlMillis++
	if _, e = client.BuildSnapshot(ctx, changed); status.Code(e) != codes.AlreadyExists {
		t.Fatal(e)
	}
	if _, e = client.ReadSnapshotPage(ctx, &pb.ReadSnapshotPageRequest{SnapshotId: otherID, PageSize: 1}); status.Code(e) != codes.FailedPrecondition {
		t.Fatal(e)
	}
	foreign, e := storage.BuildRequest(ctx, "other-matcher", req.RequestId, time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	defer storage.Abandon(ctx, "other-matcher", foreign.ID)
	if _, e = client.ReadSnapshotPage(ctx, &pb.ReadSnapshotPageRequest{SnapshotId: foreign.ID, PageSize: 1}); status.Code(e) != codes.FailedPrecondition {
		t.Fatal(e)
	}
	if e = storage.Abandon(ctx, matcherURI, first.SnapshotId); e != nil {
		t.Fatal(e)
	}
	if _, e = client.BuildSnapshot(ctx, req); status.Code(e) != codes.FailedPrecondition {
		t.Fatal(e)
	}
	if _, e = client.ReadSnapshotPage(ctx, &pb.ReadSnapshotPageRequest{SnapshotId: first.SnapshotId, PageSize: 1}); status.Code(e) != codes.FailedPrecondition {
		t.Fatal(e)
	}
	short := request()
	short.RequestId = fmt.Sprintf("expire-%d", time.Now().UnixNano())
	short.TtlMillis = expiryTestTTL.Milliseconds()
	p, e := client.BuildSnapshot(ctx, short)
	if e != nil {
		t.Fatal(e)
	}
	until, e := time.Parse(time.RFC3339Nano, p.ExpiresAt)
	if e != nil {
		t.Fatal(e)
	}
	<-time.After(time.Until(until) + time.Millisecond)
	if _, e = client.ReadSnapshotPage(ctx, &pb.ReadSnapshotPageRequest{SnapshotId: p.SnapshotId, PageSize: 1}); status.Code(e) != codes.FailedPrecondition {
		t.Fatal(e)
	}
	if _, e = client.BuildSnapshot(ctx, short); status.Code(e) != codes.FailedPrecondition {
		t.Fatal(e)
	}
}
func TestPostgresCanceledBuildOverTLS(t *testing.T) {
	pool := postgrestest.Open(t)
	requireSnapshotClocks(t, pool)
	ctx := context.Background()
	conn, e := pool.Acquire(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Release()
	if _, e = conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, sub.RetentionLock); e != nil {
		t.Fatal(e)
	}
	defer conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, sub.RetentionLock)
	client := rpc.NewClient(connect(t, rpc.NewServer(timedStore{sub.NewSnapshots(pool), t}), matcherURI))
	req := request()
	req.RequestId = fmt.Sprintf("cancel-%d", time.Now().UnixNano())
	limited, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	if _, e = client.BuildSnapshot(limited, req); status.Code(e) != codes.DeadlineExceeded {
		t.Fatal(e)
	}
	if _, e = conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, sub.RetentionLock); e != nil {
		t.Fatal(e)
	}
	// Retry on the same key succeeds after the cancelled lock wait; no partial resource.
	p, e := client.BuildSnapshot(ctx, req)
	if e != nil {
		t.Fatal(e)
	}
	defer sub.NewSnapshots(pool).Abandon(ctx, matcherURI, p.SnapshotId)
}

// clockDomainStore models SQL timestamps from a VM behind the API host. It
// shifts only the timestamp crossing the Store/RPC boundary, never system
// clocks or durable rows. SQL BuildRequest and Page still run for real.
type clockDomainStore struct {
	rpc.Store
	offset time.Duration
	t      *testing.T
}

func (s clockDomainStore) Page(ctx context.Context, owner, id, after string, n int) (sub.SnapshotPage, error) {
	out, err := s.Store.Page(ctx, owner, id, after, n)
	s.t.Logf("clock-domain injection: SQL Page outcome=%v actual_expires=%s offset=%s", err, out.Snapshot.ExpiresAt.UTC().Format(time.RFC3339Nano), s.offset)
	out.Snapshot.ExpiresAt = out.Snapshot.ExpiresAt.Add(s.offset)
	return out, err
}
func TestPostgresSnapshotClockDomains(t *testing.T) {
	pool := postgrestest.Open(t)
	requireSnapshotClocks(t, pool)
	ctx := context.Background()
	storage := sub.NewSnapshots(pool)
	for _, offset := range []time.Duration{0, -2 * time.Minute} {
		t.Run(offset.String(), func(t *testing.T) {
			client := rpc.NewClient(connect(t, rpc.NewServer(clockDomainStore{storage, offset, t}), matcherURI))
			req := request()
			req.RequestId = fmt.Sprintf("clock-%d", time.Now().UnixNano())
			_, err := client.BuildSnapshot(ctx, req)
			want := codes.OK
			if offset != 0 {
				want = codes.FailedPrecondition
			}
			if status.Code(err) != want {
				t.Fatalf("RPC code=%s want=%s", status.Code(err), want)
			}
			// Both SQL operations succeed with the same request after the RPC failure:
			// this isolates rejection to the RPC host-clock check, not SQL resource loss.
			snap, err := storage.BuildRequest(ctx, matcherURI, req.RequestId, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			defer storage.Abandon(ctx, matcherURI, snap.ID)
			if _, err := storage.Page(ctx, matcherURI, snap.ID, "", 1); err != nil {
				t.Fatal(err)
			}
			before := time.Now().UTC()
			var database time.Time
			if err := pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&database); err != nil {
				t.Fatal("clock sample failed")
			}
			after := time.Now().UTC()
			prerequisite := snapshotClockError(before, database.Add(offset), after)
			t.Logf("modeled clock prerequisite: %v", prerequisite)
			if (prerequisite != nil) != (offset != 0) {
				t.Fatal("clock prerequisite did not distinguish the injected skew")
			}
		})
	}
}
