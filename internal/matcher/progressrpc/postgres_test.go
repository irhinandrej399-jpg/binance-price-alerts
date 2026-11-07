package progressrpc_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	observed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	apb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/matcher/v1"
	f "github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/fencing"
	w "github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricework"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/progressrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/projection"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/snapshotload"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/grpctest"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type snapshot struct{ id string }

func (s snapshot) BuildSnapshot(context.Context, *apb.BuildSnapshotRequest) (*apb.SnapshotPageResponse, error) {
	return &apb.SnapshotPageResponse{SnapshotId: s.id, BoundaryC: 100, ExpiresAt: time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)}, nil
}
func (s snapshot) ReadSnapshotPage(context.Context, *apb.ReadSnapshotPageRequest) (*apb.SnapshotPageResponse, error) {
	panic("unexpected page")
}
func TestSQLProofOverMTLS(t *testing.T) {
	pool := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	other := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	ca := newAuthority(t)
	srv := ca.issue(t, nil, true, false)
	client := ca.issue(t, []string{"spiffe://test/cleaner"}, false, false)
	opts, err := progressrpc.ServerOptions(srv.cert, srv.key, ca.pem, "spiffe://test/cleaner")
	must(err)
	dial := func() *progressrpc.Client {
		return progressrpc.NewClient(grpctest.StartWithClientOptions(t, func(g *grpc.Server) {
			pb.RegisterMatcherProgressServer(g, progressrpc.NewServer(progressrpc.NewPostgres(other)))
		}, []grpc.DialOption{grpc.WithTransportCredentials(clientCreds(t, client, ca.pem)), grpc.WithAuthority("market.test")}, opts...))
	}
	rpc := dial()
	src := kafkasource.Identity{ClusterID: "cluster", Topic: "prices", TopicID: [16]byte{1}}
	q := request()
	q.ConsumerId = t.Name()
	check := func(q *pb.ReadProgressRequest, n int64, code codes.Code) {
		t.Helper()
		out, e := rpc.ReadProgress(ctx, q)
		if status.Code(e) != code {
			t.Fatal(out, e)
		}
		if code == codes.OK {
			if out.NextOffset == nil || *out.NextOffset != n || out.ConsumerId != q.ConsumerId || out.ClusterId != q.ClusterId || out.Topic != q.Topic || out.Partition != q.Partition {
				t.Fatal(out)
			}
		} else if out != nil {
			t.Fatal("error has proof", out)
		}
	}
	check(q, 0, codes.FailedPrecondition)
	grant, err := f.New(pool).Assign(ctx, f.Request{Identity: f.Identity{Environment: t.Name(), Group: "matcher", Topic: src.Topic, Partition: 2}, Scope: q.ConsumerId, ProjectionID: t.Name() + "-view", RequestID: "first"})
	must(err)
	store := w.NewSourceBound(pool, grant, src)
	p, _ := price.Parse("100")
	in := w.Input{Key: "one", ProjectionID: grant.ProjectionID, C: 999, Quote: observed.Observed{SchemaVersion: 1, Pair: "BTCUSDT", Price: &p, SourceID: "trade", Revision: observed.Revision{StreamID: "test", Generation: 1, Sequence: 1}, TradeTime: time.Now().UTC(), ReceivedAt: time.Now().UTC(), StreamState: observed.Live}, Position: &w.Position{Source: &src, Topic: src.Topic, Partition: 2, Offset: 40}, Candidates: []w.Candidate{{OwnerID: "owner", SubscriptionID: "sub", Version: 1}}}
	must(store.Accept(ctx, in))
	check(q, 0, codes.FailedPrecondition) // ledger + Pending never invent a Prefix
	if err = store.Complete(ctx, in.Key, nil, &w.Prefix{Source: src, Topic: src.Topic, Partition: 2, From: 40, Next: 41}, func(context.Context) error { return nil }); err == nil {
		t.Fatal("pending completed")
	}
	check(q, 0, codes.FailedPrecondition)
	must(store.Record(ctx, in.Key, "sub", w.Result{Outcome: "condition_false"}))
	must(store.Complete(ctx, in.Key, nil, &w.Prefix{Source: src, Topic: src.Topic, Partition: 2, From: 40, Next: 41}, func(context.Context) error { return nil }))
	check(q, 41, codes.OK)
	// Advance in an uncommitted transaction; an independent SQL reader keeps
	// reporting the committed Prefix, including after rollback.
	pendingTx, err := pool.Begin(ctx)
	must(err)
	defer pendingTx.Rollback(context.Background())
	_, err = pendingTx.Exec(ctx, `SELECT app.matcher_prices_advance($1,$2,$3,$4,$5,41,42)`, grant.Scope, src.Topic, int32(2), src.ClusterID, src.TopicID[:])
	must(err)
	check(q, 41, codes.OK)
	must(pendingTx.Rollback(ctx))
	check(q, 41, codes.OK)
	in.Key = "two"
	in.Position.Offset = 41
	must(store.Accept(ctx, in))
	check(q, 41, codes.OK)
	next, err := f.New(pool).Assign(ctx, f.Request{Identity: grant.Identity, Scope: grant.Scope, ProjectionID: t.Name() + "-new", RequestID: "takeover", ExpectedToken: grant.Token})
	must(err)
	must(f.New(pool).Revoke(ctx, next))
	rpc = dial() // new server/store/connection, no dependence on local token/assignment
	check(q, 41, codes.OK)
	q.ConsumerId += "-other"
	check(q, 0, codes.FailedPrecondition)
	q.ConsumerId = grant.Scope
	q.TopicId[0] = 2
	check(q, 0, codes.FailedPrecondition)
	q.TopicId[0] = 1
	q.Partition = 3
	check(q, 0, codes.FailedPrecondition)
	q.Partition = 2
	// Real uncommitted SQL cursor is not visible. This legacy scope has no binding,
	// so even after commit it remains unconfirmed.
	tx, err := pool.Begin(ctx)
	must(err)
	defer tx.Rollback(context.Background())
	_, err = tx.Exec(ctx, `INSERT INTO app.matcher_price_checkpoints(scope,topic,partition,next_offset) VALUES($1,'prices',2,999)`, t.Name()+"-legacy")
	must(err)
	q.ConsumerId = t.Name() + "-legacy"
	check(q, 0, codes.FailedPrecondition)
	must(tx.Rollback(ctx))
	check(q, 0, codes.FailedPrecondition)
	_, err = pool.Exec(ctx, `INSERT INTO app.matcher_price_checkpoints(scope,topic,partition,next_offset) VALUES($1,'prices',2,999)`, q.ConsumerId)
	must(err)
	check(q, 0, codes.FailedPrecondition)
	// A complete snapshot/replay with C=100 has no Kafka cursor.
	snapID := "a9560000-0000-4000-8000-000000000001"
	_, err = snapshotload.New(pool, snapshot{snapID}).Load(ctx, &apb.BuildSnapshotRequest{RequestId: t.Name(), TtlMillis: 60000, PageSize: 100})
	must(err)
	changes := src
	changes.Topic = "changes"
	changes.TopicID[0] = 3
	q = &pb.ReadProgressRequest{Kind: pb.ConsumerKind_CONSUMER_KIND_PROJECTION, ConsumerId: t.Name() + "-projection", ClusterId: changes.ClusterID, TopicId: append([]byte(nil), changes.TopicID[:]...), Topic: changes.Topic}
	proj := projection.NewSourceBound(pool, q.ConsumerId, nil, changes)
	must(proj.Initialize(ctx, snapID))
	check(q, 0, codes.FailedPrecondition)
	batch := projection.Batch{Source: changes, Topic: changes.Topic, NextOffset: 8, Events: [][]byte{wire(101)}}
	must(proj.Handle(ctx, batch, func(context.Context, int64) error { return nil }))
	check(q, 8, codes.OK)
	pendingTx, err = pool.Begin(ctx)
	must(err)
	defer pendingTx.Rollback(context.Background())
	_, err = pendingTx.Exec(ctx, `SELECT app.matcher_projection_advance($1,$2,0,$3,$4,102,9)`, q.ConsumerId, changes.Topic, changes.ClusterID, changes.TopicID[:])
	must(err)
	check(q, 8, codes.OK)
	must(pendingTx.Rollback(ctx))
	check(q, 8, codes.OK)
	// 8 != C=101; never max across different projections
	q.ConsumerId += "-slow"
	slow := projection.NewSourceBound(pool, q.ConsumerId, nil, changes)
	must(slow.Initialize(ctx, snapID))
	batch.NextOffset = 4
	must(slow.Handle(ctx, batch, func(context.Context, int64) error { return nil }))
	check(q, 4, codes.OK)
	// Hold an actual table lock: SQL must be cancelled by the caller deadline.
	lock, err := pool.Begin(ctx)
	must(err)
	defer lock.Rollback(context.Background())
	_, err = lock.Exec(ctx, `LOCK TABLE app.matcher_checkpoints IN ACCESS EXCLUSIVE MODE`)
	must(err)
	short, stop := context.WithTimeout(ctx, 50*time.Millisecond)
	out, err := rpc.ReadProgress(short, q)
	stop()
	if status.Code(err) != codes.DeadlineExceeded || out != nil {
		t.Fatal(out, err)
	}
	must(lock.Rollback(ctx))
	check(q, 4, codes.OK)
	// A real closed pool yields an explicit SQL fault without exposing internals.
	broken := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	broken.Close()
	out, err = progressrpc.NewServer(progressrpc.NewPostgres(broken)).ReadProgress(ctx, q)
	if out != nil || status.Code(err) != codes.Unavailable {
		t.Fatal(out, err)
	}
	// Reading/exporting does not mutate a pin or checkpoint.
	var c string
	must(pool.QueryRow(ctx, `SELECT journal_c::text FROM app.matcher_checkpoints WHERE projection_id=$1`, q.ConsumerId).Scan(&c))
	if c != "101" {
		t.Fatal(c)
	}
}
func wire(n int) []byte {
	return []byte(fmt.Sprintf(`{"event_id":"proof-%d","type":"subscription.changed","version":1,"occurred_at":"2026-10-29T10:00:00Z","correlation_id":"test","ordering_key":"sub","payload":{"schema_version":2,"owner_id":"a4444444-4444-4444-8444-444444444444","position":%d,"transaction_end":%d,"subscription_id":"sub","subscription_version":1,"state":"active","pair":"BTCUSDT","threshold":"100","direction":"up","activation":"latest_price","creation_snapshot":{"price":"99","trade_time":"2026-10-29T09:00:00Z"}}}`, n, n, n))
}
