package replayrpc_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/auth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/journal"
	rpc "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/replayrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/session"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	changed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/subscriptionchanged"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func entry(id string, state domain.State) journal.Entry {
	p, _ := price.Parse("999999999999999999.123456789012345678")
	return journal.Entry{Metadata: event.Envelope{EventID: id, Type: changed.Type, Version: 1, OccurredAt: time.Now().UTC(), CorrelationID: id, OrderingKey: "sub-replay"}, Change: changed.Changed{SchemaVersion: 1, SubscriptionID: "sub-replay", SubscriptionVersion: 1, State: state, Pair: "BTCUSDT", Threshold: &p, Direction: price.Up, Activation: changed.LatestPrice, CreationSnapshot: &changed.Snapshot{Price: &p, TradeTime: time.Now().UTC()}}}
}
func appendBatch(t *testing.T, pool *pgxpool.Pool, states ...domain.State) (pgx.Tx, uint64) {
	t.Helper()
	ctx := context.Background()
	tx, e := pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { tx.Rollback(ctx) })
	entries := make([]journal.Entry, len(states))
	for i, s := range states {
		entries[i] = entry(fmt.Sprintf("replay-%d-%d", time.Now().UnixNano(), i), s)
	}
	end, e := journal.Append(ctx, tx, entries)
	if e != nil {
		t.Fatal(e)
	}
	return tx, end
}
func begin(t *testing.T, c *rpc.Client, id string, ttl int64) *pb.ReplayState {
	t.Helper()
	s, e := c.BeginReplay(context.Background(), &pb.BeginReplayRequest{SnapshotId: id, TtlMillis: ttl})
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func TestPostgresReplayRecovery(t *testing.T) {
	pool := postgrestest.Open(t)
	other := postgrestest.Open(t)
	ctx := context.Background()
	pins := sub.NewSnapshots(pool)
	snap, e := pins.Build(ctx, matcherURI, time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	defer pins.Abandon(ctx, matcherURI, snap.ID)
	var dropped atomic.Bool
	lost := rpc.NewClient(connect(t, rpc.NewServer(pins, pool), matcherURI, grpc.ChainUnaryInterceptor(func(c context.Context, r any, i *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		out, e := h(c, r)
		if e == nil && i.FullMethod == pb.SubscriptionReplay_BeginReplay_FullMethodName && dropped.CompareAndSwap(false, true) {
			return nil, status.Error(codes.Unavailable, "lost committed handoff")
		}
		return out, e
	})))
	if _, e = lost.BeginReplay(ctx, &pb.BeginReplayRequest{SnapshotId: snap.ID, TtlMillis: 60000}); status.Code(e) != codes.Unavailable {
		t.Fatal(e)
	}
	client := rpc.NewClient(connect(t, rpc.NewServer(sub.NewSnapshots(other), other), matcherURI))
	initial := begin(t, client, snap.ID, 60000)
	if initial.Progress != snap.C || initial.Issued != snap.C || initial.Head != snap.C {
		t.Fatal(initial)
	}
	if repeat := begin(t, client, snap.ID, 86400000); !proto.Equal(initial, repeat) {
		t.Fatal("retry extended pin", repeat)
	}
	tx, end := appendBatch(t, pool, domain.Active, domain.Cancelled)
	// A delayed concurrent commit is absent until it commits, then appears whole.
	pageReq := &pb.ReadReplayPageRequest{ResourceId: snap.ID, After: snap.C, MaxTransactions: 1}
	empty, e := client.ReadReplayPage(ctx, pageReq)
	if e != nil || len(empty.Events) != 0 || empty.Next != snap.C {
		t.Fatal(empty, e)
	}
	committed := make(chan error, 1)
	go func() { committed <- tx.Commit(ctx) }()
	if e = <-committed; e != nil {
		t.Fatal(e)
	}
	if _, e = client.AcknowledgeReplay(ctx, &pb.AcknowledgeReplayRequest{ResourceId: snap.ID, Boundary: end}); status.Code(e) != codes.InvalidArgument {
		t.Fatal("ack unissued", e)
	}
	first, e := client.ReadReplayPage(ctx, pageReq)
	if e != nil || len(first.Events) != 2 || first.Next != end || first.State.Head != end {
		t.Fatal(first, e)
	}
	for i, body := range first.Events {
		_, c, e := changed.Decode(body)
		if e != nil || c.Position != snap.C+uint64(i)+1 || c.TransactionEnd != end || c.Threshold.String() != "999999999999999999.123456789012345678" {
			t.Fatal(c, e)
		}
		if (i == 0 && c.State != domain.Active) || (i == 1 && c.State != domain.Cancelled) {
			t.Fatal(c)
		}
	}
	// Another transaction between pages does not alter the prior event bytes.
	tx2, end2 := appendBatch(t, pool, domain.Active)
	if e = tx2.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	again, e := client.ReadReplayPage(ctx, pageReq)
	if e != nil || again.Next != end || len(again.Events) != 2 || !bytes.Equal(again.Events[0], first.Events[0]) || !bytes.Equal(again.Events[1], first.Events[1]) {
		t.Fatal(again, e)
	}
	if _, e = client.AcknowledgeReplay(ctx, &pb.AcknowledgeReplayRequest{ResourceId: snap.ID, Boundary: end - 1}); status.Code(e) != codes.InvalidArgument {
		t.Fatal("partial ack", e)
	}
	ack := &pb.AcknowledgeReplayRequest{ResourceId: snap.ID, Boundary: end}
	var wg sync.WaitGroup
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, e := client.AcknowledgeReplay(ctx, ack)
			if e != nil || p.Progress != end {
				t.Error(p, e)
			}
		}()
	}
	wg.Wait()
	if _, e = client.AcknowledgeReplay(ctx, &pb.AcknowledgeReplayRequest{ResourceId: snap.ID, Boundary: snap.C}); status.Code(e) != codes.InvalidArgument {
		t.Fatal("rollback ack", e)
	}
	if _, e = client.ReadReplayPage(ctx, pageReq); status.Code(e) != codes.InvalidArgument {
		t.Fatal("cursor before durable progress", e)
	}
	second, e := client.ReadReplayPage(ctx, &pb.ReadReplayPageRequest{ResourceId: snap.ID, After: end, MaxTransactions: 1})
	if e != nil || second.Next != end2 || len(second.Events) != 1 {
		t.Fatal(second, e)
	}
	// Lose an acknowledgement response after durable SQL commit, retry on another API.
	var lostAck atomic.Bool
	ackClient := rpc.NewClient(connect(t, rpc.NewServer(pins, pool), matcherURI, grpc.ChainUnaryInterceptor(func(c context.Context, r any, i *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		out, e := h(c, r)
		if e == nil && i.FullMethod == pb.SubscriptionReplay_AcknowledgeReplay_FullMethodName && lostAck.CompareAndSwap(false, true) {
			return nil, status.Error(codes.Unavailable, "lost SQL ack")
		}
		return out, e
	})))
	ack.Boundary = end2
	if _, e = ackClient.AcknowledgeReplay(ctx, ack); status.Code(e) != codes.Unavailable {
		t.Fatal(e)
	}
	if p, e := client.AcknowledgeReplay(ctx, ack); e != nil || p.Progress != end2 {
		t.Fatal(p, e)
	}
	// Foreign owner is not readable/acknowledgeable; abandoning it cannot revoke it.
	foreign, e := pins.Build(ctx, "foreign", time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	defer pins.Abandon(ctx, "foreign", foreign.ID)
	if _, e = client.BeginReplay(ctx, &pb.BeginReplayRequest{SnapshotId: foreign.ID, TtlMillis: 1000}); status.Code(e) != codes.FailedPrecondition {
		t.Fatal(e)
	}
	if _, e = client.AcknowledgeReplay(ctx, &pb.AcknowledgeReplayRequest{ResourceId: foreign.ID}); status.Code(e) != codes.FailedPrecondition {
		t.Fatal(e)
	}
	if _, e = client.AbandonReplay(ctx, &pb.AbandonReplayRequest{ResourceId: foreign.ID}); e != nil {
		t.Fatal(e)
	}
	if _, e = pins.CheckPin(ctx, "foreign", foreign.ID); e != nil {
		t.Fatal(e)
	}
	for range 2 {
		if _, e = client.AbandonReplay(ctx, &pb.AbandonReplayRequest{ResourceId: snap.ID}); e != nil {
			t.Fatal(e)
		}
	}
	if _, e = client.ReadReplayPage(ctx, &pb.ReadReplayPageRequest{ResourceId: snap.ID, After: end2, MaxTransactions: 1}); status.Code(e) != codes.FailedPrecondition {
		t.Fatal(e)
	}
}
func TestPostgresExpiryAndCanceledHandoff(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	pins := sub.NewSnapshots(pool)
	client := rpc.NewClient(connect(t, rpc.NewServer(pins, pool), matcherURI))
	snap, e := pins.Build(ctx, matcherURI, time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	defer pins.Abandon(ctx, matcherURI, snap.ID)
	lock, e := pool.Acquire(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer lock.Release()
	if _, e = lock.Exec(ctx, `SELECT pg_advisory_lock($1)`, sub.RetentionLock); e != nil {
		t.Fatal(e)
	}
	defer lock.Exec(ctx, `SELECT pg_advisory_unlock($1)`, sub.RetentionLock)
	limited, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, e = client.BeginReplay(limited, &pb.BeginReplayRequest{SnapshotId: snap.ID, TtlMillis: 1000}); status.Code(e) != codes.DeadlineExceeded {
		t.Fatal(e)
	}
	if _, e = lock.Exec(ctx, `SELECT pg_advisory_unlock($1)`, sub.RetentionLock); e != nil {
		t.Fatal(e)
	}
	p := begin(t, client, snap.ID, 100)
	until, e := time.Parse(time.RFC3339Nano, p.ExpiresAt)
	if e != nil {
		t.Fatal(e)
	}
	<-time.After(time.Until(until) + time.Millisecond)
	if _, e = client.ReadReplayPage(ctx, &pb.ReadReplayPageRequest{ResourceId: snap.ID, After: p.Progress, MaxTransactions: 1}); status.Code(e) != codes.FailedPrecondition {
		t.Fatal(e)
	}
	if _, e = client.AcknowledgeReplay(ctx, &pb.AcknowledgeReplayRequest{ResourceId: snap.ID, Boundary: p.Progress}); status.Code(e) != codes.FailedPrecondition {
		t.Fatal(e)
	}
	if _, e = client.BeginReplay(ctx, &pb.BeginReplayRequest{SnapshotId: snap.ID, TtlMillis: 60000}); status.Code(e) != codes.FailedPrecondition {
		t.Fatal(e)
	}
}

// Read a real committed H, then commit another transaction before page SELECT.
// The first page must stop at H even if ReadAfter can already see the next batch.
type commitOnHead struct {
	*pgxpool.Pool
	tx   pgx.Tx
	once sync.Once
}
type headRow struct {
	pgx.Row
	db *commitOnHead
}

func (r headRow) Scan(dest ...any) error {
	e := r.Row.Scan(dest...)
	if e == nil {
		r.db.once.Do(func() { e = r.db.tx.Commit(context.Background()) })
	}
	return e
}
func (db *commitOnHead) QueryRow(c context.Context, q string, args ...any) pgx.Row {
	return headRow{db.Pool.QueryRow(c, q, args...), db}
}
func TestPostgresPageHeadBoundary(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	pins := sub.NewSnapshots(pool)
	snap, e := pins.Build(ctx, matcherURI, time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	defer pins.Abandon(ctx, matcherURI, snap.ID)
	normal := rpc.NewClient(connect(t, rpc.NewServer(pins, pool), matcherURI))
	begin(t, normal, snap.ID, 60000)
	first, h := appendBatch(t, pool, domain.Active)
	if e = first.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	late, end := appendBatch(t, pool, domain.Cancelled)
	reader := &commitOnHead{Pool: pool, tx: late}
	client := rpc.NewClient(connect(t, rpc.NewServer(pins, reader), matcherURI))
	p, e := client.ReadReplayPage(ctx, &pb.ReadReplayPageRequest{ResourceId: snap.ID, After: snap.C, MaxTransactions: 1000})
	if e != nil || p.State.Head != h || p.Next != h || len(p.Events) != 1 || p.State.Issued != h {
		t.Fatal(p, e)
	}
	next, e := normal.ReadReplayPage(ctx, &pb.ReadReplayPageRequest{ResourceId: snap.ID, After: h, MaxTransactions: 1})
	if e != nil || next.Next != end || len(next.Events) != 1 {
		t.Fatal(next, e)
	}
}
func TestPostgresUnavailableStorage(t *testing.T) {
	pool := postgrestest.Open(t)
	pool.Close()
	c := rpc.NewClient(connect(t, rpc.NewServer(sub.NewSnapshots(pool), pool), matcherURI))
	ctx := context.Background()
	if _, e := c.BeginReplay(ctx, &pb.BeginReplayRequest{SnapshotId: resourceID, TtlMillis: 1000}); status.Code(e) != codes.Unavailable {
		t.Fatal(e)
	}
	if _, e := c.ReadReplayPage(ctx, &pb.ReadReplayPageRequest{ResourceId: resourceID, MaxTransactions: 1}); status.Code(e) != codes.Unavailable {
		t.Fatal(e)
	}
	if _, e := c.AcknowledgeReplay(ctx, &pb.AcknowledgeReplayRequest{ResourceId: resourceID}); status.Code(e) != codes.Unavailable {
		t.Fatal(e)
	}
	if _, e := c.AbandonReplay(ctx, &pb.AbandonReplayRequest{ResourceId: resourceID}); status.Code(e) != codes.Unavailable {
		t.Fatal(e)
	}
}

func TestPostgresOversizedTransaction(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	pins := sub.NewSnapshots(pool)
	snap, e := pins.Build(ctx, matcherURI, time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	defer pins.Abandon(ctx, matcherURI, snap.ID)
	c := rpc.NewClient(connect(t, rpc.NewServer(pins, pool), matcherURI))
	begin(t, c, snap.ID, 60000)
	tx, e := pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	large := entry(fmt.Sprintf("large-%d", time.Now().UnixNano()), domain.Active)
	large.Metadata.CorrelationID = strings.Repeat("x", 3<<20)
	if _, e = journal.Append(ctx, tx, []journal.Entry{large}); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e = c.ReadReplayPage(ctx, &pb.ReadReplayPageRequest{ResourceId: snap.ID, After: snap.C, MaxTransactions: 1}); status.Code(e) != codes.ResourceExhausted {
		t.Fatal("oversized whole transaction was not rejected", e)
	}
	p, e := pins.CheckPin(ctx, matcherURI, snap.ID)
	if e != nil || p.Issued != snap.C {
		t.Fatal("failed response advanced issued", p, e)
	}
}

func TestPostgresCreateCancelAfterSnapshot(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	pins := sub.NewSnapshots(pool)
	user, e := account.New(pool).Create(ctx, fmt.Sprintf("replay-%d@example.com", time.Now().UnixNano()), "hash")
	if e != nil {
		t.Fatal(e)
	}
	defer pool.Exec(ctx, `DELETE FROM app.users WHERE id=$1`, user.ID)
	defer pool.Exec(ctx, `DELETE FROM app.subscriptions WHERE owner_id=$1`, user.ID)
	if _, e = pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=123,telegram_version=1 WHERE id=$1`, user.ID); e != nil {
		t.Fatal(e)
	}
	snap, e := pins.Build(ctx, matcherURI, time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	defer pins.Abandon(ctx, matcherURI, snap.ID)
	c := rpc.NewClient(connect(t, rpc.NewServer(pins, pool), matcherURI))
	begin(t, c, snap.ID, 60000)
	tx, e := pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	quota, e := sub.LockQuota(ctx, tx, user.ID)
	if e != nil {
		t.Fatal(e)
	}
	input := entry(fmt.Sprintf("create-%d", time.Now().UnixNano()), domain.Active)
	input.Change.SubscriptionID = user.ID
	input.Metadata.OrderingKey = user.ID
	v := input.Change
	if e = quota.Create(ctx, sub.Record{ID: user.ID, OwnerID: user.ID, Pair: v.Pair, Threshold: *v.Threshold, Direction: v.Direction, CreationPrice: *v.CreationSnapshot.Price, CreationTradeTime: v.CreationSnapshot.TradeTime, State: domain.Active, Version: 1, Activation: sub.LatestPrice}); e != nil {
		t.Fatal(e)
	}
	end, e := journal.Append(ctx, tx, []journal.Entry{input})
	if e != nil {
		t.Fatal(e)
	}
	// Concurrent provisional creation is invisible to replay until SQL commit.
	before, e := c.ReadReplayPage(ctx, &pb.ReadReplayPageRequest{ResourceId: snap.ID, After: snap.C, MaxTransactions: 1})
	if e != nil || len(before.Events) != 0 {
		t.Fatal(before, e)
	}
	committed := make(chan error, 1)
	go func() { committed <- tx.Commit(ctx) }()
	if e = <-committed; e != nil {
		t.Fatal(e)
	}
	created, e := c.ReadReplayPage(ctx, &pb.ReadReplayPageRequest{ResourceId: snap.ID, After: snap.C, MaxTransactions: 1})
	if e != nil || created.Next != end || len(created.Events) != 1 {
		t.Fatal(created, e)
	}
	sessions := session.New(pool, nil)
	credentials, e := sessions.Issue(ctx, user.ID)
	if e != nil {
		t.Fatal(e)
	}
	h := auth.New(sessions, sub.NewCancelHandler(pool))
	request := httptest.NewRequest("DELETE", "/v1/subscriptions/"+user.ID, nil)
	request.Header.Set("Authorization", "Bearer "+credentials.AccessToken)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, request)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	cancelled, e := c.ReadReplayPage(ctx, &pb.ReadReplayPageRequest{ResourceId: snap.ID, After: end, MaxTransactions: 1})
	if e != nil || len(cancelled.Events) != 1 {
		t.Fatal(cancelled, e)
	}
	_, change, e := changed.Decode(cancelled.Events[0])
	if e != nil || change.State != domain.Cancelled || change.SubscriptionVersion != 2 || change.SubscriptionID != user.ID {
		t.Fatal(change, e)
	}
	frozen, e := pins.Page(ctx, matcherURI, snap.ID, "", 1000)
	if e != nil {
		t.Fatal(e)
	}
	for _, r := range frozen.Records {
		if r.ID == user.ID {
			t.Fatal("creation leaked into old snapshot")
		}
	}
}
func TestPostgresCancelPinWaits(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	pins := sub.NewSnapshots(pool)
	snap, e := pins.Build(ctx, matcherURI, time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	defer pins.Abandon(ctx, matcherURI, snap.ID)
	c := rpc.NewClient(connect(t, rpc.NewServer(pins, pool), matcherURI))
	begin(t, c, snap.ID, 60000)
	for _, method := range []string{"read", "ack"} {
		tx, e := pool.Begin(ctx)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = tx.Exec(ctx, `SELECT id FROM app.subscription_replay_pins WHERE id=$1 FOR UPDATE`, snap.ID); e != nil {
			tx.Rollback(ctx)
			t.Fatal(e)
		}
		limited, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		if method == "read" {
			_, e = c.ReadReplayPage(limited, &pb.ReadReplayPageRequest{ResourceId: snap.ID, After: snap.C, MaxTransactions: 1})
		} else {
			_, e = c.AcknowledgeReplay(limited, &pb.AcknowledgeReplayRequest{ResourceId: snap.ID, Boundary: snap.C})
		}
		cancel()
		tx.Rollback(ctx)
		if status.Code(e) != codes.DeadlineExceeded {
			t.Fatal(method, e)
		}
	}
	p, e := pins.CheckPin(ctx, matcherURI, snap.ID)
	if e != nil || p.Progress != snap.C || p.Issued != snap.C {
		t.Fatal(p, e)
	}
}

type advancingPins struct {
	rpc.Pins
	calls atomic.Int32
	end   uint64
}

func (p *advancingPins) CheckPin(c context.Context, o, id string) (sub.ReplayPin, error) {
	if p.calls.Add(1) == 2 {
		if _, e := p.Pins.Acknowledge(c, o, id, p.end); e != nil {
			return sub.ReplayPin{}, e
		}
	}
	return p.Pins.CheckPin(c, o, id)
}
func TestPostgresConcurrentProgressInvalidatesOldPage(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	pins := sub.NewSnapshots(pool)
	snap, e := pins.Build(ctx, matcherURI, time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	defer pins.Abandon(ctx, matcherURI, snap.ID)
	c := rpc.NewClient(connect(t, rpc.NewServer(pins, pool), matcherURI))
	begin(t, c, snap.ID, 60000)
	tx, end := appendBatch(t, pool, domain.Active)
	if e = tx.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	req := &pb.ReadReplayPageRequest{ResourceId: snap.ID, After: snap.C, MaxTransactions: 1}
	if _, e = c.ReadReplayPage(ctx, req); e != nil {
		t.Fatal(e)
	}
	racing := rpc.NewClient(connect(t, rpc.NewServer(&advancingPins{Pins: pins, end: end}, pool), matcherURI))
	if _, e = racing.ReadReplayPage(ctx, req); status.Code(e) != codes.InvalidArgument {
		t.Fatal("returned page below concurrent durable progress", e)
	}
}
