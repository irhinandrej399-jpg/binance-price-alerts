package replayrpc_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/deliveryrpc"
	rpc "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/replayrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/snapshotrpc"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	observed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	mpb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/changefeed"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/fencing"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/live"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricefeed"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricework"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/projection"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/replica"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/snapshotload"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/grpctest"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"google.golang.org/grpc"
)

// The API commits candidate two, but its response reaches the old worker only
// after a different Kafka member has recovered the durable pending price.
func TestMatcherFaultTakeover(t *testing.T) {
	if os.Getenv("KAFKA_INTEGRATION") != "1" {
		t.Skip("requires joint Kafka/PostgreSQL runner")
	}
	pool := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	reader := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	api := postgrestest.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
	defer cancel()
	must := func(e error) {
		t.Helper()
		if e != nil {
			t.Fatal(e)
		}
	}
	broker := os.Getenv("KAFKA_TEST_BROKER")
	topic := os.Getenv("KAFKA_TEST_PREFIX") + "-fault-prices"
	producer, e := kgo.NewClient(kgo.SeedBrokers(broker), kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)), kgo.TransactionalID(topic+"-producer"))
	must(e)
	defer producer.Close()
	create := kmsg.NewPtrCreateTopicsRequest()
	item := kmsg.NewCreateTopicsRequestTopic()
	item.Topic = topic
	item.NumPartitions = 16
	item.ReplicationFactor = 1
	create.Topics = []kmsg.CreateTopicsRequestTopic{item}
	created, e := create.RequestWith(ctx, producer)
	must(e)
	if len(created.Topics) != 1 || created.Topics[0].ErrorCode != 0 {
		t.Fatal(created)
	}
	pair := ""
	for i := 0; pair == ""; i++ {
		s := fmt.Sprintf("FAULT%dUSDT", i)
		if pricefeed.PairPartition(s) == 0 {
			pair = s
		}
	}
	owner, e := account.New(api).Create(ctx, fmt.Sprintf("fault-%d@example.com", time.Now().UnixNano()), "hash")
	must(e)
	_, e = api.Exec(ctx, `UPDATE app.users SET telegram_chat_id=1451,telegram_version=1 WHERE id=$1`, owner.ID)
	must(e)
	t.Cleanup(func() {
		_, e := api.Exec(context.Background(), `DELETE FROM app.subscriptions WHERE owner_id=$1`, owner.ID)
		if e != nil {
			t.Error(e)
		}
		_, e = api.Exec(context.Background(), `DELETE FROM app.users WHERE id=$1`, owner.ID)
		if e != nil {
			t.Error(e)
		}
	})
	ids := []string{}
	threshold, _ := price.Parse("100")
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("00000000-0000-4000-8000-%012d", 711000+i)
		ids = append(ids, id)
		tx, e := api.Begin(ctx)
		must(e)
		q, e := sub.LockQuota(ctx, tx, owner.ID)
		must(e)
		e = q.Create(ctx, sub.Record{ID: id, OwnerID: owner.ID, Pair: pair, Threshold: threshold, Direction: price.Up, CreationPrice: threshold, CreationTradeTime: time.Now().UTC(), State: domain.Active, Version: 1, Activation: sub.LatestPrice})
		if e != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(e)
		}
		must(tx.Commit(ctx))
	}
	var seq atomic.Uint64
	seq.Store(1)
	latest := liveLatest(func(_ context.Context, pair string) (*mpb.LatestResponse, error) {
		n := seq.Load()
		value := "99"
		if n > 1 {
			value = "101"
		}
		return &mpb.LatestResponse{Freshness: mpb.Freshness_FRESHNESS_FRESH, Quote: &mpb.Quote{Pair: pair, Price: value, SourceId: fmt.Sprint(n), Revision: &mpb.Revision{StreamId: "fault", Generation: 1, Sequence: n}, TradeTime: time.Now().UTC().Format(time.RFC3339Nano), ReceivedAt: time.Now().UTC().Format(time.RFC3339Nano), StreamState: "live"}}, nil
	})
	pins := sub.NewSnapshots(api)
	var snapshotMu sync.Mutex
	var snapshotIDs []string
	// Existing protected loopback fixture, with the production RPC servers.
	ca := newAuthority(t)
	server := ca.issue(t, nil, true, false)
	client := ca.issue(t, []string{matcherURI}, false, false)
	opts, e := serviceauth.ServerOptions(server.cert, server.key, ca.pem, map[string][]string{matcherURI: {pb.Delivery_RequestDelivery_FullMethodName, pb.SubscriptionSnapshots_BuildSnapshot_FullMethodName, pb.SubscriptionSnapshots_ReadSnapshotPage_FullMethodName}})
	must(e)
	conn := grpctest.StartWithClientOptions(t, func(g *grpc.Server) {
		pb.RegisterDeliveryServer(g, deliveryrpc.NewServer(deliveryrpc.NewPostgres(api), latest, time.Now))
		pb.RegisterSubscriptionSnapshotsServer(g, snapshotrpc.NewServer(pins))
	}, []grpc.DialOption{grpc.WithTransportCredentials(clientCreds(t, client, ca.pem)), grpc.WithAuthority("market.test")}, opts...)
	delivery := deliveryrpc.NewClient(conn)
	base := replica.Config{Pool: pool, Environment: t.Name(), Group: topic + "-group", Prices: pricefeed.Config{Brokers: []string{broker}, Topic: topic, Buffer: 512}, Replay: rpc.NewClient(connect(t, rpc.NewServer(pins, api), matcherURI)), Latest: latest, Delivery: delivery, Live: live.Config{ReplayTTL: time.Minute, HeadInterval: 50 * time.Millisecond}, Bootstrap: map[int32]int64{}}
	for p := int32(0); p < 16; p++ {
		base.Bootstrap[p] = 0
	}
	base.Snapshot = func(c context.Context, id string) (string, error) {
		snap, e := snapshotload.New(pool, snapshotrpc.NewClient(conn)).Load(c, &pb.BuildSnapshotRequest{RequestId: strings.ReplaceAll(id, ":", "-"), TtlMillis: 60000, PageSize: 1000})
		if e != nil {
			return "", e
		}
		snapshotMu.Lock()
		snapshotIDs = append(snapshotIDs, snap.SnapshotID)
		snapshotMu.Unlock()
		return snap.SnapshotID, nil
	}
	base.Changes = func(_ context.Context, pos *changefeed.Position) (live.Registration, error) {
		f := newLiveFeed()
		if pos != nil {
			return restartedLiveFeed{liveFeed: f, position: *pos}, nil
		}
		return f, nil
	}
	// Each lifecycle is joined; the deliberately delayed old response is always
	// released before teardown, including when an assertion fails.
	releaseOld := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseOld) }) }
	type running struct {
		r    *replica.Replica
		stop context.CancelFunc
		done chan error
	}
	runs := []running{}
	t.Cleanup(func() {
		release()
		cancel()
		for _, r := range runs {
			r.stop()
			select {
			case <-r.done:
			case <-time.After(5 * time.Second):
				t.Error("replica leaked")
			}
		}
		cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		snapshotMu.Lock()
		defer snapshotMu.Unlock()
		for _, id := range snapshotIDs {
			if e := pins.Abandon(cleanup, matcherURI, id); e != nil {
				t.Error(e)
			}
		}
	})
	start := func(id string, d replicaDelivery) running {
		cfg := base
		cfg.ID = id
		if d != nil {
			cfg.Delivery = d
		}
		r, e := replica.New(cfg)
		must(e)
		c, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- r.Run(c) }()
		out := running{r, stop, done}
		runs = append(runs, out)
		return out
	}
	wait := func(label string, ok func() bool) {
		t.Helper()
		end := time.Now().Add(20 * time.Second)
		for !ok() {
			if time.Now().After(end) || ctx.Err() != nil {
				t.Fatal("timeout: " + label)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	committed := make(chan *pb.RequestDeliveryResponse, 1)
	old := start("old", replicaDelivery(func(c context.Context, o, id string, v int64) (*pb.RequestDeliveryResponse, error) {
		response, e := delivery.RequestDelivery(c, o, id, v)
		if e == nil && id == ids[1] {
			committed <- response
			<-releaseOld
		}
		return response, e
	}))
	wait("old ready", old.r.Ready)
	oldGrants := old.r.Grants()
	var oldGrant fencing.Grant
	for _, g := range oldGrants {
		if g.Identity.Partition == 0 {
			oldGrant = g
		}
	}
	if oldGrant.Token == 0 {
		t.Fatal("partition missing")
	}
	publish := func(n uint64) *kgo.Record {
		seq.Store(n)
		value, _ := price.Parse("101")
		quote := observed.Observed{SchemaVersion: 1, Pair: pair, Price: &value, SourceID: fmt.Sprint(n), Revision: observed.Revision{StreamID: "fault", Generation: 1, Sequence: n}, TradeTime: time.Now().UTC(), ReceivedAt: time.Now().UTC(), StreamState: observed.Live}
		b, e := observed.Encode(event.Envelope{EventID: fmt.Sprintf("fault-%d", n), Type: observed.Type, Version: 1, CorrelationID: "fault", OccurredAt: time.Now().UTC(), OrderingKey: pair}, quote)
		must(e)
		record := &kgo.Record{Topic: topic, Key: []byte(pair), Value: b}
		must(producer.BeginTransaction())
		must(producer.ProduceSync(ctx, record).FirstErr())
		must(producer.EndTransaction(ctx, kgo.TryCommit))
		return record
	}
	record := publish(2)
	var lost *pb.RequestDeliveryResponse
	select {
	case lost = <-committed:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	pos := pricework.Position{Topic: topic, Partition: 0, Offset: record.Offset}
	transportKey := replica.TransportKey(pos)
	store := pricework.New(reader, oldGrant.Scope)
	// Changed and the full transport reader can race to accept this revision.
	// Both are durable price work; resume the exact accepted key, never assume
	// which scheduler reached the API first.
	pending, e := store.Pending(ctx)
	must(e)
	if len(pending) != 1 {
		t.Fatal("expected one interrupted evaluation", pending)
	}
	key := pending[0]
	w, e := store.Get(ctx, key)
	must(e)
	if w.State != "pending" || w.Completed != 1 || len(w.Entries) != 3 || w.Input.Quote.Revision.Sequence != 2 {
		t.Fatal("not stopped in middle of price", w)
	}
	brokerOffset := func() int64 {
		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = base.Group
		item := kmsg.NewOffsetFetchRequestTopic()
		item.Topic = topic
		item.Partitions = []int32{0}
		req.Topics = []kmsg.OffsetFetchRequestTopic{item}
		response, e := req.RequestWith(ctx, producer)
		must(e)
		if len(response.Topics) != 1 || len(response.Topics[0].Partitions) != 1 || response.Topics[0].Partitions[0].ErrorCode != 0 {
			t.Fatal(response)
		}
		return response.Topics[0].Partitions[0].Offset
	}
	if off := brokerOffset(); off > record.Offset {
		t.Fatal("ack overtook pending work", off)
	}
	old.stop() // The group stops even though an already finished RPC response is delayed.
	wait("old readiness revoked", func() bool { return !old.r.Ready() })
	recoveryEntered := make(chan struct{}, 1)
	resume := make(chan struct{}, 1)
	defer close(resume)
	var once sync.Once
	successor := start("successor", replicaDelivery(func(c context.Context, o, id string, v int64) (*pb.RequestDeliveryResponse, error) {
		once.Do(func() {
			recoveryEntered <- struct{}{}
			select {
			case <-resume:
			case <-c.Done():
			}
		})
		return delivery.RequestDelivery(c, o, id, v)
	}))
	select {
	case <-recoveryEntered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if successor.r.Ready() {
		t.Fatal("ready before pending recovered")
	}
	// Buffered release also permits cleanup if the assignment was cancelled.
	resume <- struct{}{}
	wait("successor ready", successor.r.Ready)
	var grant fencing.Grant
	for _, g := range successor.r.Grants() {
		if g.Identity.Partition == 0 {
			grant = g
		}
	}
	if grant.Token <= oldGrant.Token || grant.Scope != oldGrant.Scope || grant.ProjectionID == oldGrant.ProjectionID {
		t.Fatal(oldGrant, grant)
	}
	wait("transport prefix restored", func() bool { next, e := store.Checkpoint(ctx, topic, 0); return e == nil && next == record.Offset+1 })
	transportWork, e := store.Get(ctx, transportKey)
	must(e)
	if transportWork.State != "completed" || transportWork.Input.Position == nil || *transportWork.Input.Position != pos {
		t.Fatal("transport price not recovered", transportWork)
	}
	w, e = store.Get(ctx, key)
	must(e)
	if w.State != "completed" || w.Completed != 3 || w.Entries[1].Result.TriggerID != lost.Workflow.TriggerId {
		t.Fatal("canonical response not recovered", w)
	}
	// A revived worker cannot write SQL or invoke its durable ack, even with a
	// fresh context after the broker has completed reassignment.
	stale := pricework.NewFenced(pool, oldGrant)
	acked := false
	if e = stale.Record(ctx, key, ids[1], *w.Entries[1].Result); !errors.Is(e, fencing.ErrAuthority) {
		t.Fatal("late Record", e)
	}
	if e = stale.Accept(ctx, w.Input); !errors.Is(e, fencing.ErrAuthority) {
		t.Fatal("late Accept", e)
	}
	var prefix *pricework.Prefix
	if w.Input.Position != nil {
		prefix = &pricework.Prefix{Topic: topic, Partition: 0, From: 0, Next: record.Offset + 1}
	}
	if e = stale.Complete(ctx, key, nil, prefix, func(context.Context) error { acked = true; return nil }); !errors.Is(e, fencing.ErrAuthority) || acked {
		t.Fatal("late Complete/ack", e, acked)
	}
	recovery, e := projection.New(reader, oldGrant.ProjectionID).Recover(ctx)
	must(e)
	if e = projection.NewFenced(pool, oldGrant.ProjectionID, oldGrants).Initialize(ctx, recovery.SnapshotID); !errors.Is(e, fencing.ErrAuthority) {
		t.Fatal("late projection", e)
	}
	release()
	select {
	case e := <-old.done:
		if e == nil {
			t.Fatal("old worker succeeded")
		}
		old.done <- e
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Its conditional cleanup must leave the successor alive and ready.
	current, active, e := fencing.New(reader).Current(ctx, grant.Identity)
	must(e)
	if !active || current != grant || !successor.r.Ready() {
		t.Fatal("old cleanup revoked successor", current, active)
	}
	triggers := map[string]bool{}
	for _, entry := range w.Entries {
		result := entry.Result
		if result == nil || triggers[result.TriggerID] {
			t.Fatal(entry)
		}
		triggers[result.TriggerID] = true
		response, e := delivery.RequestDelivery(ctx, owner.ID, entry.Candidate.SubscriptionID, 1)
		must(e)
		if response.Workflow.TriggerId != result.TriggerID {
			t.Fatal("late RPC changed canonical workflow")
		}
		var count int
		var trigger string
		must(api.QueryRow(ctx, `SELECT count(*),min(trigger_id::text) FROM app.delivery_outbox WHERE subscription_id=$1`, entry.Candidate.SubscriptionID).Scan(&count, &trigger))
		if count != 1 || trigger != result.TriggerID {
			t.Fatal(count, trigger, result)
		}
	}
	// Stop before a third price; commit its result through the real Store boundary
	// and lose the ack. This models crash after SQL commit, before Confirm sends.
	wait("broker prefix", func() bool { return brokerOffset() == record.Offset+1 })
	successor.stop()
	select {
	case e := <-successor.done:
		successor.done <- e
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	record3 := publish(3)
	current, _, e = fencing.New(reader).Current(ctx, grant.Identity)
	must(e)
	grant, e = fencing.New(pool).Assign(ctx, fencing.Request{Identity: grant.Identity, Scope: grant.Scope, ProjectionID: grant.ProjectionID, ExpectedToken: current.Token, RequestID: "lost-confirm"})
	must(e)
	value, _ := price.Parse("101")
	quote := observed.Observed{SchemaVersion: 1, Pair: pair, Price: &value, SourceID: "3", Revision: observed.Revision{StreamID: "fault", Generation: 1, Sequence: 3}, TradeTime: time.Now().UTC(), ReceivedAt: time.Now().UTC(), StreamState: observed.Live}
	p3 := pricework.Position{Topic: topic, Partition: 0, Offset: record3.Offset}
	k3 := replica.TransportKey(p3)
	ws := pricework.NewFenced(pool, grant)
	must(ws.Accept(ctx, pricework.Input{Key: k3, ProjectionID: grant.ProjectionID, C: recovery.Checkpoint.C, Quote: quote, Position: &p3}))
	lostAck := errors.New("crash before broker Confirm")
	e = ws.Complete(ctx, k3, nil, &pricework.Prefix{Topic: topic, Partition: 0, From: record.Offset + 1, Next: record3.Offset + 1}, func(context.Context) error { return lostAck })
	if !errors.Is(e, lostAck) || brokerOffset() != record.Offset+1 {
		t.Fatal("ack failure not retained", e)
	}
	restarted := start("successor", nil) // Same assignment set: NewResume of full projection.
	wait("restart from SQL despite broker lag", restarted.r.Ready)
	finalGrant := fencing.Grant{}
	for _, g := range restarted.r.Grants() {
		if g.Identity.Partition == 0 {
			finalGrant = g
		}
	}
	if finalGrant.Token <= grant.Token || finalGrant.ProjectionID != grant.ProjectionID {
		t.Fatal("restart identity/token", finalGrant, grant)
	}
	next, e := store.Checkpoint(ctx, topic, 0)
	must(e)
	if next != record3.Offset+1 {
		t.Fatal(next)
	}
	record4 := publish(4)
	wait("next price from SQL prefix", func() bool {
		next, e := store.Checkpoint(ctx, topic, 0)
		return e == nil && next == record4.Offset+1 && brokerOffset() == next
	})
	pending, e = store.Pending(ctx)
	must(e)
	if len(pending) != 0 {
		t.Fatal(pending)
	}
	w3, e := store.Get(ctx, k3)
	must(e)
	if w3.State != "completed" || len(w3.Entries) != 0 {
		t.Fatal(w3)
	}
	for _, id := range ids {
		var count int
		must(api.QueryRow(ctx, `SELECT count(*) FROM app.delivery_outbox WHERE subscription_id=$1`, id).Scan(&count))
		if count != 1 {
			t.Fatal("duplicate workflow after restart", count)
		}
	}
	// A stale revoke after the final restart is also conditional.
	if e = fencing.New(pool).Revoke(ctx, grant); !errors.Is(e, fencing.ErrAuthority) {
		t.Fatal(e)
	}
}
