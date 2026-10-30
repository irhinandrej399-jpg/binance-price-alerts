package replayrpc_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	rpc "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/replayrpc"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	observed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	mpb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/changefeed"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/live"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricefeed"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricework"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/projection"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/replica"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/snapshotload"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type replicaDelivery func(context.Context, string, string, int64) (*pb.RequestDeliveryResponse, error)

func (f replicaDelivery) RequestDelivery(c context.Context, o, s string, v int64) (*pb.RequestDeliveryResponse, error) {
	return f(c, o, s, v)
}

// Reuses the real replay RPC/pin, snapshot loader, PostgreSQL and Kafka runner.
func TestMatcherTwoReplicas(t *testing.T) {
	if os.Getenv("KAFKA_INTEGRATION") != "1" {
		t.Skip("requires joint Kafka/PostgreSQL runner")
	}
	pool := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	readerPool := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	apiPool := postgrestest.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	defer cancel()
	broker := os.Getenv("KAFKA_TEST_BROKER")
	topic := os.Getenv("KAFKA_TEST_PREFIX") + "-replica-prices"
	producer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)), kgo.TransactionalID(topic+"-producer"))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	req := kmsg.NewPtrCreateTopicsRequest()
	item := kmsg.NewCreateTopicsRequestTopic()
	item.Topic = topic
	item.NumPartitions = 16
	item.ReplicationFactor = 1
	req.Topics = []kmsg.CreateTopicsRequestTopic{item}
	res, err := req.RequestWith(ctx, producer)
	if err != nil || len(res.Topics) != 1 || res.Topics[0].ErrorCode != 0 {
		t.Fatal(res, err)
	}
	pairAt := func(p int32) string {
		for i := 0; ; i++ {
			s := fmt.Sprintf("REPLICA%dUSDT", i)
			if pricefeed.PairPartition(s) == p {
				return s
			}
		}
	}
	pairs := []string{pairAt(0), pairAt(15)}
	pins := sub.NewSnapshots(apiPool)
	replay := rpc.NewClient(connect(t, rpc.NewServer(pins, apiPool), matcherURI))
	var mu sync.Mutex
	calls := map[string]int{}
	var gate <-chan struct{}
	entered := make(chan struct{}, 16)
	delivery := replicaDelivery(func(c context.Context, _ string, id string, _ int64) (*pb.RequestDeliveryResponse, error) {
		mu.Lock()
		g := gate
		mu.Unlock()
		if g != nil {
			select {
			case entered <- struct{}{}:
			default:
			}
			select {
			case <-g:
			case <-c.Done():
				return nil, c.Err()
			}
		}
		mu.Lock()
		defer mu.Unlock()
		calls[id]++
		return &pb.RequestDeliveryResponse{SubscriptionId: id, SubscriptionVersion: 2, State: "pending_delivery", Workflow: &pb.Workflow{TriggerId: "canonical-" + id, Version: 1}}, nil
	})
	base := replica.Config{Pool: pool, Environment: t.Name(), Group: topic + "-group", Prices: pricefeed.Config{Brokers: []string{broker}, Topic: topic, Buffer: 512}, Replay: replay, Delivery: delivery, Live: live.Config{ReplayTTL: time.Minute, HeadInterval: 200 * time.Millisecond}}
	base.Bootstrap = map[int32]int64{}
	for p := int32(0); p < 16; p++ {
		base.Bootstrap[p] = 0
	}
	base.Snapshot = func(c context.Context, id string) (string, error) {
		snap, e := pins.Build(c, matcherURI, time.Minute)
		if e != nil {
			return "", e
		}
		t.Cleanup(func() {
			c, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			_ = pins.Abandon(c, matcherURI, snap.ID)
		})
		page := &pb.SnapshotPageResponse{SnapshotId: snap.ID, BoundaryC: snap.C, ExpiresAt: snap.ExpiresAt.Format(time.RFC3339Nano)}
		for i, pair := range pairs {
			page.Subscriptions = append(page.Subscriptions, &pb.SnapshotSubscription{SubscriptionId: fmt.Sprintf("00000000-0000-4000-8000-%012d", i+20), OwnerId: "a4444444-4444-4444-8444-444444444444", Version: 1, Pair: pair, Threshold: "100", Direction: "up", State: "active", Activation: "latest_price", CreationPrice: "99", CreationTradeTime: time.Now().UTC().Format(time.RFC3339Nano)})
		}
		_, e = snapshotload.New(pool, frozenLiveSnapshot{page: page, during: func() {}}).Load(c, &pb.BuildSnapshotRequest{RequestId: strings.ReplaceAll(id, ":", "-"), TtlMillis: 60000, PageSize: 1000})
		return snap.ID, e
	}
	base.Changes = func(c context.Context, pos *changefeed.Position) (live.Registration, error) {
		// Existing deterministic feed: full SQL replay is a real mTLS RPC.
		f := newLiveFeed()
		if pos != nil {
			return restartedLiveFeed{liveFeed: f, position: *pos}, nil
		}
		return f, nil
	}
	base.Latest = liveLatest(func(_ context.Context, pair string) (*mpb.LatestResponse, error) {
		return &mpb.LatestResponse{Freshness: mpb.Freshness_FRESHNESS_FRESH, Quote: &mpb.Quote{Pair: pair, Price: "101", SourceId: "1", Revision: &mpb.Revision{StreamId: "replicas", Generation: 1, Sequence: 1}, TradeTime: time.Now().UTC().Format(time.RFC3339Nano), ReceivedAt: time.Now().UTC().Format(time.RFC3339Nano), StreamState: "live"}}, nil
	})
	var results []chan error
	t.Cleanup(func() {
		cancel()
		for _, d := range results {
			select {
			case <-d:
			case <-time.After(5 * time.Second):
				t.Error("replica leaked")
			}
		}
	})
	newReplica := func(id string) (*replica.Replica, chan error) {
		cfg := base
		cfg.ID = id
		r, e := replica.New(cfg)
		if e != nil {
			t.Fatal(e)
		}
		done := make(chan error, 1)
		go func() { done <- r.Run(ctx) }()
		results = append(results, done)
		return r, done
	}
	r1, d1 := newReplica("one")
	wait := func(ok func() bool) {
		t.Helper()
		end := time.Now().Add(20 * time.Second)
		for !ok() {
			if time.Now().After(end) {
				t.Fatal("replicas did not become ready")
			}
			select {
			case e := <-d1:
				d1 <- e
				t.Fatalf("first replica stopped: %v", e)
			default:
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	wait(r1.Ready)
	// Latest must not invent a transport cursor. Seed a durable candidate in
	// the original stable scope, then let normal rebalance recover it.
	oldGrants := r1.Grants()
	var seededScope string
	for _, g := range oldGrants {
		ws := pricework.NewFenced(pool, g)
		if _, e := ws.Checkpoint(ctx, topic, g.Identity.Partition); !errors.Is(e, pgx.ErrNoRows) {
			t.Fatal("Latest invented a checkpoint")
		}
		if g.Identity.Partition == 0 {
			seededScope = g.Scope
			cp, e := projection.New(readerPool, g.ProjectionID).Checkpoint(ctx)
			if e != nil {
				t.Fatal(e)
			}
			value, _ := price.Parse("101")
			q := observed.Observed{SchemaVersion: 1, Pair: pairs[0], Price: &value, SourceID: "1", Revision: observed.Revision{StreamID: "replicas", Generation: 1, Sequence: 1}, TradeTime: time.Now().UTC(), ReceivedAt: time.Now().UTC(), StreamState: observed.Live}
			if e = ws.Accept(ctx, pricework.Input{Key: "pending-before-rebalance", ProjectionID: g.ProjectionID, C: cp.C, Quote: q, Candidates: []pricework.Candidate{{OwnerID: "a4444444-4444-4444-8444-444444444444", SubscriptionID: "00000000-0000-4000-8000-000000000020", Version: 1}}}); e != nil {
				t.Fatal(e)
			}
		}
	}
	release := make(chan struct{})
	mu.Lock()
	gate = release
	mu.Unlock()
	r2, _ := newReplica("two")
	select {
	case <-entered:
	case <-time.After(20 * time.Second):
		t.Fatal("no recovery/initial evaluation")
	}
	if r1.Ready() || r2.Ready() {
		t.Fatal("replica ready before recovery/current evaluation completed")
	}
	mu.Lock()
	gate = nil
	mu.Unlock()
	close(release)
	wait(func() bool { return r1.Ready() && r2.Ready() && len(r1.Grants()) == 8 && len(r2.Grants()) == 8 })
	recovered, e := pricework.New(readerPool, seededScope).Get(ctx, "pending-before-rebalance")
	if e != nil || recovered.State != "completed" || recovered.Entries[0].Result == nil {
		t.Fatal("Pending not recovered", recovered, e)
	}
	grants := append(r1.Grants(), r2.Grants()...)
	seen := map[int32]bool{}
	for _, g := range grants {
		if seen[g.Identity.Partition] || g.Token < 2 {
			t.Fatal("overlap/missing token", g)
		}
		seen[g.Identity.Partition] = true
		// Full replica projection contains BOTH pairs despite price ownership.
		store := projection.New(readerPool, g.ProjectionID)
		for i := range pairs {
			if _, e := store.ReadImage(ctx, fmt.Sprintf("00000000-0000-4000-8000-%012d", i+20)); e != nil {
				t.Fatal(e)
			}
		}
	}
	if len(seen) != 16 {
		t.Fatal(seen)
	}
	p, _ := price.Parse("101")
	records := []*kgo.Record{}
	for seq := uint64(2); seq <= 3; seq++ {
		for part := int32(0); part < 16; part++ {
			pair := pairAt(part)
			q := observed.Observed{SchemaVersion: 1, Pair: pair, Price: &p, SourceID: fmt.Sprint(seq), Revision: observed.Revision{StreamID: "replicas", Generation: 1, Sequence: seq}, TradeTime: time.Now().UTC(), ReceivedAt: time.Now().UTC(), StreamState: observed.Live}
			b, e := observed.Encode(event.Envelope{EventID: fmt.Sprintf("replica-%d-%d", part, seq), Type: observed.Type, Version: 1, CorrelationID: "replicas", OccurredAt: time.Now().UTC(), OrderingKey: pair}, q)
			if e != nil {
				t.Fatal(e)
			}
			records = append(records, &kgo.Record{Topic: topic, Key: []byte(pair), Value: b})
		}
	}
	// Hold the canonical API while incoming prices are being processed.
	// Broker offsets must never get ahead of durable SQL completion.
	for len(entered) > 0 {
		<-entered
	}
	releasePrices := make(chan struct{})
	mu.Lock()
	gate = releasePrices
	mu.Unlock()
	commitBatch := func(rs []*kgo.Record, commit kgo.TransactionEndTry) {
		t.Helper()
		if e := producer.BeginTransaction(); e != nil {
			t.Fatal(e)
		}
		if e := producer.ProduceSync(ctx, rs...).FirstErr(); e != nil {
			t.Fatal(e)
		}
		if e := producer.EndTransaction(ctx, commit); e != nil {
			t.Fatal(e)
		}
	}
	commitBatch(records[:16], kgo.TryCommit)
	aborted := make([]*kgo.Record, 16)
	for i, r := range records[:16] {
		aborted[i] = &kgo.Record{Topic: r.Topic, Key: r.Key, Value: r.Value}
	}
	commitBatch(aborted, kgo.TryAbort)
	commitBatch(records[16:], kgo.TryCommit)
	expected := map[int32]int64{}
	offsets := map[int32][]int64{}
	for _, record := range records {
		expected[record.Partition] = record.Offset + 1
		offsets[record.Partition] = append(offsets[record.Partition], record.Offset)
	}
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("price evaluation never reached API")
	}
	fetchBefore := kmsg.NewPtrOffsetFetchRequest()
	fetchBefore.Group = base.Group
	beforeTopic := kmsg.NewOffsetFetchRequestTopic()
	beforeTopic.Topic = topic
	for part := int32(0); part < 16; part++ {
		beforeTopic.Partitions = append(beforeTopic.Partitions, part)
	}
	fetchBefore.Topics = []kmsg.OffsetFetchRequestTopic{beforeTopic}
	before, e := fetchBefore.RequestWith(ctx, producer)
	if e != nil {
		t.Fatal(e)
	}
	for _, responseTopic := range before.Topics {
		for _, part := range responseTopic.Partitions {
			for _, g := range grants {
				if g.Identity.Partition != part.Partition {
					continue
				}
				cp, e := pricework.New(readerPool, g.Scope).Checkpoint(ctx, topic, part.Partition)
				if errors.Is(e, pgx.ErrNoRows) {
					cp = -1
				} else if e != nil {
					t.Fatal(e)
				}
				if part.Offset > cp {
					t.Fatalf("broker %d ahead of SQL %d", part.Offset, cp)
				}
			}
		}
	}
	mu.Lock()
	gate = nil
	mu.Unlock()
	close(releasePrices)
	wait(func() bool {
		for _, g := range grants {
			s := pricework.New(readerPool, g.Scope)
			next, e := s.Checkpoint(ctx, topic, g.Identity.Partition)
			if e != nil || next != expected[g.Identity.Partition] {
				return false
			}
		}
		return true
	})
	for _, g := range grants {
		s := pricework.New(readerPool, g.Scope)
		for _, off := range offsets[g.Identity.Partition] {
			w, e := s.Get(ctx, replica.TransportKey(pricework.Position{Topic: topic, Partition: g.Identity.Partition, Offset: off}))
			if e != nil || (w.State != "completed" && w.State != "superseded") || w.Input.Position.Offset != off {
				t.Fatal(w, e)
			}
		}
	}
	for _, record := range aborted {
		for _, g := range grants {
			if g.Identity.Partition == record.Partition {
				_, e := pricework.New(readerPool, g.Scope).Get(ctx, replica.TransportKey(pricework.Position{Topic: topic, Partition: record.Partition, Offset: record.Offset}))
				if !errors.Is(e, pgx.ErrNoRows) {
					t.Fatal("aborted price processed", e)
				}
			}
		}
	}
	wait(func() bool {
		if !r1.Ready() || !r2.Ready() {
			return false
		}
		for _, g := range grants {
			pending, e := pricework.New(readerPool, g.Scope).Pending(ctx)
			if e != nil || len(pending) > 0 {
				return false
			}
		}
		return true
	})
	// OffsetCommit may lag SQL briefly, but must converge to the same prefix.
	fetch := kmsg.NewPtrOffsetFetchRequest()
	fetch.Group = base.Group
	ft := kmsg.NewOffsetFetchRequestTopic()
	ft.Topic = topic
	for part := int32(0); part < 16; part++ {
		ft.Partitions = append(ft.Partitions, part)
	}
	fetch.Topics = []kmsg.OffsetFetchRequestTopic{ft}
	wait(func() bool {
		res, e := fetch.RequestWith(ctx, producer)
		if e != nil || len(res.Topics) != 1 {
			return false
		}
		for _, part := range res.Topics[0].Partitions {
			if part.Offset != expected[part.Partition] {
				return false
			}
		}
		return len(res.Topics[0].Partitions) == 16
	})
	// Latest evaluation has no invented offset/checkpoint and canonical outcomes
	// exist before the initial price event arrives. Unsubscribed pairs also advance.
	mu.Lock()
	defer mu.Unlock()
	for i := range pairs {
		if calls[fmt.Sprintf("00000000-0000-4000-8000-%012d", i+20)] == 0 {
			t.Fatal("initial current view was not evaluated")
		}
	}
}
