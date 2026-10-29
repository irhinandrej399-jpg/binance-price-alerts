package replayrpc_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/journal"
	rpc "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/replayrpc"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	changed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/subscriptionchanged"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	mpb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/changefeed"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/live"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/priceconnection"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricefeed"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/projection"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/snapshotload"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/twmb/franz-go/pkg/kgo"
)

type frozenLiveSnapshot struct {
	page   *pb.SnapshotPageResponse
	during func()
}

func (s frozenLiveSnapshot) BuildSnapshot(context.Context, *pb.BuildSnapshotRequest) (*pb.SnapshotPageResponse, error) {
	s.during()
	return s.page, nil
}
func (s frozenLiveSnapshot) ReadSnapshotPage(context.Context, *pb.ReadSnapshotPageRequest) (*pb.SnapshotPageResponse, error) {
	panic("single page")
}

// The existing runner can provide both disposable PostgreSQL and Kafka:
// KAFKA_RUN_POSTGRES=1 bash infra/kafka/verify-go.sh.
// PostgreSQL-only runs use a controlled changefeed for the same coordinator.
func TestMatcherSnapshotReplayLive(t *testing.T) {
	apiPool := postgrestest.Open(t)
	matcherPool := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	reader := projection.New(postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL"), t.Name())
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	pins := sub.NewSnapshots(apiPool)
	snap, err := pins.Build(ctx, matcherURI, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer pins.Abandon(context.Background(), matcherURI, snap.ID)
	var last uint64
	appendChanges := func(ids []string, versions []uint64, states []domain.State) [][]byte {
		t.Helper()
		tx, err := apiPool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		es := make([]journal.Entry, len(ids))
		p, _ := price.Parse("100")
		for i, id := range ids {
			es[i] = entry(fmt.Sprintf("live-%d-%d", time.Now().UnixNano(), i), states[i])
			es[i].Metadata.OrderingKey = id
			es[i].Change.SubscriptionID = id
			es[i].Change.SubscriptionVersion = versions[i]
			es[i].Change.Threshold = &p
		}
		before, err := journal.Current(ctx, tx)
		if err != nil {
			t.Fatal(err)
		}
		last, err = journal.Append(ctx, tx, es)
		if err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		rows, err := journal.ReadAfter(ctx, apiPool, before, 1)
		if err != nil {
			t.Fatal(err)
		}
		out := make([][]byte, len(rows))
		for i, r := range rows {
			out[i], err = changed.Encode(r.Metadata, r.Change)
			if err != nil {
				t.Fatal(err)
			}
		}
		return out
	}
	// The create commits while the snapshot is being loaded. It is absent from S.
	var created [][]byte
	source := frozenLiveSnapshot{page: &pb.SnapshotPageResponse{SnapshotId: snap.ID, BoundaryC: snap.C, ExpiresAt: snap.ExpiresAt.Format(time.RFC3339Nano)}, during: func() { created = appendChanges([]string{"live-cancel"}, []uint64{1}, []domain.State{domain.Active}) }}
	if _, err = snapshotload.New(matcherPool, source).Load(ctx, &pb.BuildSnapshotRequest{RequestId: t.Name(), TtlMillis: 60000, PageSize: 1000}); err != nil {
		t.Fatal(err)
	}
	store := projection.New(matcherPool, t.Name())
	client := rpc.NewClient(connect(t, rpc.NewServer(pins, apiPool), matcherURI))
	controlled := newLiveFeed()
	var register func(context.Context) (live.Registration, error)
	var send func([][]byte)
	var kafkaReg *changefeed.Registration
	var kafkaMu sync.Mutex
	if os.Getenv("KAFKA_INTEGRATION") == "1" {
		broker, prefix := os.Getenv("KAFKA_TEST_BROKER"), os.Getenv("KAFKA_CHANGEFEED_TEST_PREFIX")
		if broker == "" || prefix == "" {
			t.Fatal("Kafka opt-in requires configuration")
		}
		topic := prefix + "-subscription-changes"
		source, err := changefeed.New(ctx, changefeed.Config{Brokers: []string{broker}, Topic: topic})
		if err != nil {
			t.Fatal(err)
		}
		defer source.Close()
		producer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.TransactionalID(prefix+"-live-coordinator"), kgo.RecordPartitioner(kgo.ManualPartitioner()))
		if err != nil {
			t.Fatal(err)
		}
		defer producer.Close()
		register = func(c context.Context) (live.Registration, error) {
			r, e := source.Register(c)
			kafkaMu.Lock()
			kafkaReg = r
			kafkaMu.Unlock()
			return r, e
		}
		send = func(events [][]byte) {
			t.Helper()
			if err := producer.BeginTransaction(); err != nil {
				t.Fatal(err)
			}
			rs := make([]*kgo.Record, len(events))
			for i, b := range events {
				_, ch, e := changed.Decode(b)
				if e != nil {
					t.Fatal(e)
				}
				rs[i] = &kgo.Record{Topic: topic, Partition: 0, Key: []byte(ch.SubscriptionID), Value: b}
			}
			if err := producer.ProduceSync(ctx, rs...).FirstErr(); err != nil {
				t.Fatal(err)
			}
			if err := producer.EndTransaction(ctx, kgo.TryCommit); err != nil {
				t.Fatal(err)
			}
		}
	} else {
		register = func(context.Context) (live.Registration, error) { return controlled, nil }
		send = controlled.push
	}
	var priceReg *livePrices
	var connection *priceconnection.Connection
	var connectionMu sync.Mutex
	// MAT-012 folds 99 -> 101 -> 99 during Latest before activation.
	quotes := func(c context.Context, pair string) (live.QuoteConnection, error) {
		priceRegLocal := newLivePrices()
		q, e := priceconnection.Connect(c, pair, 0, func(context.Context, string, int32) (priceconnection.Registration, error) { return priceRegLocal, nil }, liveLatest(func(context.Context, string) (*mpb.LatestResponse, error) {
			priceRegLocal.push(2, "101")
			priceRegLocal.push(3, "99")
			return &mpb.LatestResponse{Freshness: mpb.Freshness_FRESHNESS_STALE, Quote: &mpb.Quote{Pair: pair, Price: "99", SourceId: "1", Revision: &mpb.Revision{StreamId: "live", Generation: 1, Sequence: 1}, TradeTime: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano), ReceivedAt: time.Now().UTC().Format(time.RFC3339Nano), StreamState: "live"}}, nil
		}), time.Now)
		connectionMu.Lock()
		connection = q
		priceReg = priceRegLocal
		connectionMu.Unlock()
		return q, e
	}
	registered := make(chan struct{})
	original := register
	register = func(c context.Context) (live.Registration, error) {
		r, e := original(c)
		if e == nil {
			// Cancellation and another create commit after registration and before H.
			appendChanges([]string{"live-cancel", "live-active"}, []uint64{2, 1}, []domain.State{domain.Cancelled, domain.Active})
			close(registered)
		}
		return r, e
	}
	coordinator, err := live.New(store, client, register, quotes, live.Config{SnapshotID: snap.ID, ReplayTTL: time.Minute, PageSize: 1, HeadInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	stopped := make(chan error, 1)
	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	go func() { stopped <- coordinator.Run(runCtx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-stopped:
		case <-time.After(3 * time.Second):
			t.Error("coordinator leaked")
		}
	})
	<-registered
	sample := func(c uint64, n int, seq uint64) live.Selection {
		t.Helper()
		deadline := time.NewTimer(8 * time.Second)
		defer deadline.Stop()
		for {
			s, e := coordinator.Current("BTCUSDT")
			if e == nil && s.C >= c && len(s.Candidates) == n && s.Quote.Revision.Sequence >= seq {
				return s
			}
			select {
			case e := <-stopped:
				stopped <- e
				t.Fatalf("coordinator stopped: %v", e)
			case <-deadline.C:
				t.Fatalf("view never reached C=%d candidates=%d sequence=%d (last=%+v %v)", c, n, seq, s, e)
			case <-coordinator.Changed():
			case <-time.After(5 * time.Millisecond):
			}
		}
	}
	sample(last, 0, 3) // historical 101 and cancelled entry cannot become candidates
	connectionMu.Lock()
	q := connection
	activePrices := priceReg
	connectionMu.Unlock()
	if q == nil {
		t.Fatal("no MAT-012 connection")
	}
	// A Kafka duplicate of the original create cannot resurrect the cancelled row.
	send(created)
	// SQL-only commit closes publication lag even when Kafka stays empty.
	appendChanges([]string{"live-sql-only"}, []uint64{1}, []domain.State{domain.Active})
	sample(last, 0, 3)
	activePrices.push(4, "101")
	s := sample(last, 2, 4)
	for _, e := range s.Candidates {
		if e.ID == "live-cancel" {
			t.Fatal("resurrected cancellation")
		}
	}
	// Publish a later C while omitting an entire committed API transaction.
	appendChanges([]string{"live-gap"}, []uint64{1}, []domain.State{domain.Active})
	later := appendChanges([]string{"live-later"}, []uint64{1}, []domain.State{domain.Active})
	send(later)
	sample(last, 4, 4)
	// Verify the durable transport prefix (including commit marker) via another pool.
	end := time.Now().Add(8 * time.Second)
	for {
		cp, e := reader.Checkpoint(ctx)
		if e != nil {
			t.Fatal(e)
		}
		if cp.C >= last && cp.NextOffset > 0 {
			break
		}
		if time.Now().After(end) {
			t.Fatal("missing durable Kafka prefix", cp)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Restart discards the ephemeral view and uses only the durable generation.
	// The fresh price handshake returns 99, so the earlier 101 cannot trigger.
	stopRun()
	if e := <-stopped; e == nil {
		t.Fatal("cancelled worker succeeded")
	}
	if coordinator.Ready() {
		t.Fatal("old worker ready after restart")
	}
	resumeRegister := func(c context.Context, pos *changefeed.Position) (live.Registration, error) {
		if pos == nil {
			t.Fatal("lost durable transport identity")
		}
		if os.Getenv("KAFKA_INTEGRATION") == "1" {
			src, e := changefeed.New(ctx, changefeed.Config{Brokers: []string{os.Getenv("KAFKA_TEST_BROKER")}, Topic: pos.Topic})
			if e != nil {
				return nil, e
			}
			t.Cleanup(src.Close)
			return src.RegisterAt(c, *pos)
		}
		f := newLiveFeed()
		f.next = pos.Offset
		send = f.push
		return restartedLiveFeed{liveFeed: f, position: *pos}, nil
	}
	coordinator, err = live.NewResume(reader, client, resumeRegister, quotes, live.Config{ReplayTTL: time.Minute, PageSize: 1, HeadInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	go func() { stopped <- coordinator.Run(ctx) }()
	sample(last, 0, 3)
	connectionMu.Lock()
	q, activePrices = connection, priceReg
	connectionMu.Unlock()
	activePrices.push(4, "101")
	sample(last, 4, 4)
	// A durable duplicate after restart still cannot resurrect the cancelled row.
	send(created)
	sample(last, 4, 4)

	// Removing the last active subscription releases its price registration;
	// a later create on the same pair must run a new MAT-012 handshake.
	appendChanges([]string{"live-active", "live-sql-only", "live-gap", "live-later"}, []uint64{2, 2, 2, 2}, []domain.State{domain.Cancelled, domain.Cancelled, domain.Cancelled, domain.Cancelled})
	until := time.Now().Add(3 * time.Second)
	for {
		select {
		case <-q.Done():
			if coordinator.Ready() {
				goto emptyReady
			}
		default:
		}
		if time.Now().After(until) {
			t.Fatal("last cancellation did not release price registration")
		}
		time.Sleep(time.Millisecond)
	}
emptyReady:
	if _, err := coordinator.Current("BTCUSDT"); err == nil {
		t.Fatal("removed pair still exposed")
	}
	appendChanges([]string{"live-after-empty"}, []uint64{1}, []domain.State{domain.Active})
	sample(last, 0, 3)
	connectionMu.Lock()
	newQ, newPrices := connection, priceReg
	connectionMu.Unlock()
	if newQ == q {
		t.Fatal("reused closed price connection")
	}
	newPrices.push(4, "101")
	sample(last, 1, 4)
	// No new market traffic: price registration loss revokes readiness immediately.
	newPrices.Close()
	deadline := time.Now().Add(time.Second)
	for coordinator.Ready() {
		if time.Now().After(deadline) {
			t.Fatal("idle loss retained readiness")
		}
		time.Sleep(time.Millisecond)
	}
	if _, e := coordinator.Current("BTCUSDT"); e == nil {
		t.Fatal("view survived price loss")
	}
	select {
	case e := <-stopped:
		if e == nil {
			t.Fatal("missing loss error")
		}
		stopped <- e
	case <-time.After(3 * time.Second):
		t.Fatal("idle loss did not stop coordinator")
	}
	kafkaMu.Lock()
	r := kafkaReg
	kafkaMu.Unlock()
	if r != nil && r.Confirmed() <= r.Start().Offset {
		t.Fatal("real Kafka prefix never confirmed")
	}
}

type liveFeed struct {
	mu    sync.Mutex
	done  chan struct{}
	queue chan changefeed.Transaction
	next  int64
}

func newLiveFeed() *liveFeed {
	return &liveFeed{done: make(chan struct{}), queue: make(chan changefeed.Transaction, 16)}
}
func (r *liveFeed) Start() changefeed.Position {
	return changefeed.Position{Topic: "live-changes", Partition: 0}
}
func (r *liveFeed) Done() <-chan struct{} { return r.done }
func (r *liveFeed) Err() error            { return errors.New("feed lost") }
func (r *liveFeed) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	select {
	case <-r.done:
	default:
		close(r.done)
	}
}
func (r *liveFeed) Confirm(int64) error { return nil }
func (r *liveFeed) Next(c context.Context) (changefeed.Transaction, error) {
	select {
	case <-c.Done():
		return changefeed.Transaction{}, c.Err()
	case <-r.done:
		return changefeed.Transaction{}, r.Err()
	case tx := <-r.queue:
		return tx, nil
	}
}
func (r *liveFeed) push(events [][]byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	tx := changefeed.Transaction{}
	for i, b := range events {
		_, ch, _ := changed.Decode(b)
		if i == 0 {
			tx.First = ch.Position
		}
		tx.C = ch.TransactionEnd
		tx.Records = append(tx.Records, changefeed.Record{JSON: b, Change: ch})
	}
	r.next += int64(len(events)) + 1
	tx.NextOffset = r.next
	r.queue <- tx
}

type liveLatest func(context.Context, string) (*mpb.LatestResponse, error)

func (f liveLatest) Latest(c context.Context, s string) (*mpb.LatestResponse, error) { return f(c, s) }

type livePrices struct {
	mu    sync.Mutex
	done  chan struct{}
	queue chan pricefeed.Update
}

func newLivePrices() *livePrices {
	return &livePrices{done: make(chan struct{}), queue: make(chan pricefeed.Update, 16)}
}
func (r *livePrices) Done() <-chan struct{} { return r.done }
func (r *livePrices) Err() error {
	select {
	case <-r.done:
		return errors.New("prices lost")
	default:
		return nil
	}
}
func (r *livePrices) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	select {
	case <-r.done:
	default:
		close(r.done)
	}
}
func (r *livePrices) Next(c context.Context) (pricefeed.Update, error) {
	select {
	case <-c.Done():
		return pricefeed.Update{}, c.Err()
	case <-r.done:
		return pricefeed.Update{}, r.Err()
	case u := <-r.queue:
		return u, nil
	}
}
func (r *livePrices) TryNext() (pricefeed.Update, bool, error) {
	if e := r.Err(); e != nil {
		return pricefeed.Update{}, false, e
	}
	select {
	case u := <-r.queue:
		return u, true, nil
	default:
		return pricefeed.Update{}, false, nil
	}
}
func (r *livePrices) push(seq uint64, value string) {
	p, _ := price.Parse(value)
	r.queue <- pricefeed.Update{Quote: priceobserved.Observed{SchemaVersion: 1, Pair: "BTCUSDT", Price: &p, SourceID: value, Revision: priceobserved.Revision{StreamID: "live", Generation: 1, Sequence: seq}, TradeTime: time.Now().Add(-time.Hour), ReceivedAt: time.Now(), StreamState: priceobserved.Live}}
}

// A controlled registration also has to start at the exact SQL checkpoint.
type restartedLiveFeed struct {
	*liveFeed
	position changefeed.Position
}

func (r restartedLiveFeed) Start() changefeed.Position { return r.position }
