// Package matcherprocess supplies an API-owned fixture and protected loopback
// RPCs for disposable matcher process tests. It contains no matcher runtime.
package matcherprocess

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/deliveryrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/journal"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/relay"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/replayrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/snapshotrpc"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	observed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	changed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/subscriptionchanged"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	mpb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/latest"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/storage"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/changefeed"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/live"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricefeed"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafka"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/grpctest"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"google.golang.org/grpc"
)

const MatcherURI = "spiffe://process.test/matcher"
const ServerName = "market.test"

// Fixture must not be logged: it contains paths to ephemeral service secrets.
// API SQL is used only by the parent. Every child environment is an allowlist.
type Fixture struct {
	Conn                      *grpc.ClientConn
	Target, Broker, Prefix    string
	CAFile, CertFile, KeyFile string
	api                       *pgxpool.Pool
	quotes                    *quotes
	delivery                  *deliveryGate
}

type quotes struct {
	mu     sync.Mutex
	values map[string]observed.Observed
}

func (q *quotes) Latest(_ context.Context, pair string) (observed.Observed, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	v, ok := q.values[pair]
	if !ok {
		return v, storage.ErrNotFound
	}
	return v, nil
}

// New uses the existing grpctest lifecycle and production API adapters. api is
// the parent API-owner pool; nil is allowed only for Latest-only unit tests.
func New(t testing.TB, api *pgxpool.Pool, broker, prefix string) *Fixture {
	t.Helper()
	f := &Fixture{api: api, Broker: broker, Prefix: prefix, quotes: &quotes{values: map[string]observed.Observed{}}}
	ca := newAuthority(t)
	server := ca.issue(t, nil, true, false)
	client := ca.issue(t, []string{MatcherURI}, false, false)
	dir := t.TempDir()
	f.CAFile, f.CertFile, f.KeyFile = filepath.Join(dir, "ca.pem"), filepath.Join(dir, "matcher.pem"), filepath.Join(dir, "matcher.key")
	for path, data := range map[string][]byte{f.CAFile: ca.pem, f.CertFile: client.cert, f.KeyFile: client.key} {
		if os.WriteFile(path, data, 0600) != nil {
			t.Fatal("write ephemeral credentials")
		}
	}
	methods := []string{pb.Delivery_RequestDelivery_FullMethodName, pb.SubscriptionSnapshots_BuildSnapshot_FullMethodName, pb.SubscriptionSnapshots_ReadSnapshotPage_FullMethodName, pb.SubscriptionReplay_BeginReplay_FullMethodName, pb.SubscriptionReplay_ReadReplayPage_FullMethodName, pb.SubscriptionReplay_AcknowledgeReplay_FullMethodName, pb.SubscriptionReplay_AbandonReplay_FullMethodName}
	opts, err := serviceauth.ServerOptions(server.cert, server.key, ca.pem, map[string][]string{MatcherURI: append(methods, mpb.Quotes_Latest_FullMethodName)})
	if err != nil {
		t.Fatal("server credentials")
	}
	creds, err := serviceauth.ClientCredentials(client.cert, client.key, ca.pem)
	if err != nil {
		t.Fatal("client credentials")
	}
	// The controlled source uses the production Latest wire adapter, including
	// freshness. Delivery itself calls Latest over mTLS, as the API owner would.
	f.delivery = &deliveryGate{}
	f.Conn = grpctest.StartWithClientOptions(t, func(g *grpc.Server) {
		// Configured below before any request is made through the lazy connection.
		registerLatest(g, f.quotes)
		pins := sub.NewSnapshots(api)
		pb.RegisterSubscriptionSnapshotsServer(g, snapshotrpc.NewServer(pins))
		pb.RegisterSubscriptionReplayServer(g, replayrpc.NewServer(pins, api))
		pb.RegisterDeliveryServer(g, f.delivery)
	}, []grpc.DialOption{grpc.WithTransportCredentials(creds), grpc.WithAuthority(ServerName)}, opts...)
	f.Target = strings.TrimPrefix(f.Conn.Target(), "passthrough:///")
	f.delivery.base = deliveryrpc.NewServer(deliveryrpc.NewPostgres(api), latest.NewClient(f.Conn), time.Now)
	t.Cleanup(f.delivery.release)
	return f
}

// ChildEnv never copies the parent environment or the fixture's API DSN.
func (f *Fixture) ChildEnv(matcherDSN string) map[string]string {
	return map[string]string{
		"MATCHER_DATABASE_URL":       matcherDSN,
		"KAFKA_BROKERS":              f.Broker,
		"SUBSCRIPTION_CHANGES_TOPIC": f.Prefix + "-subscription-changes",
		"PRICES_TOPIC":               f.Prefix + "-prices",
		"API_RPC_ADDR":               f.Target,
		"LATEST_RPC_ADDR":            f.Target,
		"RPC_SERVER_NAME":            ServerName,
		"RPC_CA_FILE":                f.CAFile,
		"RPC_CERT_FILE":              f.CertFile,
		"RPC_KEY_FILE":               f.KeyFile,
	}
}

// SetPrice returns the exact immutable observation served by Latest and encoded
// by PublishPrice. Repeated reads never renew its timestamps.
func (f *Fixture) SetPrice(pair, value string, seq uint64) observed.Observed {
	p, err := price.Parse(value)
	if err != nil {
		panic("invalid fixture price")
	}
	now := time.Now().UTC()
	q := observed.Observed{SchemaVersion: 1, Pair: pair, Price: &p, SourceID: fmt.Sprint(seq), Revision: observed.Revision{StreamID: "matcher-process", Generation: 1, Sequence: seq}, TradeTime: now, ReceivedAt: now, StreamState: observed.Live}
	f.quotes.mu.Lock()
	f.quotes.values[pair] = q
	f.quotes.mu.Unlock()
	return q
}

func uuid() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("fixture entropy unavailable")
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}

type Dataset struct {
	OwnerID, Pair string
	IDs           []string
}

// Seed creates three active candidates in one canonical API transaction. A
// failed seed requires discarding the disposable dataset; no handcrafted journal.
func (f *Fixture) Seed(ctx context.Context) (Dataset, error) {
	var d Dataset
	tx, err := f.api.Begin(ctx)
	if err != nil {
		return d, err
	}
	defer tx.Rollback(context.Background())
	owner, err := account.New(tx).Create(ctx, uuid()+"@example.test", "fixture-hash")
	if err != nil {
		return d, err
	}
	d.OwnerID, d.Pair = owner.ID, "BTCUSDT"
	// Independent datasets in one disposable DB need distinct channel bindings.
	chat, err := strconv.ParseInt(strings.ReplaceAll(owner.ID, "-", "")[:15], 16, 64)
	if err != nil {
		return d, err
	}
	if _, err = tx.Exec(ctx, `UPDATE app.users SET telegram_chat_id=$2,telegram_version=1 WHERE id=$1`, owner.ID, chat+1); err != nil {
		return d, err
	}
	quota, err := sub.LockQuota(ctx, tx, owner.ID)
	if err != nil {
		return d, err
	}
	p, _ := price.Parse("100")
	var entries []journal.Entry
	for i := 0; i < 3; i++ {
		id := uuid()
		d.IDs = append(d.IDs, id)
		err = quota.Create(ctx, sub.Record{ID: id, OwnerID: owner.ID, Pair: d.Pair, Threshold: p, Direction: price.Up, CreationPrice: p, CreationTradeTime: time.Now().UTC(), State: domain.Active, Version: 1, Activation: sub.LatestPrice})
		if err != nil {
			return d, err
		}
		// Match the API command: journal the persisted after-image, including
		// PostgreSQL timestamp precision. Append the whole batch exactly once,
		// after every business lock/read/write has finished.
		record, readErr := sub.New(tx).ByID(ctx, owner.ID, id)
		if readErr != nil {
			return d, readErr
		}
		eventID := uuid()
		entries = append(entries, journal.Entry{
			Metadata: event.Envelope{EventID: eventID, Type: changed.Type, Version: event.Version1, OccurredAt: time.Now().UTC(), CorrelationID: eventID, OrderingKey: id},
			Change:   changed.Changed{SchemaVersion: changed.Version2, OwnerID: record.OwnerID, SubscriptionID: record.ID, SubscriptionVersion: uint64(record.Version), State: record.State, Pair: record.Pair, Threshold: &record.Threshold, Direction: record.Direction, Activation: record.Activation, CreationSnapshot: &changed.Snapshot{Price: &record.CreationPrice, TradeTime: record.CreationTradeTime}},
		})
	}
	if _, err = journal.Append(ctx, tx, entries); err != nil {
		return d, err
	}
	return d, tx.Commit(ctx)
}

// PublishChanges flushes canonical whole journal transactions using the actual
// API relay. Invoke again explicitly after API changes; no background reset.
func (f *Fixture) PublishChanges(ctx context.Context) error {
	r, err := relay.New(f.api, kafka.Config{Brokers: []string{f.Broker}, TransactionalID: f.Prefix + "-changes-relay"}, f.Prefix+"-subscription-changes")
	if err != nil {
		return err
	}
	for {
		worked, err := r.Step(ctx)
		if err != nil {
			return err
		}
		if !worked {
			return nil
		}
	}
}

// Bootstrap captures the complete read_committed LSO vector BEFORE test prices
// are published. It is never recomputed on restart; SQL prefixes take priority.
func (f *Fixture) Bootstrap(ctx context.Context) (map[int32]int64, error) {
	c, err := kgo.NewClient(kgo.SeedBrokers(f.Broker))
	if err != nil {
		return nil, err
	}
	defer c.Close()
	req := kmsg.NewPtrListOffsetsRequest()
	req.IsolationLevel = 1
	topic := kmsg.NewListOffsetsRequestTopic()
	topic.Topic = f.Prefix + "-prices"
	for p := int32(0); p < 16; p++ {
		part := kmsg.NewListOffsetsRequestTopicPartition()
		part.Partition = p
		part.Timestamp = -1
		topic.Partitions = append(topic.Partitions, part)
	}
	req.Topics = []kmsg.ListOffsetsRequestTopic{topic}
	res, err := req.RequestWith(ctx, c)
	if err != nil {
		return nil, err
	}
	offsets := map[int32]int64{}
	if len(res.Topics) != 1 {
		return nil, fmt.Errorf("bootstrap topic missing")
	}
	for _, p := range res.Topics[0].Partitions {
		if p.ErrorCode != 0 || p.Partition < 0 || p.Partition >= 16 || p.Offset < 0 {
			return nil, fmt.Errorf("bootstrap partition unavailable")
		}
		offsets[p.Partition] = p.Offset
	}
	if len(offsets) != 16 {
		return nil, fmt.Errorf("bootstrap incomplete")
	}
	return offsets, nil
}

func (f *Fixture) PublishPrice(ctx context.Context, q observed.Observed) (*kgo.Record, error) {
	b, err := observed.Encode(event.Envelope{EventID: uuid(), Type: observed.Type, Version: 1, OccurredAt: q.ReceivedAt, CorrelationID: "matcher-process", OrderingKey: q.Pair}, q)
	if err != nil {
		return nil, err
	}
	c, err := kgo.NewClient(kgo.SeedBrokers(f.Broker), kgo.TransactionalID(f.Prefix+"-price-fixture"), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		return nil, err
	}
	defer c.Close()
	r := &kgo.Record{Topic: f.Prefix + "-prices", Partition: pricefeed.PairPartition(q.Pair), Key: []byte(q.Pair), Value: b}
	if err = c.BeginTransaction(); err != nil {
		return nil, err
	}
	if err = c.ProduceSync(ctx, r).FirstErr(); err != nil {
		return nil, err
	}
	err = c.EndTransaction(ctx, kgo.TryCommit)
	return r, err
}

// Changes opens the real Kafka feed at registration LSO, or the exact durable
// cursor on resume. Close also joins the owning Source and releases its client.
func (f *Fixture) Changes(ctx context.Context, pos *changefeed.Position) (live.Registration, error) {
	source, err := changefeed.New(ctx, changefeed.Config{Brokers: []string{f.Broker}, Topic: f.Prefix + "-subscription-changes"})
	if err != nil {
		return nil, err
	}
	var reg *changefeed.Registration
	if pos == nil {
		reg, err = source.Register(ctx)
	} else {
		reg, err = source.RegisterAt(ctx, *pos)
	}
	if err != nil {
		source.Close()
		return nil, err
	}
	return &registration{Registration: reg, source: source}, nil
}

type registration struct {
	*changefeed.Registration
	source *changefeed.Source
}

func (r *registration) Close() { r.source.Close() }
