// Package notificationsprocess supplies disposable parent-owned API/RPC/HTTP
// fixtures. Process ownership belongs exclusively to apiprocess.
package notificationsprocess

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
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/admissionrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/deliveryrelay"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/resultrpc"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	observed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	mpb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/latest"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/storage"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafka"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/grpctest"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
)

const NotificationsURI = "spiffe://process.test/notifications"
const apiURI = "spiffe://process.test/api"
const ServerName = "market.test"

// Fixture includes ephemeral secret paths: never log it or its environment.
type Fixture struct {
	Conn                      *grpc.ClientConn
	Target, Broker, Prefix    string
	CAFile, CertFile, KeyFile string
	Telegram                  *Telegram
	api                       *pgxpool.Pool
	quotes                    *quotes
	gates                     *rpcGates
}
type quotes struct {
	mu    sync.Mutex
	value *observed.Observed
}

func (q *quotes) Latest(_ context.Context, pair string) (observed.Observed, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.value == nil || q.value.Pair != pair {
		return observed.Observed{}, storage.ErrNotFound
	}
	return *q.value, nil
}

func New(t testing.TB, api *pgxpool.Pool, broker, prefix string) *Fixture {
	t.Helper()
	f := &Fixture{api: api, Broker: broker, Prefix: prefix, quotes: &quotes{}, gates: &rpcGates{}}
	f.Telegram = NewTelegram(t)
	ca := newAuthority(t)
	server := ca.issue(t, nil, true, false)
	notification := ca.issue(t, []string{NotificationsURI}, false, false)
	apiClient := ca.issue(t, []string{apiURI}, false, false)
	dir := t.TempDir()
	f.CAFile, f.CertFile, f.KeyFile = filepath.Join(dir, "ca.pem"), filepath.Join(dir, "notifications.pem"), filepath.Join(dir, "notifications.key")
	for path, data := range map[string][]byte{f.CAFile: ca.pem, f.CertFile: notification.cert, f.KeyFile: notification.key} {
		if os.WriteFile(path, data, 0600) != nil {
			t.Fatal("write ephemeral credentials")
		}
	}
	opts, err := serviceauth.ServerOptions(server.cert, server.key, ca.pem, map[string][]string{
		NotificationsURI: {pb.Admission_PrepareRecipient_FullMethodName, pb.Admission_AdmitDelivery_FullMethodName, pb.DeliveryResults_SaveDeliveryResult_FullMethodName},
		apiURI:           {mpb.Quotes_Latest_FullMethodName},
	})
	if err != nil {
		t.Fatal("server credentials")
	}
	dial := func(p pair) []grpc.DialOption {
		creds, e := serviceauth.ClientCredentials(p.cert, p.key, ca.pem)
		if e != nil {
			t.Fatal("client credentials")
		}
		return []grpc.DialOption{grpc.WithTransportCredentials(creds), grpc.WithAuthority(ServerName)}
	}
	// API's Latest client uses its own identity; the child cannot call Latest or
	// obtain matcher Delivery/Snapshot rights with its notification certificate.
	quotesConn := grpctest.StartWithClientOptions(t, func(g *grpc.Server) { mpb.RegisterQuotesServer(g, latest.NewServer(f.quotes, time.Now)) }, dial(apiClient), opts...)
	admission, err := admissionrpc.NewServer(admissionrpc.NewPostgres(api), latest.NewClient(quotesConn), time.Minute, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	opts = append(opts, grpc.ChainUnaryInterceptor(f.gates.intercept))
	f.Conn = grpctest.StartWithClientOptions(t, func(g *grpc.Server) {
		pb.RegisterAdmissionServer(g, admission)
		pb.RegisterDeliveryResultsServer(g, resultrpc.NewServer(resultrpc.NewPostgres(api)))
	}, dial(notification), opts...)
	f.Target = strings.TrimPrefix(f.Conn.Target(), "passthrough:///")
	t.Cleanup(f.gates.release)
	return f
}

func (f *Fixture) ChildEnv(dsn string) map[string]string {
	return map[string]string{
		"NOTIFICATIONS_DATABASE_URL": dsn, "KAFKA_BROKERS": f.Broker, "DELIVERY_REQUESTS_TOPIC": f.Prefix + "-delivery-requests",
		"API_RPC_ADDR": f.Target, "RPC_SERVER_NAME": ServerName, "RPC_CA_FILE": f.CAFile, "RPC_CERT_FILE": f.CertFile, "RPC_KEY_FILE": f.KeyFile,
		"TELEGRAM_BASE_URL": f.Telegram.URL, "TELEGRAM_BOT_TOKEN": TestToken,
	}
}

// SetPrice retains original timestamps. Latest reads never silently refresh it.
func (f *Fixture) SetPrice(value string, seq uint64) observed.Observed {
	p, e := price.Parse(value)
	if e != nil {
		panic("invalid fixture price")
	}
	now := time.Now().UTC()
	q := observed.Observed{SchemaVersion: 1, Pair: "BTCUSDT", Price: &p, SourceID: fmt.Sprint(seq), Revision: observed.Revision{StreamID: "notifications-process", Generation: 1, Sequence: seq}, TradeTime: now, ReceivedAt: now, StreamState: observed.Live}
	f.quotes.mu.Lock()
	f.quotes.value = &q
	f.quotes.mu.Unlock()
	return q
}
func uuid() string {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic("fixture entropy unavailable")
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}

type Dataset struct {
	Record    sub.Record
	Key, Body []byte
}

// Seed creates one active subscription and canonical API workflow/outbox using
// quota.Create and RequestDelivery. Discard the dataset on partial seed failure.
// It does not set Latest: event candidate price is never admission permission.
func (f *Fixture) Seed(ctx context.Context) (Dataset, error) {
	var d Dataset
	tx, e := f.api.Begin(ctx)
	if e != nil {
		return d, e
	}
	defer tx.Rollback(context.Background())
	owner, e := account.New(tx).Create(ctx, uuid()+"@example.test", "fixture-hash")
	if e != nil {
		return d, e
	}
	chat, e := strconv.ParseInt(strings.ReplaceAll(owner.ID, "-", "")[:15], 16, 64)
	if e != nil {
		return d, e
	}
	if _, e = tx.Exec(ctx, `UPDATE app.users SET telegram_chat_id=$2,telegram_version=1 WHERE id=$1`, owner.ID, chat+1); e != nil {
		return d, e
	}
	quota, e := sub.LockQuota(ctx, tx, owner.ID)
	if e != nil {
		return d, e
	}
	p, _ := price.Parse("100")
	r := sub.Record{ID: uuid(), OwnerID: owner.ID, Pair: "BTCUSDT", Threshold: p, Direction: price.Up, CreationPrice: p, CreationTradeTime: time.Now().UTC().Truncate(time.Microsecond), State: domain.Active, Version: 1, Activation: sub.LatestPrice}
	if e = quota.Create(ctx, r); e != nil {
		return d, e
	}
	if e = tx.Commit(ctx); e != nil {
		return d, e
	}
	q := observed.Observed{SchemaVersion: 1, Pair: r.Pair, Price: &p, SourceID: "seed", Revision: observed.Revision{StreamID: "seed", Generation: 1, Sequence: 1}, TradeTime: r.CreationTradeTime, ReceivedAt: r.CreationTradeTime, StreamState: observed.Unconfirmed}
	d.Record, e = sub.RequestDelivery(ctx, f.api, sub.DeliveryCandidate{OwnerID: r.OwnerID, SubscriptionID: r.ID, ExpectedVersion: 1, Quote: q})
	if e != nil {
		return d, e
	}
	d.Key = []byte(r.ID)
	e = f.api.QueryRow(ctx, `SELECT body FROM app.delivery_outbox WHERE subscription_id=$1`, r.ID).Scan(&d.Body)
	return d, e
}
func (f *Fixture) Publish(ctx context.Context) error {
	r, e := deliveryrelay.New(f.api, kafka.Config{Brokers: []string{f.Broker}, TransactionalID: f.Prefix + "-delivery-relay"}, f.Prefix+"-delivery-requests")
	if e != nil {
		return e
	}
	for {
		worked, e := r.Step(ctx)
		if e != nil {
			return e
		}
		if !worked {
			return nil
		}
	}
}
func (f *Fixture) Result(ctx context.Context, d Dataset) (sub.Record, error) {
	return sub.New(f.api).ByID(ctx, d.Record.OwnerID, d.Record.ID)
}
