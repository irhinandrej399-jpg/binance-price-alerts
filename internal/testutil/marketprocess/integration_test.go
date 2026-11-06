package marketprocess

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/publish"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/storage"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/trade"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafka"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/postgres"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/apiprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/twmb/franz-go/pkg/kgo"
)

func rawTrade(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile("../../marketdata/trade/testdata/trade.json")
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// This child only verifies connectivity/credentials and the raw endpoint. It
// does not ingest, publish, reconnect or implement cmd/market-data.
func TestChild(t *testing.T) {
	if os.Getenv("MARKET_PROCESS_CHILD") != "1" {
		return
	}
	for _, name := range []string{"DATABASE_URL", "API_DATABASE_URL", "MATCHER_DATABASE_URL", "NOTIFICATIONS_DATABASE_URL", "PARENT_SECRET"} {
		if os.Getenv(name) != "" {
			os.Exit(24)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if dsn := os.Getenv("MARKET_DATA_DATABASE_URL"); dsn != "" {
		pool, err := postgres.Open(ctx, dsn)
		if err != nil {
			os.Exit(25)
		}
		var db, role string
		var own, foreign bool
		err = pool.QueryRow(ctx, `SELECT current_database(),current_user,has_database_privilege(current_user,'market_data','CONNECT'),has_database_privilege(current_user,'api','CONNECT') OR has_database_privilege(current_user,'matcher','CONNECT') OR has_database_privilege(current_user,'notifications','CONNECT')`).Scan(&db, &role, &own, &foreign)
		pool.Close()
		if err != nil || db != "market_data" || role != "market_data_runtime" || !own || foreign {
			os.Exit(26)
		}
	}
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, os.Getenv("RAW_TRADE_URL"), nil)
	if err != nil {
		os.Exit(27)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, raw, err := conn.ReadMessage()
	conn.Close()
	if err != nil || !bytes.Equal(raw, []byte(os.Getenv("EXPECTED_TRADE"))) {
		os.Exit(28)
	}
	q, err := trade.Decode(raw, trade.Metadata{Unit: priceobserved.Milliseconds, ReceivedAt: time.Now(), Generation: 1, Sequence: 1, StreamState: priceobserved.Unconfirmed})
	if err != nil || q.SourceID != "12345" || q.Pair != "BTCUSDT" {
		os.Exit(29)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	_ = http.ListenAndServe(os.Getenv("HTTP_ADDR"), mux)
	os.Exit(30)
}

func checkChild(t *testing.T, dsn string) {
	t.Helper()
	raw := rawTrade(t)
	feed := NewRawFeed(raw)
	defer feed.Close()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("DATABASE_URL", "must-not-leak")
	t.Setenv("PARENT_SECRET", "must-not-leak")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = apiprocess.With(ctx, apiprocess.Config{Executable: executable, Args: []string{"-test.run=^TestChild$"}, Env: map[string]string{"MARKET_PROCESS_CHILD": "1", "RAW_TRADE_URL": feed.URL, "EXPECTED_TRADE": string(raw), "MARKET_DATA_DATABASE_URL": dsn}, StartupTimeout: 7 * time.Second, StopTimeout: time.Second}, func(*apiprocess.Process) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
}

func TestRawTradeFromChild(t *testing.T) { checkChild(t, "") }

func TestDisposableFixture(t *testing.T) {
	if os.Getenv("MARKET_DATA_PROCESS_INTEGRATION") != "1" {
		t.Skip("run python3 scripts/check_marketdata_process.py")
	}
	if os.Getenv("POSTGRES_TEST") != "1" {
		t.Fatal("market-data opt-in requires POSTGRES_TEST=1 and its owner DSN")
	}
	pool := postgrestest.OpenWithDSN(t, "MARKET_DATA_DATABASE_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	f, err := Seed(ctx, pool, rawTrade(t))
	if err != nil {
		t.Fatal("seed failed")
	}
	meta, q, err := priceobserved.Decode(f.Value)
	if err != nil || meta.EventID != f.EventID || q.SourceID != "12345" || q.Pair != string(f.Key) || q.Revision.Generation != f.Generation {
		t.Fatal("canonical identity mismatch")
	}
	checkChild(t, os.Getenv("MARKET_DATA_DATABASE_URL"))
	// Recreate the repository, as a new owner would after restart. The fixture
	// has not claimed work; the restored claim must retain the original bytes.
	repo := storage.New(pool)
	work, err := repo.Claim(ctx, time.Minute)
	if err != nil || work.EventID != f.EventID || !bytes.Equal(work.Data, f.Value) {
		t.Fatal("restored bytes changed")
	}
	broker, prefix := os.Getenv("KAFKA_TEST_BROKER"), os.Getenv("KAFKA_TEST_PREFIX")
	if broker == "" || prefix == "" {
		t.Fatal("missing Kafka opt-in configuration")
	}
	topic := prefix + "-prices"
	producer, err := publish.New(ctx, kafka.Config{Brokers: []string{broker}, TransactionalID: prefix + "-market-relay"}, topic)
	if err != nil {
		t.Fatal("producer startup failed")
	}
	defer producer.Close()
	if err = producer.Publish(ctx, work.Data); err != nil {
		t.Fatal("publish failed")
	}
	if err = repo.MarkPublished(ctx, work.EventID, work.Token); err != nil {
		t.Fatal("mark failed")
	}
	consumer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxWait(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	for {
		fetch := consumer.PollFetches(ctx)
		if ctx.Err() != nil || len(fetch.Errors()) > 0 {
			t.Fatal("consumer failed or timed out")
		}
		records := fetch.Records()
		if len(records) == 0 {
			continue
		}
		if len(records) != 1 || !bytes.Equal(records[0].Key, f.Key) || !bytes.Equal(records[0].Value, f.Value) {
			t.Fatal("Kafka changed key/value")
		}
		got, _, err := priceobserved.Decode(records[0].Value)
		if err != nil || got.EventID != f.EventID {
			t.Fatal("Kafka changed event ID")
		}
		break
	}
	if _, err := repo.Claim(ctx, time.Second); !errors.Is(err, storage.ErrNotFound) {
		t.Fatal("work remains unpublished")
	}
}
