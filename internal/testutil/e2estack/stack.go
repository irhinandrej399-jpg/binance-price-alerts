// Package e2estack composes the existing disposable runner, process lifecycle,
// and vendor fakes with four production executables. It installs no RPC fixtures.
package e2estack

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/intake"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/postgres"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/apiprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/binancefake"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/telegramfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"google.golang.org/grpc"
)

const (
	apiURI           = "spiffe://process.test/api"
	matcherURI       = "spiffe://process.test/matcher"
	notificationsURI = "spiffe://process.test/notifications"
	cleanerURI       = "spiffe://process.test/cleaner"
)

// Stack contains ephemeral handles and must not be logged. New provisions once;
// With may restart the same dataset without changing source IDs or bootstraps.
// Use sequentially, under the E2E-005 runner, and keep ctx bounded.
type Stack struct {
	Binance                             *binancefake.Fake
	Telegram                            *telegramfake.Fake
	API, Market, Matcher, Notifications *apiprocess.Process
	MarketConn                          *grpc.ClientConn // API identity: Fetch/Latest on real market-data.
	configs                             []apiprocess.Config
}

// New composes ready infrastructure interfaces; it never modifies SQL feed state.
func New(t testing.TB, ctx context.Context) *Stack {
	t.Helper()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	if os.Getenv("E2E_PROCESS_INTEGRATION") != "1" {
		t.Fatal("e2estack requires disposable e2e runner")
	}
	for _, k := range []string{"DATABASE_URL", "MARKET_DATA_DATABASE_URL", "MATCHER_DATABASE_URL", "NOTIFICATIONS_DATABASE_URL", "KAFKA_TEST_BROKER", "KAFKA_TEST_PREFIX"} {
		if os.Getenv(k) == "" {
			t.Fatal("e2estack requires all four DBs and Kafka")
		}
	}
	s := &Stack{Binance: binancefake.New(), Telegram: telegramfake.New()}
	t.Cleanup(s.Binance.Close)
	t.Cleanup(s.Telegram.Close)
	ca := newAuthority(t)
	dir := t.TempDir()
	write := func(name string, b []byte) string {
		path := filepath.Join(dir, name)
		must(os.WriteFile(path, b, 0600))
		return path
	}
	caFile := write("ca.pem", ca.pem)
	serverCert, serverKey := map[string]string{}, map[string]string{}
	clientCert, clientKey := map[string]string{}, map[string]string{}
	for _, name := range []string{"market-data", "api", "matcher", "notifications"} {
		server := ca.issue(t, nil, true)
		serverCert[name], serverKey[name] = write(name+"-server.pem", server.cert), write(name+"-server.key", server.key)
	}
	var apiClient pair
	for name, uri := range map[string]string{"api": apiURI, "matcher": matcherURI, "notifications": notificationsURI} {
		p := ca.issue(t, []string{uri}, false)
		clientCert[name], clientKey[name] = write(name+"-client.pem", p.cert), write(name+"-client.key", p.key)
		if name == "api" {
			apiClient = p
		}
	}
	addr := func() string {
		l, err := net.Listen("tcp4", "127.0.0.1:0")
		must(err)
		a := l.Addr().String()
		must(l.Close())
		return a
	}
	marketAddr, apiAddr, matcherAddr, notificationsAddr := addr(), addr(), addr(), addr()
	broker, prefix := os.Getenv("KAFKA_TEST_BROKER"), os.Getenv("KAFKA_TEST_PREFIX")
	c, err := kgo.NewClient(kgo.SeedBrokers(broker))
	must(err)
	defer c.Close()
	sources := map[string]kafkasource.Identity{}
	for name, count := range map[string]int{"prices": 16, "subscription-changes": 1, "delivery-requests": 16} {
		source, parts, err := kafkasource.Metadata(ctx, c, prefix+"-"+name)
		must(err)
		if len(parts) != count {
			t.Fatal("unexpected partition count")
		}
		sources[name] = source
		// Bootstrap is permitted only on a new empty dataset, before any producer.
		for _, timestamp := range []int64{-2, -1} {
			req := kmsg.NewPtrListOffsetsRequest()
			req.IsolationLevel = 1
			topic := kmsg.NewListOffsetsRequestTopic()
			topic.Topic = source.Topic
			for _, p := range parts {
				part := kmsg.NewListOffsetsRequestTopicPartition()
				part.Partition = p.Partition
				part.Timestamp = timestamp
				topic.Partitions = append(topic.Partitions, part)
			}
			req.Topics = []kmsg.ListOffsetsRequestTopic{topic}
			res, err := req.RequestWith(ctx, c)
			must(err)
			if len(res.Topics) != 1 || len(res.Topics[0].Partitions) != count {
				t.Fatal("incomplete empty dataset proof")
			}
			seen := map[int32]bool{}
			for _, p := range res.Topics[0].Partitions {
				if p.ErrorCode != 0 || p.Offset != 0 || p.Partition < 0 || int(p.Partition) >= count || seen[p.Partition] {
					t.Fatal("dataset not new and empty")
				}
				seen[p.Partition] = true
			}
		}
		t.Logf("new source %s cluster=%s topic_id=%x partitions=%d", name, source.ClusterID, source.TopicID, count)
	}
	if sources["prices"].ClusterID != sources["subscription-changes"].ClusterID || sources["prices"].ClusterID != sources["delivery-requests"].ClusterID {
		t.Fatal("different source clusters")
	}
	binding := intake.Binding{ConsumerID: prefix + "-notifications", Generation: prefix + "-generation-1", Source: sources["delivery-requests"]}
	pool, err := postgres.Open(ctx, os.Getenv("NOTIFICATIONS_DATABASE_URL"))
	must(err)
	partitions := make([]int32, 16)
	bootstrap := map[int32]int64{}
	for p := int32(0); p < 16; p++ {
		partitions[p] = p
		bootstrap[p] = 0
	}
	err = intake.NewStore(pool).Bootstrap(ctx, binding, partitions)
	pool.Close()
	must(err)
	vector, err := json.Marshal(bootstrap)
	must(err)
	topicID := func(name string) string { id := sources[name].TopicID; return hex.EncodeToString(id[:]) }
	market := map[string]string{
		"MARKET_DATA_DATABASE_URL": os.Getenv("MARKET_DATA_DATABASE_URL"), "KAFKA_BROKERS": broker, "PRICES_TOPIC": prefix + "-prices", "MARKET_DATA_RELAY_TRANSACTIONAL_ID": prefix + "-market-relay",
		"RAW_TRADE_URL": s.Binance.WSURL(), "CATALOG_BASE_URL": s.Binance.URL(),
		"RPC_ADDR": marketAddr, "RPC_CERT_FILE": serverCert["market-data"], "RPC_KEY_FILE": serverKey["market-data"], "RPC_CA_FILE": caFile, "API_SERVICE_URI": apiURI, "MATCHER_SERVICE_URI": matcherURI,
	}
	api := map[string]string{
		"DATABASE_URL": os.Getenv("DATABASE_URL"), "KAFKA_BROKERS": broker, "SUBSCRIPTION_CHANGES_TOPIC": prefix + "-subscription-changes", "DELIVERY_REQUESTS_TOPIC": prefix + "-delivery-requests",
		"SUBSCRIPTION_RELAY_TRANSACTIONAL_ID": prefix + "-api-changes", "DELIVERY_RELAY_TRANSACTIONAL_ID": prefix + "-api-delivery", "TELEGRAM_WEBHOOK_SECRET": telegramfake.Secret,
		"RPC_ADDR": apiAddr, "RPC_CERT_FILE": serverCert["api"], "RPC_KEY_FILE": serverKey["api"], "RPC_CA_FILE": caFile, "MATCHER_SERVICE_URI": matcherURI, "NOTIFICATIONS_SERVICE_URI": notificationsURI, "ADMISSION_PERMIT_TTL": "10s",
		"MARKET_RPC_ADDR": marketAddr, "MARKET_RPC_SERVER_NAME": "market.test", "MARKET_RPC_CERT_FILE": clientCert["api"], "MARKET_RPC_KEY_FILE": clientKey["api"], "MARKET_RPC_CA_FILE": caFile,
	}
	matcher := map[string]string{
		"MATCHER_DATABASE_URL": os.Getenv("MATCHER_DATABASE_URL"), "KAFKA_BROKERS": broker, "PRICES_TOPIC": prefix + "-prices", "SUBSCRIPTION_CHANGES_TOPIC": prefix + "-subscription-changes",
		"KAFKA_CLUSTER_ID": sources["prices"].ClusterID, "PRICES_TOPIC_ID": topicID("prices"), "SUBSCRIPTION_CHANGES_TOPIC_ID": topicID("subscription-changes"), "PRICES_BOOTSTRAP": string(vector),
		"MATCHER_ENVIRONMENT": prefix, "MATCHER_GROUP": prefix + "-matcher", "MATCHER_REPLICA_ID": "one",
		"API_RPC_ADDR": apiAddr, "LATEST_RPC_ADDR": marketAddr, "RPC_SERVER_NAME": "market.test", "RPC_CA_FILE": caFile, "RPC_CERT_FILE": clientCert["matcher"], "RPC_KEY_FILE": clientKey["matcher"],
		"MATCHER_RPC_ADDR": matcherAddr, "MATCHER_RPC_CERT_FILE": serverCert["matcher"], "MATCHER_RPC_KEY_FILE": serverKey["matcher"], "CLEANER_SERVICE_URI": cleanerURI,
		"SNAPSHOT_TTL": "1h", "REPLAY_TTL": "1h", "HEAD_INTERVAL": "100ms", "SNAPSHOT_PAGE_SIZE": "1000", "REPLAY_PAGE_SIZE": "100",
	}
	notifications := map[string]string{
		"NOTIFICATIONS_DATABASE_URL": os.Getenv("NOTIFICATIONS_DATABASE_URL"), "KAFKA_BROKERS": broker, "DELIVERY_REQUESTS_TOPIC": prefix + "-delivery-requests", "NOTIFICATIONS_GROUP": prefix + "-notifications",
		"NOTIFICATIONS_CONSUMER_ID": binding.ConsumerID, "NOTIFICATIONS_GENERATION": binding.Generation, "KAFKA_CLUSTER_ID": binding.Source.ClusterID, "DELIVERY_REQUESTS_TOPIC_ID": topicID("delivery-requests"),
		"API_RPC_ADDR": apiAddr, "RPC_SERVER_NAME": "market.test", "RPC_IDENTITY": notificationsURI, "RPC_CA_FILE": caFile, "RPC_CERT_FILE": clientCert["notifications"], "RPC_KEY_FILE": clientKey["notifications"],
		"RPC_ADDR": notificationsAddr, "RPC_SERVER_CERT_FILE": serverCert["notifications"], "RPC_SERVER_KEY_FILE": serverKey["notifications"], "CLEANER_SERVICE_URI": cleanerURI,
		"TELEGRAM_BASE_URL": s.Telegram.URL(), "TELEGRAM_BOT_TOKEN": telegramfake.Token, "TELEGRAM_TIMEOUT": "1s", "BOT_RATE": "25", "CHAT_INTERVAL": "1s",
		"RETRY_BASE_DELAY": "100ms", "RETRY_MAX_DELAY": "2s", "CONDITION_INTERVAL": "100ms", "MAX_FAILURES": "5", "LEASE_TTL": "5s", "RECOVERY_INTERVAL": "1s", "REPORT_LIMIT": "10", "WORKER_CONCURRENCY": "2", "WORKER_INTERVAL": "20ms", "WORKER_TIMEOUT": "3s", "WRITE_TIMEOUT": "1s",
	}
	root, err := os.Getwd()
	must(err)
	for {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(root)
		if parent == root {
			t.Fatal("repository root unavailable")
		}
		root = parent
	}
	for i, name := range []string{"market-data", "api", "matcher", "notifications"} {
		binary := filepath.Join(dir, name)
		cmd := exec.CommandContext(ctx, "go", "build", "-race", "-o", binary, "./cmd/"+name)
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build %s failed: %s", name, out)
		}
		s.configs = append(s.configs, apiprocess.Config{Executable: binary, Env: []map[string]string{market, api, matcher, notifications}[i], StartupProbePath: "/healthz", StartupTimeout: 20 * time.Second, StopTimeout: 22 * time.Second})
	}
	creds, err := serviceauth.ClientCredentials(apiClient.cert, apiClient.key, ca.pem)
	must(err)
	s.MarketConn, err = grpc.NewClient(marketAddr, grpc.WithTransportCredentials(creds), grpc.WithAuthority("market.test"))
	must(err)
	t.Cleanup(func() { s.MarketConn.Close() })
	return s
}

// With starts in dependency order and stops in reverse order. Existing lifecycle
// handles error/panic/Goexit too. A successful return requires graceful exit 0.
// The callback is entered at HTTP liveness; call Ready after driving the feed.
func (s *Stack) With(ctx context.Context, check func(*Stack) error) error {
	handles := []**apiprocess.Process{&s.Market, &s.API, &s.Matcher, &s.Notifications}
	names := []string{"market-data", "api", "matcher", "notifications"}
	var start func(int) error
	start = func(i int) error {
		if i == len(s.configs) {
			return check(s)
		}
		return apiprocess.With(ctx, s.configs[i], func(p *apiprocess.Process) error {
			*handles[i] = p
			err := start(i + 1)
			stopErr := p.Stop()
			code, ok := p.ExitCode()
			if !ok || code != 0 {
				stopErr = errors.Join(stopErr, fmt.Errorf("%s non-graceful exit %d", names[i], code))
			}
			return errors.Join(err, stopErr)
		})
	}
	return start(0)
}

// Ready checks the profile's actual readiness, including unconfirmed raw feed.
// All four processes are observed while waiting; this never mutates SQL state.
func (s *Stack) Ready(ctx context.Context) error {
	transport := &http.Transport{Proxy: nil}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for {
		ready := true
		for i, p := range []*apiprocess.Process{s.Market, s.API, s.Matcher, s.Notifications} {
			select {
			case <-p.Done():
				return errors.New("required process exited during readiness")
			default:
			}
			for _, path := range []string{"/healthz", "/readyz"} {
				req, err := http.NewRequestWithContext(ctx, "GET", p.URL+path, nil)
				if err != nil {
					return err
				}
				res, err := client.Do(req)
				if err != nil {
					ready = false
					continue
				}
				body, readErr := io.ReadAll(io.LimitReader(res.Body, 4096))
				res.Body.Close()
				expected := 200
				if i == 0 && path == "/readyz" {
					expected = 503
					var state struct {
						Feed string `json:"feed"`
					}
					if json.Unmarshal(body, &state) != nil || state.Feed != "unconfirmed" {
						ready = false
					}
				}
				if readErr != nil || res.StatusCode != expected {
					ready = false
				}
			}
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.New("profile readiness deadline exceeded")
		case <-time.After(25 * time.Millisecond):
		}
	}
}

// CheckQuote verifies transport, catalog and unconfirmed Latest on the actual
// market-data executable. Scenario must send a fresh Trade first.
func (s *Stack) CheckQuote(ctx context.Context, sourceID, price string) error {
	catalog, err := pb.NewCatalogClient(s.MarketConn).Fetch(ctx, &pb.CatalogRequest{}, grpc.WaitForReady(true))
	if err != nil {
		return errors.New("runtime catalog unavailable")
	}
	pairs := map[string]bool{}
	for _, p := range catalog.Pairs {
		pairs[p.Symbol] = true
	}
	if len(pairs) != 2 || !pairs["BTCUSDT"] || !pairs["SOLUSDT"] {
		return errors.New("runtime catalog differs from fake")
	}
	for {
		q, err := pb.NewQuotesClient(s.MarketConn).Latest(ctx, &pb.LatestRequest{Pair: "BTCUSDT"})
		if err == nil && q.Quote != nil && q.Quote.SourceId == sourceID {
			if q.Quote.Price != price || q.Quote.StreamState != "unconfirmed" || q.Quote.Revision == nil || q.Quote.Revision.Generation == 0 {
				return errors.New("runtime quote changed")
			}
			for _, w := range q.Warnings {
				if w == pb.Warning_WARNING_STREAM_UNCONFIRMED {
					return nil
				}
			}
			return errors.New("runtime quote lacks unconfirmed warning")
		}
		select {
		case <-ctx.Done():
			return errors.New("runtime quote deadline exceeded")
		case <-time.After(25 * time.Millisecond):
		}
	}
}

// AssertStopped checks reaping and closed HTTP listeners after With returns.
func (s *Stack) AssertStopped(ctx context.Context) error {
	for _, p := range []*apiprocess.Process{s.Notifications, s.Matcher, s.API, s.Market} {
		if p == nil {
			return errors.New("missing process handle")
		}
		if code, ok := p.ExitCode(); !ok || code != 0 {
			return errors.New("process did not exit gracefully")
		}
		conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", strings.TrimPrefix(p.URL, "http://"))
		if err == nil {
			conn.Close()
			return errors.New("HTTP listener survived stop")
		}
	}
	return nil
}

// NextFeed waits for the catalog-owned BTC/SOL subscription ACKs.
func (s *Stack) NextFeed(ctx context.Context) (*binancefake.Connection, error) {
	c, err := s.Binance.Next(ctx)
	if err != nil {
		return nil, err
	}
	for {
		names := c.Streams()
		if len(names) == 2 && names[0] == "btcusdt@trade" && names[1] == "solusdt@trade" {
			return c, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}
