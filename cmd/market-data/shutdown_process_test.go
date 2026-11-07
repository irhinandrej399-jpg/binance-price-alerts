package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/apiprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/kafkafault"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/marketprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/twmb/franz-go/pkg/kgo"
)

// A test executable uses the production signal/owner path with the existing
// Kafka fault dialer. Production configuration gains no test fault switches.
func TestShutdownChild(t *testing.T) {
	if os.Getenv("SHUTDOWN_CHILD") != "1" {
		return
	}
	c, err := loadConfig(os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	proxy := kafkafault.New(c.producer.Brokers[0])
	defer proxy.Close()
	proxy.Arm()
	c.producer.Dialer = proxy.DialContext
	observeCtx, stopObserve := context.WithCancel(context.Background())
	defer stopObserve()
	go func() {
		select {
		case <-observeCtx.Done():
			return
		case code := <-proxy.Observed:
			if code == 0 {
				_ = os.WriteFile(os.Getenv("COMMIT_MARKER"), []byte("committed"), 0600)
			}
		}
	}()
	if err := execute(c); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv("EXIT_MARKER"), []byte("joined"), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownProcess(t *testing.T) {
	if os.Getenv("MARKET_DATA_PROCESS_INTEGRATION") != "1" {
		t.Skip("disposable process opt-in")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	pool := postgrestest.OpenWithDSN(t, "MARKET_DATA_DATABASE_URL")
	// Independent fixture pair: other runtime tests use BTCUSDT/ETHUSDT.
	raw, err := os.ReadFile("../../internal/marketdata/trade/testdata/trade.json")
	if err != nil {
		t.Fatal(err)
	}
	raw = bytes.ReplaceAll(raw, []byte("BTCUSDT"), []byte("SOLUSDT"))
	seed, err := marketprocess.Seed(ctx, pool, raw)
	if err != nil {
		t.Fatal("seed")
	}
	env := validEnv()
	delete(env, "HTTP_ADDR")
	env["MARKET_DATA_DATABASE_URL"] = os.Getenv("MARKET_DATA_DATABASE_URL")
	env["KAFKA_BROKERS"] = os.Getenv("KAFKA_TEST_BROKER")
	env["PRICES_TOPIC"] = os.Getenv("KAFKA_TEST_PREFIX") + "-prices"
	env["MARKET_DATA_RELAY_TRANSACTIONAL_ID"] = os.Getenv("KAFKA_TEST_PREFIX") + "-shutdown"
	env["RAW_TRADE_PAIR"] = "SOLUSDT"
	// Hold a handshake so SIGTERM must join the reconnect attempt, with no retry.
	var dials atomic.Int32
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { dials.Add(1); <-release }))
	defer server.Close()
	defer close(release)
	env["RAW_TRADE_URL"] = "ws" + strings.TrimPrefix(server.URL, "http") + "/ws/solusdt@trade"
	marker := filepath.Join(t.TempDir(), "commit")
	exit := filepath.Join(t.TempDir(), "exit")
	env["SHUTDOWN_CHILD"] = "1"
	env["COMMIT_MARKER"] = marker
	env["EXIT_MARKER"] = exit
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cfg := apiprocess.Config{Executable: binary, Args: []string{"-test.run=^TestShutdownChild$"}, Env: env, StartupProbePath: "/healthz", StartupTimeout: 15 * time.Second, StopTimeout: 22 * time.Second}
	wait := func(check func() bool) error {
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			if check() {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-tick.C:
			}
		}
	}
	err = apiprocess.With(ctx, cfg, func(p *apiprocess.Process) error {
		if e := wait(func() bool { _, e := os.Stat(marker); return e == nil && dials.Load() > 0 }); e != nil {
			return e
		}
		committed, e := kgo.NewClient(kgo.SeedBrokers(env["KAFKA_BROKERS"]), kgo.ConsumeTopics(env["PRICES_TOPIC"]), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxWait(100*time.Millisecond))
		if e != nil {
			return e
		}
		defer committed.Close()
		seen := false
		for !seen {
			fetch := committed.PollFetches(ctx)
			if ctx.Err() != nil || len(fetch.Errors()) > 0 {
				return errors.New("first commit not independently visible")
			}
			for _, r := range fetch.Records() {
				if bytes.Equal(r.Value, seed.Value) && bytes.Equal(r.Key, seed.Key) {
					seen = true
				}
			}
		}
		if _, e := pool.Exec(ctx, `UPDATE app.market_streams SET gap_detected=false WHERE pair='SOLUSDT'`); e != nil {
			return e
		}
		if e := syscall.Kill(p.PID(), syscall.SIGTERM); e != nil {
			return e
		}
		select {
		case <-p.Done():
		case <-time.After(18 * time.Second):
			return errors.New("shutdown exceeded join/status budget")
		}
		if _, e := os.Stat(exit); e != nil {
			return errors.New("child did not return after graceful join")
		}
		var published, gap bool
		var data []byte
		if e := pool.QueryRow(ctx, `SELECT published,data FROM app.price_outbox WHERE event_id=$1`, seed.EventID).Scan(&published, &data); e != nil {
			return e
		}
		if published || !bytes.Equal(data, seed.Value) {
			return errors.New("uncertain publish lost canonical work")
		}
		if e := pool.QueryRow(ctx, `SELECT gap_detected FROM app.market_streams WHERE pair='SOLUSDT'`).Scan(&gap); e != nil || !gap {
			return errors.New("shutdown gap not durable")
		}
		if dials.Load() != 1 {
			return errors.New("reconnect continued during shutdown")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Restart the real executable after the untouched DB-clock lease expires.
	if err = wait(func() bool {
		var free bool
		e := pool.QueryRow(ctx, `SELECT lease_until<=clock_timestamp() FROM app.price_outbox WHERE event_id=$1`, seed.EventID).Scan(&free)
		return e == nil && free
	}); err != nil {
		t.Fatal(err)
	}
	binary = filepath.Join(t.TempDir(), "market-data")
	if out, e := exec.CommandContext(ctx, "go", "build", "-race", "-o", binary, ".").CombinedOutput(); e != nil {
		t.Fatalf("build: %s", out)
	}
	// The real executable accepts a new trade and must close the active socket.
	peerClosed := make(chan struct{}, 1)
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, e := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if e != nil {
			return
		}
		defer c.Close()
		payload := bytes.Replace(raw, []byte(`"t":12345`), []byte(`"t":12346`), 1)
		if c.WriteMessage(websocket.TextMessage, payload) != nil {
			return
		}
		for {
			if _, _, e = c.ReadMessage(); e != nil {
				select {
				case peerClosed <- struct{}{}:
				default:
				}
				return
			}
		}
	}))
	defer live.Close()
	env["RAW_TRADE_URL"] = "ws" + strings.TrimPrefix(live.URL, "http") + "/ws/solusdt@trade"
	cfg.Executable = binary
	cfg.Args = nil
	delete(env, "SHUTDOWN_CHILD")
	delete(env, "COMMIT_MARKER")
	delete(env, "EXIT_MARKER")
	err = apiprocess.With(ctx, cfg, func(p *apiprocess.Process) error {
		if e := wait(func() bool {
			var ack bool
			e := pool.QueryRow(ctx, `SELECT published FROM app.price_outbox WHERE event_id=$1`, seed.EventID).Scan(&ack)
			return e == nil && ack
		}); e != nil {
			return e
		}
		if e := wait(func() bool {
			var source string
			e := pool.QueryRow(ctx, `SELECT latest->>'source_id' FROM app.market_streams WHERE pair='SOLUSDT'`).Scan(&source)
			return e == nil && source == "12346"
		}); e != nil {
			return e
		}
		if _, e := pool.Exec(ctx, `UPDATE app.market_streams SET gap_detected=false WHERE pair='SOLUSDT'`); e != nil {
			return e
		}
		if e := syscall.Kill(p.PID(), syscall.SIGTERM); e != nil {
			return e
		}
		select {
		case <-p.Done():
		case <-time.After(18 * time.Second):
			return errors.New("real main shutdown exceeded deadline")
		}
		select {
		case <-peerClosed:
		case <-ctx.Done():
			return errors.New("feed socket remained open")
		}
		var gap bool
		if e := pool.QueryRow(ctx, `SELECT gap_detected FROM app.market_streams WHERE pair='SOLUSDT'`).Scan(&gap); e != nil || !gap {
			return errors.New("real main did not persist shutdown gap")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := kgo.NewClient(kgo.SeedBrokers(env["KAFKA_BROKERS"]), kgo.ConsumeTopics(env["PRICES_TOPIC"]), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxWait(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	copies := 0
	for copies < 2 {
		fetch := consumer.PollFetches(ctx)
		if ctx.Err() != nil || len(fetch.Errors()) > 0 {
			t.Fatal("read_committed recovery")
		}
		for _, r := range fetch.Records() {
			if bytes.Equal(r.Value, seed.Value) {
				if !bytes.Equal(r.Key, seed.Key) {
					t.Fatal("key changed")
				}
				copies++
			}
		}
	}
}
