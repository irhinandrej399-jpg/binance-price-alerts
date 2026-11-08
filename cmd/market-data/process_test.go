package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/health"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/storage"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/apiprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/marketprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestMarketDataProcess(t *testing.T) {
	if os.Getenv("MARKET_DATA_PROCESS_INTEGRATION") != "1" {
		t.Skip("run python3 scripts/check_marketdata_process.py --package ./cmd/market-data")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	pool := postgrestest.OpenWithDSN(t, "MARKET_DATA_DATABASE_URL")
	raw, err := os.ReadFile("../../internal/marketdata/trade/testdata/trade.json")
	if err != nil {
		t.Fatal(err)
	}
	seed, err := marketprocess.Seed(ctx, pool, raw)
	if err != nil {
		t.Fatal("seed failed")
	}
	next := bytes.Replace(raw, []byte(`"t":12345`), []byte(`"t":12346`), 1)
	if bytes.Equal(next, raw) {
		t.Fatal("fixture substitution failed")
	}
	feed := marketprocess.NewRawFeed(next)
	defer feed.Close()
	binary := filepath.Join(t.TempDir(), "market-data")
	if out, err := exec.CommandContext(ctx, "go", "build", "-race", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %s", out)
	}
	env := validEnv()
	ca, rpcAddr := configureRPC(t, env)
	delete(env, "HTTP_ADDR")
	env["MARKET_DATA_DATABASE_URL"] = os.Getenv("MARKET_DATA_DATABASE_URL")
	env["KAFKA_BROKERS"] = os.Getenv("KAFKA_TEST_BROKER")
	prefix := os.Getenv("KAFKA_TEST_PREFIX")
	if prefix == "" || env["KAFKA_BROKERS"] == "" {
		t.Fatal("missing Kafka config")
	}
	env["PRICES_TOPIC"] = prefix + "-prices"
	env["MARKET_DATA_RELAY_TRANSACTIONAL_ID"] = prefix + "-market-relay"
	env["RAW_TRADE_URL"] = feed.URL
	rpc := rpcConn(t, ca, rpcAddr, env["API_SERVICE_URI"])
	cfg := apiprocess.Config{Executable: binary, Env: env, StartupProbePath: "/healthz", StartupTimeout: 20 * time.Second, StopTimeout: time.Second}
	consumer, err := kgo.NewClient(kgo.SeedBrokers(env["KAFKA_BROKERS"]), kgo.ConsumeTopics(env["PRICES_TOPIC"]), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxWait(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	wait := func(check func() bool) error {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			if check() {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}
		}
	}
	checkBytes := func(want map[string][]byte) error {
		for len(want) > 0 {
			fetch := consumer.PollFetches(ctx)
			if ctx.Err() != nil || len(fetch.Errors()) > 0 {
				return errors.New("Kafka fetch failed")
			}
			for _, r := range fetch.Records() {
				meta, _, e := priceobserved.Decode(r.Value)
				if e != nil {
					return e
				}
				if expected, ok := want[meta.EventID]; ok {
					if string(r.Key) != "BTCUSDT" || !bytes.Equal(r.Value, expected) {
						return errors.New("Kafka changed canonical bytes")
					}
					delete(want, meta.EventID)
				}
			}
		}
		return nil
	}
	repo := storage.New(pool)
	var generation uint64
	var firstValue []byte
	err = apiprocess.With(ctx, cfg, func(p *apiprocess.Process) error {
		if e := wait(func() bool { q, e := repo.Latest(ctx, "BTCUSDT"); return e == nil && q.SourceID == "12346" }); e != nil {
			return e
		}
		var id string
		if e := pool.QueryRow(ctx, `SELECT event_id,data FROM app.price_outbox WHERE event_id<>$1 ORDER BY position LIMIT 1`, seed.EventID).Scan(&id, &firstValue); e != nil {
			return errors.New("new outbox missing")
		}
		_, q, e := priceobserved.Decode(firstValue)
		if e != nil || q.Revision.Generation <= seed.Generation || q.Revision.Sequence != 1 || !q.GapDetected || q.StreamState != priceobserved.Unconfirmed {
			return errors.New("ingest generation/state mismatch")
		}
		generation = q.Revision.Generation
		client := &http.Client{Timeout: time.Second}
		req, _ := http.NewRequestWithContext(ctx, "GET", p.URL+"/readyz", nil)
		resp, e := client.Do(req)
		if e != nil {
			return e
		}
		body, e := io.ReadAll(resp.Body)
		resp.Body.Close()
		if e != nil || resp.StatusCode != 503 || !bytes.Contains(body, []byte(`"feed":"unconfirmed"`)) {
			return errors.New("readiness falsely confirmed live")
		}
		if e = checkBytes(map[string][]byte{seed.EventID: seed.Value, id: firstValue}); e != nil {
			return e
		}
		feed.Close()
		return wait(func() bool {
			var gen uint64
			var gap bool
			e := pool.QueryRow(ctx, `SELECT generation,gap_detected FROM app.market_streams WHERE pair='BTCUSDT'`).Scan(&gen, &gap)
			return e == nil && gen > generation && gap
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	// Pending canonical bytes survive an owner restart; the next trade advances
	// from the persisted source boundary in a new generation.
	pendingRaw := bytes.Replace(next, []byte(`"t":12346`), []byte(`"t":12347`), 1)
	pending, err := marketprocess.Seed(ctx, pool, pendingRaw)
	if err != nil {
		t.Fatal("restart seed failed")
	}
	restartedRaw := bytes.Replace(next, []byte(`"t":12346`), []byte(`"t":12348`), 1)
	feed2 := marketprocess.NewRawFeed(restartedRaw)
	defer feed2.Close()
	env["RAW_TRADE_URL"] = feed2.URL
	err = apiprocess.With(ctx, cfg, func(p *apiprocess.Process) error {
		if e := wait(func() bool {
			q, e := repo.Latest(ctx, "BTCUSDT")
			return e == nil && q.SourceID == "12348" && q.Revision.Generation > pending.Generation && q.Revision.Sequence == 1
		}); e != nil {
			return e
		}
		var id string
		var value []byte
		if e := pool.QueryRow(ctx, `SELECT event_id,data FROM app.price_outbox ORDER BY position DESC LIMIT 1`).Scan(&id, &value); e != nil {
			return errors.New("restart outbox missing")
		}
		if e := checkBytes(map[string][]byte{pending.EventID: pending.Value, id: value}); e != nil {
			return e
		}
		var count int
		if e := pool.QueryRow(ctx, `SELECT count(*) FROM app.price_outbox`).Scan(&count); e != nil || count != 4 {
			return errors.New("unexpected restart work")
		}
		if _, e := pool.Exec(ctx, `INSERT INTO app.price_outbox(event_id,pair,data) VALUES('ops048-oversized','BTCUSDT',$1)`, bytes.Repeat([]byte("x"), storage.MaxEventBytes+1)); e != nil {
			return errors.New("terminal fixture failed")
		}
		defer func() {
			// Retire only the deliberately invalid terminal fixture on this disposable
			// dataset so subsequent owner tests can publish their valid work.
			if _, e := pool.Exec(ctx, `UPDATE app.price_outbox SET published=true,lease_until=NULL WHERE event_id='ops048-oversized'`); e != nil {
				t.Error("retire terminal fixture")
			}
		}()
		select {
		case <-p.Done():
			if code, ok := p.ExitCode(); !ok || code == 0 {
				return errors.New("terminal failure exited successfully")
			}
			call, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
			defer cancel()
			if _, e := pb.NewQuotesClient(rpc).Latest(call, &pb.LatestRequest{Pair: "BTCUSDT"}); e == nil {
				return errors.New("terminal failure left RPC alive")
			}
			return nil
		case <-time.After(8 * time.Second):
			return errors.New("terminal relay left owner running")
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err != nil {
		t.Fatal(err)
	}

	// Invalid config and DB startup failure exit before the HTTP probe.
	for _, bad := range []struct{ k, v string }{{"RAW_TRADE_PAIR", "bad"}, {"MARKET_DATA_DATABASE_URL", "postgres://market_data_runtime:secret@127.0.0.1:1/market_data"}, {"KAFKA_BROKERS", "127.0.0.1:1"}} {
		old := env[bad.k]
		env[bad.k] = bad.v
		e := apiprocess.With(ctx, cfg, func(*apiprocess.Process) error { return fmt.Errorf("unexpected startup") })
		env[bad.k] = old
		if !errors.Is(e, apiprocess.ErrEarlyExit) {
			t.Fatalf("%s failure: %v", bad.k, e)
		}
	}
	// Closed SQL cannot acknowledge acceptance. Older callbacks cannot affect a
	// later generation (the SQL tests below use the actual runtime adapter).
}

func TestPreparedSQLFencing(t *testing.T) {
	if os.Getenv("MARKET_DATA_PROCESS_INTEGRATION") != "1" {
		t.Skip("process integration opt-in")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool := postgrestest.OpenWithDSN(t, "MARKET_DATA_DATABASE_URL")
	repo := storage.New(pool)
	var state atomic.Value
	p := prepare(repo, "ETHUSDT", &state, func(bool) {}, new(atomic.Uint64))
	old, err := p(ctx)
	if err != nil {
		t.Fatal(err)
	}
	current, err := p(ctx)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile("../../internal/marketdata/trade/testdata/trade.json")
	if err != nil {
		t.Fatal(err)
	}
	raw = []byte(strings.ReplaceAll(string(raw), "BTCUSDT", "ETHUSDT"))
	if err = old.Handle(ctx, raw); !errors.Is(err, storage.ErrStale) {
		t.Fatal("old handler not fenced")
	}
	if err = old.Disconnected(ctx, errors.New("disconnect"), time.Second); !errors.Is(err, storage.ErrStale) {
		t.Fatal("old disconnect not fenced")
	}
	if state.Load() != health.Unconfirmed {
		t.Fatal("stale disconnect changed current snapshot")
	}
	if err = current.Handle(ctx, raw); err != nil {
		t.Fatal(err)
	}
	before, err := repo.Latest(ctx, "ETHUSDT")
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := p(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = restarted.Handle(ctx, raw); err != nil {
		t.Fatal(err)
	}
	after, err := repo.Latest(ctx, "ETHUSDT")
	if err != nil || after.SourceID != before.SourceID || after.Revision != before.Revision {
		t.Fatal("restart did not suppress duplicate")
	}
	var count int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM app.price_outbox WHERE pair='ETHUSDT'`).Scan(&count); err != nil || count != 1 {
		t.Fatal("duplicate outbox")
	}
	if err = restarted.Handle(ctx, []byte("invalid")); err == nil {
		t.Fatal("invalid raw accepted")
	}
	pool.Close()
	if _, err = p(ctx); err == nil {
		t.Fatal("SQL prepare failure hidden")
	}
	if err = old.Handle(ctx, raw); err == nil {
		t.Fatal("SQL accept failure hidden")
	}
}
