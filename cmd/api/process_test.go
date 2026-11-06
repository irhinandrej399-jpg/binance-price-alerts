package main

import (
	"bytes"
	"context"
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

	store "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	changed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/subscriptionchanged"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/apiprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestBindFailure(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	c := config{addr: l.Addr().String(), dsn: "secret"}
	if err = run(context.Background(), c); err == nil || err.Error() != "api: HTTP bind failed" {
		t.Fatal("bind failure masked")
	}
}
func TestAPIProcess(t *testing.T) {
	if os.Getenv("API_PROCESS_INTEGRATION") != "1" {
		t.Skip("run python3 scripts/check_api_process.py --package ./cmd/api")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Second)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "api")
	build := exec.CommandContext(ctx, "go", "build", "-race", "-o", binary, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %s", out)
	}
	env := validEnv()
	delete(env, "HTTP_ADDR")
	env["DATABASE_URL"] = os.Getenv("DATABASE_URL")
	env["KAFKA_BROKERS"] = os.Getenv("KAFKA_TEST_BROKER")
	prefix := os.Getenv("KAFKA_TEST_PREFIX")
	if env["KAFKA_BROKERS"] == "" || prefix == "" {
		t.Fatal("missing integration configuration")
	}
	env["SUBSCRIPTION_CHANGES_TOPIC"] = prefix + "-subscription-changes"
	env["DELIVERY_REQUESTS_TOPIC"] = prefix + "-delivery-requests"
	env["SUBSCRIPTION_RELAY_TRANSACTIONAL_ID"] = prefix + "-api-subscriptions"
	env["DELIVERY_RELAY_TRANSACTIONAL_ID"] = prefix + "-api-delivery"
	cfg := apiprocess.Config{Executable: binary, Env: env, StartupTimeout: 12 * time.Second, StopTimeout: time.Second}
	pool := postgrestest.Open(t)
	testGracefulProcess(t, ctx, cfg, pool)
	f, err := apiprocess.Seed(ctx, pool)
	if err != nil {
		t.Fatal("fixture failed")
	}
	err = apiprocess.With(ctx, cfg, func(p *apiprocess.Process) error {
		client := &http.Client{Timeout: 3 * time.Second}
		request := func(method, path, token string) (int, error) {
			req, e := http.NewRequestWithContext(ctx, method, p.URL+path, nil)
			if e != nil {
				return 0, e
			}
			if token != "" {
				req.Header.Set("Authorization", "Bearer "+token)
			}
			resp, e := client.Do(req)
			if e != nil {
				return 0, e
			}
			defer resp.Body.Close()
			_, e = io.Copy(io.Discard, resp.Body)
			return resp.StatusCode, e
		}
		for _, path := range []string{"/healthz", "/readyz"} {
			if code, e := request("GET", path, ""); e != nil || code != 200 {
				return fmt.Errorf("probe failed: %d", code)
			}
		}
		// Keep the test in one quota window even if wall time crosses a minute.
		// Both POST routes must share the direct TCP peer's API-owned row.
		if _, e := pool.Exec(ctx, `INSERT INTO app.auth_limits(ip,window_start,used) VALUES('127.0.0.1',date_trunc('minute',clock_timestamp())+interval '1 minute',0) ON CONFLICT(ip) DO UPDATE SET window_start=EXCLUDED.window_start,used=0`); e != nil {
			return errors.New("auth quota fixture failed")
		}
		for i := 0; i < 21; i++ {
			path := "/v1/accounts"
			if i%2 == 0 {
				path = "/v1/sessions"
			}
			r, e := http.NewRequestWithContext(ctx, "POST", p.URL+path, strings.NewReader("{"))
			if e != nil {
				return e
			}
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("X-Forwarded-For", fmt.Sprintf("192.0.2.%d", i))
			resp, e := client.Do(r)
			if e != nil {
				return errors.New("auth HTTP request failed")
			}
			body, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			want := 400
			if i == 20 {
				want = 429
			}
			if readErr != nil || resp.StatusCode != want {
				return fmt.Errorf("auth route %d: %d %s", i, resp.StatusCode, body)
			}
			if i == 20 && resp.Header.Get("Retry-After") == "" {
				return errors.New("auth quota missing retry header")
			}
		}
		if code, e := request("GET", "/healthz", ""); e != nil || code != 200 {
			return errors.New("auth quota blocked health")
		}
		path := "/v1/subscriptions/" + f.Active.ID
		if code, e := request("DELETE", path, ""); e != nil || code != 401 {
			return errors.New("missing authentication")
		}
		if code, e := request("DELETE", path, f.Token); e != nil || code != 200 {
			return errors.New("HTTP cancellation failed")
		}
		record, e := store.New(pool).ByID(ctx, f.OwnerID, f.Active.ID)
		if e != nil || record.State != domain.Cancelled || record.Version != f.Active.Version+1 {
			return errors.New("cancellation not durable")
		}
		var original []byte
		e = pool.QueryRow(ctx, `SELECT body FROM app.api_journal ORDER BY position DESC LIMIT 1`).Scan(&original)
		if e != nil {
			return errors.New("journal missing")
		}
		meta, change, e := changed.Decode(original)
		if e != nil || change.SubscriptionID != f.Active.ID || change.State != domain.Cancelled {
			return errors.New("cancel event invalid")
		}
		consumer, e := kgo.NewClient(kgo.SeedBrokers(env["KAFKA_BROKERS"]), kgo.ConsumeTopics(env["SUBSCRIPTION_CHANGES_TOPIC"], env["DELIVERY_REQUESTS_TOPIC"]), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxWait(100*time.Millisecond))
		if e != nil {
			return errors.New("consumer setup failed")
		}
		defer consumer.Close()
		sawCancel, sawDelivery := false, false
		for !sawCancel || !sawDelivery {
			fetch := consumer.PollFetches(ctx)
			if len(fetch.Errors()) > 0 {
				return errors.New("consumer failed")
			}
			for _, r := range fetch.Records() {
				if r.Topic == env["SUBSCRIPTION_CHANGES_TOPIC"] && bytes.Equal(r.Value, original) && string(r.Key) == meta.OrderingKey {
					sawCancel = true
				}
				if r.Topic == env["DELIVERY_REQUESTS_TOPIC"] && bytes.Equal(r.Value, f.DeliveryValue) && bytes.Equal(r.Key, f.DeliveryKey) {
					sawDelivery = true
				}
			}
		}
		// A malformed durable journal entry is a terminal relay error. Runtime
		// must stop the whole owner, rather than leave a healthy HTTP-only process.
		tx, e := pool.Begin(ctx)
		if e != nil {
			return errors.New("poison transaction failed")
		}
		defer tx.Rollback(ctx)
		var end int64
		e = tx.QueryRow(ctx, `UPDATE app.api_journal_counter SET position=position+1 WHERE singleton RETURNING position`).Scan(&end)
		if e != nil {
			return errors.New("counter failed")
		}
		if _, e = tx.Exec(ctx, `INSERT INTO app.api_journal_batches(transaction_id,transaction_end) VALUES(pg_current_xact_id(),$1)`, end); e != nil {
			return errors.New("batch failed")
		}
		if _, e = tx.Exec(ctx, `INSERT INTO app.api_journal(position,transaction_end,event_id,body) VALUES($1,$1,'ops-046-invalid',$2)`, end, []byte("invalid")); e != nil {
			return errors.New("poison insert failed")
		}
		if e = tx.Commit(ctx); e != nil {
			return errors.New("poison commit failed")
		}
		select {
		case <-p.Done():
			return nil
		case <-time.After(5 * time.Second):
			return errors.New("terminal relay left owner running")
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	// The same corrupt queue must fail startup before readiness as well.
	err = apiprocess.With(ctx, cfg, func(*apiprocess.Process) error { return errors.New("unexpected readiness") })
	if !errors.Is(err, apiprocess.ErrEarlyExit) {
		t.Fatalf("startup failure: %v", err)
	}
	// PostgreSQL initialization failure also exits without readiness.
	env["DATABASE_URL"] = "postgres://api_runtime:secret@127.0.0.1:1/api?connect_timeout=1"
	err = apiprocess.With(ctx, cfg, func(*apiprocess.Process) error { return errors.New("unexpected readiness") })
	if !errors.Is(err, apiprocess.ErrEarlyExit) {
		t.Fatalf("database startup failure: %v", err)
	}
}

func TestStartupFailureReleasesListener(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = run(ctx, config{addr: addr, dsn: "invalid-secret-dsn"}); err == nil {
		t.Fatal("startup succeeded")
	}
	l, err = net.Listen("tcp", addr)
	if err != nil {
		t.Fatal("startup leaked listener")
	}
	l.Close()
}
