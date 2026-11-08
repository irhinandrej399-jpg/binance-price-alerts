package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/apiprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// configureRPC supplies only loopback catalog/ephemeral credentials to children.
func configureRPC(t testing.TB, env map[string]string) (authority, string) {
	t.Helper()
	ca := newAuthority(t)
	server := ca.issue(t, nil, true, false)
	for name, data := range map[string][]byte{"RPC_CERT_FILE": server.cert, "RPC_KEY_FILE": server.key, "RPC_CA_FILE": ca.pem} {
		path := filepath.Join(t.TempDir(), name)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		env[name] = path
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	env["RPC_ADDR"] = addr
	catalog := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v3/exchangeInfo" {
			http.NotFound(w, r)
			return
		}
		fmt.Fprint(w, `{"symbols":[{"symbol":"BTCUSDT","baseAsset":"BTC","quoteAsset":"USDT","status":"TRADING","isSpotTradingAllowed":true}]}`)
	}))
	t.Cleanup(catalog.Close)
	env["CATALOG_BASE_URL"] = catalog.URL
	return ca, addr
}
func rpcConn(t testing.TB, ca authority, addr, uri string) *grpc.ClientConn {
	t.Helper()
	cert := ca.issue(t, []string{uri}, false, false)
	creds, err := serviceauth.ClientCredentials(cert.cert, cert.key, ca.pem)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds), grpc.WithAuthority("market.test"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}
func TestMarketRPCProcess(t *testing.T) {
	if os.Getenv("MARKET_DATA_PROCESS_INTEGRATION") != "1" {
		t.Skip("disposable process opt-in")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Second)
	defer cancel()
	raw := []byte(fmt.Sprintf(`{"e":"trade","E":%d,"s":"BTCUSDT","t":23456,"p":"1.234","q":"1","T":%d,"m":false,"M":true}`, time.Now().UnixMilli(), time.Now().UnixMilli()))
	feed := newRuntimeFeed(t, raw)
	defer feed.Close()
	binary := filepath.Join(t.TempDir(), "market-data")
	if out, err := exec.CommandContext(ctx, "go", "build", "-race", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %s", out)
	}
	env := validEnv()
	delete(env, "HTTP_ADDR")
	env["MARKET_DATA_DATABASE_URL"] = os.Getenv("MARKET_DATA_DATABASE_URL")
	env["KAFKA_BROKERS"] = os.Getenv("KAFKA_TEST_BROKER")
	env["PRICES_TOPIC"] = os.Getenv("KAFKA_TEST_PREFIX") + "-prices"
	env["MARKET_DATA_RELAY_TRANSACTIONAL_ID"] = os.Getenv("KAFKA_TEST_PREFIX") + "-rpc"
	env["CATALOG_REFRESH_INTERVAL"] = "100ms"
	env["RAW_TRADE_URL"] = feed.URL
	ca, addr := configureRPC(t, env)
	var hold atomic.Bool
	entered := make(chan struct{}, 2)
	canceled := make(chan struct{}, 2)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hold.Load() {
			entered <- struct{}{}
			<-r.Context().Done()
			canceled <- struct{}{}
			return
		}
		fmt.Fprint(w, `{"symbols":[{"symbol":"BTCUSDT","baseAsset":"BTC","quoteAsset":"USDT","status":"TRADING","isSpotTradingAllowed":true}]}`)
	}))
	defer origin.Close()
	env["CATALOG_BASE_URL"] = origin.URL
	api := rpcConn(t, ca, addr, env["API_SERVICE_URI"])
	matcher := rpcConn(t, ca, addr, env["MATCHER_SERVICE_URI"])
	stranger := rpcConn(t, ca, addr, "spiffe://local/notifications")
	cfg := apiprocess.Config{Executable: binary, Env: env, StartupProbePath: "/healthz", StartupTimeout: 20 * time.Second, StopTimeout: 22 * time.Second}
	pool := postgrestest.OpenWithDSN(t, "MARKET_DATA_DATABASE_URL")
	var firstGeneration uint64
	for restart := 0; restart < 2; restart++ {
		if restart == 1 {
			if _, e := pool.Exec(ctx, `UPDATE app.market_streams SET state='live',gap_detected=false WHERE pair='BTCUSDT'`); e != nil {
				t.Fatal(e)
			}
		}
		err := apiprocess.With(ctx, cfg, func(p *apiprocess.Process) error {
			call, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			got, err := pb.NewCatalogClient(api).Fetch(call, &pb.CatalogRequest{}, grpc.WaitForReady(true))
			if err != nil {
				return fmt.Errorf("Fetch runtime: %w", err)
			}
			if len(got.Pairs) != 1 || got.Pairs[0].Symbol != "BTCUSDT" || got.UpdatedAt == "" {
				return errors.New("catalog snapshot changed")
			}
			for _, tc := range []struct {
				conn   *grpc.ClientConn
				method string
			}{{matcher, "catalog"}, {stranger, "catalog"}, {stranger, "latest"}} {
				if tc.method == "catalog" {
					_, err = pb.NewCatalogClient(tc.conn).Fetch(call, &pb.CatalogRequest{})
				} else {
					_, err = pb.NewQuotesClient(tc.conn).Latest(call, &pb.LatestRequest{Pair: "BTCUSDT"})
				}
				if status.Code(err) != codes.PermissionDenied {
					return fmt.Errorf("wrong authorization: %v", err)
				}
			}
			for {
				q, e := pb.NewQuotesClient(api).Latest(call, &pb.LatestRequest{Pair: "BTCUSDT"})
				if e == nil && q.Quote != nil && q.Quote.SourceId == "23456" {
					if q.Quote.Price != "1.234" || q.Quote.SourceId != "23456" || q.Quote.StreamState != "unconfirmed" || q.Quote.Revision.Generation == 0 {
						return errors.New("quote bytes/state changed")
					}
					warned := false
					for _, w := range q.Warnings {
						warned = warned || w == pb.Warning_WARNING_STREAM_UNCONFIRMED
					}
					if !warned {
						return errors.New("missing unconfirmed warning")
					}
					if restart == 0 {
						firstGeneration = q.Quote.Revision.Generation
					} else if q.Quote.Revision.Generation != firstGeneration || !q.Quote.GapDetected {
						return errors.New("restart lost quote revision or gap")
					}
					break
				}
				select {
				case <-call.Done():
					return fmt.Errorf("Latest runtime: %w", call.Err())
				case <-time.After(20 * time.Millisecond):
				}
			}
			if _, err = pb.NewQuotesClient(matcher).Latest(call, &pb.LatestRequest{Pair: "BTCUSDT"}); err != nil {
				return err
			}
			hold.Store(true)
			// RPC reads the applied snapshot, independently of a held refresh.
			if _, e := pb.NewCatalogClient(api).Fetch(call, &pb.CatalogRequest{}); e != nil {
				return e
			}
			select {
			case <-entered:
			case <-call.Done():
				return errors.New("held Fetch not entered")
			}
			if err = syscall.Kill(p.PID(), syscall.SIGTERM); err != nil {
				return err
			}
			select {
			case <-p.Done():
			case <-call.Done():
				return errors.New("RPC shutdown failed")
			}
			select {
			case <-canceled:
			case <-call.Done():
				return errors.New("catalog HTTP not canceled")
			}
			hold.Store(false)
			if code, ok := p.ExitCode(); !ok || code != 0 {
				return errors.New("SIGTERM was not graceful")
			}
			probe, stop := context.WithTimeout(ctx, 300*time.Millisecond)
			defer stop()
			if _, e := pb.NewCatalogClient(api).Fetch(probe, &pb.CatalogRequest{}); e == nil {
				return errors.New("RPC survived shutdown")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	// Invalid credentials and occupied RPC port exit before HTTP startup.
	occupied, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal("RPC listener leaked")
	}
	err = apiprocess.With(ctx, cfg, func(*apiprocess.Process) error { return errors.New("unexpected startup") })
	occupied.Close()
	if !errors.Is(err, apiprocess.ErrEarlyExit) {
		t.Fatalf("RPC bind failure: %v", err)
	}
	old := env["RPC_CERT_FILE"]
	env["RPC_CERT_FILE"] = filepath.Join(t.TempDir(), "absent")
	err = apiprocess.With(ctx, cfg, func(*apiprocess.Process) error { return errors.New("unexpected startup") })
	if !errors.Is(err, apiprocess.ErrEarlyExit) {
		t.Fatalf("credential failure: %v", err)
	}
	badCert := filepath.Join(t.TempDir(), "invalid.pem")
	if e := os.WriteFile(badCert, []byte("invalid certificate"), 0600); e != nil {
		t.Fatal(e)
	}
	env["RPC_CERT_FILE"] = badCert
	err = apiprocess.With(ctx, cfg, func(*apiprocess.Process) error { return errors.New("unexpected TLS startup") })
	if !errors.Is(err, apiprocess.ErrEarlyExit) {
		t.Fatalf("invalid TLS failure: %v", err)
	}
	env["RPC_CERT_FILE"] = old
	// Lock the disposable schema to force its startup probe to expire. The owner
	// must release RPC/SQL resources, even though no handlers were started.
	tx, e := pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(context.Background())
	if _, e = tx.Exec(ctx, `LOCK TABLE app.market_streams IN ACCESS EXCLUSIVE MODE`); e != nil {
		t.Fatal(e)
	}
	err = apiprocess.With(ctx, cfg, func(*apiprocess.Process) error { return errors.New("unexpected schema startup") })
	if !errors.Is(err, apiprocess.ErrEarlyExit) {
		t.Fatalf("schema startup failure: %v", err)
	}
	if e = tx.Rollback(ctx); e != nil {
		t.Fatal(e)
	}
	if l, e := net.Listen("tcp", addr); e != nil {
		t.Fatal("startup leaked RPC listener")
	} else {
		l.Close()
	}
}
