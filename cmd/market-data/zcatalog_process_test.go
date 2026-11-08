package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/catalog"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/storage"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/apiprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/binancefake"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"google.golang.org/grpc"
)

func TestCatalogRuntimeProcess(t *testing.T) {
	if os.Getenv("MARKET_DATA_PROCESS_INTEGRATION") != "1" {
		t.Skip("disposable process opt-in")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	f := binancefake.New()
	defer f.Close()
	binary := filepath.Join(t.TempDir(), "market-data")
	if out, err := exec.CommandContext(ctx, "go", "build", "-race", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %s", out)
	}
	// Retain only sanitized application diagnostics if the executable fails.
	stderr := filepath.Join(t.TempDir(), "stderr")
	wrapper := filepath.Join(t.TempDir(), "start")
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
	if e := os.WriteFile(wrapper, []byte("#!/bin/sh\nexec "+quote(binary)+" 2>"+quote(stderr)+"\n"), 0700); e != nil {
		t.Fatal(e)
	}
	defer func() {
		if t.Failed() {
			b, _ := os.ReadFile(stderr)
			for _, line := range strings.Split(string(b), "\n") {
				if strings.HasPrefix(line, "market-data:") {
					t.Log(line)
				}
			}
		}
	}()
	binary = wrapper
	env := validEnv()
	ca, addr := configureRPC(t, env)
	delete(env, "HTTP_ADDR")
	delete(env, "RAW_TRADE_PAIR")
	env["RAW_TRADE_URL"], env["CATALOG_BASE_URL"], env["CATALOG_REFRESH_INTERVAL"] = f.WSURL(), f.URL(), "100ms"
	env["MARKET_DATA_DATABASE_URL"] = os.Getenv("MARKET_DATA_DATABASE_URL")
	env["KAFKA_BROKERS"] = os.Getenv("KAFKA_TEST_BROKER")
	env["PRICES_TOPIC"] = os.Getenv("KAFKA_TEST_PREFIX") + "-prices"
	env["MARKET_DATA_RELAY_TRANSACTIONAL_ID"] = os.Getenv("KAFKA_TEST_PREFIX") + "-dynamic"
	rpc := rpcConn(t, ca, addr, env["API_SERVICE_URI"])
	pool := postgrestest.OpenWithDSN(t, "MARKET_DATA_DATABASE_URL")
	repo := storage.New(pool)
	wait := func(check func() bool) error {
		for {
			if check() {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	err := apiprocess.With(ctx, apiprocess.Config{Executable: binary, Env: env, StartupProbePath: "/healthz", StartupTimeout: 8 * time.Second, StopTimeout: 8 * time.Second}, func(p *apiprocess.Process) error {
		next := func(want []string) (*binancefake.Connection, error) {
			c, e := f.Next(ctx)
			if e != nil {
				return nil, e
			}
			e = wait(func() bool { return reflect.DeepEqual(c.Streams(), want) })
			return c, e
		}
		fetch := func(want ...string) error {
			return wait(func() bool {
				s, e := pb.NewCatalogClient(rpc).Fetch(ctx, &pb.CatalogRequest{}, grpc.WaitForReady(true))
				if e != nil {
					return false
				}
				got := []string{}
				for _, pair := range s.Pairs {
					got = append(got, pair.Symbol)
				}
				return reflect.DeepEqual(got, want)
			})
		}
		c, e := next([]string{"btcusdt@trade", "solusdt@trade"})
		if e != nil {
			return e
		}
		if e = fetch("BTCUSDT", "SOLUSDT"); e != nil {
			return e
		}
		send := func(pair, price string, id uint64) error {
			now := time.Now().UnixMilli()
			if e := c.TradePair(ctx, pair, binancefake.Trade{ID: id, Price: price, TradeTime: now, EventTime: now}); e != nil {
				return e
			}
			return wait(func() bool {
				q, e := repo.Latest(ctx, pair)
				return e == nil && q.Price.String() == price && q.SourceID == fmt.Sprint(id) && q.StreamState == "unconfirmed" && q.TradeTime.UnixMilli() == now
			})
		}
		if e = send("BTCUSDT", "101.000000000000000001", 90001); e != nil {
			return e
		}
		if e = send("SOLUSDT", "23.000000000000000002", 90001); e != nil {
			return e
		}
		pairs := []catalog.Pair{{Symbol: "BTCUSDT", BaseAsset: "BTC", QuoteAsset: "USDT"}, {Symbol: "SOLUSDT", BaseAsset: "SOL", QuoteAsset: "USDT"}, {Symbol: "ADAUSDT", BaseAsset: "ADA", QuoteAsset: "USDT"}}
		if e = f.SetCatalog(pairs); e != nil {
			return e
		}
		c, e = next([]string{"adausdt@trade", "btcusdt@trade", "solusdt@trade"})
		if e != nil {
			return e
		}
		if e = fetch("BTCUSDT", "SOLUSDT", "ADAUSDT"); e != nil {
			return e
		}
		if e = send("ADAUSDT", "0.25", 90001); e != nil {
			return e
		}
		if e = f.SetCatalog(pairs[1:]); e != nil {
			return e
		}
		old := c
		c, e = next([]string{"adausdt@trade", "solusdt@trade"})
		if e != nil {
			return e
		}
		if e = fetch("SOLUSDT", "ADAUSDT"); e != nil {
			return e
		}
		if len(old.Streams()) != 0 {
			return errors.New("removed subscriptions retained")
		}
		before, e := repo.Latest(ctx, "BTCUSDT")
		if e != nil || !before.GapDetected {
			return errors.New("removed quote/gap lost")
		}
		if e = c.Disconnect(); e != nil {
			return e
		}
		c, e = next([]string{"adausdt@trade", "solusdt@trade"})
		if e != nil {
			return e
		}
		// Duplicate source must preserve the committed revision and outbox.
		if e = c.TradePair(ctx, "SOLUSDT", binancefake.Trade{ID: 90001, Price: "999", TradeTime: time.Now().UnixMilli(), EventTime: time.Now().UnixMilli()}); e != nil {
			return e
		}
		if e = send("SOLUSDT", "24", 90002); e != nil {
			return e
		}
		var accepted [][]byte
		rows, e := pool.Query(ctx, `SELECT data FROM app.price_outbox WHERE pair IN ('BTCUSDT','SOLUSDT','ADAUSDT') AND convert_from(data,'UTF8') LIKE '%9000%' ORDER BY position`)
		if e != nil {
			return e
		}
		for rows.Next() {
			var b []byte
			if e = rows.Scan(&b); e != nil {
				rows.Close()
				return e
			}
			accepted = append(accepted, b)
		}
		rows.Close()
		if len(accepted) != 4 {
			return fmt.Errorf("accepted outbox count %d", len(accepted))
		}
		if e = syscall.Kill(p.PID(), syscall.SIGTERM); e != nil {
			return e
		}
		select {
		case <-p.Done():
		case <-ctx.Done():
			return ctx.Err()
		}
		if code, ok := p.ExitCode(); !ok || code != 0 {
			return errors.New("shutdown failed")
		}
		rows, e = pool.Query(ctx, `SELECT data FROM app.price_outbox WHERE pair IN ('BTCUSDT','SOLUSDT','ADAUSDT') AND convert_from(data,'UTF8') LIKE '%9000%' ORDER BY position`)
		if e != nil {
			return e
		}
		defer rows.Close()
		i := 0
		for rows.Next() {
			var b []byte
			if e = rows.Scan(&b); e != nil {
				return e
			}
			if i >= len(accepted) || !bytes.Equal(b, accepted[i]) {
				return errors.New("accepted work changed on stop")
			}
			i++
		}
		if i != 4 {
			return errors.New("accepted work lost")
		}
		return rows.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
}
