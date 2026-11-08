package e2estack

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/binancefake"
)

func TestFourProcesses(t *testing.T) {
	if os.Getenv("E2E_PROCESS_INTEGRATION") != "1" {
		t.Skip("run python3 scripts/check_e2e_environment.py --package ./internal/testutil/e2estack")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	s := New(t, ctx)
	// A restart retains all four databases, source identities, vector, generation,
	// and transactional IDs. A separate runner invocation creates a fresh dataset.
	for round := 0; round < 2; round++ {
		err := s.With(ctx, func(s *Stack) error {
			probe, done := context.WithTimeout(ctx, 45*time.Second)
			defer done()
			conn, err := s.Binance.Next(probe)
			if err != nil {
				return err
			}
			if conn.Symbol != "BTCUSDT" {
				return fmt.Errorf("unexpected raw stream")
			}
			now := time.Now().UnixMilli()
			if err = conn.Trade(probe, binancefake.Trade{ID: uint64(100 + round), Price: "100.000000000000000001", TradeTime: now, EventTime: now}); err != nil {
				return err
			}
			if err = conn.Ping(probe, fmt.Sprint("round-", round)); err != nil {
				return err
			}
			if err = s.CheckQuote(probe, fmt.Sprint(100+round), "100.000000000000000001"); err != nil {
				return err
			}
			if err = s.Ready(probe); err != nil {
				return err
			}
			if len(s.Telegram.Requests()) != 0 {
				return fmt.Errorf("empty stack unexpectedly sent Telegram")
			}
			t.Logf("round %d: four real executables live; API/matcher/notifications ready; raw market-data unconfirmed; Catalog/Latest and ping/pong verified", round)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if err = s.AssertStopped(ctx); err != nil {
			t.Fatal(err)
		}
		t.Logf("round %d: four exits 0, reaped and HTTP listeners closed", round)
	}
}
