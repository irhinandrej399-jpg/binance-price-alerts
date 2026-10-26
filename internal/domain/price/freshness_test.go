package price_test

import (
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
)

func TestCheckFreshness(t *testing.T) {
	now := time.Date(2026, 10, 26, 12, 0, 10, 0, time.UTC)
	for _, tt := range []struct {
		name     string
		hasPrice bool
		tradedAt time.Time
		want     price.Freshness
	}{
		{"age zero", true, now, price.Fresh},
		{"age 4.999 seconds", true, now.Add(-4999 * time.Millisecond), price.Fresh},
		{"age 5 seconds", true, now.Add(-5 * time.Second), price.Fresh},
		{"age 5.001 seconds", true, now.Add(-5001 * time.Millisecond), price.Stale},
		{"one nanosecond before boundary", true, now.Add(-5*time.Second + time.Nanosecond), price.Fresh},
		{"one nanosecond after boundary", true, now.Add(-5*time.Second - time.Nanosecond), price.Stale},
		{"future millisecond", true, now.Add(time.Millisecond), price.InvalidTime},
		{"future nanosecond", true, now.Add(time.Nanosecond), price.InvalidTime},
		{"missing price", false, now, price.Missing},
		{"missing price and time", false, time.Time{}, price.Missing},
		{"missing trade time", true, time.Time{}, price.InvalidTime},
		{"invalid trade year zero", true, time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), price.InvalidTime},
		{"invalid trade year 10000", true, time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), price.InvalidTime},
		{"old quote remains stale", true, now.Add(-time.Hour), price.Stale},
		{"same instant in another zone", true, now.Add(-5 * time.Second).In(time.FixedZone("UTC+3", 3*60*60)), price.Fresh},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := price.CheckFreshness(tt.hasPrice, tt.tradedAt, now); got != tt.want {
				t.Fatalf("CheckFreshness = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFreshnessUsesTradeTimeAndRechecksClock(t *testing.T) {
	tradedAt := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	// Reception and subsequent reads supply their own clock, never a new trade time.
	for _, tt := range []struct {
		name string
		now  time.Time
		want price.Freshness
	}{
		{"on boundary", tradedAt.Add(5 * time.Second), price.Fresh},
		{"next read", tradedAt.Add(5001 * time.Millisecond), price.Stale},
		{"late reception", tradedAt.Add(9999 * time.Millisecond), price.Stale},
		{"replay", tradedAt.Add(time.Minute), price.Stale},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := price.CheckFreshness(true, tradedAt, tt.now); got != tt.want {
				t.Fatalf("CheckFreshness = %q, want %q", got, tt.want)
			}
		})
	}
}
