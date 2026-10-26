package price

import "time"

// Freshness classifies the age of a quote independently of stream health.
type Freshness string

const (
	Fresh       Freshness = "fresh"
	Stale       Freshness = "stale"
	Missing     Freshness = "missing"
	InvalidTime Freshness = "invalid_time"
)

// CheckFreshness uses the supplied clock reading and Binance trade time T.
// Callers pass now from their clock on each check, without substituting reception
// or event time for tradedAt. No system clock is read here.
//
// hasPrice distinguishes an absent quote from a valid zero price. Missing takes
// precedence over time validation. Time inputs must be parsed by the adapter;
// absent or malformed trade times are represented by the zero time. Nonzero
// trade times must have a year in the RFC3339 range [1, 9999].
//
// An age from zero through five seconds is Fresh; a greater age is Stale.
// Stale is advisory and does not prohibit using the quote. Future trade times
// are InvalidTime, with no clock-skew tolerance. Stream warnings and HTTP policy
// belong to callers, not this classification.
func CheckFreshness(hasPrice bool, tradedAt, now time.Time) Freshness {
	if !hasPrice {
		return Missing
	}
	if tradedAt.IsZero() || tradedAt.Year() < 1 || tradedAt.Year() > 9999 {
		return InvalidTime
	}
	// UTC removes monotonic clock readings so persisted and in-memory timestamps
	// use the same wall-clock comparison.
	tradedAt, now = tradedAt.UTC(), now.UTC()
	if tradedAt.After(now) {
		return InvalidTime
	}
	if now.Sub(tradedAt) > 5*time.Second {
		return Stale
	}
	return Fresh
}
