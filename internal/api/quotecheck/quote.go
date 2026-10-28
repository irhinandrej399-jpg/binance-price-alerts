// Package quotecheck validates coherent market-data responses for API commands.
package quotecheck

import (
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	market "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
)

// Convert the complete coherent quote; stale/unconfirmed/gap are diagnostics,
// never a TTL. Future or malformed observations cannot authorize new work.
func Observation(r *market.LatestResponse, pair string, now time.Time) (priceobserved.Observed, bool) {
	if r == nil || r.Quote == nil || (r.Freshness != market.Freshness_FRESHNESS_FRESH && r.Freshness != market.Freshness_FRESHNESS_STALE) {
		return priceobserved.Observed{}, false
	}
	q := r.Quote
	if q.Pair != pair || q.Revision == nil {
		return priceobserved.Observed{}, false
	}
	p, err := price.Parse(q.Price)
	if err != nil {
		return priceobserved.Observed{}, false
	}
	trade, err := time.Parse(time.RFC3339Nano, q.TradeTime)
	if err != nil {
		return priceobserved.Observed{}, false
	}
	received, err := time.Parse(time.RFC3339Nano, q.ReceivedAt)
	if err != nil {
		return priceobserved.Observed{}, false
	}
	observed := priceobserved.Observed{SchemaVersion: priceobserved.Version1, Pair: q.Pair, Price: &p, SourceID: q.SourceId, Revision: priceobserved.Revision{StreamID: q.Revision.StreamId, Generation: q.Revision.Generation, Sequence: q.Revision.Sequence}, TradeTime: trade.UTC(), ReceivedAt: received.UTC(), StreamState: priceobserved.StreamState(q.StreamState), GapDetected: q.GapDetected}
	return observed, observed.Validate() == nil && price.CheckFreshness(true, trade, now) != price.InvalidTime
}

// Proto preserves the stored observation, including its diagnostic stream state.
func Proto(q priceobserved.Observed) *market.Quote {
	return &market.Quote{Pair: q.Pair, Price: q.Price.String(), SourceId: q.SourceID,
		Revision:  &market.Revision{StreamId: q.Revision.StreamID, Generation: q.Revision.Generation, Sequence: q.Revision.Sequence},
		TradeTime: q.TradeTime.UTC().Format(time.RFC3339Nano), ReceivedAt: q.ReceivedAt.UTC().Format(time.RFC3339Nano), StreamState: string(q.StreamState), GapDetected: q.GapDetected}
}
