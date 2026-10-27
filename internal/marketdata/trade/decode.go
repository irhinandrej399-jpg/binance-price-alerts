// Package trade converts one Binance Spot raw trade into PriceObserved.
package trade

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
)

const StreamID = "binance-spot-trade"

var (
	ErrInvalidTrade     = errors.New("invalid Binance trade")
	ErrUnsupportedEvent = errors.New("unsupported Binance event")
)

// Metadata is supplied by the caller. Decode never assigns durable revisions,
// infers timestamp units or confirms a stream from an individual message.
type Metadata struct {
	Unit        priceobserved.TimeUnit
	ReceivedAt  time.Time
	Generation  uint64
	Sequence    uint64
	StreamState priceobserved.StreamState
	GapDetected bool
}

// Decode accepts a raw trade payload, without a combined-stream wrapper.
// Every error returns a zero observation. Unknown extra source fields are ignored.
func Decode(data []byte, meta Metadata) (priceobserved.Observed, error) {
	invalid := func(field string) (priceobserved.Observed, error) {
		return priceobserved.Observed{}, fmt.Errorf("%w: %s", ErrInvalidTrade, field)
	}
	// Exact keys matter: Binance uses both e/E and t/T. A struct decoder's
	// case-insensitive fallback could silently use t as a missing T (or vice versa).
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return invalid("JSON")
	}
	read := func(key string, dst any) bool {
		raw, ok := fields[key]
		return ok && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) && json.Unmarshal(raw, dst) == nil
	}
	var kind, pair, rawPrice string
	var id uint64
	var traded, eventTime int64
	if !read("e", &kind) || kind == "" {
		return invalid("e")
	}
	if kind != "trade" {
		return priceobserved.Observed{}, ErrUnsupportedEvent
	}
	if !read("s", &pair) || !read("t", &id) || !read("p", &rawPrice) || !read("T", &traded) || !read("E", &eventTime) {
		return invalid("required fields s/t/p/T/E")
	}
	p, err := price.Parse(rawPrice)
	if err != nil {
		return invalid("price")
	}
	tt, err := priceobserved.NormalizeTimestamp(traded, meta.Unit)
	if err != nil {
		return invalid("T/unit")
	}
	et, err := priceobserved.NormalizeTimestamp(eventTime, meta.Unit)
	if err != nil {
		return invalid("E/unit")
	}
	observed := priceobserved.Observed{
		SchemaVersion: priceobserved.Version1, Pair: pair, Price: &p, SourceID: strconv.FormatUint(id, 10),
		Revision:  priceobserved.Revision{StreamID: StreamID, Generation: meta.Generation, Sequence: meta.Sequence},
		TradeTime: tt, SourceEventTime: &et, ReceivedAt: meta.ReceivedAt.UTC(),
		StreamState: meta.StreamState, GapDetected: meta.GapDetected,
	}
	if err := observed.Validate(); err != nil {
		return invalid("observation/metadata")
	}
	return observed, nil
}
