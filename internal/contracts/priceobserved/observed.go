// Package priceobserved defines the exact market quote event, without ingest,
// persistence, publication or consumer state management.
package priceobserved

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
)

const (
	Type                     = "price.observed"
	Version1                 = 1
	Unconfirmed  StreamState = "unconfirmed"
	Live         StreamState = "live"
	Milliseconds TimeUnit    = "milliseconds"
	Microseconds TimeUnit    = "microseconds"
)

var (
	ErrInvalidObservation   = errors.New("invalid price observation")
	ErrUnsupportedVersion   = errors.New("unsupported price observation version")
	ErrIncomparableRevision = errors.New("incomparable price revisions")
)

type StreamState string
type TimeUnit string

// Revision is ordered lexicographically within one pair and stable StreamID.
// Market-data must persist generations and sequences; neither resets on replay.
// Generation increases before Sequence can restart. StreamID is not a socket ID.
type Revision struct {
	StreamID   string `json:"stream_id"`
	Generation uint64 `json:"generation"`
	Sequence   uint64 `json:"sequence"`
}

type Observed struct {
	SchemaVersion   int          `json:"schema_version"`
	Pair            string       `json:"pair"`
	Price           *price.Price `json:"price"`
	SourceID        string       `json:"source_id"`
	Revision        Revision     `json:"revision"`
	TradeTime       time.Time    `json:"trade_time"`
	ReceivedAt      time.Time    `json:"received_at"`
	SourceEventTime *time.Time   `json:"source_event_time,omitempty"`
	StreamState     StreamState  `json:"stream_state"`
	GapDetected     bool         `json:"gap_detected"`
}

func validTime(t time.Time) bool { return !t.IsZero() && t.Year() >= 1 && t.Year() <= 9999 }

func (p Observed) Validate() error {
	if p.SchemaVersion != Version1 {
		return fmt.Errorf("%w: %d", ErrUnsupportedVersion, p.SchemaVersion)
	}
	invalid := func(field string) error { return fmt.Errorf("%w: %s", ErrInvalidObservation, field) }
	if p.Pair == "" {
		return invalid("pair")
	}
	for _, r := range p.Pair {
		if !(r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
			return invalid("pair")
		}
	}
	if p.Price == nil || p.Price.Cmp(price.Price{}) <= 0 {
		return invalid("positive price required")
	}
	if strings.TrimSpace(p.SourceID) == "" {
		return invalid("source_id")
	}
	if strings.TrimSpace(p.Revision.StreamID) == "" || p.Revision.Generation == 0 || p.Revision.Sequence == 0 {
		return invalid("revision")
	}
	if !validTime(p.TradeTime) || !validTime(p.ReceivedAt) {
		return invalid("trade_time/received_at")
	}
	if p.SourceEventTime != nil && !validTime(*p.SourceEventTime) {
		return invalid("source_event_time")
	}
	if p.StreamState != Unconfirmed && p.StreamState != Live {
		return invalid("stream_state")
	}
	return nil
}

type wireObserved Observed

func (p Observed) MarshalJSON() ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(wireObserved(p))
}
func (p *Observed) UnmarshalJSON(data []byte) error {
	// A pointer distinguishes an explicit false from an omitted/null gap marker.
	var wire struct {
		wireObserved
		GapDetected *bool `json:"gap_detected"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidObservation, err)
	}
	next := Observed(wire.wireObserved)
	if err := next.Validate(); err != nil {
		return err
	}
	if wire.GapDetected == nil {
		return fmt.Errorf("%w: gap_detected required", ErrInvalidObservation)
	}
	next.GapDetected = *wire.GapDetected
	*p = next
	return nil
}

// NewerThan exposes quote order only; callers apply the result to their own
// latest-price state. Duplicate/older events return false. No clock is consulted.
func (p Observed) NewerThan(current Observed) (bool, error) {
	if err := p.Validate(); err != nil {
		return false, err
	}
	if err := current.Validate(); err != nil {
		return false, err
	}
	if p.Pair != current.Pair || p.Revision.StreamID != current.Revision.StreamID {
		return false, ErrIncomparableRevision
	}
	a, b := p.Revision, current.Revision
	return a.Generation > b.Generation || a.Generation == b.Generation && a.Sequence > b.Sequence, nil
}

// NormalizeTimestamp converts explicit source units to UTC without floating
// point or a whole-value nanosecond multiplication that could overflow int64.
func NormalizeTimestamp(value int64, unit TimeUnit) (time.Time, error) {
	var perSecond int64
	switch unit {
	case Milliseconds:
		perSecond = 1000
	case Microseconds:
		perSecond = 1000000
	default:
		return time.Time{}, fmt.Errorf("%w: timestamp unit", ErrInvalidObservation)
	}
	tm := time.Unix(value/perSecond, (value%perSecond)*(1000000000/perSecond)).UTC()
	if !validTime(tm) {
		return time.Time{}, fmt.Errorf("%w: timestamp range", ErrInvalidObservation)
	}
	return tm, nil
}

// Encode preserves caller-persisted envelope metadata, replacing only Payload.
func Encode(meta event.Envelope, p Observed) ([]byte, error) {
	payload, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	if err = validateMetadata(meta, p); err != nil {
		return nil, err
	}
	meta.Payload = payload
	return json.Marshal(meta)
}
func Decode(data []byte) (event.Envelope, Observed, error) {
	var meta event.Envelope
	var p Observed
	if err := json.Unmarshal(data, &meta); err != nil {
		return event.Envelope{}, Observed{}, err
	}
	if err := json.Unmarshal(meta.Payload, &p); err != nil {
		return event.Envelope{}, Observed{}, fmt.Errorf("%w: %w", ErrInvalidObservation, err)
	}
	if err := validateMetadata(meta, p); err != nil {
		return event.Envelope{}, Observed{}, err
	}
	return meta, p, nil
}
func validateMetadata(meta event.Envelope, p Observed) error {
	if err := meta.Validate(); err != nil {
		return err
	}
	if meta.Type != Type || meta.OrderingKey != p.Pair {
		return fmt.Errorf("%w: type/ordering_key", ErrInvalidObservation)
	}
	return nil
}
