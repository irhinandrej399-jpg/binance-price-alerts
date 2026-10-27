package priceobserved_test

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	observed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
)

const message = `{"event_id":"outbox-1","type":"price.observed","version":1,"occurred_at":"2026-10-27T10:00:01Z","correlation_id":"ingest-1","ordering_key":"BTCUSDT","payload":{"schema_version":1,"pair":"BTCUSDT","price":"999999999999999999.123456789012345678","source_id":"9007199254740993","revision":{"stream_id":"binance-spot-trade","generation":2,"sequence":9007199254740993},"trade_time":"2026-10-27T10:00:00.123456Z","received_at":"2026-10-27T10:00:01.123456Z","source_event_time":"2026-10-27T10:00:00.123457Z","stream_state":"unconfirmed","gap_detected":true}}`

func TestRoundTrip(t *testing.T) {
	meta, p, err := observed.Decode([]byte(message))
	if err != nil {
		t.Fatal(err)
	}
	if p.Price.String() != "999999999999999999.123456789012345678" || p.Revision.Sequence != 9007199254740993 || p.SourceID != "9007199254740993" {
		t.Fatal("lost precision", p)
	}
	out, err := observed.Encode(meta, p)
	if err != nil {
		t.Fatal(err)
	}
	m2, p2, err := observed.Decode(out)
	if err != nil {
		t.Fatal(err)
	}
	meta.Payload, m2.Payload = nil, nil
	if !reflect.DeepEqual(meta, m2) || !reflect.DeepEqual(p, p2) {
		t.Fatal("roundtrip changed event")
	}
	// E is optional; confirmation and gap history are independent.
	p.SourceEventTime = nil
	p.StreamState = observed.Live
	p.GapDetected = false
	out, err = observed.Encode(meta, p)
	if err != nil {
		t.Fatal(err)
	}
	_, p2, err = observed.Decode(out)
	if err != nil || p2.GapDetected || p2.SourceEventTime != nil {
		t.Fatal(p2, err)
	}
}

func TestInvalidContracts(t *testing.T) {
	tests := []struct {
		old, new string
		want     error
	}{
		{`"schema_version":1`, `"schema_version":2`, observed.ErrUnsupportedVersion},
		{`"schema_version":1,`, ``, observed.ErrUnsupportedVersion},
		{`"source_id":"9007199254740993",`, ``, observed.ErrInvalidObservation},
		{`"source_id":"9007199254740993"`, `"source_id":" "`, observed.ErrInvalidObservation},
		{`"pair":"BTCUSDT"`, `"pair":"btcusdt"`, observed.ErrInvalidObservation},
		{`"price":"999999999999999999.123456789012345678"`, `"price":1.25`, observed.ErrInvalidObservation},
		{`"price":"999999999999999999.123456789012345678"`, `"price":"0"`, observed.ErrInvalidObservation},
		{`"price":"999999999999999999.123456789012345678",`, ``, observed.ErrInvalidObservation},
		{`"trade_time":"2026-10-27T10:00:00.123456Z"`, `"trade_time":null`, observed.ErrInvalidObservation},
		{`"received_at":"2026-10-27T10:00:01.123456Z",`, ``, observed.ErrInvalidObservation},
		{`"source_event_time":"2026-10-27T10:00:00.123457Z"`, `"source_event_time":"0001-01-01T00:00:00Z"`, observed.ErrInvalidObservation},
		{`"generation":2`, `"generation":0`, observed.ErrInvalidObservation},
		{`"sequence":9007199254740993`, `"sequence":0`, observed.ErrInvalidObservation},
		{`"stream_id":"binance-spot-trade"`, `"stream_id":" "`, observed.ErrInvalidObservation},
		{`"stream_state":"unconfirmed"`, `"stream_state":"connected"`, observed.ErrInvalidObservation},
		{`"gap_detected":true`, `"gap_detected":null`, observed.ErrInvalidObservation},
		{`,"gap_detected":true`, ``, observed.ErrInvalidObservation},
		{`"ordering_key":"BTCUSDT"`, `"ordering_key":"ETHUSDT"`, observed.ErrInvalidObservation},
		{`"type":"price.observed"`, `"type":"other"`, observed.ErrInvalidObservation},
	}
	for _, tt := range tests {
		t.Run(tt.new, func(t *testing.T) {
			_, _, err := observed.Decode([]byte(strings.Replace(message, tt.old, tt.new, 1)))
			if !errors.Is(err, tt.want) {
				t.Fatalf("got %v want %v", err, tt.want)
			}
		})
	}
}

func TestAtomicDecodeAndInvalidOutput(t *testing.T) {
	meta, p, err := observed.Decode([]byte(message))
	if err != nil {
		t.Fatal(err)
	}
	before := p
	for _, raw := range []string{`null`, `[]`, `{}`, `{"schema_version":1}`, `{"schema_version":1} trailing`} {
		if err = json.Unmarshal([]byte(raw), &p); err == nil {
			t.Fatal("accepted", raw)
		}
		if !reflect.DeepEqual(p, before) {
			t.Fatal("changed receiver on error")
		}
	}
	p.Price = nil
	if _, err = observed.Encode(meta, p); !errors.Is(err, observed.ErrInvalidObservation) {
		t.Fatal(err)
	}
	meta.Payload = nil
	raw, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = observed.Decode(raw); !errors.Is(err, observed.ErrInvalidObservation) {
		t.Fatal(err)
	}
}

func TestRevisionOrder(t *testing.T) {
	_, current, err := observed.Decode([]byte(message))
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		generation, sequence uint64
		want                 bool
	}{{2, 1, false}, {2, 9007199254740993, false}, {2, 9007199254740994, true}, {1, math.MaxUint64, false}, {3, 1, true}} {
		candidate := current
		candidate.Revision.Generation = tt.generation
		candidate.Revision.Sequence = tt.sequence
		got, err := candidate.NewerThan(current)
		if err != nil || got != tt.want {
			t.Fatal(tt, got, err)
		}
	}
	for _, field := range []string{"pair", "stream"} {
		candidate := current
		if field == "pair" {
			candidate.Pair = "ETHUSDT"
		} else {
			candidate.Revision.StreamID = "different"
		}
		if _, err := candidate.NewerThan(current); !errors.Is(err, observed.ErrIncomparableRevision) {
			t.Fatal(err)
		}
	}
}

func TestNormalizeTimestamp(t *testing.T) {
	for _, tt := range []struct {
		value int64
		unit  observed.TimeUnit
		want  string
	}{
		{1793095200123, observed.Milliseconds, "2026-10-27T10:00:00.123Z"},
		{1793095200123456, observed.Microseconds, "2026-10-27T10:00:00.123456Z"},
		{-1, observed.Microseconds, "1969-12-31T23:59:59.999999Z"},
	} {
		got, err := observed.NormalizeTimestamp(tt.value, tt.unit)
		if err != nil || got.Format(time.RFC3339Nano) != tt.want {
			t.Fatal(got, err, tt.want)
		}
	}
	for _, tt := range []struct {
		v int64
		u observed.TimeUnit
	}{{1, "seconds"}, {math.MaxInt64, observed.Milliseconds}, {math.MinInt64, observed.Microseconds}} {
		if _, err := observed.NormalizeTimestamp(tt.v, tt.u); !errors.Is(err, observed.ErrInvalidObservation) {
			t.Fatal(err)
		}
	}
	// Replay does not substitute receipt or publication time for trade time.
	meta, p, err := observed.Decode([]byte(message))
	if err != nil {
		t.Fatal(err)
	}
	trade := p.TradeTime
	meta.OccurredAt = meta.OccurredAt.Add(time.Hour)
	raw, err := observed.Encode(meta, p)
	if err != nil {
		t.Fatal(err)
	}
	_, again, err := observed.Decode(raw)
	if err != nil || !again.TradeTime.Equal(trade) {
		t.Fatal(again, err)
	}
}
