package event_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
)

const validJSON = `{"event_id":"outbox-42","type":"example.changed","version":1,"occurred_at":"2026-10-26T10:00:00Z","correlation_id":"request-7","ordering_key":"BTCUSDT","payload":{"revision":18,"generation":"market-1"}}`

func TestEnvelopeRoundTripAndRetry(t *testing.T) {
	var original event.Envelope
	if err := json.Unmarshal([]byte(validJSON), &original); err != nil {
		t.Fatal(err)
	}
	if original.EventID != "outbox-42" || original.Type != "example.changed" || original.Version != event.Version1 || original.CorrelationID != "request-7" || original.OrderingKey != "BTCUSDT" || !original.OccurredAt.Equal(time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("wrong metadata: %+v", original)
	}
	// A relay re-reads the persisted message after a lost publish result.
	current := original
	for range 3 {
		data, err := json.Marshal(current)
		if err != nil {
			t.Fatal(err)
		}
		var restored event.Envelope
		if err := json.Unmarshal(data, &restored); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(original, restored) {
			t.Fatalf("retry changed event: %+v", restored)
		}
		current = restored
	}
}

func TestEnvelopeRejectsInvalidMetadata(t *testing.T) {
	tests := []struct {
		name, old, replacement string
		want                   error
	}{
		{"missing ID", `"event_id":"outbox-42",`, "", event.ErrInvalidEnvelope},
		{"blank ID", `"outbox-42"`, `"  "`, event.ErrInvalidEnvelope},
		{"missing type", `"type":"example.changed",`, "", event.ErrInvalidEnvelope},
		{"missing version", `"version":1,`, "", event.ErrUnsupportedVersion},
		{"unknown version", `"version":1`, `"version":2`, event.ErrUnsupportedVersion},
		{"missing time", `"occurred_at":"2026-10-26T10:00:00Z",`, "", event.ErrInvalidEnvelope},
		{"bad time", `"2026-10-26T10:00:00Z"`, `"yesterday"`, event.ErrInvalidEnvelope},
		{"missing correlation", `"correlation_id":"request-7",`, "", event.ErrInvalidEnvelope},
		{"blank key", `"BTCUSDT"`, `" "`, event.ErrInvalidEnvelope},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got event.Envelope
			err := json.Unmarshal([]byte(strings.Replace(validJSON, tt.old, tt.replacement, 1)), &got)
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestEnvelopeRejectsInvalidOutput(t *testing.T) {
	var valid event.Envelope
	if err := json.Unmarshal([]byte(validJSON), &valid); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		change func(*event.Envelope)
		want   error
	}{
		{"ID", func(e *event.Envelope) { e.EventID = "" }, event.ErrInvalidEnvelope},
		{"version", func(e *event.Envelope) { e.Version = 99 }, event.ErrUnsupportedVersion},
		{"time", func(e *event.Envelope) { e.OccurredAt = time.Time{} }, event.ErrInvalidEnvelope},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := valid
			tt.change(&e)
			if _, err := json.Marshal(e); !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestEnvelopeDecodeFailurePreservesReceiver(t *testing.T) {
	var got event.Envelope
	if err := json.Unmarshal([]byte(validJSON), &got); err != nil {
		t.Fatal(err)
	}
	before := got
	for _, data := range []string{`null`, `[]`, `{"version":2}`, validJSON + `{}`, `{"version":1,"occurred_at":false}`} {
		if err := json.Unmarshal([]byte(data), &got); err == nil {
			t.Fatalf("accepted %s", data)
		}
		if !reflect.DeepEqual(before, got) {
			t.Fatalf("failed decode changed receiver: %+v", got)
		}
	}
}
