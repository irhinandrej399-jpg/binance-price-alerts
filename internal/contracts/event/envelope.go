// Package event defines the shared event envelope, independently of transport
// positions and event-specific payload schemas.
package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const Version1 = 1

var (
	ErrInvalidEnvelope    = errors.New("invalid event envelope")
	ErrUnsupportedVersion = errors.New("unsupported event envelope version")
)

// Envelope carries producer-assigned metadata. EventID and OccurredAt are
// persisted with the outbox record and reused on every publication attempt.
// OrderingKey selects an ordered stream; it is not a revision or cursor.
// Payload is opaque here: its schema and versioning belong to the event type.
type Envelope struct {
	EventID       string          `json:"event_id"`
	Type          string          `json:"type"`
	Version       int             `json:"version"`
	OccurredAt    time.Time       `json:"occurred_at"`
	CorrelationID string          `json:"correlation_id"`
	OrderingKey   string          `json:"ordering_key"`
	Payload       json.RawMessage `json:"payload"`
}

// Validate checks common metadata only. IDs are opaque nonblank strings;
// callers are responsible for uniqueness, persistence and payload validation.
func (e Envelope) Validate() error {
	if e.Version != Version1 {
		return fmt.Errorf("%w: %d", ErrUnsupportedVersion, e.Version)
	}
	for _, field := range []struct{ name, value string }{
		{"event_id", e.EventID},
		{"type", e.Type},
		{"correlation_id", e.CorrelationID},
		{"ordering_key", e.OrderingKey},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("%w: %s is required", ErrInvalidEnvelope, field.name)
		}
	}
	if e.OccurredAt.IsZero() || e.OccurredAt.Year() < 1 || e.OccurredAt.Year() > 9999 {
		return fmt.Errorf("%w: occurred_at must be a nonzero RFC3339 time", ErrInvalidEnvelope)
	}
	return nil
}

// wireEnvelope avoids recursively invoking the JSON methods.
type wireEnvelope Envelope

func (e Envelope) MarshalJSON() ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(wireEnvelope(e))
}

// UnmarshalJSON validates before replacing the receiver. Unknown fields are
// tolerated for additive changes, but unsupported envelope versions fail.
func (e *Envelope) UnmarshalJSON(data []byte) error {
	var decoded wireEnvelope
	if err := json.Unmarshal(data, &decoded); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidEnvelope, err)
	}
	next := Envelope(decoded)
	if err := next.Validate(); err != nil {
		return err
	}
	*e = next
	return nil
}
