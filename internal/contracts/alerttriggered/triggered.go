// Package alerttriggered defines a delivery work request. It does not authorize
// a Telegram attempt, resolve recipients, or implement durable workflow storage.
package alerttriggered

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
)

const (
	Type              = "alert.triggered"
	Version1          = 1
	DeliveryRequested = "delivery_requested"
)

var (
	ErrInvalidTrigger     = errors.New("invalid alert trigger")
	ErrUnsupportedVersion = errors.New("unsupported alert trigger version")
)

// Triggered requests work for one stable TriggerID. Versions are independent
// of the envelope version, subscription version and candidate quote revision.
// CandidateQuote is diagnostic: each send attempt needs fresh API admission,
// including a checked quote and the current recipient. No attempt permit or
// chat snapshot is carried here.
type Triggered struct {
	SchemaVersion       int                     `json:"schema_version"`
	Intent              string                  `json:"intent"`
	TriggerID           string                  `json:"trigger_id"`
	WorkflowVersion     uint64                  `json:"workflow_version"`
	UserID              string                  `json:"user_id"`
	SubscriptionID      string                  `json:"subscription_id"`
	SubscriptionVersion uint64                  `json:"subscription_version"`
	Pair                string                  `json:"pair"`
	Threshold           *price.Price            `json:"threshold"`
	Direction           price.Direction         `json:"direction"`
	CandidateQuote      *priceobserved.Observed `json:"candidate_quote"`
}

func (p Triggered) Validate() error {
	if p.SchemaVersion != Version1 {
		return fmt.Errorf("%w: %d", ErrUnsupportedVersion, p.SchemaVersion)
	}
	invalid := func(field string) error { return fmt.Errorf("%w: %s", ErrInvalidTrigger, field) }
	if p.Intent != DeliveryRequested {
		return invalid("intent")
	}
	for _, field := range []struct{ name, value string }{
		{"trigger_id", p.TriggerID}, {"user_id", p.UserID}, {"subscription_id", p.SubscriptionID},
	} {
		if strings.TrimSpace(field.value) == "" {
			return invalid(field.name)
		}
	}
	if p.WorkflowVersion == 0 || p.SubscriptionVersion == 0 {
		return invalid("workflow/subscription version")
	}
	if p.Threshold == nil {
		return invalid("threshold")
	}
	if p.Direction != price.Up && p.Direction != price.Down {
		return invalid("direction")
	}
	if p.CandidateQuote == nil {
		return invalid("candidate_quote")
	}
	if err := p.CandidateQuote.Validate(); err != nil {
		return fmt.Errorf("%w: candidate_quote: %w", ErrInvalidTrigger, err)
	}
	// The nested quote validates normalized pair syntax.
	if p.Pair != p.CandidateQuote.Pair {
		return invalid("pair/candidate_quote.pair")
	}
	return nil
}

type wireTriggered Triggered

func (p Triggered) MarshalJSON() ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(wireTriggered(p))
}

// UnmarshalJSON replaces the receiver only after the entire payload validates.
// Unknown additive fields are ignored, as in the other event contracts.
func (p *Triggered) UnmarshalJSON(data []byte) error {
	var wire wireTriggered
	if err := json.Unmarshal(data, &wire); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidTrigger, err)
	}
	next := Triggered(wire)
	if err := next.Validate(); err != nil {
		return err
	}
	*p = next
	return nil
}

// Encode preserves persisted metadata; it creates no IDs, times or versions.
func Encode(meta event.Envelope, p Triggered) ([]byte, error) {
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

func Decode(data []byte) (event.Envelope, Triggered, error) {
	var meta event.Envelope
	var p Triggered
	if err := json.Unmarshal(data, &meta); err != nil {
		return event.Envelope{}, Triggered{}, err
	}
	if err := json.Unmarshal(meta.Payload, &p); err != nil {
		return event.Envelope{}, Triggered{}, fmt.Errorf("%w: %w", ErrInvalidTrigger, err)
	}
	if err := validateMetadata(meta, p); err != nil {
		return event.Envelope{}, Triggered{}, err
	}
	return meta, p, nil
}

func validateMetadata(meta event.Envelope, p Triggered) error {
	if err := meta.Validate(); err != nil {
		return err
	}
	if meta.Type != Type || meta.OrderingKey != p.SubscriptionID {
		return fmt.Errorf("%w: type/ordering_key", ErrInvalidTrigger)
	}
	return nil
}
