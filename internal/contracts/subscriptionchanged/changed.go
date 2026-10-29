// Package subscriptionchanged defines the full subscription journal record.
// It performs no storage, replay, quote retrieval or delivery authorization.
package subscriptionchanged

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
)

const (
	Type        = "subscription.changed"
	Version1    = 1
	Version2    = 2
	LatestPrice = "latest_price"
)

var (
	ErrInvalidChange      = errors.New("invalid subscription change")
	ErrUnsupportedVersion = errors.New("unsupported subscription change version")
)

// Snapshot is the immutable creation quote for threshold calculation and audit.
// TradeTime is never refreshed on replay. It is not a delivery permission.
type Snapshot struct {
	Price     *price.Price `json:"price"`
	TradeTime time.Time    `json:"trade_time"`
}

type Workflow struct {
	TriggerID string `json:"trigger_id"`
	Version   uint64 `json:"version"`
}

// Changed is a full after-image, including terminal states. Position is one
// journal record; TransactionEnd is C of its whole committed transaction.
// All allocated positions must be published/replayed, even when a consumer's
// projection is unaffected. SubscriptionVersion and Workflow.Version are
// independent of journal positions and of market quote revisions.
type Changed struct {
	SchemaVersion       int                `json:"schema_version"`
	Position            uint64             `json:"position"`
	TransactionEnd      uint64             `json:"transaction_end"`
	OwnerID             string             `json:"owner_id,omitempty"`
	SubscriptionID      string             `json:"subscription_id"`
	SubscriptionVersion uint64             `json:"subscription_version"`
	State               subscription.State `json:"state"`
	Pair                string             `json:"pair"`
	Threshold           *price.Price       `json:"threshold"`
	Direction           price.Direction    `json:"direction"`
	Activation          string             `json:"activation"`
	CreationSnapshot    *Snapshot          `json:"creation_snapshot"`
	Workflow            *Workflow          `json:"workflow,omitempty"`
}

func (c Changed) Validate() error {
	if c.SchemaVersion != Version1 && c.SchemaVersion != Version2 {
		return fmt.Errorf("%w: %d", ErrUnsupportedVersion, c.SchemaVersion)
	}
	invalid := func(field string) error { return fmt.Errorf("%w: %s", ErrInvalidChange, field) }
	if c.SchemaVersion == Version2 && strings.TrimSpace(c.OwnerID) == "" {
		return invalid("owner_id")
	}
	if c.Position == 0 || c.TransactionEnd < c.Position {
		return invalid("journal position/transaction_end")
	}
	if strings.TrimSpace(c.SubscriptionID) == "" || c.SubscriptionVersion == 0 {
		return invalid("subscription identity/version")
	}
	switch c.State {
	case subscription.Active, subscription.PendingDelivery, subscription.Sending, subscription.DeliveryBlocked, subscription.Delivered, subscription.Cancelled:
	default:
		return invalid("state")
	}
	if c.Pair == "" {
		return invalid("pair")
	}
	for _, r := range c.Pair {
		if !(r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
			return invalid("pair")
		}
	}
	if c.Threshold == nil {
		return invalid("threshold")
	}
	if c.Direction != price.Up && c.Direction != price.Down {
		return invalid("direction")
	}
	if c.Activation != LatestPrice {
		return invalid("activation")
	}
	if c.CreationSnapshot == nil || c.CreationSnapshot.Price == nil {
		return invalid("creation_snapshot")
	}
	tm := c.CreationSnapshot.TradeTime
	if tm.IsZero() || tm.Year() < 1 || tm.Year() > 9999 {
		return invalid("creation_snapshot.trade_time")
	}
	if c.Workflow != nil && (strings.TrimSpace(c.Workflow.TriggerID) == "" || c.Workflow.Version == 0) {
		return invalid("workflow")
	}
	if c.Workflow == nil && (c.State == subscription.PendingDelivery || c.State == subscription.Sending || c.State == subscription.Delivered) {
		return invalid("workflow required for state")
	}
	return nil
}

type wireChanged Changed

func (c Changed) MarshalJSON() ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(wireChanged(c))
}
func (c *Changed) UnmarshalJSON(data []byte) error {
	var wire wireChanged
	if err := json.Unmarshal(data, &wire); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidChange, err)
	}
	next := Changed(wire)
	if err := next.Validate(); err != nil {
		return err
	}
	*c = next
	return nil
}

// Encode uses caller-persisted metadata; no IDs or versions are generated.
// The supplied opaque Payload is replaced by the validated after-image.
func Encode(meta event.Envelope, c Changed) ([]byte, error) {
	payload, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	if err = validateMetadata(meta, c); err != nil {
		return nil, err
	}
	meta.Payload = payload
	return json.Marshal(meta)
}
func Decode(data []byte) (event.Envelope, Changed, error) {
	var meta event.Envelope
	var c Changed
	if err := json.Unmarshal(data, &meta); err != nil {
		return event.Envelope{}, Changed{}, err
	}
	if err := json.Unmarshal(meta.Payload, &c); err != nil {
		return event.Envelope{}, Changed{}, fmt.Errorf("%w: %w", ErrInvalidChange, err)
	}
	if err := validateMetadata(meta, c); err != nil {
		return event.Envelope{}, Changed{}, err
	}
	return meta, c, nil
}
func validateMetadata(meta event.Envelope, c Changed) error {
	if err := meta.Validate(); err != nil {
		return err
	}
	if meta.Type != Type || meta.OrderingKey != c.SubscriptionID {
		return fmt.Errorf("%w: type/ordering_key", ErrInvalidChange)
	}
	return nil
}
