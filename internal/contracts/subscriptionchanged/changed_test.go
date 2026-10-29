package subscriptionchanged_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	changed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/subscriptionchanged"
)

const message = `{"event_id":"outbox-42","type":"subscription.changed","version":1,"occurred_at":"2026-10-26T10:00:00Z","correlation_id":"request-7","ordering_key":"sub-7","payload":{"schema_version":1,"position":9007199254740993,"transaction_end":9007199254740994,"subscription_id":"sub-7","subscription_version":1,"state":"active","pair":"BTCUSDT","threshold":"999999999999999999.123456789012345678","direction":"up","activation":"latest_price","creation_snapshot":{"price":"0.000000000000000001","trade_time":"2026-10-26T09:59:59Z"}}}`

func TestRoundTripCreationAndLifecycle(t *testing.T) {
	for _, state := range []string{"active", "pending_delivery", "sending", "delivery_blocked", "delivered", "cancelled"} {
		t.Run(state, func(t *testing.T) {
			input := strings.Replace(message, `"state":"active"`, `"state":"`+state+`","workflow":{"trigger_id":"trigger-1","version":3}`, 1)
			meta, payload, err := changed.Decode([]byte(input))
			if err != nil {
				t.Fatal(err)
			}
			if payload.Threshold.String() != "999999999999999999.123456789012345678" || payload.CreationSnapshot.Price.String() != "0.000000000000000001" {
				t.Fatal("lost price precision")
			}
			if payload.Position != 9007199254740993 || payload.TransactionEnd != 9007199254740994 {
				t.Fatal("lost journal precision")
			}
			out, err := changed.Encode(meta, payload)
			if err != nil {
				t.Fatal(err)
			}
			againMeta, again, err := changed.Decode(out)
			if err != nil {
				t.Fatal(err)
			}
			meta.Payload, againMeta.Payload = nil, nil
			if !reflect.DeepEqual(meta, againMeta) || !reflect.DeepEqual(payload, again) {
				t.Fatal("round trip changed event")
			}
		})
	}
	// A subscription may be created or cancelled before any workflow exists.
	for _, input := range []string{message, strings.Replace(message, `"active"`, `"cancelled"`, 1)} {
		meta, payload, err := changed.Decode([]byte(input))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = changed.Encode(meta, payload); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRejectInvalidContracts(t *testing.T) {
	tests := []struct {
		old, new string
		want     error
	}{
		{`"schema_version":1`, `"schema_version":99`, changed.ErrUnsupportedVersion},
		{`"schema_version":1,`, ``, changed.ErrUnsupportedVersion},
		{`"position":9007199254740993`, `"position":0`, changed.ErrInvalidChange},
		{`"transaction_end":9007199254740994`, `"transaction_end":1`, changed.ErrInvalidChange},
		{`"subscription_version":1`, `"subscription_version":0`, changed.ErrInvalidChange},
		{`"subscription_id":"sub-7"`, `"subscription_id":" "`, changed.ErrInvalidChange},
		{`"state":"active"`, `"state":"triggered"`, changed.ErrInvalidChange},
		{`"state":"active"`, `"state":"sending"`, changed.ErrInvalidChange},
		{`"pair":"BTCUSDT"`, `"pair":"btcusdt"`, changed.ErrInvalidChange},
		{`"direction":"up"`, `"direction":"sideways"`, changed.ErrInvalidChange},
		{`"creation_snapshot":{`, `"unused_snapshot":{`, changed.ErrInvalidChange},
		{`"price":"0.000000000000000001",`, ``, changed.ErrInvalidChange},
		{`"state":"active"`, `"state":"active","workflow":{"trigger_id":" ","version":1}`, changed.ErrInvalidChange},
		{`"activation":"latest_price"`, `"activation":"replay_from_creation"`, changed.ErrInvalidChange},
		{`"threshold":"999999999999999999.123456789012345678"`, `"threshold":1.1`, changed.ErrInvalidChange},
		{`"threshold":"999999999999999999.123456789012345678",`, ``, changed.ErrInvalidChange},
		{`"trade_time":"2026-10-26T09:59:59Z"`, `"trade_time":null`, changed.ErrInvalidChange},
		{`"state":"active"`, `"state":"active","workflow":{"trigger_id":"x","version":0}`, changed.ErrInvalidChange},
		{`"type":"subscription.changed"`, `"type":"example.changed"`, changed.ErrInvalidChange},
		{`"ordering_key":"sub-7"`, `"ordering_key":"other"`, changed.ErrInvalidChange},
		{`"event_id":"outbox-42"`, `"event_id":""`, event.ErrInvalidEnvelope},
	}
	for _, tt := range tests {
		t.Run(tt.new, func(t *testing.T) {
			_, _, err := changed.Decode([]byte(strings.Replace(message, tt.old, tt.new, 1)))
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestInvalidOutputAndAtomicDecode(t *testing.T) {
	meta, payload, err := changed.Decode([]byte(message))
	if err != nil {
		t.Fatal(err)
	}
	before := payload
	if err = json.Unmarshal([]byte(`{"schema_version":1}`), &payload); !errors.Is(err, changed.ErrInvalidChange) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, payload) {
		t.Fatal("failed decode changed receiver")
	}
	payload.Position = 0
	if _, err = changed.Encode(meta, payload); !errors.Is(err, changed.ErrInvalidChange) {
		t.Fatal(err)
	}
	for _, raw := range []string{`null`, `[]`, `{}`, `{"schema_version":1} trailing`} {
		if err = json.Unmarshal([]byte(raw), &payload); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}

func TestDownAndWholeTransaction(t *testing.T) {
	// Two records in one transaction retain separate positions and the same C.
	for _, pos := range []uint64{41, 42} {
		meta, c, err := changed.Decode([]byte(message))
		if err != nil {
			t.Fatal(err)
		}
		c.Position, c.TransactionEnd = pos, 42
		c.Direction = "down"
		out, err := changed.Encode(meta, c)
		if err != nil {
			t.Fatal(err)
		}
		_, got, err := changed.Decode(out)
		if err != nil {
			t.Fatal(err)
		}
		if got.Position != pos || got.TransactionEnd != 42 || got.Direction != "down" {
			t.Fatal(got)
		}
	}
}

func TestMissingPayload(t *testing.T) {
	var meta event.Envelope
	if err := json.Unmarshal([]byte(message), &meta); err != nil {
		t.Fatal(err)
	}
	meta.Payload = nil
	raw, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = changed.Decode(raw); err == nil {
		t.Fatal("accepted null payload")
	}
	// The common envelope permits omission; this event contract must reject it.
	raw = []byte(strings.Replace(string(raw), `,"payload":null`, "", 1))
	if _, _, err = changed.Decode(raw); !errors.Is(err, changed.ErrInvalidChange) {
		t.Fatal(err)
	}
}

func TestVersion2Owner(t *testing.T) {
	input := strings.Replace(message, `"schema_version":1`, `"schema_version":2,"owner_id":"canonical-owner"`, 1)
	meta, c, err := changed.Decode([]byte(input))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := changed.Encode(meta, c)
	if err != nil || !strings.Contains(string(raw), `"owner_id":"canonical-owner"`) {
		t.Fatalf("owner lost: %s %v", raw, err)
	}
	for _, owner := range []string{``, `,"owner_id":""`, `,"owner_id":" "`, `,"owner_id":42`} {
		bad := strings.Replace(input, `,"owner_id":"canonical-owner"`, owner, 1)
		if _, _, err := changed.Decode([]byte(bad)); !errors.Is(err, changed.ErrInvalidChange) {
			t.Fatalf("accepted invalid owner: %v", err)
		}
	}
}
