package alerttriggered_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	alert "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/alerttriggered"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	observed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
)

const message = `{"event_id":"outbox-1","type":"alert.triggered","version":1,"occurred_at":"2026-10-27T10:00:01Z","correlation_id":"request-1","ordering_key":"sub-1","payload":{"schema_version":1,"intent":"delivery_requested","trigger_id":"workflow-1","workflow_version":9007199254740993,"user_id":"user-1","subscription_id":"sub-1","subscription_version":9007199254740994,"pair":"BTCUSDT","threshold":"999999999999999999.123456789012345677","direction":"up","candidate_quote":{"schema_version":1,"pair":"BTCUSDT","price":"999999999999999999.123456789012345678","source_id":"9007199254740993","revision":{"stream_id":"binance-spot-trade","generation":2,"sequence":9007199254740993},"trade_time":"2026-10-27T10:00:00.123456Z","received_at":"2026-10-27T10:00:01.123456Z","stream_state":"unconfirmed","gap_detected":true}}}`

func TestExactSerializationAndReplay(t *testing.T) {
	meta, p, err := alert.Decode([]byte(message))
	if err != nil {
		t.Fatal(err)
	}
	if p.Intent != alert.DeliveryRequested || p.TriggerID != "workflow-1" || p.WorkflowVersion != 9007199254740993 || p.SubscriptionVersion != 9007199254740994 || p.Threshold.String() != "999999999999999999.123456789012345677" || p.CandidateQuote.Price.String() != "999999999999999999.123456789012345678" {
		t.Fatal("lost identity/precision", p)
	}
	for range 2 {
		out, err := alert.Encode(meta, p)
		if err != nil {
			t.Fatal(err)
		}
		if string(out) != message {
			t.Fatalf("wire changed: %s", out)
		}
		m2, p2, err := alert.Decode(out)
		if err != nil || !reflect.DeepEqual(p, p2) || !reflect.DeepEqual(meta, m2) {
			t.Fatal("replay changed event", err)
		}
		meta, p = m2, p2
	}
}

func TestInvalidContracts(t *testing.T) {
	tests := []struct {
		old, new string
		want     error
	}{
		{`"schema_version":1`, `"schema_version":2`, alert.ErrUnsupportedVersion},
		{`"schema_version":1,`, ``, alert.ErrUnsupportedVersion},
		{`"intent":"delivery_requested"`, `"intent":"delivered"`, alert.ErrInvalidTrigger},
		{`"intent":"delivery_requested",`, ``, alert.ErrInvalidTrigger},
		{`"trigger_id":"workflow-1"`, `"trigger_id":" "`, alert.ErrInvalidTrigger},
		{`"trigger_id":"workflow-1",`, ``, alert.ErrInvalidTrigger},
		{`"workflow_version":9007199254740993`, `"workflow_version":0`, alert.ErrInvalidTrigger},
		{`"workflow_version":9007199254740993,`, ``, alert.ErrInvalidTrigger},
		{`"user_id":"user-1"`, `"user_id":" "`, alert.ErrInvalidTrigger},
		{`"user_id":"user-1",`, ``, alert.ErrInvalidTrigger},
		{`"subscription_id":"sub-1"`, `"subscription_id":" "`, alert.ErrInvalidTrigger},
		{`"subscription_version":9007199254740994`, `"subscription_version":0`, alert.ErrInvalidTrigger},
		{`"pair":"BTCUSDT"`, `"pair":"ETHUSDT"`, alert.ErrInvalidTrigger},
		{`"pair":"BTCUSDT"`, `"pair":"btcusdt"`, alert.ErrInvalidTrigger},
		{`"threshold":"999999999999999999.123456789012345677"`, `"threshold":1.25`, alert.ErrInvalidTrigger},
		{`"threshold":"999999999999999999.123456789012345677"`, `"threshold":null`, alert.ErrInvalidTrigger},
		{`"direction":"up"`, `"direction":"sideways"`, alert.ErrInvalidTrigger},
		{`"candidate_quote":{`, `"candidate_quote":null,"unused":{`, alert.ErrInvalidTrigger},
		{`"source_id":"9007199254740993",`, ``, observed.ErrInvalidObservation},
		{`"gap_detected":true`, `"gap_detected":null`, observed.ErrInvalidObservation},
		{`"trade_time":"2026-10-27T10:00:00.123456Z"`, `"trade_time":null`, observed.ErrInvalidObservation},
		{`"ordering_key":"sub-1"`, `"ordering_key":"other"`, alert.ErrInvalidTrigger},
		{`"type":"alert.triggered"`, `"type":"other"`, alert.ErrInvalidTrigger},
		{`"event_id":"outbox-1"`, `"event_id":" "`, event.ErrInvalidEnvelope},
		{`"version":1`, `"version":2`, event.ErrUnsupportedVersion},
	}
	for _, tt := range tests {
		t.Run(tt.old+tt.new, func(t *testing.T) {
			_, _, err := alert.Decode([]byte(strings.Replace(message, tt.old, tt.new, 1)))
			if !errors.Is(err, tt.want) {
				t.Fatalf("got %v want %v", err, tt.want)
			}
		})
	}
}

func TestAtomicDecodeAndInvalidOutput(t *testing.T) {
	meta, p, err := alert.Decode([]byte(message))
	if err != nil {
		t.Fatal(err)
	}
	before := p
	for _, raw := range []string{`null`, `[]`, `{}`, `{"schema_version":1}`, `{"schema_version":1} trailing`} {
		if err = json.Unmarshal([]byte(raw), &p); err == nil {
			t.Fatal("accepted", raw)
		}
		if !reflect.DeepEqual(p, before) {
			t.Fatal("changed receiver on failure")
		}
	}
	p.TriggerID = ""
	if _, err = alert.Encode(meta, p); !errors.Is(err, alert.ErrInvalidTrigger) {
		t.Fatal(err)
	}
	p = before
	p.CandidateQuote.Price = nil
	if _, err = alert.Encode(meta, p); !errors.Is(err, observed.ErrInvalidObservation) {
		t.Fatal(err)
	}
}

func TestOldCandidateRemainsOnlyDeliveryRequest(t *testing.T) {
	meta, p, err := alert.Decode([]byte(message))
	if err != nil {
		t.Fatal(err)
	}
	// A replay may happen after the condition disappeared or after relinking.
	// It still carries only a request and the original diagnostic quote.
	p.Direction = "down" // The candidate does not even satisfy this condition.
	p.WorkflowVersion++
	meta.EventID = "outbox-resume"
	out, err := alert.Encode(meta, p)
	if err != nil {
		t.Fatal(err)
	}
	_, resumed, err := alert.Decode(out)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.TriggerID != p.TriggerID || resumed.Intent != alert.DeliveryRequested || !reflect.DeepEqual(resumed.CandidateQuote, p.CandidateQuote) {
		t.Fatal("changed workflow or diagnostic quote")
	}
	var wire struct {
		Payload map[string]json.RawMessage `json:"payload"`
	}
	if err = json.Unmarshal(out, &wire); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"chat_id", "binding_version", "attempt_id", "permission", "authorized", "delivered"} {
		if _, ok := wire.Payload[field]; ok {
			t.Fatal("request contains attempt authority", field)
		}
	}
	// Unknown additive fields are ignored, never retained as send authority.
	injected := strings.Replace(string(out), `"intent":"delivery_requested"`, `"intent":"delivery_requested","chat_id":"old-chat","authorized":true`, 1)
	m2, p2, err := alert.Decode([]byte(injected))
	if err != nil {
		t.Fatal(err)
	}
	clean, err := alert.Encode(m2, p2)
	if err != nil {
		t.Fatal(err)
	}
	if string(clean) != string(out) {
		t.Fatal("retained unknown authority")
	}
}
