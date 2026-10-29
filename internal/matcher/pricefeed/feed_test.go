package pricefeed

import (
	"context"
	"errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"testing"
)

func TestInvalidConfig(t *testing.T) {
	for _, cfg := range []Config{{}, {Brokers: []string{"localhost:1"}, Topic: "prices", Partitions: []int32{0, 0}}, {Brokers: []string{"localhost:1"}, Topic: "prices", Partitions: []int32{0}, Buffer: 513}} {
		if _, err := New(context.Background(), cfg); !errors.Is(err, ErrConfig) {
			t.Fatalf("invalid config: %v", err)
		}
	}
}

func fixture() []byte {
	return []byte(`{"event_id":"original-event","type":"price.observed","version":1,"occurred_at":"2026-10-29T10:00:01Z","correlation_id":"ingest-1","ordering_key":"BTCUSDT","payload":{"schema_version":1,"pair":"BTCUSDT","price":"123.456789012345678901","source_id":"9007199254740993","revision":{"stream_id":"binance-spot-trade","generation":2,"sequence":17},"trade_time":"2026-10-29T10:00:00.123456Z","received_at":"2026-10-29T10:00:01Z","stream_state":"live","gap_detected":false}}`)
}

func TestDirectClientOptions(t *testing.T) {
	c, err := kgo.NewClient(clientOptions(context.Background(), Config{Brokers: []string{"localhost:1"}})...)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.OptValue(kgo.ConsumerGroup) != "" {
		t.Fatal("direct feed must never join or auto-commit to a group")
	}
}

func TestTryNextDrainsAndFailureWins(t *testing.T) {
	s := &Source{regs: make(map[string]*Registration)}
	r := &Registration{source: s, changed: make(chan struct{}), terminated: make(chan struct{})}
	s.regs["BTCUSDT"] = r
	if _, ok, err := r.TryNext(); ok || err != nil {
		t.Fatalf("empty: %v %v", ok, err)
	}
	r.queue = []queued{{Position{"prices", 0, 42}, "BTCUSDT", fixture()}}
	s.buffered = 1
	u, ok, err := r.TryNext()
	if err != nil || !ok || u.Position.Offset != 42 || u.Envelope.EventID != "original-event" || s.buffered != 0 {
		t.Fatalf("drain: %+v %v %v", u, ok, err)
	}
	r.queue = []queued{{payload: fixture()}}
	s.buffered = 1
	r.Close()
	if _, ok, err = r.TryNext(); ok || !errors.Is(err, ErrClosed) {
		t.Fatalf("failure: %v %v", ok, err)
	}
}
