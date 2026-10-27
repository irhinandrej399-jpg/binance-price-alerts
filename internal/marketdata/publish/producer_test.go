package publish

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/storage"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafka"
)

// Deliberately noncanonical JSON: publication must preserve persisted bytes,
// including additive fields unknown to this version of the contract.
func observation() []byte {
	return []byte(`{ "event_id":"stable-event", "type":"price.observed", "version":1,
 "occurred_at":"2026-10-27T10:00:01Z", "correlation_id":"ingest-1", "ordering_key":"BTCUSDT",
 "extra":"preserve me", "payload":{"schema_version":1,"pair":"BTCUSDT",
 "price":"123.456789012345678901","source_id":"9007199254740993",
 "revision":{"stream_id":"binance-spot-trade","generation":2,"sequence":17},
 "trade_time":"2026-10-27T10:00:00.123456Z","received_at":"2026-10-27T10:00:01Z",
 "stream_state":"unconfirmed","gap_detected":true}}`)
}

type capture struct {
	records []kafka.Record
	err     error
}

func (c *capture) Publish(_ context.Context, rs ...kafka.Record) error {
	c.records = append(c.records, rs...)
	return c.err
}
func (*capture) Close() {}

func TestPublishPreservesOriginalRecordAndError(t *testing.T) {
	for _, outcome := range []error{nil, kafka.ErrPublish, kafka.ErrCommitUnknown, kafka.ErrFenced, context.DeadlineExceeded} {
		c := &capture{err: outcome}
		p := &Producer{publisher: c, topic: "test-prices"}
		original := observation()
		for range 2 {
			err := p.Publish(context.Background(), original)
			if !errors.Is(err, outcome) {
				t.Fatalf("outcome %v: %v", outcome, err)
			}
		}
		if len(c.records) != 2 {
			t.Fatal(c.records)
		}
		for _, r := range c.records {
			if r.Topic != "test-prices" || string(r.Key) != "BTCUSDT" || !bytes.Equal(r.Value, original) {
				t.Fatalf("changed record: %+v", r)
			}
		}
	}
}

func TestInvalidObservationNeverPublished(t *testing.T) {
	for _, payload := range [][]byte{
		nil, []byte(`{}`),
		bytes.Replace(observation(), []byte(`"ordering_key":"BTCUSDT"`), []byte(`"ordering_key":"ETHUSDT"`), 1),
		bytes.Replace(observation(), []byte(`"source_id":"9007199254740993"`), []byte(`"source_id":""`), 1),
		bytes.Replace(observation(), []byte(`"generation":2`), []byte(`"generation":0`), 1),
		bytes.Replace(observation(), []byte(`"version":1`), []byte(`"version":99`), 1),
	} {
		c := &capture{}
		p := &Producer{publisher: c, topic: "test-prices"}
		if err := p.Publish(context.Background(), payload); err == nil {
			t.Fatalf("accepted %s", payload)
		}
		if len(c.records) != 0 {
			t.Fatal("invalid event reached broker")
		}
	}
}

func TestEmptyTopic(t *testing.T) {
	if _, err := New(context.Background(), kafka.Config{}, ""); !errors.Is(err, kafka.ErrConfig) {
		t.Fatal(err)
	}
}

func TestEventSizeBoundary(t *testing.T) {
	c := &capture{}
	p := &Producer{publisher: c, topic: "test-prices"}
	payload := observation()
	payload = append(payload, bytes.Repeat([]byte(" "), 4096-len(payload))...)
	if err := p.Publish(context.Background(), payload); err != nil {
		t.Fatal(err)
	}
	if err := p.Publish(context.Background(), append(payload, ' ')); !errors.Is(err, storage.ErrTooLarge) {
		t.Fatalf("oversize: %v", err)
	}
	if len(c.records) != 1 {
		t.Fatal("oversize reached broker")
	}
}
