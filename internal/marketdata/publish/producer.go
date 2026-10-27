// Package publish commits one persisted PriceObserved to Kafka without changing
// its identity, revision or original wire bytes.
package publish

import (
	"context"
	"strings"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafka"
)

type transactionalPublisher interface {
	Publish(context.Context, ...kafka.Record) error
	Close()
}

// Producer owns one serial transactional Kafka producer. The caller assigns a
// stable environment/relay-shard transactional ID in cfg, and serializes all
// Publish/Close calls, including publications for the same pair.
type Producer struct {
	publisher transactionalPublisher
	topic     string
}

// New initializes the Kafka epoch and fences the previous shard owner before
// returning. ctx must have a finite deadline. topic is the environment's prices
// topic; its partition count and pair mapping must remain stable.
func New(ctx context.Context, cfg kafka.Config, topic string) (*Producer, error) {
	if strings.TrimSpace(topic) == "" {
		return nil, kafka.ErrConfig
	}
	p, err := kafka.New(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Producer{publisher: p, topic: topic}, nil
}

// Publish validates a complete persisted envelope, using its pair as Kafka key.
// It forwards the original bytes, preserving unknown additive fields too. Only
// nil confirms Kafka commit. Errors retain kafka.ErrFenced, ErrCommitUnknown,
// ErrRetired and context classifications; no SQL acknowledgement is performed.
// After a transaction error, close and replace this producer with the same
// transactional ID before retrying the original bytes. There is no retry loop.
func (p *Producer) Publish(ctx context.Context, payload []byte) error {
	_, observed, err := priceobserved.Decode(payload)
	if err != nil {
		return err
	}
	return p.publisher.Publish(ctx, kafka.Record{Topic: p.topic, Key: []byte(observed.Pair), Value: payload})
}

func (p *Producer) Close() { p.publisher.Close() }
