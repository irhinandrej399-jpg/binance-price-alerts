// Package kafka provides a serial transactional producer. It never logs configuration
// or payloads. Callers own the stable environment/owner/shard transactional ID.
package kafka

import (
	"context"
	"errors"
	"math"
	"net"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	ErrConfig        = errors.New("kafka: brokers and stable transactional ID required")
	ErrDeadline      = errors.New("kafka: finite context deadline required")
	ErrFenced        = errors.New("kafka: producer fenced; stop this owner")
	ErrCommitUnknown = errors.New("kafka: commit outcome unknown; retain original event identity")
	ErrRetired       = errors.New("kafka: producer retired; initialize a replacement")
	ErrPublish       = errors.New("kafka: publish failed before commit confirmation")
)

type Config struct {
	// SingleRecord bounds client buffers for a serial one-event outbox relay.
	SingleRecord    bool
	Brokers         []string
	TransactionalID string
	// Dialer permits TLS/authenticated transports or test fault injection. Nil uses TCP.
	Dialer func(context.Context, string, string) (net.Conn, error)
}

// Producer must be used serially, including Close. After any transaction error,
// close it and initialize a replacement with the same ID before retrying.
type Producer struct {
	client  *kgo.Client
	retired bool
}

type Record struct {
	Topic      string
	Key, Value []byte
}

func clientOptions(cfg Config) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...), kgo.TransactionalID(cfg.TransactionalID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Idempotence is enabled by default. With idempotence franz-go enforces <=5
		// in-flight requests; MaxProduceRequestsInflightPerBroker only applies when disabled.
		kgo.RecordRetries(math.MaxInt), kgo.RequestRetries(20),
		kgo.RetryTimeout(10 * time.Second), kgo.DialTimeout(3 * time.Second),
		kgo.RequestTimeoutOverhead(2 * time.Second), kgo.ProduceRequestTimeout(5 * time.Second),
		kgo.RecordDeliveryTimeout(10 * time.Second), kgo.TransactionTimeout(30 * time.Second),
		kgo.MaxBufferedRecords(1000), kgo.ProducerBatchCompression(kgo.NoCompression()),
	}
	if cfg.SingleRecord {
		opts = append(opts, kgo.MaxBufferedRecords(1), kgo.MaxBufferedBytes(8<<10), kgo.ProducerBatchMaxBytes(16<<10), kgo.BrokerMaxWriteBytes(64<<10), kgo.BrokerMaxReadBytes(1<<20), kgo.FetchMaxBytes(1<<20))
	}
	if cfg.Dialer != nil {
		opts = append(opts, kgo.Dialer(cfg.Dialer))
	}
	return opts
}

func deadline(ctx context.Context) error {
	if _, ok := ctx.Deadline(); !ok {
		return ErrDeadline
	}
	return ctx.Err()
}

// New initializes the producer ID/epoch on the broker, fencing the old owner
// before returning. A constructor alone (kgo.NewClient) would not do that.
func New(ctx context.Context, cfg Config) (*Producer, error) {
	if len(cfg.Brokers) == 0 || cfg.TransactionalID == "" {
		return nil, ErrConfig
	}
	for _, b := range cfg.Brokers {
		if b == "" {
			return nil, ErrConfig
		}
	}
	if err := deadline(ctx); err != nil {
		return nil, err
	}
	cl, err := kgo.NewClient(clientOptions(cfg)...)
	if err != nil {
		// Do not expose configuration in library errors.
		return nil, ErrConfig
	}
	if _, _, err = cl.ProducerID(ctx); err != nil {
		cl.Close()
		return nil, safeError(err, false)
	}
	return &Producer{client: cl}, nil
}

// Publish commits a nonempty batch in one Kafka transaction. Only nil proves a
// commit. An error never authorizes advancing a SQL outbox. Even a pre-commit
// failure requires a replacement producer to resolve any ongoing transaction.
func (p *Producer) Publish(ctx context.Context, records ...Record) error {
	if p.retired {
		return ErrRetired
	}
	if err := deadline(ctx); err != nil {
		return err
	}
	if len(records) == 0 {
		return ErrConfig
	}
	rs := make([]*kgo.Record, len(records))
	for i, r := range records {
		if r.Topic == "" {
			return ErrConfig
		}
		rs[i] = &kgo.Record{Topic: r.Topic, Key: r.Key, Value: r.Value}
	}
	if err := p.client.BeginTransaction(); err != nil {
		p.retired = true
		return safeError(err, false)
	}
	if err := p.client.ProduceSync(ctx, rs...).FirstErr(); err != nil {
		p.retired = true
		return safeError(err, false)
	}
	if err := p.client.EndTransaction(ctx, kgo.TryCommit); err != nil {
		p.retired = true
		return safeError(err, true)
	}
	return nil
}

func (p *Producer) Close() { p.retired = true; p.client.Close() }

func safeError(err error, committing bool) error {
	// Avoid propagating broker messages or configured addresses/credentials.
	if errors.Is(err, kerr.ProducerFenced) || errors.Is(err, kerr.InvalidProducerEpoch) {
		return ErrFenced
	}
	kind := ErrPublish
	if committing {
		kind = ErrCommitUnknown
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errors.Join(kind, context.DeadlineExceeded)
	}
	if errors.Is(err, context.Canceled) {
		return errors.Join(kind, context.Canceled)
	}
	return kind
}
