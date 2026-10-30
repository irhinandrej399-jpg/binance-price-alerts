package intake

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func kafkaFixture(t *testing.T) (Config, *kgo.Client, *Store) {
	t.Helper()
	if os.Getenv("KAFKA_INTEGRATION") != "1" {
		t.Skip("run KAFKA_RUN_POSTGRES=1 bash infra/kafka/verify-go.sh")
	}
	p := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
	broker, prefix := os.Getenv("KAFKA_TEST_BROKER"), os.Getenv("KAFKA_TEST_PREFIX")
	if broker == "" || prefix == "" {
		t.Fatal("missing opt-in Kafka configuration")
	}
	topic := prefix + "-" + strings.ToLower(t.Name())
	producer, e := kgo.NewClient(kgo.SeedBrokers(broker), kgo.TransactionalID(topic+"-writer"), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(producer.Close)
	req := kmsg.NewPtrCreateTopicsRequest()
	spec := kmsg.NewCreateTopicsRequestTopic()
	spec.Topic = topic
	spec.NumPartitions = 16
	spec.ReplicationFactor = 1
	req.Topics = []kmsg.CreateTopicsRequestTopic{spec}
	resp, e := req.RequestWith(bounded(t), producer)
	if e != nil {
		t.Fatal(e)
	}
	if len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != 0 {
		t.Fatal(resp)
	}
	return Config{Brokers: []string{broker}, Topic: topic, Group: topic + "-intake"}, producer, NewStore(p)
}
func produce(t *testing.T, p *kgo.Client, c Config, part int32, body []byte, commit bool) *kgo.Record {
	t.Helper()
	if e := p.BeginTransaction(); e != nil {
		t.Fatal(e)
	}
	r := &kgo.Record{Topic: c.Topic, Partition: part, Key: []byte("sub-1"), Value: body}
	if e := p.ProduceSync(bounded(t), r).FirstErr(); e != nil {
		t.Fatal(e)
	}
	how := kgo.TryAbort
	if commit {
		how = kgo.TryCommit
	}
	if e := p.EndTransaction(bounded(t), how); e != nil {
		t.Fatal(e)
	}
	return r
}
func consumer(t *testing.T, c Config, s *Store) *Consumer {
	t.Helper()
	v, e := New(c, s)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(v.Close)
	return v
}
func step(t *testing.T, c *Consumer) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return c.Step(ctx)
}
func brokerOffset(t *testing.T, p *kgo.Client, cfg Config) int64 {
	t.Helper()
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = cfg.Group
	topic := kmsg.NewOffsetFetchRequestTopic()
	topic.Topic = cfg.Topic
	topic.Partitions = []int32{0}
	req.Topics = []kmsg.OffsetFetchRequestTopic{topic}
	resp, e := req.RequestWith(bounded(t), p)
	if e != nil || len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 || resp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("offset fetch: %+v %v", resp, e)
	}
	return resp.Topics[0].Partitions[0].Offset
}
func TestKafkaDurableBeforeAckAndRestart(t *testing.T) {
	cfg, p, s := kafkaFixture(t)
	ctx := bounded(t)
	// An aborted request must never reach SQL. Include transactional offset gaps.
	produce(t, p, cfg, 0, wire(cfg.Topic+"-aborted"), false)
	r := produce(t, p, cfg, 0, wire(cfg.Topic), true)
	c := consumer(t, cfg, s)
	c.ack = func(ctx context.Context, r *kgo.Record) error {
		n, e := s.Checkpoint(ctx, cfg.Topic, 0)
		if e != nil || n != r.Offset+1 {
			t.Errorf("ack before SQL: %d %v", n, e)
		}
		return ErrAck // model process failure after SQL commit before broker commit
	}
	if e := step(t, c); !errors.Is(e, ErrAck) {
		t.Fatal(e)
	}
	if n := brokerOffset(t, p, cfg); n != -1 {
		t.Fatal("implicit broker commit", n)
	}
	c.Close()
	n, e := s.Checkpoint(ctx, cfg.Topic, 0)
	if e != nil || n != r.Offset+1 {
		t.Fatal(n, e)
	}
	c2 := consumer(t, cfg, s)
	// Replayed logical request preserves the existing job.
	duplicate := produce(t, p, cfg, 0, wire(cfg.Topic), true)
	if e := step(t, c2); e != nil {
		t.Fatal(e)
	}
	if n, e = s.Checkpoint(ctx, cfg.Topic, 0); e != nil || n != duplicate.Offset+1 {
		t.Fatal(n, e)
	}
	var count int
	if e = s.pool.QueryRow(ctx, "SELECT count(*) FROM app.delivery_jobs WHERE trigger_id IN ($1,$2)", cfg.Topic, cfg.Topic+"-aborted").Scan(&count); e != nil || count != 1 {
		t.Fatal(count, e)
	}
	// Successful Step really commits the broker offset, but SQL remains authority.
	if n := brokerOffset(t, p, cfg); n != duplicate.Offset+1 {
		t.Fatal("missing broker commit", n)
	}
}
func TestKafkaInvalidStopsAndCommitFailureNoAck(t *testing.T) {
	cfg, p, s := kafkaFixture(t)
	bad := produce(t, p, cfg, 1, []byte(`{"version":99}`), true)
	produce(t, p, cfg, 1, wire(cfg.Topic), true)
	c := consumer(t, cfg, s)
	ackCount := 0
	c.ack = func(context.Context, *kgo.Record) error { ackCount++; return nil }
	if e := step(t, c); !errors.Is(e, ErrContract) {
		t.Fatal(e)
	}
	if e := step(t, c); !errors.Is(e, ErrContract) {
		t.Fatal(e)
	}
	if n, e := s.Checkpoint(bounded(t), cfg.Topic, 1); e != nil || n != 0 || ackCount != 0 {
		t.Fatal(n, e, ackCount, bad.Offset)
	}
	c.Close()
	cfg.Group += "-sql-failure"
	// Use another partition with valid schema but a deferred SQL commit failure.
	// Invalid partition 1 stops this consumer too, so the separate SQL failure is
	// exercised through acceptAndAck directly on a registered partition.
	if _, e := s.Checkpoint(bounded(t), cfg.Topic, 2); e != nil {
		t.Fatal(e)
	}
	c2 := consumer(t, cfg, s)
	c2.cursors[2] = 0
	c2.ack = func(context.Context, *kgo.Record) error { ackCount++; return nil }
	e := c2.acceptAndAck(bounded(t), &kgo.Record{Topic: cfg.Topic, Partition: 2, Offset: 0, Key: []byte("sub-1"), Value: wire("fail-commit")})
	if e == nil || ackCount != 0 {
		t.Fatal(e, ackCount)
	}
}
