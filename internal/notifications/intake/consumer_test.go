package intake

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func kafkaFixture(t *testing.T) (Config, *kgo.Client, *Store) {
	t.Helper()
	if os.Getenv("KAFKA_INTEGRATION") != "1" || os.Getenv("INTAKE_TRANSPORT_TEST") != "1" {
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
	// check_postgres runs these tests in an isolated phase before shared writers.
	// The existing test-only owner fixture brackets each source-bound intake.
	reset := func() {
		t.Helper()
		if _, e := p.Exec(bounded(t), `SELECT app.test_reset_delivery_source()`); e != nil {
			t.Fatal(e)
		}
	}
	reset()
	t.Cleanup(reset)
	source, _, e := kafkasource.Metadata(bounded(t), producer, topic)
	if e != nil {
		t.Fatal(e)
	}
	cfg := Config{Brokers: []string{broker}, Topic: topic, Group: topic + "-intake", Binding: Binding{ConsumerID: "notifications", Generation: topic + "-bootstrap", Source: source}}
	s := NewStore(p)
	parts := make([]int32, 16)
	for i := range parts {
		parts[i] = int32(i)
	}
	if e := s.Bootstrap(bounded(t), cfg.Binding, parts); e != nil {
		t.Fatal(e)
	}
	return cfg, producer, s
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
	before := map[int32]int64{}
	for part := range int32(16) {
		p, e := c.store.ReadProgress(ctx, c.cfg.Binding, part)
		if e != nil {
			t.Fatal(e)
		}
		before[part] = p.Next
	}
	for {
		if e := c.Step(ctx); e != nil {
			return e
		}
		c.mu.Lock()
		advanced := false
		for p, n := range c.cursors {
			if n > before[p] {
				advanced = true
			}
		}
		c.mu.Unlock()
		if advanced {
			return nil
		}
	}
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
		n, e := nextOffset(ctx, s, cfg.Binding, 0)
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
	n, e := nextOffset(ctx, s, cfg.Binding, 0)
	if e != nil || n != r.Offset+1 {
		t.Fatal(n, e)
	}
	c2 := consumer(t, cfg, s)
	// Replayed logical request preserves the existing job.
	duplicate := produce(t, p, cfg, 0, wire(cfg.Topic), true)
	if e := step(t, c2); e != nil {
		t.Fatal(e)
	}
	if n, e = nextOffset(ctx, s, cfg.Binding, 0); e != nil || n != duplicate.Offset+1 {
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
	// An actual broker commit ahead of SQL cannot skip the next durable job.
	if e := c2.commit(ctx, &kgo.Record{Topic: cfg.Topic, Partition: 0, Offset: duplicate.Offset + 100}); e != nil {
		t.Fatal(e)
	}
	c2.Close()
	next := produce(t, p, cfg, 0, wire(cfg.Topic+"-after-ahead"), true)
	c3 := consumer(t, cfg, s)
	if e := step(t, c3); e != nil {
		t.Fatal(e)
	}
	if n, e := nextOffset(ctx, s, cfg.Binding, 0); e != nil || n != next.Offset+1 {
		t.Fatal(n, e)
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
	if n, e := nextOffset(bounded(t), s, cfg.Binding, 1); e != nil || n != 0 || ackCount != 0 {
		t.Fatal(n, e, ackCount, bad.Offset)
	}
	c.Close()
	cfg.Group += "-sql-failure"
	// Use another partition with valid schema but a deferred SQL commit failure.
	// Invalid partition 1 stops this consumer too, so the separate SQL failure is
	// exercised through acceptAndAck directly on a registered partition.
	c2 := consumer(t, cfg, s)
	c2.cursors[2] = 0
	c2.ack = func(context.Context, *kgo.Record) error { ackCount++; return nil }
	e := c2.acceptAndAck(bounded(t), &kgo.Record{Topic: cfg.Topic, Partition: 2, Offset: 0, Key: []byte("sub-1"), Value: wire("fail-commit")})
	if e == nil || ackCount != 0 {
		t.Fatal(e, ackCount)
	}
	if n, e := nextOffset(bounded(t), s, cfg.Binding, 2); e != nil || n != 0 {
		t.Fatal("commit failure advanced SQL", n, e)
	}
	if e := c2.acceptAndAck(bounded(t), &kgo.Record{Topic: cfg.Topic, Partition: 2, Offset: 0, Key: []byte("sub-1"), Value: wire(cfg.Topic + "-success")}); e != nil || ackCount != 1 {
		t.Fatal(e, ackCount)
	}
	stale := consumer(t, cfg, s)
	stale.cursors[2] = 0
	stale.ack = func(context.Context, *kgo.Record) error { ackCount++; return nil }
	if e := stale.acceptAndAck(bounded(t), &kgo.Record{Topic: cfg.Topic, Partition: 2, Offset: 1, Key: []byte("sub-1"), Value: wire(cfg.Topic + "-stale")}); !errors.Is(e, ErrCheckpoint) || ackCount != 1 {
		t.Fatal("CAS allowed ack", e, ackCount)
	}
	var n int
	if e := s.pool.QueryRow(bounded(t), `SELECT count(*) FROM app.delivery_jobs WHERE trigger_id IN ('fail-commit',$1)`, cfg.Topic+"-stale").Scan(&n); e != nil || n != 0 {
		t.Fatal(n, e)
	}
}

func nextOffset(ctx context.Context, s *Store, b Binding, part int32) (int64, error) {
	p, e := s.ReadProgress(ctx, b, part)
	return p.Next, e
}

func TestKafkaAssignmentProvenanceAndHistory(t *testing.T) {
	cfg, p, s := kafkaFixture(t)
	ctx := bounded(t)
	assign := func(c *Consumer) (map[string]map[int32]kgo.Offset, error) {
		return c.assign(ctx, map[string]map[int32]kgo.Offset{cfg.Topic: {0: kgo.NewOffset().At(999)}})
	}
	// Broker/member coordinates cannot override the persisted SQL position.
	c := consumer(t, cfg, s)
	offsets, e := assign(c)
	if e != nil || offsets[cfg.Topic][0].EpochOffset().Offset != 0 {
		t.Fatal(offsets, e)
	}
	for _, field := range []string{"consumer", "generation", "source"} {
		bad := cfg
		switch field {
		case "consumer":
			bad.Binding.ConsumerID += "-foreign"
		case "generation":
			bad.Binding.Generation += "-foreign"
		case "source":
			bad.Binding.Source.TopicID[0] ^= 1
		}
		b := consumer(t, bad, s)
		if _, e := assign(b); !errors.Is(e, kafkasource.ErrMismatch) {
			t.Fatal(field, e)
		}
	}
	// Unknown/legacy assignment is read-only; it must not provision next=0.
	reset := func() {
		t.Helper()
		if _, e := s.pool.Exec(ctx, `SELECT app.test_reset_delivery_source()`); e != nil {
			t.Fatal(e)
		}
	}
	reset()
	missing := consumer(t, cfg, s)
	if _, e := assign(missing); !errors.Is(e, ErrMissing) {
		t.Fatal(e)
	}
	var n int
	if e := s.pool.QueryRow(ctx, `SELECT count(*) FROM app.delivery_checkpoints`).Scan(&n); e != nil || n != 0 {
		t.Fatal(n, e)
	}
	if _, e := s.Checkpoint(ctx, cfg.Topic, 0); e != nil {
		t.Fatal(e)
	}
	legacy := consumer(t, cfg, s)
	if _, e := assign(legacy); !errors.Is(e, ErrUnbound) {
		t.Fatal(e)
	}
	reset()
	if e := s.Bootstrap(ctx, cfg.Binding, []int32{0}); e != nil {
		t.Fatal(e)
	}
	// A durable cursor cannot skip removed history, even if the broker offset is ahead.
	r := produce(t, p, cfg, 0, wire("retained"), true)
	req := kmsg.NewPtrDeleteRecordsRequest()
	req.Topics = []kmsg.DeleteRecordsRequestTopic{{Topic: cfg.Topic, Partitions: []kmsg.DeleteRecordsRequestTopicPartition{{Partition: 0, Offset: r.Offset + 1}}}}
	resp, e := req.RequestWith(ctx, p)
	if e != nil || len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 || resp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatal(resp, e)
	}
	lost := consumer(t, cfg, s)
	if _, e := assign(lost); !errors.Is(e, ErrPosition) {
		t.Fatal(e)
	}
	if n, e := nextOffset(ctx, s, cfg.Binding, 0); e != nil || n != 0 {
		t.Fatal(n, e)
	}
}

func TestKafkaSourceRecreatedOverlappingOffsets(t *testing.T) {
	cfg, p, s := kafkaFixture(t)
	produce(t, p, cfg, 0, wire(cfg.Topic), true)
	c := consumer(t, cfg, s)
	if e := step(t, c); e != nil {
		t.Fatal(e)
	}
	before, e := s.ReadProgress(bounded(t), cfg.Binding, 0)
	if e != nil {
		t.Fatal(e)
	}
	var body []byte
	if e := s.pool.QueryRow(bounded(t), `SELECT body FROM app.delivery_jobs WHERE trigger_id=$1`, cfg.Topic).Scan(&body); e != nil {
		t.Fatal(e)
	}
	del := kmsg.NewPtrDeleteTopicsRequest()
	del.TopicNames = []string{cfg.Topic}
	del.Topics = []kmsg.DeleteTopicsRequestTopic{{Topic: &cfg.Topic}}
	dr, e := del.RequestWith(bounded(t), p)
	if e != nil || len(dr.Topics) != 1 || dr.Topics[0].ErrorCode != 0 {
		t.Fatal(dr, e)
	}
	create := kmsg.NewPtrCreateTopicsRequest()
	spec := kmsg.NewCreateTopicsRequestTopic()
	spec.Topic = cfg.Topic
	spec.NumPartitions = 16
	spec.ReplicationFactor = 1
	create.Topics = []kmsg.CreateTopicsRequestTopic{spec}
	cr, e := create.RequestWith(bounded(t), p)
	if e != nil || len(cr.Topics) != 1 || cr.Topics[0].ErrorCode != 0 {
		t.Fatal(cr, e)
	}
	fresh, e := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.TransactionalID(cfg.Topic+"-replacement"), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if e != nil {
		t.Fatal(e)
	}
	defer fresh.Close()
	r := produce(t, fresh, cfg, 0, wire(cfg.Topic+"-foreign"), true)
	if r.Offset != 0 {
		t.Fatal("offsets do not overlap", r.Offset)
	}
	// Kafka may remove group offsets when deleting the topic. Compare with
	// the post-recreate broker baseline, independently of the durable SQL cursor.
	brokerBefore := brokerOffset(t, fresh, cfg)
	// Already open and fresh old-config consumers both retire before job/ack.
	acks := 0
	for _, v := range []*Consumer{c, consumer(t, cfg, s)} {
		v.ack = func(context.Context, *kgo.Record) error { acks++; return nil }
		if e := v.Step(bounded(t)); !errors.Is(e, kafkasource.ErrMismatch) {
			t.Fatal(e)
		}
	}
	if acks != 0 {
		t.Fatal("replacement acknowledged", acks)
	}
	// Relabeling configuration with today's source cannot relabel SQL provenance.
	replacement, _, e := kafkasource.Metadata(bounded(t), fresh, cfg.Topic)
	if e != nil || replacement == cfg.Binding.Source {
		t.Fatal(replacement, e)
	}
	newCfg := cfg
	newCfg.Binding.Source = replacement
	current := consumer(t, newCfg, s)
	if _, e := current.assign(bounded(t), map[string]map[int32]kgo.Offset{cfg.Topic: {0: kgo.NewOffset().AtStart()}}); !errors.Is(e, kafkasource.ErrMismatch) {
		t.Fatal(e)
	}
	after, e := s.ReadProgress(bounded(t), cfg.Binding, 0)
	if e != nil || after != before {
		t.Fatal(after, e)
	}
	var count int
	var same bool
	if e := s.pool.QueryRow(bounded(t), `SELECT count(*),bool_and(body=$2) FROM app.delivery_jobs WHERE trigger_id IN ($1,$3)`, cfg.Topic, body, cfg.Topic+"-foreign").Scan(&count, &same); e != nil || count != 1 || !same {
		t.Fatal(count, same, e)
	}
	if n := brokerOffset(t, fresh, cfg); n != brokerBefore {
		t.Fatal("replacement acknowledged", n)
	}
}

func TestKafkaRebalanceSameBinding(t *testing.T) {
	cfg, p, s := kafkaFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	first := consumer(t, cfg, s)
	done1 := make(chan error, 1)
	go func() { done1 <- first.Run(ctx) }()
	// Each Run is joined before Close, including failure paths.
	defer func() { cancel(); <-done1 }()
	wait := func(f func() bool) {
		t.Helper()
		for !f() {
			select {
			case <-ctx.Done():
				t.Fatal("rebalance/progress timeout")
			case <-time.After(25 * time.Millisecond):
			}
		}
	}
	assigned := func(c *Consumer, n int) bool { c.mu.Lock(); defer c.mu.Unlock(); return len(c.cursors) == n }
	wait(func() bool { return assigned(first, 16) })
	initial := produce(t, p, cfg, 15, wire(cfg.Topic+"-before-rebalance"), true)
	wait(func() bool { n, e := nextOffset(ctx, s, cfg.Binding, 15); return e == nil && n == initial.Offset+1 })
	second := consumer(t, cfg, s)
	done2 := make(chan error, 1)
	go func() { done2 <- second.Run(ctx) }()
	defer func() { cancel(); <-done2 }()
	wait(func() bool { return assigned(first, 8) && assigned(second, 8) })
	for _, c := range []*Consumer{first, second} {
		c.mu.Lock()
		n, owns := c.cursors[15]
		c.mu.Unlock()
		if owns && n != initial.Offset+1 {
			t.Fatal("rebalance did not restore SQL", n)
		}
	}
	for _, part := range []int32{0, 15} {
		r := produce(t, p, cfg, part, wire(fmt.Sprintf("%s-%d", cfg.Topic, part)), true)
		wait(func() bool { n, e := nextOffset(ctx, s, cfg.Binding, part); return e == nil && n == r.Offset+1 })
	}
	for _, part := range []int32{0, 15} {
		v, e := s.ReadProgress(ctx, cfg.Binding, part)
		if e != nil || v.Binding != cfg.Binding {
			t.Fatal(v, e)
		}
	}
}

func TestKafkaAssignmentMetadataBoundary(t *testing.T) {
	cfg, _, s := kafkaFixture(t)
	// Real SQL persisted provenance; a controlled wire changes metadata only
	// after ReadProgress/ListOffsets, before assignment can publish a cursor.
	if _, e := s.pool.Exec(bounded(t), `SELECT app.test_reset_delivery_source()`); e != nil {
		t.Fatal(e)
	}
	cfg.Topic = wireIdentity.Topic
	cfg.Binding.Source = wireIdentity
	if e := s.Bootstrap(bounded(t), cfg.Binding, []int32{0}); e != nil {
		t.Fatal(e)
	}
	for _, mode := range []string{"mismatch", "error"} {
		t.Run(mode, func(t *testing.T) {
			w := &generationWire{}
			if mode == "error" {
				w.failAfter = 2
			}
			defer w.close()
			cfg.Dialer = w.dial
			c := consumer(t, cfg, s)
			_, e := c.assign(bounded(t), map[string]map[int32]kgo.Offset{cfg.Topic: {0: kgo.NewOffset().At(100)}})
			want := kafkasource.ErrMismatch
			if mode == "error" {
				want = kafkasource.ErrMetadata
			}
			if !errors.Is(e, want) {
				t.Fatal(e)
			}
			if len(c.cursors) != 0 {
				t.Fatal("unverified assignment published", c.cursors)
			}
			if n, e := nextOffset(bounded(t), s, cfg.Binding, 0); e != nil || n != 0 {
				t.Fatal(n, e)
			}
		})
	}
}
