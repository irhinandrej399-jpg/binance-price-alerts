package intake

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type Config struct {
	Brokers      []string
	Topic, Group string
}

// Consumer is used serially (Step/Run/Close). A failed Step retires it: construct
// a replacement, which restores SQL on assignment. No worker/Telegram is needed
// for intake acknowledgement. Callers own the stable environment group name.
type Consumer struct {
	client  *kgo.Client
	store   *Store
	cfg     Config
	mu      sync.Mutex
	cursors map[int32]int64
	failed  error
	ack     func(context.Context, *kgo.Record) error
}

func New(cfg Config, store *Store) (*Consumer, error) {
	if store == nil || store.pool == nil || len(cfg.Brokers) == 0 || strings.TrimSpace(cfg.Topic) == "" || strings.TrimSpace(cfg.Group) == "" {
		return nil, ErrConfig
	}
	for _, b := range cfg.Brokers {
		if strings.TrimSpace(b) == "" {
			return nil, ErrConfig
		}
	}
	c := &Consumer{store: store, cfg: cfg, cursors: map[int32]int64{}}
	cl, e := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.ConsumerGroup(cfg.Group), kgo.ConsumeTopics(cfg.Topic),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NoResetOffset()), kgo.KeepRetryableFetchErrors(),
		kgo.Balancers(kgo.RangeBalancer()), kgo.BlockRebalanceOnPoll(),
		kgo.AdjustFetchOffsetsFn(c.assign),
		kgo.FetchMaxWait(100*time.Millisecond), kgo.FetchMaxBytes(1<<20), kgo.FetchMaxPartitionBytes(1<<20),
		kgo.MaxDecompressBatchBytes(1<<20), kgo.BrokerMaxReadBytes(2<<20), kgo.MaxConcurrentFetches(1),
		kgo.DialTimeout(time.Second), kgo.RequestTimeoutOverhead(time.Second), kgo.RequestRetries(2), kgo.RetryTimeout(2*time.Second))
	if e != nil {
		return nil, ErrConfig
	}
	c.client = cl
	c.ack = c.commit
	return c, nil
}
func (c *Consumer) fail(err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed == nil {
		c.failed = err
	}
	return c.failed
}
func (c *Consumer) failure() error { c.mu.Lock(); defer c.mu.Unlock(); return c.failed }

// Assignment ignores broker offsets, including offsets ahead of SQL. Missing
// retained history is an error, never a reset. Register all assigned partitions
// before fetching; each starts at zero on the first ever assignment.
func (c *Consumer) assign(parent context.Context, offsets map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	if e := c.failure(); e != nil {
		return nil, e
	}
	starts := map[int32]int64{}
	for topic, ps := range offsets {
		if topic != c.cfg.Topic {
			return nil, c.fail(ErrConfig)
		}
		for part := range ps {
			if part < 0 || part >= 16 {
				return nil, c.fail(ErrConfig)
			}
			n, e := c.store.Checkpoint(ctx, topic, part)
			if e != nil {
				return nil, c.fail(e)
			}
			starts[part] = n
		}
	}
	for _, timestamp := range []int64{-2, -1} {
		req := kmsg.NewPtrListOffsetsRequest()
		req.IsolationLevel = 1
		topic := kmsg.NewListOffsetsRequestTopic()
		topic.Topic = c.cfg.Topic
		for part := range starts {
			p := kmsg.NewListOffsetsRequestTopicPartition()
			p.Partition = part
			p.Timestamp = timestamp
			topic.Partitions = append(topic.Partitions, p)
		}
		if len(starts) == 0 {
			break
		}
		req.Topics = []kmsg.ListOffsetsRequestTopic{topic}
		resp, e := req.RequestWith(ctx, c.client)
		if e != nil {
			return nil, c.fail(ErrPosition)
		}
		if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != len(starts) {
			return nil, c.fail(ErrPosition)
		}
		seen := map[int32]bool{}
		for _, p := range resp.Topics[0].Partitions {
			n, ok := starts[p.Partition]
			if !ok || seen[p.Partition] || p.ErrorCode != 0 || p.Offset < 0 || (timestamp == -2 && n < p.Offset) || (timestamp == -1 && n > p.Offset) {
				return nil, c.fail(ErrPosition)
			}
			seen[p.Partition] = true
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cursors = starts
	for part, n := range starts {
		offsets[c.cfg.Topic][part] = kgo.NoResetOffset().At(n)
	}
	return offsets, nil
}

// Step consumes at most one record. SQL Accept is the sole gate before ack.
// Kafka's hidden abort/control records may cause numeric offset gaps: PollRecords
// ordering, rather than offset+1 equality, establishes the delivered prefix.
func (c *Consumer) Step(ctx context.Context) error {
	if e := c.failure(); e != nil {
		return e
	}
	fs := c.client.PollRecords(ctx, 1)
	defer c.client.AllowRebalance()
	if e := c.failure(); e != nil {
		return e
	}
	if e := ctx.Err(); e != nil {
		return c.fail(e)
	}
	if len(fs.Errors()) > 0 {
		return c.fail(ErrPosition)
	}
	for _, r := range fs.Records() {
		if e := c.acceptAndAck(ctx, r); e != nil {
			return c.fail(e)
		}
	}
	return nil
}
func (c *Consumer) acceptAndAck(ctx context.Context, r *kgo.Record) error {
	c.mu.Lock()
	expected, ok := c.cursors[r.Partition]
	c.mu.Unlock()
	if !ok || r.Topic != c.cfg.Topic {
		return ErrPosition
	}
	if len(r.Value) > 8<<10 || len(r.Key) > 256 {
		return ErrContract
	}
	sqlCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if e := c.store.Accept(sqlCtx, r.Topic, r.Partition, expected, r.Offset, r.Key, r.Value); e != nil {
		// Include only transport coordinates, never payload/recipient/configuration.
		return fmt.Errorf("intake partition %d offset %d: %w", r.Partition, r.Offset, e)
	}
	c.mu.Lock()
	c.cursors[r.Partition] = r.Offset + 1
	c.mu.Unlock()
	return c.ack(ctx, r)
}
func (c *Consumer) commit(parent context.Context, r *kgo.Record) error {
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	var result error
	c.client.CommitOffsetsSync(ctx, map[string]map[int32]kgo.EpochOffset{r.Topic: {r.Partition: {Epoch: -1, Offset: r.Offset + 1}}}, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, e error) {
		if e != nil || resp == nil || len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 || resp.Topics[0].Partitions[0].ErrorCode != 0 {
			result = ErrAck
		}
	})
	return result
}
func (c *Consumer) Run(ctx context.Context) error {
	for {
		if e := c.Step(ctx); e != nil {
			return e
		}
		if e := ctx.Err(); e != nil {
			return e
		}
	}
}
func (c *Consumer) Close() { c.fail(errors.New("intake: closed")); c.client.CloseAllowingRebalance() }
