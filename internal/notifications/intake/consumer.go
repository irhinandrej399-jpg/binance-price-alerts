package intake

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type Config struct {
	Binding      Binding // operator expectation, never derived from current metadata
	Dialer       func(context.Context, string, string) (net.Conn, error)
	Brokers      []string
	Topic, Group string
}

// Consumer is used serially (Step/Run/Close). A failed Step retires it: construct
// a replacement, which restores SQL on assignment. No worker/Telegram is needed
// for intake acknowledgement. Callers own the stable environment group name.
type Consumer struct {
	client  *kgo.Client
	cancel  context.CancelFunc
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
	if err := cfg.Binding.Validate(); err != nil {
		return nil, err
	}
	if cfg.Binding.Source.Topic != cfg.Topic {
		return nil, kafkasource.ErrMismatch
	}
	clientCtx, cancel := context.WithCancel(context.Background())
	c := &Consumer{store: store, cfg: cfg, cursors: map[int32]int64{}, cancel: cancel}
	opts := []kgo.Opt{kgo.WithContext(clientCtx), kgo.SeedBrokers(cfg.Brokers...), kgo.ConsumerGroup(cfg.Group), kgo.ConsumeTopics(cfg.Topic),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.KeepControlRecords(), kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NoResetOffset()), kgo.KeepRetryableFetchErrors(),
		kgo.Balancers(kgo.RangeBalancer()), kgo.BlockRebalanceOnPoll(),
		kgo.AdjustFetchOffsetsFn(c.assign),
		kgo.FetchMaxWait(100 * time.Millisecond), kgo.FetchMaxBytes(1 << 20), kgo.FetchMaxPartitionBytes(1 << 20),
		kgo.MaxDecompressBatchBytes(1 << 20), kgo.BrokerMaxReadBytes(2 << 20), kgo.MaxConcurrentFetches(1),
		kgo.DialTimeout(time.Second), kgo.RequestTimeoutOverhead(time.Second), kgo.RequestRetries(2), kgo.RetryTimeout(2 * time.Second)}
	if cfg.Dialer != nil {
		opts = append(opts, kgo.Dialer(cfg.Dialer))
	}
	cl, e := kgo.NewClient(opts...)
	if e != nil {
		cancel()
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
// retained history is an error, never a reset. Only an explicitly bootstrapped
// SQL binding supplies positions; assignment performs no provisioning.
func (c *Consumer) assign(parent context.Context, offsets map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	if e := c.failure(); e != nil {
		return nil, e
	}
	if e := c.verify(ctx); e != nil {
		return nil, c.fail(e)
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
			progress, e := c.store.ReadProgress(ctx, c.cfg.Binding, part)
			if e != nil {
				return nil, c.fail(e)
			}
			if progress.Binding != c.cfg.Binding || progress.Partition != part {
				return nil, c.fail(kafkasource.ErrMismatch)
			}
			starts[part] = progress.Next
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
		if len(resp.Topics) != 1 || resp.Topics[0].Topic != c.cfg.Topic || len(resp.Topics[0].Partitions) != len(starts) {
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
	if e := c.verify(ctx); e != nil {
		return nil, c.fail(e)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cursors = starts
	for part, n := range starts {
		offsets[c.cfg.Topic][part] = kgo.NoResetOffset().At(n)
	}
	return offsets, nil
}

func (c *Consumer) verify(ctx context.Context) error {
	_, err := c.cfg.Binding.Source.Verify(ctx, c.client)
	return err
}

// Step consumes at most one record and can return nil after an idle poll or
// a control marker. SQL AcceptBound is the sole gate before ack. Kafka markers
// cause numeric gaps; ordered fetching establishes the delivered prefix.
func (c *Consumer) Step(ctx context.Context) error {
	if e := c.failure(); e != nil {
		return e
	}
	if e := c.verify(ctx); e != nil {
		return c.fail(e)
	}
	// A bounded poll also schedules metadata verification on an idle stream.
	poll, cancel := context.WithTimeout(ctx, time.Second)
	fs := c.client.PollRecords(poll, 1)
	timedOut := poll.Err() != nil
	cancel()
	defer c.client.AllowRebalance()
	if e := c.failure(); e != nil {
		return e
	}
	if e := ctx.Err(); e != nil {
		return c.fail(e)
	}
	if e := c.verify(ctx); e != nil {
		return c.fail(e)
	}
	if e := c.acceptFetches(ctx, fs, timedOut); e != nil {
		return c.fail(e)
	}
	return nil
}

// Inspect original responses before flattening/decode, even for control-only
// fetches. Markers are retained by the client for this check, then skipped.
func (c *Consumer) acceptFetches(ctx context.Context, fs kgo.Fetches, timedOut bool) error {
	if err := c.cfg.Binding.Source.CheckFetches(fs); err != nil {
		return err
	}
	for _, e := range fs.Errors() {
		if timedOut && e.Topic == "" && e.Partition == -1 && errors.Is(e.Err, context.DeadlineExceeded) {
			continue
		}
		return ErrPosition
	}
	for _, r := range fs.Records() {
		if r.Attrs.IsControl() {
			continue
		}
		if e := c.acceptAndAck(ctx, r); e != nil {
			return e
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
	if e := c.store.AcceptBound(sqlCtx, c.cfg.Binding, r.Partition, expected, r.Offset, r.Key, r.Value); e != nil {
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

// Close joins local work without waiting indefinitely for broker membership.
// A timed-out LeaveGroup may keep partitions assigned until session expiry;
// SQL checkpoints, rather than the leave acknowledgement, govern restart.
// Call only after Run/Step has returned.
func (c *Consumer) Close() {
	c.fail(errors.New("intake: closed"))
	c.client.AllowRebalance()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = c.client.LeaveGroupContext(ctx)
	cancel()
	// JoinGroup/SyncGroup use the client context, even after group cancellation.
	// Cancel it after the bounded leave so Close can join an inflight rebalance.
	c.cancel()
	c.client.Close()
}
