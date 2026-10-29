// Package pricefeed provides bounded read_committed price streams. Direct
// sources never commit offsets; group assignments require explicit durable
// confirmation. Neither adapter decides whether a quote is current.
package pricefeed

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var (
	ErrConfig     = errors.New("pricefeed: invalid configuration or registration")
	ErrContinuity = errors.New("pricefeed: continuity lost; bootstrap required")
	ErrOverflow   = errors.New("pricefeed: buffer exhausted")
	ErrContract   = errors.New("pricefeed: invalid price event")
	ErrClosed     = errors.New("pricefeed: closed")
	ErrRevoked    = errors.New("pricefeed: assignment revoked")
	ErrPosition   = errors.New("pricefeed: position unavailable")
)

const MaxBuffer = 512
const maxPayload = 4096

// Partitions is a fixed, externally owned assignment (at most 16 partitions).
// Buffer is a source-wide record limit, 1..512; zero selects 256.
// Dialer supports the existing disposable fault proxy.
type Config struct {
	Brokers    []string
	Topic      string
	Partitions []int32
	Buffer     int
	Dialer     func(context.Context, string, string) (net.Conn, error)
}

type Position struct {
	Topic     string
	Partition int32
	Offset    int64
}
type Update struct {
	Position Position
	Key      string
	Envelope event.Envelope
	Quote    priceobserved.Observed
}
type queued struct {
	position Position
	key      string
	payload  []byte
}

type Source struct {
	mu              sync.Mutex
	client          *kgo.Client
	topic           string
	assigned        map[int32]bool
	regs            map[string]*Registration
	limit, buffered int
	err             error
	ctx             context.Context
	cancel          context.CancelFunc
	done            chan struct{}
	closeOnce       sync.Once
	groupAssignment *Assignment // non-nil only for the single group reader
}

type Registration struct {
	source     *Source
	pair       string
	start      Position
	queue      []queued
	changed    chan struct{}
	terminated chan struct{}
	err        error
	stop       func() bool
}

func New(parent context.Context, cfg Config) (*Source, error) {
	if len(cfg.Brokers) == 0 || cfg.Topic == "" || len(cfg.Partitions) == 0 || len(cfg.Partitions) > 16 || cfg.Buffer < 0 || cfg.Buffer > MaxBuffer {
		return nil, ErrConfig
	}
	assigned := make(map[int32]bool)
	for _, p := range cfg.Partitions {
		if p < 0 || p >= 16 || assigned[p] {
			return nil, ErrConfig
		}
		assigned[p] = true
	}
	if cfg.Buffer == 0 {
		cfg.Buffer = 256
	}
	ctx, cancel := context.WithCancel(parent)
	c, err := kgo.NewClient(clientOptions(ctx, cfg)...)
	if err != nil {
		cancel()
		return nil, ErrConfig
	}
	s := &Source{client: c, topic: cfg.Topic, assigned: assigned, regs: make(map[string]*Registration), limit: cfg.Buffer, ctx: ctx, cancel: cancel, done: make(chan struct{})}
	offsets, err := s.positions(ctx, cfg.Partitions)
	if err != nil {
		cancel()
		c.Close()
		return nil, err
	}
	start := make(map[int32]kgo.Offset)
	for p, o := range offsets {
		start[p] = kgo.NoResetOffset().At(o)
	}
	c.AddConsumePartitions(map[string]map[int32]kgo.Offset{cfg.Topic: start})
	go s.read()
	return s, nil
}

// Direct assignment has no group and therefore no auto-commit mechanism.
// franz-go rejects DisableAutoCommit without a ConsumerGroup; never add a
// group just to enable that option.
func clientOptions(ctx context.Context, cfg Config) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...), kgo.WithContext(ctx),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.ConsumeResetOffset(kgo.NoResetOffset()), kgo.KeepRetryableFetchErrors(),
		kgo.FetchMaxWait(100 * time.Millisecond), kgo.FetchMaxBytes(64 << 10), kgo.FetchMaxPartitionBytes(64 << 10),
		kgo.BrokerMaxReadBytes(1 << 20), kgo.MaxConcurrentFetches(1), kgo.MaxDecompressBatchBytes(64 << 10),
		kgo.DialTimeout(time.Second), kgo.RequestTimeoutOverhead(time.Second), kgo.RequestRetries(2), kgo.RetryTimeout(2 * time.Second),
	}
	if cfg.Dialer != nil {
		opts = append(opts, kgo.Dialer(cfg.Dialer))
	}
	return opts
}

// positions resolves the current last stable offset, including any open
// transaction's future records. Never use the high watermark in its place.
func (s *Source) positions(parent context.Context, partitions []int32) (map[int32]int64, error) {
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	req := kmsg.NewPtrListOffsetsRequest()
	req.IsolationLevel = 1
	topic := kmsg.NewListOffsetsRequestTopic()
	topic.Topic = s.topic
	for _, p := range partitions {
		v := kmsg.NewListOffsetsRequestTopicPartition()
		v.Partition = p
		v.Timestamp = -1
		topic.Partitions = append(topic.Partitions, v)
	}
	req.Topics = append(req.Topics, topic)
	resp, err := req.RequestWith(ctx, s.client)
	if err != nil {
		return nil, errors.Join(ErrContinuity, ErrPosition, ctx.Err())
	}
	result := make(map[int32]int64)
	for _, t := range resp.Topics {
		if t.Topic != s.topic {
			continue
		}
		for _, p := range t.Partitions {
			if p.ErrorCode != 0 || p.Offset < 0 {
				return nil, errors.Join(ErrContinuity, ErrPosition)
			}
			result[p.Partition] = p.Offset
		}
	}
	if len(result) != len(partitions) {
		return nil, errors.Join(ErrContinuity, ErrPosition)
	}
	return result, nil
}

// Register establishes the broker barrier while holding the dispatch lock.
// Any record dispatched before the barrier is below it. Records fetched during
// the request wait for installation and are filtered by the absolute cursor.
// ctx owns the registration lifetime; cancellation releases all its buffers.
func (s *Source) Register(ctx context.Context, pair string, partition int32) (*Registration, error) {
	if pair == "" || len(pair) > 64 {
		return nil, ErrConfig
	}
	for _, r := range pair {
		if !(r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
			return nil, ErrConfig
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.groupAssignment != nil && s.ctx.Err() != nil && s.err == nil {
		s.failLocked(s.ctx.Err())
	}
	if s.err != nil {
		return nil, s.err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !s.assigned[partition] || s.regs[pair] != nil || len(s.regs) >= 1024 {
		return nil, ErrConfig
	}
	offsets, err := s.positions(ctx, []int32{partition})
	if err != nil {
		s.failLocked(err)
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	reg := &Registration{source: s, pair: pair, start: Position{s.topic, partition, offsets[partition]}, changed: make(chan struct{}), terminated: make(chan struct{})}
	s.regs[pair] = reg
	reg.stop = context.AfterFunc(ctx, func() { reg.close(ctx.Err()) })
	return reg, nil
}

// Start is the immutable inclusive registration barrier, not a durable ack.
func (r *Registration) Start() Position { return r.start }

// Done closes on cancellation, explicit close, or any loss of continuity.
// It must be observed while MAT-012 is obtaining and merging Latest.
func (r *Registration) Done() <-chan struct{} { return r.terminated }
func (r *Registration) Err() error            { r.source.mu.Lock(); defer r.source.mu.Unlock(); return r.err }
func (r *Registration) wake()                 { close(r.changed); r.changed = make(chan struct{}) }
func (r *Registration) close(err error) {
	s := r.source
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.err != nil {
		return
	}
	r.err = err
	close(r.terminated)
	s.buffered -= len(r.queue)
	r.queue = nil
	delete(s.regs, r.pair)
	if r.stop != nil {
		r.stop()
	}
	r.wake()
}
func (r *Registration) Close() { r.close(ErrClosed) }

// Next never acknowledges a durable price. A failed registration rejects even
// previously queued records. Cancelling this wait alone leaves the registration;
// cancelling Register's ctx or Close removes it.
func (r *Registration) Next(ctx context.Context) (Update, error) {
	s := r.source
	for {
		s.mu.Lock()
		if s.groupAssignment != nil && s.ctx.Err() != nil && s.err == nil {
			s.failLocked(s.ctx.Err())
		}
		if r.err != nil {
			err := r.err
			s.mu.Unlock()
			return Update{}, err
		}
		if err := ctx.Err(); err != nil {
			s.mu.Unlock()
			return Update{}, err
		}
		if len(r.queue) > 0 {
			u, err := r.popLocked()
			s.mu.Unlock()
			return u, err
		}
		changed := r.changed
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return Update{}, ctx.Err()
		case <-changed:
		}
	}
}

// TryNext removes one already buffered update without waiting. Failure takes
// priority over buffered work, just as in Next. It never acknowledges offsets.
func (r *Registration) TryNext() (Update, bool, error) {
	s := r.source
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.groupAssignment != nil && s.ctx.Err() != nil && s.err == nil {
		s.failLocked(s.ctx.Err())
	}
	if r.err != nil {
		return Update{}, false, r.err
	}
	if len(r.queue) == 0 {
		return Update{}, false, nil
	}
	u, err := r.popLocked()
	return u, err == nil, err
}

func (r *Registration) popLocked() (Update, error) {
	q := r.queue[0]
	r.queue[0] = queued{}
	r.queue = r.queue[1:]
	if a := r.source.groupAssignment; a != nil && r == a.all {
		a.delivered[q.position.Partition] = append(a.delivered[q.position.Partition], q.position)
	} else {
		r.source.buffered--
	}
	meta, quote, err := priceobserved.Decode(q.payload)
	if err != nil {
		return Update{}, ErrContract
	} // validated before queueing
	return Update{q.position, q.key, meta, quote}, nil
}

func (s *Source) failLocked(err error) {
	if s.err != nil {
		return
	}
	s.err = errors.Join(ErrContinuity, err)
	for _, r := range s.regs {
		r.err = s.err
		close(r.terminated)
		r.queue = nil
		if r.stop != nil {
			r.stop()
		}
		r.wake()
	}
	s.regs = make(map[string]*Registration)
	s.buffered = 0
	if s.groupAssignment != nil {
		s.groupAssignment.delivered = nil
	}
	s.cancel()
}
func (s *Source) fail(err error) { s.mu.Lock(); defer s.mu.Unlock(); s.failLocked(err) }

// Revoke must be called by the external assignment owner before losing any
// partition. This conservative adapter invalidates the entire fixed source.
func (s *Source) Revoke() { s.fail(ErrRevoked); s.Close() }
func (s *Source) Close()  { s.closeOnce.Do(func() { s.fail(ErrClosed); <-s.done }) }

func (s *Source) read() {
	defer close(s.done)
	defer s.client.Close()
	for {
		poll, cancel := context.WithTimeout(s.ctx, time.Second)
		fs := s.client.PollRecords(poll, 1)
		timedOut := poll.Err() != nil
		cancel()
		if s.ctx.Err() != nil {
			s.fail(s.ctx.Err())
			return
		}
		for _, e := range fs.Errors() {
			if timedOut && errors.Is(e.Err, context.DeadlineExceeded) {
				continue
			}
			s.fail(ErrPosition)
			return
		}
		for _, r := range fs.Records() {
			if !s.dispatch(r) {
				return
			}
		}
		if timedOut {
			partitions := make([]int32, 0, len(s.assigned))
			for p := range s.assigned {
				partitions = append(partitions, p)
			}
			if _, err := s.positions(s.ctx, partitions); err != nil {
				s.fail(err)
				return
			}
		}
	}
}
func (s *Source) dispatch(r *kgo.Record) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.groupAssignment != nil && s.ctx.Err() != nil && s.err == nil {
		s.failLocked(s.ctx.Err())
	}
	if s.err != nil {
		return false
	}
	if r.Topic != s.topic || !s.assigned[r.Partition] || r.Offset < 0 {
		s.failLocked(ErrPosition)
		return false
	}
	if len(r.Value) > maxPayload || len(r.Key) > 64 {
		s.failLocked(ErrContract)
		return false
	}
	_, quote, err := priceobserved.Decode(r.Value)
	if err != nil || string(r.Key) != quote.Pair {
		s.failLocked(ErrContract)
		return false
	}
	if a := s.groupAssignment; a != nil {
		if PairPartition(quote.Pair) != r.Partition {
			s.failLocked(ErrContract)
			return false
		}
		if s.buffered >= s.limit {
			s.failLocked(ErrOverflow)
			return false
		}
		a.all.queue = append(a.all.queue, queued{Position{r.Topic, r.Partition, r.Offset}, string(r.Key), append([]byte(nil), r.Value...)})
		s.buffered++
		a.all.wake()
	}
	reg := s.regs[quote.Pair]
	if reg == nil {
		return true
	}
	if reg.start.Partition != r.Partition {
		s.failLocked(ErrContract)
		return false
	}
	if r.Offset < reg.start.Offset {
		return true
	}
	if s.buffered >= s.limit {
		s.failLocked(ErrOverflow)
		return false
	}
	reg.queue = append(reg.queue, queued{Position{r.Topic, r.Partition, r.Offset}, string(r.Key), append([]byte(nil), r.Value...)})
	s.buffered++
	reg.wake()
	return true
}
