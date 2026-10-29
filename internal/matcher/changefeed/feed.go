// Package changefeed reads bounded, whole API journal transactions from Kafka.
// It does not infer journal currency or commit consumer group offsets.
package changefeed

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/subscriptionchanged"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var (
	ErrConfig       = errors.New("changefeed: invalid configuration")
	ErrContinuity   = errors.New("changefeed: registration lost; replay required")
	ErrPosition     = errors.New("changefeed: position unavailable")
	ErrContract     = errors.New("changefeed: invalid event")
	ErrBoundary     = errors.New("changefeed: invalid transaction boundary")
	ErrOverflow     = errors.New("changefeed: buffer limit exceeded")
	ErrConfirmation = errors.New("changefeed: confirmation is not the delivered prefix")
	ErrClosed       = errors.New("changefeed: closed")
	ErrRevoked      = errors.New("changefeed: assignment revoked")
)

const MaxBuffer = 1024
const MaxPayload = 4096

type Config struct {
	Brokers []string
	Topic   string
	Buffer  int // records including assembly, queue and the unconfirmed delivery; zero = 1024
	Dialer  func(context.Context, string, string) (net.Conn, error)
}
type Position struct {
	Topic     string
	Partition int32
	Offset    int64
}
type Record struct {
	Position Position
	Key      string
	JSON     []byte
	Envelope event.Envelope
	Change   subscriptionchanged.Changed
}
type Transaction struct {
	First, C   uint64
	Records    []Record
	NextOffset int64 // includes the Kafka commit marker
}
type delivery struct {
	nextOffset int64
	count      int
}

type Source struct {
	mu         sync.Mutex
	client     *kgo.Client
	cfg        Config
	ctx        context.Context
	cancel     context.CancelFunc
	reg        *Registration
	closed     bool
	readerDone chan struct{}
}
type Registration struct {
	mu              sync.Mutex
	topic           string
	start           Position
	limit, buffered int
	pending         []Record
	producer        int64
	epoch           int16
	queue           []Transaction
	inflight        *delivery
	confirmed       int64
	lastOffset      int64
	changed, done   chan struct{}
	err             error
	ctx             context.Context
	cancel          context.CancelFunc
}

func New(parent context.Context, cfg Config) (*Source, error) {
	if len(cfg.Brokers) == 0 || cfg.Topic == "" || cfg.Buffer < 0 || cfg.Buffer > MaxBuffer {
		return nil, ErrConfig
	}
	if cfg.Buffer == 0 {
		cfg.Buffer = MaxBuffer
	}
	ctx, cancel := context.WithCancel(parent)
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...), kgo.WithContext(ctx),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.KeepControlRecords(),
		kgo.ConsumeResetOffset(kgo.NoResetOffset()), kgo.KeepRetryableFetchErrors(),
		kgo.FetchMaxWait(100 * time.Millisecond),
		kgo.FetchMaxBytes(4 << 20), kgo.FetchMaxPartitionBytes(4 << 20),
		kgo.MaxDecompressBatchBytes(4 << 20), kgo.BrokerMaxReadBytes(8 << 20),
		kgo.MaxConcurrentFetches(1), kgo.DialTimeout(time.Second),
		kgo.RequestTimeoutOverhead(time.Second), kgo.RequestRetries(2),
		kgo.RetryTimeout(2 * time.Second),
	}
	if cfg.Dialer != nil {
		opts = append(opts, kgo.Dialer(cfg.Dialer))
	}
	c, err := kgo.NewClient(opts...)
	if err != nil {
		cancel()
		return nil, ErrConfig
	}
	return &Source{client: c, cfg: cfg, ctx: ctx, cancel: cancel}, nil
}

// Register can be called once. Its context owns registration validity, even on
// an idle topic. Call before BeginReplay fixes H. No records are read before LSO.
func (s *Source) Register(parent context.Context) (*Registration, error) {
	return s.register(parent, nil)
}

// RegisterAt resumes the exact SQL NextOffset, never a broker group offset.
// Missing/out-of-range positions fail explicitly; there is no reset to latest.
// Use only a whole-transaction boundary produced by this source and stored with
// the projection. As with Register, this must precede SQL BeginReplay/H.
func (s *Source) RegisterAt(parent context.Context, position Position) (*Registration, error) {
	if position.Topic != s.cfg.Topic || position.Partition != 0 || position.Offset < 0 {
		return nil, ErrPosition
	}
	return s.register(parent, &position)
}
func (s *Source) register(parent context.Context, resume *Position) (*Registration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.reg != nil {
		return nil, ErrConfig
	}
	ctx, cancel := context.WithCancel(s.ctx)
	stop := context.AfterFunc(parent, cancel)
	ok := false
	defer func() {
		if !ok {
			stop()
			cancel()
		}
	}()
	if err := parent.Err(); err != nil {
		return nil, err
	}
	if err := s.singlePartition(ctx); err != nil {
		return nil, err
	}
	offset, err := s.position(ctx)
	if err != nil {
		return nil, err
	}
	if resume != nil {
		earliest, e := s.offsetAt(ctx, -2)
		if e != nil {
			return nil, e
		}
		if resume.Offset < earliest || resume.Offset > offset {
			return nil, ErrPosition
		}
		offset = resume.Offset
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	r := &Registration{topic: s.cfg.Topic, start: Position{s.cfg.Topic, 0, offset}, limit: s.cfg.Buffer, confirmed: offset, lastOffset: offset - 1, changed: make(chan struct{}), done: make(chan struct{}), ctx: ctx, cancel: cancel}
	s.reg = r
	s.client.AddConsumePartitions(map[string]map[int32]kgo.Offset{s.cfg.Topic: {0: kgo.NoResetOffset().At(offset)}})
	s.readerDone = make(chan struct{})
	go func() { defer close(s.readerDone); defer stop(); s.read(r) }()
	ok = true
	return r, nil
}
func (s *Source) singlePartition(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	req := kmsg.NewPtrMetadataRequest()
	t := kmsg.NewMetadataRequestTopic()
	t.Topic = &s.cfg.Topic
	req.Topics = []kmsg.MetadataRequestTopic{t}
	resp, err := req.RequestWith(ctx, s.client)
	if err != nil {
		return errors.Join(ErrContinuity, ErrPosition, ctx.Err())
	}
	if len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != 0 || len(resp.Topics[0].Partitions) != 1 || resp.Topics[0].Partitions[0].Partition != 0 || resp.Topics[0].Partitions[0].ErrorCode != 0 {
		return errors.Join(ErrContinuity, ErrPosition)
	}
	return nil
}
func (s *Source) position(parent context.Context) (int64, error) {
	return s.offsetAt(parent, -1)
}
func (s *Source) offsetAt(parent context.Context, timestamp int64) (int64, error) {
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	req := kmsg.NewPtrListOffsetsRequest()
	req.IsolationLevel = 1
	t := kmsg.NewListOffsetsRequestTopic()
	t.Topic = s.cfg.Topic
	p := kmsg.NewListOffsetsRequestTopicPartition()
	p.Partition = 0
	p.Timestamp = timestamp
	t.Partitions = []kmsg.ListOffsetsRequestTopicPartition{p}
	req.Topics = []kmsg.ListOffsetsRequestTopic{t}
	resp, err := req.RequestWith(ctx, s.client)
	if err != nil {
		return 0, errors.Join(ErrContinuity, ErrPosition, ctx.Err())
	}
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		return 0, errors.Join(ErrContinuity, ErrPosition)
	}
	v := resp.Topics[0].Partitions[0]
	if v.ErrorCode != 0 || v.Offset < 0 {
		return 0, errors.Join(ErrContinuity, ErrPosition)
	}
	return v.Offset, nil
}
func (s *Source) read(r *Registration) {
	for {
		poll, cancel := context.WithTimeout(r.ctx, time.Second)
		fs := s.client.PollRecords(poll, 1)
		timedOut := poll.Err() != nil
		cancel()
		if r.ctx.Err() != nil || s.ctx.Err() != nil {
			r.fail(errors.Join(r.ctx.Err(), s.ctx.Err()))
			return
		}
		for _, e := range fs.Errors() {
			if timedOut && errors.Is(e.Err, context.DeadlineExceeded) {
				continue
			}
			r.fail(ErrPosition)
			return
		}
		for _, v := range fs.Records() {
			if !r.accept(v) {
				return
			}
		}
		if timedOut {
			if _, err := s.position(r.ctx); err != nil {
				r.fail(err)
				return
			}
		}
	}
}
func (r *Registration) wakeLocked() { close(r.changed); r.changed = make(chan struct{}) }
func (r *Registration) failLocked(err error) {
	if r.err != nil {
		return
	}
	r.err = errors.Join(ErrContinuity, err)
	r.pending = nil
	r.queue = nil
	r.inflight = nil
	r.buffered = 0
	close(r.done)
	r.wakeLocked()
	r.cancel()
}
func (r *Registration) fail(err error) { r.mu.Lock(); defer r.mu.Unlock(); r.failLocked(err) }
func (r *Registration) accept(v *kgo.Record) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return false
	}
	fail := func(err error) bool { r.failLocked(err); return false }
	if v.Topic != r.topic || v.Partition != 0 || v.Offset < r.start.Offset || v.Offset <= r.lastOffset {
		return fail(ErrPosition)
	}
	r.lastOffset = v.Offset
	if v.Attrs.IsControl() {
		if len(v.Key) != 4 || v.Key[0] != 0 || v.Key[1] != 0 || v.Key[2] != 0 || v.Key[3] > 1 {
			return fail(ErrBoundary)
		}
		if len(r.pending) == 0 {
			return true
		} // abort markers have no visible data
		last := r.pending[len(r.pending)-1]
		// KIP-890 writes markers with the bumped epoch; older producers
		// use the data epoch. Records inside the transaction must still agree.
		if v.Key[3] != 1 || v.ProducerID != r.producer || (v.ProducerEpoch != r.epoch && int32(v.ProducerEpoch) != int32(r.epoch)+1) || last.Change.Position != last.Change.TransactionEnd {
			return fail(ErrBoundary)
		}
		r.queue = append(r.queue, Transaction{r.pending[0].Change.Position, last.Change.TransactionEnd, r.pending, v.Offset + 1})
		r.pending = nil
		r.wakeLocked()
		return true
	}
	if !v.Attrs.IsTransactional() {
		return fail(ErrBoundary)
	}
	if len(v.Value) > MaxPayload || len(v.Key) > 256 {
		return fail(ErrOverflow)
	}
	_, c, err := subscriptionchanged.Decode(v.Value)
	if err != nil || string(v.Key) != c.SubscriptionID {
		return fail(ErrContract)
	}
	if len(r.pending) > 0 {
		prev := r.pending[len(r.pending)-1].Change
		if prev.Position == prev.TransactionEnd || c.Position != prev.Position+1 || c.TransactionEnd != prev.TransactionEnd || v.ProducerID != r.producer || v.ProducerEpoch != r.epoch {
			return fail(ErrBoundary)
		}
	} else {
		if c.TransactionEnd-c.Position >= uint64(r.limit) {
			return fail(ErrOverflow)
		}
		r.producer = v.ProducerID
		r.epoch = v.ProducerEpoch
	}
	if r.buffered >= r.limit {
		return fail(ErrOverflow)
	}
	// Keep only wire bytes in the queue: decode on delivery to avoid holding both
	// JSON and a second full after-image/envelope for every buffered record.
	r.pending = append(r.pending, Record{Position: Position{v.Topic, v.Partition, v.Offset}, Key: string(v.Key), JSON: append([]byte(nil), v.Value...), Change: subscriptionchanged.Changed{Position: c.Position, TransactionEnd: c.TransactionEnd}})
	r.buffered++
	return true
}
func (r *Registration) Start() Position       { return r.start }
func (r *Registration) Done() <-chan struct{} { return r.done }
func (r *Registration) Err() error            { r.mu.Lock(); defer r.mu.Unlock(); return r.err }
func (r *Registration) Confirmed() int64      { r.mu.Lock(); defer r.mu.Unlock(); return r.confirmed }

// Confirm is a local acknowledgement of an already durable prefix. There is no
// group, broker offset commit or SQL write here. Next never calls Confirm.
func (r *Registration) Confirm(nextOffset int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	if r.inflight == nil || r.inflight.nextOffset != nextOffset {
		return ErrConfirmation
	}
	r.confirmed = nextOffset
	r.buffered -= r.inflight.count
	r.inflight = nil
	r.wakeLocked()
	return nil
}
func (r *Registration) TryNext() (Transaction, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return Transaction{}, false, r.err
	}
	if r.inflight != nil || len(r.queue) == 0 {
		return Transaction{}, false, nil
	}
	tx := r.queue[0]
	r.queue[0] = Transaction{}
	r.queue = r.queue[1:]
	// The caller owns the delivery and may mutate it; retain only its ack token.
	r.inflight = &delivery{nextOffset: tx.NextOffset, count: len(tx.Records)}
	for i := range tx.Records {
		v := &tx.Records[i]
		v.Envelope, v.Change, _ = subscriptionchanged.Decode(v.JSON)
	}
	return tx, true, nil
}
func (r *Registration) Next(ctx context.Context) (Transaction, error) {
	for {
		// Capture the notification before checking the queue to avoid missed wakeups.
		r.mu.Lock()
		changed := r.changed
		r.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return Transaction{}, err
		}
		tx, ok, err := r.TryNext()
		if ok || err != nil {
			return tx, err
		}
		select {
		case <-ctx.Done():
			return Transaction{}, ctx.Err()
		case <-changed:
		}
	}
}
func (r *Registration) Close() { r.fail(ErrClosed) }
func (s *Source) Revoke() {
	s.mu.Lock()
	r := s.reg
	s.mu.Unlock()
	if r != nil {
		r.fail(ErrRevoked)
	}
	s.Close()
}
func (s *Source) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	if s.reg != nil {
		s.reg.Close()
	}
	s.cancel()
	s.client.Close()
	if s.readerDone != nil {
		<-s.readerDone
	}
}
