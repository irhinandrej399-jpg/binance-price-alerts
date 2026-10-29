package pricefeed

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var ErrPrepare = errors.New("pricefeed: assignment preparation failed")
var ErrConfirm = errors.New("pricefeed: position not delivered or not next for assignment")
var ErrLost = errors.New("pricefeed: group assignment lost")

// GroupConfig uses the same bounds as Config. Partitions must be empty: the
// real consumer group, never a direct reader, owns the assignment.
type GroupConfig struct {
	Config
	Group string
	// Prepare installs caller authority and returns absolute next offsets from
	// the caller's checkpoint/bootstrap policy, NOT Kafka's committed offsets.
	// It must honor ctx, finish within three seconds, and make no use of Group
	// methods. ctx is also cancelled on group failure/Close. A late result never
	// enables work. SQL token issuance is the caller's responsibility (MAT-010).
	Prepare func(ctx context.Context, partitions []int32) (map[int32]int64, error)
}

type Group struct {
	mu        sync.Mutex
	client    *kgo.Client
	cfg       GroupConfig
	ctx       context.Context
	cancel    context.CancelFunc
	current   *Assignment
	changed   chan struct{}
	err       error
	done      chan struct{}
	closeOnce sync.Once
	serial    uint64
}

// Assignment is a local capability, not a durable SQL fencing token. Revoke or
// loss cancels Context/Done and invalidates every buffered record/registration.
type Assignment struct {
	source    *Source
	all       *Registration
	id        uint64
	starts    map[int32]int64
	delivered map[int32][]Position // protected by source.mu; bounded until Confirm
}

func NewGroup(parent context.Context, cfg GroupConfig) (*Group, error) {
	if len(cfg.Brokers) == 0 || strings.TrimSpace(cfg.Topic) == "" || strings.TrimSpace(cfg.Group) == "" || cfg.Prepare == nil || len(cfg.Partitions) != 0 || cfg.Buffer < 0 || cfg.Buffer > MaxBuffer {
		return nil, ErrConfig
	}
	for _, b := range cfg.Brokers {
		if strings.TrimSpace(b) == "" {
			return nil, ErrConfig
		}
	}
	if cfg.Buffer == 0 {
		cfg.Buffer = 256
	}
	ctx, cancel := context.WithCancel(parent)
	g := &Group{cfg: cfg, ctx: ctx, cancel: cancel, changed: make(chan struct{}), done: make(chan struct{})}
	opts := clientOptions(ctx, cfg.Config)
	opts = append(opts, kgo.ConsumerGroup(cfg.Group), kgo.ConsumeTopics(cfg.Topic), kgo.DisableAutoCommit(),
		// Eager range balancing deliberately invalidates the whole old assignment.
		kgo.Balancers(kgo.RangeBalancer()), kgo.SessionTimeout(10*time.Second), kgo.HeartbeatInterval(time.Second),
		kgo.RebalanceTimeout(15*time.Second), kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(func(context.Context, *kgo.Client, map[string][]int32) {}),
		kgo.OnPartitionsRevoked(func(context.Context, *kgo.Client, map[string][]int32) { g.revoke(ErrRevoked) }),
		kgo.OnPartitionsLost(func(context.Context, *kgo.Client, map[string][]int32) { g.revoke(errors.Join(ErrRevoked, ErrLost)) }),
		kgo.AdjustFetchOffsetsFn(g.assign))
	c, err := kgo.NewClient(opts...)
	if err != nil {
		cancel()
		return nil, ErrConfig
	}
	g.client = c
	if err := g.metadata(ctx); err != nil {
		cancel()
		c.Close()
		return nil, err
	}
	go g.read()
	return g, nil
}

// PairPartition uses the exact keyed partitioner used by the pinned producer.
// The fixed count is part of DEC-008; changing it requires a migration.
func PairPartition(pair string) int32 {
	p := kgo.StickyKeyPartitioner(nil).ForTopic("")
	return int32(p.Partition(&kgo.Record{Key: []byte(pair)}, 16))
}

func (g *Group) metadata(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	req := kmsg.NewPtrMetadataRequest()
	t := kmsg.NewMetadataRequestTopic()
	t.Topic = &g.cfg.Topic
	req.Topics = []kmsg.MetadataRequestTopic{t}
	resp, err := req.RequestWith(ctx, g.client)
	if err != nil {
		return errors.Join(ErrContinuity, ErrPosition, ctx.Err())
	}
	if len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != 0 || len(resp.Topics[0].Partitions) != 16 {
		return errors.Join(ErrContinuity, ErrConfig)
	}
	seen := map[int32]bool{}
	for _, p := range resp.Topics[0].Partitions {
		if p.ErrorCode != 0 || p.Partition < 0 || p.Partition >= 16 || seen[p.Partition] {
			return errors.Join(ErrContinuity, ErrConfig)
		}
		seen[p.Partition] = true
	}
	return nil
}

func (g *Group) signalLocked() { close(g.changed); g.changed = make(chan struct{}) }
func (g *Group) revoke(err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if a := g.current; a != nil {
		a.source.fail(err)
		g.current = nil
		g.signalLocked()
	}
}
func (g *Group) fail(err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.err != nil {
		return
	}
	g.err = errors.Join(ErrContinuity, err)
	if g.current != nil {
		g.current.source.fail(g.err)
		g.current = nil
	}
	g.cancel()
	g.signalLocked()
}

// assign runs inside the group protocol, before fetching. Broker committed
// offsets are intentionally ignored, even if present. NoResetOffset stays set.
func (g *Group) assign(session context.Context, fetched map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
	parts := make([]int32, 0, 16)
	for topic, offsets := range fetched {
		if topic != g.cfg.Topic {
			g.fail(ErrConfig)
			return nil, ErrConfig
		}
		for p := range offsets {
			if p < 0 || p >= 16 {
				g.fail(ErrConfig)
				return nil, ErrConfig
			}
			parts = append(parts, p)
		}
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
	ctx, cancel := context.WithTimeout(session, 3*time.Second)
	defer cancel()
	stop := context.AfterFunc(g.ctx, cancel)
	defer stop()
	if err := g.metadata(ctx); err != nil {
		g.fail(err)
		return nil, err
	}
	type prepared struct {
		offsets map[int32]int64
		err     error
	}
	result := make(chan prepared, 1)
	go func() {
		offsets, err := g.cfg.Prepare(ctx, append([]int32(nil), parts...))
		// Caller transfers ownership of the returned map.
		result <- prepared{offsets, err}
	}()
	var starts map[int32]int64
	select {
	case r := <-result:
		if r.err != nil {
			g.fail(ErrPrepare)
			return nil, ErrPrepare
		}
		starts = r.offsets
	case <-ctx.Done():
		g.fail(ErrPrepare)
		return nil, ErrPrepare
	}
	if ctx.Err() != nil || len(starts) != len(parts) {
		g.fail(ErrPrepare)
		return nil, ErrPrepare
	}
	abs := make(map[int32]kgo.Offset, len(parts))
	assigned := make(map[int32]bool, len(parts))
	copyStarts := make(map[int32]int64, len(parts))
	for _, p := range parts {
		o, ok := starts[p]
		if !ok || o < 0 {
			g.fail(ErrPrepare)
			return nil, ErrPrepare
		}
		abs[p] = kgo.NoResetOffset().At(o).WithEpoch(-1)
		assigned[p] = true
		copyStarts[p] = o
	}
	life, end := context.WithCancel(g.ctx)
	s := &Source{client: g.client, topic: g.cfg.Topic, assigned: assigned, regs: make(map[string]*Registration), limit: g.cfg.Buffer, ctx: life, cancel: end}
	// Validate the caller cursor before publishing any usable capability. Both
	// ends are queried with read_committed isolation; no reset is performed.
	for _, timestamp := range []int64{-2, -1} {
		req := kmsg.NewPtrListOffsetsRequest()
		req.IsolationLevel = 1
		t := kmsg.NewListOffsetsRequestTopic()
		t.Topic = g.cfg.Topic
		for _, p := range parts {
			v := kmsg.NewListOffsetsRequestTopicPartition()
			v.Partition = p
			v.Timestamp = timestamp
			t.Partitions = append(t.Partitions, v)
		}
		req.Topics = []kmsg.ListOffsetsRequestTopic{t}
		resp, err := req.RequestWith(ctx, g.client)
		if err != nil || ctx.Err() != nil {
			end()
			g.fail(ErrPosition)
			return nil, ErrPosition
		}
		seen := map[int32]bool{}
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				o, ok := copyStarts[p.Partition]
				if t.Topic != g.cfg.Topic || !ok || seen[p.Partition] || p.ErrorCode != 0 || p.Offset < 0 || (timestamp == -2 && o < p.Offset) || (timestamp == -1 && o > p.Offset) {
					end()
					g.fail(ErrPosition)
					return nil, ErrPosition
				}
				seen[p.Partition] = true
			}
		}
		if len(seen) != len(parts) {
			end()
			g.fail(ErrPosition)
			return nil, ErrPosition
		}
	}
	a := &Assignment{source: s, starts: copyStarts, delivered: make(map[int32][]Position)}
	s.groupAssignment = a
	a.all = &Registration{source: s, changed: make(chan struct{}), terminated: make(chan struct{})}
	s.regs[""] = a.all
	g.mu.Lock()
	if g.err != nil || ctx.Err() != nil {
		g.mu.Unlock()
		end()
		return nil, ErrPrepare
	}
	if g.current != nil {
		g.current.source.fail(ErrRevoked)
	}
	g.serial++
	a.id = g.serial
	g.current = a
	g.signalLocked()
	g.mu.Unlock()
	return map[string]map[int32]kgo.Offset{g.cfg.Topic: abs}, nil
}

// NextAssignment waits for a ready capability different from after. Watching
// Assignment.Done is mandatory during bootstrap and all ongoing SQL work.
func (g *Group) NextAssignment(ctx context.Context, after *Assignment) (*Assignment, error) {
	for {
		g.mu.Lock()
		if g.err != nil {
			err := g.err
			g.mu.Unlock()
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			g.mu.Unlock()
			return nil, err
		}
		if g.current != nil && g.current != after {
			a := g.current
			g.mu.Unlock()
			return a, nil
		}
		ch := g.changed
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ch:
		}
	}
}
func (g *Group) Done() <-chan struct{}         { return g.ctx.Done() }
func (g *Group) Err() error                    { g.mu.Lock(); defer g.mu.Unlock(); return g.err }
func (g *Group) Close()                        { g.closeOnce.Do(func() { g.fail(ErrClosed); <-g.done }) }
func (a *Assignment) ID() uint64               { return a.id }
func (a *Assignment) Context() context.Context { return a.source.ctx }
func (a *Assignment) Done() <-chan struct{}    { return a.source.ctx.Done() }
func (a *Assignment) Err() error               { a.source.mu.Lock(); defer a.source.mu.Unlock(); return a.source.err }
func (a *Assignment) Starts() map[int32]int64 {
	out := make(map[int32]int64, len(a.starts))
	for p, o := range a.starts {
		out[p] = o
	}
	return out
}
func (a *Assignment) Register(ctx context.Context, pair string, partition int32) (*Registration, error) {
	if PairPartition(pair) != partition {
		return nil, ErrConfig
	}
	return a.source.Register(ctx, pair, partition)
}
func (a *Assignment) Next(ctx context.Context) (Update, error) { return a.all.Next(ctx) }

// Confirm asserts caller durable SQL commit through this exact delivered price.
// It accepts only the oldest unconfirmed record of that partition. It neither
// computes business Prefix nor treats the broker offset as a SQL checkpoint.
// On commit error the capability is retired; a new assignment must replay SQL.
func (a *Assignment) Confirm(parent context.Context, pos Position) error {
	s := a.source
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil && s.err == nil {
		s.failLocked(s.ctx.Err())
	}
	if s.err != nil {
		return s.err
	}
	if err := parent.Err(); err != nil {
		return err
	}
	ps := a.delivered[pos.Partition]
	if len(ps) == 0 || ps[0] != pos {
		return ErrConfirm
	}
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	var result error
	s.client.CommitOffsetsSync(ctx, map[string]map[int32]kgo.EpochOffset{pos.Topic: {pos.Partition: {Epoch: -1, Offset: pos.Offset + 1}}}, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		if err != nil {
			result = ErrPosition
			return
		}
		if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 || resp.Topics[0].Partitions[0].ErrorCode != 0 {
			result = ErrPosition
		}
	})
	if result != nil {
		s.failLocked(result)
		return s.err
	}
	a.delivered[pos.Partition] = ps[1:]
	s.buffered--
	return nil
}

func (g *Group) read() {
	defer close(g.done)
	defer g.client.Close()
	defer g.revoke(ErrClosed)
	checked := time.Now()
	for {
		ctx, cancel := context.WithTimeout(g.ctx, time.Second)
		fs := g.client.PollRecords(ctx, 1)
		timedOut := ctx.Err() != nil
		cancel()
		if g.ctx.Err() != nil {
			g.client.AllowRebalance()
			g.fail(g.ctx.Err())
			return
		}
		for _, e := range fs.Errors() {
			if timedOut && errors.Is(e.Err, context.DeadlineExceeded) {
				continue
			}
			g.client.AllowRebalance()
			g.fail(ErrPosition)
			return
		}
		g.mu.Lock()
		a := g.current
		g.mu.Unlock()
		for _, r := range fs.Records() {
			if a == nil || !a.source.dispatch(r) {
				g.client.AllowRebalance()
				if a != nil {
					g.fail(a.Err())
				} else {
					g.fail(ErrRevoked)
				}
				return
			}
		}
		// Inspect stream failures before releasing the gate. A regular revoke
		// after AllowRebalance retires only that capability, not the group.
		var failure error
		if a != nil {
			failure = a.Err()
		}
		g.client.AllowRebalance()
		if failure != nil && !errors.Is(failure, ErrRevoked) {
			g.fail(failure)
			return
		}
		if time.Since(checked) >= time.Second {
			if err := g.metadata(g.ctx); err != nil {
				g.fail(err)
				return
			}
			checked = time.Now()
		}
	}
}
