// Package live coordinates a fresh or restored snapshot generation through SQL replay
// and Kafka acceleration. It owns no canonical subscription or delivery state.
package live

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/replayrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	changed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/subscriptionchanged"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/changefeed"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/priceconnection"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricefeed"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/projection"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/thresholdindex"
)

var (
	ErrConfig   = errors.New("live: invalid configuration")
	ErrNotReady = errors.New("live: generation not ready")
	ErrProtocol = errors.New("live: invalid replay boundary or resource")
	ErrPin      = errors.New("live: pin lost; new snapshot required")
)

type Store interface {
	Initialize(context.Context, string) error
	Checkpoint(context.Context) (projection.Checkpoint, error)
	ApplyReplay(context.Context, [][]byte) error
	Handle(context.Context, projection.Batch, func(context.Context, int64) error) error
	Walk(context.Context, func(projection.Image) error) (projection.Checkpoint, error)
	ReadImage(context.Context, string) (projection.Image, error)
}
type Replay interface {
	BeginReplay(context.Context, *pb.BeginReplayRequest) (*pb.ReplayState, error)
	ReadReplayPage(context.Context, *pb.ReadReplayPageRequest) (*pb.ReplayPage, error)
	AcknowledgeReplay(context.Context, *pb.AcknowledgeReplayRequest) (*pb.ReplayState, error)
}
type Registration interface {
	Start() changefeed.Position
	Next(context.Context) (changefeed.Transaction, error)
	Confirm(int64) error
	Done() <-chan struct{}
	Err() error
	Close()
}

// QuoteConnection is implemented by priceconnection.Connection. Connect must
// perform MAT-012's register-before-Latest handshake for the assigned partition.
type QuoteConnection interface {
	Current() (priceobserved.Observed, error)
	Next(context.Context) (priceobserved.Observed, error)
	Done() <-chan struct{}
	Err() error
	Close()
}
type Config struct {
	PricePartitions []int32 // nil: unrestricted; empty: no price partitions
	SnapshotID      string  // completed MAT-007 generation; use a new projection ID
	ReplayTTL       time.Duration
	HeadInterval    time.Duration
	PageSize        int32
	Now             func() time.Time
}
type Selection struct {
	C          uint64
	Quote      priceobserved.Observed
	Candidates []thresholdindex.Entry
}

type Coordinator struct {
	restore         func(context.Context) error
	cfg             Config
	pricePartitions map[int32]bool
	store           Store
	replay          Replay
	register        func(context.Context) (Registration, error)
	connect         func(context.Context, string) (QuoteConnection, error)
	mu              sync.Mutex
	started, ready  bool
	index           *thresholdindex.Index
	entries         map[string]thresholdindex.Entry
	pairRefs        map[string]int
	quotes          map[string]QuoteConnection
	quoteStops      map[string]context.CancelFunc
	reg             Registration
	c               uint64
	expires         time.Time
	changed         chan struct{}
	ctx             context.Context
	cancel          context.CancelCauseFunc
	wg              sync.WaitGroup
}

func New(store Store, replay Replay, register func(context.Context) (Registration, error), connect func(context.Context, string) (QuoteConnection, error), cfg Config) (*Coordinator, error) {
	if store == nil || replay == nil || register == nil || connect == nil || cfg.SnapshotID == "" || cfg.ReplayTTL < time.Millisecond || cfg.ReplayTTL > 24*time.Hour || cfg.HeadInterval < 0 || cfg.PageSize < 0 || cfg.PageSize > 1000 {
		return nil, ErrConfig
	}
	var partitions map[int32]bool
	if cfg.PricePartitions != nil {
		partitions = make(map[int32]bool, len(cfg.PricePartitions))
		for _, p := range cfg.PricePartitions {
			if p < 0 || p >= 16 || partitions[p] {
				return nil, ErrConfig
			}
			partitions[p] = true
		}
	}
	// Only the private set retains the assignment; caller slices cannot mutate it.
	cfg.PricePartitions = nil
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.HeadInterval == 0 {
		cfg.HeadInterval = time.Second
	}
	if cfg.PageSize == 0 {
		cfg.PageSize = 100
	}
	return &Coordinator{cfg: cfg, pricePartitions: partitions, store: store, replay: replay, register: register, connect: connect, changed: make(chan struct{}, 1)}, nil
}

// Changed is a coalescing wakeup, not a queue of historical candidate batches.
// After waking, sample Current. It never acknowledges a price or API workflow.
func (c *Coordinator) Changed() <-chan struct{} { return c.changed }
func (c *Coordinator) signal() {
	select {
	case c.changed <- struct{}{}:
	default:
	}
}

// Current atomically samples this generation's index and the latest merged
// price. The consumer must still request canonical API admission (MAT-004).
func (c *Coordinator) Current(pair string) (Selection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.readyLocked() || !c.owns(pair) {
		return Selection{}, ErrNotReady
	}
	q := c.quotes[pair]
	if q == nil {
		return Selection{}, ErrNotReady
	}
	quote, err := q.Current()
	if err != nil {
		return Selection{}, err
	}
	if quote.Validate() != nil || quote.Pair != pair {
		return Selection{}, ErrProtocol
	}
	return Selection{C: c.c, Quote: quote, Candidates: c.index.Candidates(pair, *quote.Price)}, nil
}

// ActivePairs returns a sorted snapshot of active owned pairs at a ready view.
// Like Current, callers must resample after Changed; it acknowledges no price.
func (c *Coordinator) ActivePairs() ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.readyLocked() {
		return nil, ErrNotReady
	}
	pairs := make([]string, 0, len(c.quotes))
	for pair := range c.quotes {
		pairs = append(pairs, pair)
	}
	sort.Strings(pairs)
	return pairs, nil
}

func (c *Coordinator) owns(pair string) bool {
	return c.pricePartitions == nil || c.pricePartitions[pricefeed.PairPartition(pair)]
}

// Run is single-use, including after failure. It owns registration/connections
// until return. Any error revokes the view; caller restores SQL or bootstraps.
// There are no hidden retries of uncertain writes, no group offsets or fencing.
func (c *Coordinator) Run(parent context.Context) (result error) {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return ErrConfig
	}
	c.started = true
	c.ctx, c.cancel = context.WithCancelCause(parent)
	c.entries = make(map[string]thresholdindex.Entry)
	c.pairRefs = make(map[string]int)
	c.quotes = make(map[string]QuoteConnection)
	c.quoteStops = make(map[string]context.CancelFunc)
	c.mu.Unlock()
	defer func() {
		c.cancel(result)
		c.mu.Lock()
		c.ready = false
		c.mu.Unlock()
		c.signal()
		c.wg.Wait()
		for _, q := range c.quotes {
			q.Close()
		}
		if err := context.Cause(c.ctx); result == nil && err != nil {
			result = err
		}
		if c.restore != nil {
			result = bootstrapError(result)
		}
	}()
	if c.restore != nil {
		if err := c.restore(c.ctx); err != nil {
			return err
		}
	}
	reg, err := c.register(c.ctx)
	if err != nil {
		return err
	}
	if reg == nil {
		return ErrProtocol
	}
	defer reg.Close()
	c.mu.Lock()
	c.reg = reg
	c.mu.Unlock()
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		select {
		case <-reg.Done():
			e := reg.Err()
			if e == nil {
				e = ErrNotReady
			}
			c.cancel(e)
			c.signal()
		case <-c.ctx.Done():
		}
	}()
	// Register before H. A restart must not reinitialize or require the old
	// snapshot's loading TTL; the API replay pin has its own lifetime.
	if c.restore == nil {
		if err = c.store.Initialize(c.ctx, c.cfg.SnapshotID); err != nil {
			return err
		}
	}
	if err = c.sync(c.ctx); err != nil {
		return err
	}
	idx := new(thresholdindex.Index)
	cp, err := c.store.Walk(c.ctx, func(im projection.Image) error {
		e, active, err := entry(im)
		if err != nil {
			return err
		}
		if active && c.owns(e.Pair) {
			if err = idx.Put(e); err != nil {
				return err
			}
			c.entries[e.ID] = e
			c.pairRefs[e.Pair]++
		}
		return nil
	})
	if err != nil {
		return err
	}
	c.index = idx
	for _, e := range c.entries {
		if err = c.ensureQuote(e.Pair); err != nil {
			return err
		}
	}
	// Connect/Walk may take time: check pin/head again before publishing the view.
	if err = c.sync(c.ctx); err != nil {
		return err
	}
	cp, err = c.store.Checkpoint(c.ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.c = cp.C
	c.ready = true
	c.mu.Unlock()
	c.signal()
	nextHead := c.cfg.Now().Add(c.cfg.HeadInterval)
	for {
		if err = c.ctx.Err(); err != nil {
			return context.Cause(c.ctx)
		}
		if !c.cfg.Now().Before(nextHead) {
			c.setReady(false)
			if err = c.sync(c.ctx); err != nil {
				return err
			}
			c.setReady(true)
			nextHead = c.cfg.Now().Add(c.cfg.HeadInterval)
		}
		wait := nextHead.Sub(c.cfg.Now())
		if wait < 0 {
			wait = 0
		}
		waitCtx, cancel := context.WithTimeout(c.ctx, wait)
		tx, e := reg.Next(waitCtx)
		cancel()
		if e != nil {
			if errors.Is(e, context.DeadlineExceeded) && c.ctx.Err() == nil {
				continue
			}
			return e
		}
		c.setReady(false)
		cp, e = c.store.Checkpoint(c.ctx)
		if e != nil {
			return e
		}
		if tx.C > cp.C && (cp.C == ^uint64(0) || tx.First != cp.C+1) {
			if e = c.sync(c.ctx); e != nil {
				return e
			}
		}
		events := make([][]byte, len(tx.Records))
		for i, r := range tx.Records {
			events[i] = r.JSON
		}
		start := reg.Start()
		e = c.store.Handle(c.ctx, projection.Batch{Topic: start.Topic, Partition: start.Partition, NextOffset: tx.NextOffset, Events: events}, func(_ context.Context, n int64) error { return reg.Confirm(n) })
		if e != nil {
			return e
		}
		if e = c.update(c.ctx, events); e != nil {
			return e
		}
		c.setReady(true)
	}
}
func (c *Coordinator) setReady(v bool) { c.mu.Lock(); c.ready = v; c.mu.Unlock(); c.signal() }

// sync advances the API pin only through issued SQL pages. Kafka may already
// have committed a greater local C; neither C nor a transport offset is an ack.
func (c *Coordinator) sync(ctx context.Context) error {
	state, err := c.replay.BeginReplay(ctx, &pb.BeginReplayRequest{SnapshotId: c.cfg.SnapshotID, TtlMillis: c.cfg.ReplayTTL.Milliseconds()})
	if err != nil {
		return err
	}
	if err = c.state(state); err != nil {
		return err
	}
	cp, err := c.store.Checkpoint(ctx)
	if err != nil {
		return err
	}
	if state.Progress > cp.C || cp.C > state.Head {
		return ErrProtocol
	}
	after, target := state.Progress, state.Head
	for after < target {
		page, err := c.replay.ReadReplayPage(ctx, &pb.ReadReplayPageRequest{ResourceId: c.cfg.SnapshotID, After: after, MaxTransactions: c.cfg.PageSize})
		if err != nil {
			return err
		}
		if page == nil {
			return ErrProtocol
		}
		if err = c.state(page.State); err != nil {
			return err
		}
		if page.Next <= after || page.Next > page.State.Issued || page.Next > page.State.Head {
			return ErrProtocol
		}
		batches, err := transactions(page.Events, after, page.Next)
		if err != nil {
			return err
		}
		for _, batch := range batches {
			if err = c.store.ApplyReplay(ctx, batch); err != nil {
				return err
			}
			if c.index != nil {
				if err = c.update(ctx, batch); err != nil {
					return err
				}
			}
		}
		cp, err = c.store.Checkpoint(ctx)
		if err != nil {
			return err
		}
		if cp.C < page.Next {
			return ErrProtocol
		}
		ack, err := c.replay.AcknowledgeReplay(ctx, &pb.AcknowledgeReplayRequest{ResourceId: c.cfg.SnapshotID, Boundary: page.Next})
		if err != nil {
			return err
		}
		if err = c.state(ack); err != nil {
			return err
		}
		if ack.Progress != page.Next {
			return ErrProtocol
		}
		after = page.Next
	}
	return nil
}
func (c *Coordinator) state(s *pb.ReplayState) error {
	if s == nil || s.ResourceId != c.cfg.SnapshotID || s.Progress > s.Issued || s.Issued > s.Head {
		return ErrProtocol
	}
	expiry, err := time.Parse(time.RFC3339Nano, s.ExpiresAt)
	if err != nil {
		return ErrProtocol
	}
	if !c.cfg.Now().Before(expiry) {
		return ErrPin
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.expires.IsZero() && !c.expires.Equal(expiry) {
		return ErrProtocol
	}
	c.expires = expiry
	return nil
}
func transactions(events [][]byte, after, next uint64) ([][][]byte, error) {
	var batches [][][]byte
	var batch [][]byte
	var end uint64
	for _, raw := range events {
		_, ch, err := changed.Decode(raw)
		if err != nil {
			return nil, err
		}
		if after == ^uint64(0) || ch.Position != after+1 {
			return nil, ErrProtocol
		}
		if len(batch) == 0 {
			end = ch.TransactionEnd
		} else if ch.TransactionEnd != end {
			return nil, ErrProtocol
		}
		after = ch.Position
		batch = append(batch, raw)
		if after == end {
			batches = append(batches, batch)
			batch = nil
		}
	}
	if len(batches) == 0 || len(batch) != 0 || after != next {
		return nil, ErrProtocol
	}
	return batches, nil
}
func entry(im projection.Image) (thresholdindex.Entry, bool, error) {
	if _, err := im.OwnerID(); err != nil {
		return thresholdindex.Entry{}, false, err
	}
	var e thresholdindex.Entry
	var state string
	if im.Snapshot != nil && im.Change == nil {
		s := im.Snapshot
		p, err := price.Parse(s.Threshold)
		if err != nil || s.Version <= 0 {
			return e, false, ErrProtocol
		}
		e = thresholdindex.Entry{ID: s.SubscriptionId, Version: uint64(s.Version), Pair: s.Pair, Threshold: p, Direction: price.Direction(s.Direction)}
		state = s.State
	} else if im.Change != nil && im.Snapshot == nil {
		s := im.Change
		if err := s.Validate(); err != nil {
			return e, false, err
		}
		e = thresholdindex.Entry{ID: s.SubscriptionID, Version: s.SubscriptionVersion, Pair: s.Pair, Threshold: *s.Threshold, Direction: s.Direction}
		state = string(s.State)
	} else {
		return e, false, ErrProtocol
	}
	return e, state == "active", nil
}
func (c *Coordinator) ensureQuote(pair string) error {
	if !c.owns(pair) {
		return ErrConfig
	}
	if c.quotes[pair] != nil {
		return nil
	}
	quoteCtx, stop := context.WithCancel(c.ctx)
	q, err := c.connect(quoteCtx, pair)
	if err != nil {
		stop()
		return err
	}
	if q == nil {
		stop()
		return ErrProtocol
	}
	if _, err = q.Current(); err != nil {
		stop()
		q.Close()
		return err
	}
	c.mu.Lock()
	c.quotes[pair] = q
	c.quoteStops[pair] = stop
	c.mu.Unlock()
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			if _, err := q.Next(quoteCtx); err != nil {
				if quoteCtx.Err() == nil {
					c.cancel(err)
					c.signal()
				}
				return
			}
			c.signal()
		}
	}()
	return nil
}
func (c *Coordinator) update(ctx context.Context, events [][]byte) error {
	ids := map[string]bool{}
	for _, raw := range events {
		_, ch, err := changed.Decode(raw)
		if err != nil {
			return err
		}
		ids[ch.SubscriptionID] = true
	}
	for id := range ids {
		im, err := c.store.ReadImage(ctx, id)
		if err != nil {
			return err
		}
		e, active, err := entry(im)
		if err != nil {
			return err
		}
		if active && c.owns(e.Pair) {
			if err = c.ensureQuote(e.Pair); err != nil {
				return err
			}
		}
		c.mu.Lock()
		c.index.Remove(id)
		if old, ok := c.entries[id]; ok {
			c.pairRefs[old.Pair]--
		}
		delete(c.entries, id)
		if active && c.owns(e.Pair) {
			err = c.index.Put(e)
			if err == nil {
				c.entries[id] = e
				c.pairRefs[e.Pair]++
			}
		}
		c.mu.Unlock()
		if err != nil {
			return err
		}
	}
	c.pruneQuotes()
	cp, err := c.store.Checkpoint(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.c = cp.C
	c.mu.Unlock()
	return nil
}

var _ Store = (*projection.Store)(nil)
var _ Registration = (*changefeed.Registration)(nil)

// Ready includes idle registration/price loss and local pin expiry, without
// waiting for the worker loop to process the failure notification.
func (c *Coordinator) Ready() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.readyLocked() }
func (c *Coordinator) readyLocked() bool {
	if !c.ready || c.ctx == nil || c.ctx.Err() != nil || !c.cfg.Now().Before(c.expires) {
		return false
	}
	if c.reg == nil {
		return false
	}
	select {
	case <-c.reg.Done():
		return false
	default:
	}
	for _, q := range c.quotes {
		select {
		case <-q.Done():
			return false
		default:
		}
	}
	return true
}
func (c *Coordinator) pruneQuotes() {
	for pair, q := range c.quotes {
		if c.pairRefs[pair] == 0 {
			c.mu.Lock()
			c.quoteStops[pair]()
			delete(c.quoteStops, pair)
			delete(c.pairRefs, pair)
			delete(c.quotes, pair)
			c.mu.Unlock()
			q.Close()
		}
	}
}

var _ QuoteConnection = (*priceconnection.Connection)(nil)
var _ Replay = (*replayrpc.Client)(nil)
