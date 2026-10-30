// Package replica joins Kafka ownership, SQL fencing, a full live projection,
// and durable price evaluation. It does not start a service or retry failures.
package replica

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/changefeed"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/fencing"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/live"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/priceconnection"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricefeed"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricehandler"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricework"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/projection"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrConfig = errors.New("replica: invalid configuration or assignment")

// ID is a durable replica identity, unique among concurrent replicas. Bootstrap
// is an explicitly selected absolute next offset for each of the 16 partitions,
// used ONLY if SQL has no prefix. The caller owns latest-price bootstrap policy;
// neither a missing SQL cursor nor a broker commit silently selects an offset.
// Snapshot loads a separate immutable snapshot for a new projection identity.
// Changes creates a fresh subscription feed and honors the exact resume cursor.
type Config struct {
	Pool                   *pgxpool.Pool
	ID, Environment, Group string
	Prices                 pricefeed.Config
	Bootstrap              map[int32]int64
	Replay                 live.Replay
	Latest                 priceconnection.QuoteSource
	Delivery               pricehandler.Delivery
	Snapshot               func(context.Context, string) (string, error)
	Changes                func(context.Context, *changefeed.Position) (live.Registration, error)
	Live                   live.Config
}
type prepared struct {
	grants       []fencing.Grant
	starts       map[int32]int64
	projectionID string
}
type Replica struct {
	active             *prepared
	cfg                Config
	mu                 sync.Mutex
	started, recovered bool
	prepared           *prepared
	prepareErr         error
	assignment         *pricefeed.Assignment
	view               *live.Coordinator
}

func New(cfg Config) (*Replica, error) {
	if cfg.Pool == nil || strings.TrimSpace(cfg.ID) == "" || strings.TrimSpace(cfg.Environment) == "" || strings.TrimSpace(cfg.Group) == "" || cfg.Replay == nil || cfg.Latest == nil || cfg.Delivery == nil || cfg.Snapshot == nil || cfg.Changes == nil || len(cfg.Bootstrap) != 16 || len(cfg.Prices.Partitions) != 0 {
		return nil, ErrConfig
	}
	offsets := make(map[int32]int64, 16)
	for p := int32(0); p < 16; p++ {
		o, ok := cfg.Bootstrap[p]
		if !ok || o < 0 {
			return nil, ErrConfig
		}
		offsets[p] = o
	}
	cfg.Bootstrap = offsets
	cfg.Prices.Brokers = append([]string(nil), cfg.Prices.Brokers...)
	return &Replica{cfg: cfg}, nil
}
func digest(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// TransportKey identifies the physical delivery across tokens and projections.
func TransportKey(p pricework.Position) string { return "price:" + digest(p) }
func scope(i fencing.Identity) string          { return "partition:" + digest(i) }
func (r *Replica) identity(p int32) fencing.Identity {
	return fencing.Identity{Environment: r.cfg.Environment, Group: r.cfg.Group, Topic: r.cfg.Prices.Topic, Partition: p}
}

func (r *Replica) prepare(ctx context.Context, parts []int32) (out map[int32]int64, err error) {
	// Group calls this only after retiring the former eager assignment.
	r.mu.Lock()
	former := r.prepared
	r.mu.Unlock()
	if former != nil {
		// Also retire grants from a successful Prepare whose capability was
		// replaced before the business loop ever observed it.
		for _, grant := range former.grants {
			if e := fencing.New(r.cfg.Pool).Revoke(ctx, grant); e != nil && !errors.Is(e, fencing.ErrAuthority) {
				return nil, e
			}
		}
	}
	parts = append([]int32{}, parts...)
	sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
	id := "replica:" + digest(struct {
		Environment, Group, Topic, Replica string
		Parts                              []int32
	}{r.cfg.Environment, r.cfg.Group, r.cfg.Prices.Topic, r.cfg.ID, parts})
	p := &prepared{projectionID: id, starts: map[int32]int64{}}
	defer func() {
		if err != nil {
			r.revoke(p.grants)
			r.mu.Lock()
			r.prepareErr = err
			r.mu.Unlock()
		}
	}()
	authority := fencing.New(r.cfg.Pool)
	for _, part := range parts {
		identity := r.identity(part)
		old, _, e := authority.Current(ctx, identity)
		if e != nil && !errors.Is(e, pgx.ErrNoRows) {
			return nil, e
		}
		request := fencing.Request{Identity: identity, Scope: scope(identity), ProjectionID: id, ExpectedToken: old.Token, RequestID: rand.Text()}
		// Keep the expected grant even on an ambiguous commit, so cleanup can
		// conditionally revoke it. Never reread Current to invent another request.
		expected := fencing.Grant{Identity: identity, Scope: request.Scope, ProjectionID: id, Token: old.Token + 1}
		p.grants = append(p.grants, expected)
		grant, e := authority.Assign(ctx, request)
		if e != nil {
			return nil, e
		}
		p.grants[len(p.grants)-1] = grant
		next, e := pricework.NewFenced(r.cfg.Pool, grant).Checkpoint(ctx, identity.Topic, part)
		if errors.Is(e, pgx.ErrNoRows) {
			next = r.cfg.Bootstrap[part]
		} else if e != nil {
			return nil, e
		}
		p.starts[part] = next
	}
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	r.mu.Lock()
	r.prepared = p
	r.mu.Unlock()
	out = make(map[int32]int64, len(p.starts))
	for k, v := range p.starts {
		out[k] = v
	}
	return out, nil
}
func (r *Replica) revoke(grants []fencing.Grant) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, g := range grants {
		e := fencing.New(r.cfg.Pool).Revoke(ctx, g)
		if e != nil && !errors.Is(e, fencing.ErrAuthority) {
			r.mu.Lock()
			r.prepareErr = errors.Join(r.prepareErr, e)
			r.mu.Unlock()
		}
	}
}
func (r *Replica) Ready() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.recovered && r.assignment != nil && r.assignment.Context().Err() == nil && r.view != nil && r.view.Ready()
}

// Grants is a copy for observing an assignment, never a new authority grant.
func (r *Replica) Grants() []fencing.Grant {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.recovered || r.assignment == nil || r.assignment.Context().Err() != nil {
		return nil
	}
	return append([]fencing.Grant(nil), r.active.grants...)
}

// Run is single-use. Normal eager rebalance restores a new session; SQL/RPC or
// stream failures return to the caller with durable data intact. MAT-011 covers
// failure takeover; this loop introduces no reset or automatic retry policy.
func (r *Replica) Run(ctx context.Context) (result error) {
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return ErrConfig
	}
	r.started = true
	r.mu.Unlock()
	g, err := pricefeed.NewGroup(ctx, pricefeed.GroupConfig{Config: r.cfg.Prices, Group: r.cfg.Group, Prepare: r.prepare})
	if err != nil {
		return err
	}
	defer func() {
		g.Close()
		r.mu.Lock()
		p := r.prepared
		r.recovered = false
		r.mu.Unlock()
		if p != nil {
			r.revoke(p.grants)
		}
		r.mu.Lock()
		result = errors.Join(result, r.prepareErr)
		r.mu.Unlock()
	}()
	var previous *pricefeed.Assignment
	for {
		a, e := g.NextAssignment(ctx, previous)
		if e != nil {
			return e
		}
		previous = a
		r.mu.Lock()
		p := r.prepared
		r.mu.Unlock()
		if a.Context().Err() != nil {
			continue
		}
		if p == nil || !sameStarts(a.Starts(), p.starts) {
			return ErrConfig
		}
		e = r.runAssignment(ctx, a, p)
		r.mu.Lock()
		cleanupErr := r.prepareErr
		r.mu.Unlock()
		if cleanupErr != nil {
			return errors.Join(e, cleanupErr)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if a.Context().Err() != nil && errors.Is(a.Err(), pricefeed.ErrRevoked) && g.Err() == nil {
			continue
		}
		return e
	}
}
func sameStarts(a, b map[int32]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for p, o := range a {
		v, ok := b[p]
		if !ok || v != o {
			return false
		}
	}
	return true
}

func (r *Replica) runAssignment(parent context.Context, a *pricefeed.Assignment, p *prepared) (result error) {
	ctx, cancel := context.WithCancel(a.Context())
	stop := context.AfterFunc(parent, cancel)
	defer stop()
	defer cancel()
	defer r.revoke(p.grants)
	store := projection.NewFenced(r.cfg.Pool, p.projectionID, p.grants)
	parts := make([]int32, 0, len(p.grants))
	for _, g := range p.grants {
		parts = append(parts, g.Identity.Partition)
	}
	cfg := r.cfg.Live
	cfg.PricePartitions = parts
	cfg.SnapshotID = ""
	connect := func(c context.Context, pair string) (live.QuoteConnection, error) {
		return priceconnection.Connect(c, pair, pricefeed.PairPartition(pair), func(c context.Context, pair string, part int32) (priceconnection.Registration, error) {
			return a.Register(c, pair, part)
		}, r.cfg.Latest, time.Now)
	}
	var view *live.Coordinator
	_, err := store.Recover(ctx)
	if errors.Is(err, projection.ErrRecovery) {
		cfg.SnapshotID, err = r.cfg.Snapshot(ctx, p.projectionID)
		if err != nil {
			return err
		}
		view, err = live.New(store, r.cfg.Replay, func(c context.Context) (live.Registration, error) { return r.cfg.Changes(c, nil) }, connect, cfg)
	} else if err == nil {
		view, err = live.NewResume(store, r.cfg.Replay, r.cfg.Changes, connect, cfg)
	}
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.assignment = a
	r.active = p
	r.view = view
	r.recovered = false
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.recovered = false
		r.view = nil
		r.assignment = nil
		r.active = nil
		r.mu.Unlock()
	}()
	stopped := make(chan error, 1)
	go func() { stopped <- view.Run(ctx) }()
	// Wait for the view worker on every exit. Cancellation also joins its quote
	// readers before any subsequent generation is used by this replica.
	defer func() {
		cancel()
		e := <-stopped
		if result == nil {
			result = e
		}
	}()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	wait := func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-stopped:
			stopped <- e
			return e
		case <-view.Changed():
			return nil
		case <-ticker.C:
			return nil
		}
	}
	for !view.Ready() {
		if e := wait(); e != nil {
			return e
		}
	}
	workers := map[int32]*partition{}
	for _, grant := range p.grants {
		w := pricework.NewFenced(r.cfg.Pool, grant)
		workers[grant.Identity.Partition] = &partition{work: w, handler: pricehandler.New(w, view, store, r.cfg.Delivery, p.projectionID), next: p.starts[grant.Identity.Partition]}
	}
	retry := func(f func() error) error {
		for {
			if e := ctx.Err(); e != nil {
				return e
			}
			e := f()
			if !errors.Is(e, live.ErrNotReady) && !errors.Is(e, pricehandler.ErrViewChanged) {
				return e
			}
			if e = wait(); e != nil {
				return e
			}
		}
	}
	// Recovery has no transport ack. Empty accepted sets need no active price
	// connection; candidates use MAT-005's canonical recovery path.
	for _, w := range workers {
		keys, e := w.work.Pending(ctx)
		if e != nil {
			return e
		}
		for _, key := range keys {
			old, e := w.work.Get(ctx, key)
			if e != nil {
				return e
			}
			if len(old.Entries) == 0 {
				e = w.work.Complete(ctx, key, nil, nil, nil)
			} else {
				e = retry(func() error { return w.handler.Resume(ctx, key, nil, nil) })
			}
			if e != nil {
				return e
			}
		}
	}
	evaluate := func() error {
		pairs, e := view.ActivePairs()
		if e != nil {
			return e
		}
		for _, pair := range pairs {
			w := workers[pricefeed.PairPartition(pair)]
			if w == nil {
				return ErrConfig
			}
			sel, e := view.Current(pair)
			if e != nil {
				return e
			}
			key := "view:" + digest(struct {
				Projection, Pair string
				C                uint64
				Revision         any
			}{p.projectionID, pair, sel.C, sel.Quote.Revision})
			if e = retry(func() error { return w.handler.Handle(ctx, pricehandler.Request{Key: key, Pair: pair}, nil) }); e != nil {
				return e
			}
			// Keep the accepted request across view races, then evaluate a fresh key
			// if completion superseded it or subscriptions changed during its RPCs.
			again, e := view.Current(pair)
			if e != nil {
				return e
			}
			if again.C != sel.C || again.Quote.Revision != sel.Quote.Revision {
				return pricehandler.ErrViewChanged
			}
		}
		return nil
	}
	if e := retry(evaluate); e != nil {
		return e
	}
	r.mu.Lock()
	r.recovered = true
	r.mu.Unlock()
	// Only this reader consumes the full assignment. A single buffered delivery
	// keeps Changed responsive without unbounded prefetch or a second pair reader.
	type delivery struct {
		u   pricefeed.Update
		err error
	}
	updates := make(chan delivery, 1)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			u, e := a.Next(ctx)
			select {
			case updates <- delivery{u, e}:
			case <-ctx.Done():
				return
			}
			if e != nil {
				return
			}
		}
	}()
	defer func() { cancel(); <-readerDone }()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-stopped:
			stopped <- e
			return e
		case <-view.Changed():
			if e := retry(evaluate); e != nil {
				return e
			}
		case d := <-updates:
			if d.err != nil {
				return d.err
			}
			w := workers[d.u.Position.Partition]
			if w == nil || pricefeed.PairPartition(d.u.Quote.Pair) != d.u.Position.Partition {
				return ErrConfig
			}
			if e := retry(func() error { return w.process(ctx, d.u, view, store, p.projectionID, a) }); e != nil {
				return e
			}
			if e := retry(evaluate); e != nil {
				return e
			}
		}
	}
}

type partition struct {
	work    *pricework.Store
	handler *pricehandler.Handler
	next    int64
}

func (w *partition) process(ctx context.Context, u pricefeed.Update, view *live.Coordinator, store *projection.Store, id string, a *pricefeed.Assignment) error {
	pos := pricework.Position{Topic: u.Position.Topic, Partition: u.Position.Partition, Offset: u.Position.Offset}
	if pos.Offset < w.next {
		return ErrConfig
	}
	prefix := &pricework.Prefix{Topic: pos.Topic, Partition: pos.Partition, From: w.next, Next: pos.Offset + 1}
	key := TransportKey(pos)
	old, e := w.work.Get(ctx, key)
	if e == nil {
		if old.Input.Position == nil || *old.Input.Position != pos || old.Input.Quote.Pair != u.Quote.Pair || old.Input.Quote.Revision != u.Quote.Revision {
			return pricework.ErrConflict
		}
		// Retain immutable candidates even if the last pair connection has gone.
		if old.State != "pending" || len(old.Entries) > 0 {
			e = w.handler.Resume(ctx, key, prefix, nil)
		} else {
			e = w.work.Complete(ctx, key, nil, prefix, nil)
		}
	} else if errors.Is(e, pgx.ErrNoRows) {
		pairs, pe := view.ActivePairs()
		if pe != nil {
			return pe
		}
		if slices.Contains(pairs, u.Quote.Pair) {
			// Group dispatch and the live quote reader are independent schedulers. Do
			// not accept an event until the latest merge has reached its revision.
			sel, se := view.Current(u.Quote.Pair)
			if se != nil {
				return se
			}
			newer, se := sel.Quote.NewerThan(u.Quote)
			if se != nil {
				return se
			}
			if !newer && sel.Quote.Revision != u.Quote.Revision {
				return pricehandler.ErrViewChanged
			}
			e = w.handler.Handle(ctx, pricehandler.Request{Key: key, Pair: u.Quote.Pair, Event: &u.Quote, Position: &pos, Prefix: prefix}, nil)
		} else {
			cp, ce := store.Checkpoint(ctx)
			if ce != nil {
				return ce
			}
			// Bracket a second ready absence sample with SQL C reads. A replay
			// that adds this pair must not label an empty set with its newer C.
			pairs, ce = view.ActivePairs()
			if ce != nil {
				return ce
			}
			if slices.Contains(pairs, u.Quote.Pair) {
				return pricehandler.ErrViewChanged
			}
			again, ce := store.Checkpoint(ctx)
			if ce != nil {
				return ce
			}
			if again.C != cp.C {
				return pricehandler.ErrViewChanged
			}
			if !view.Ready() {
				return live.ErrNotReady
			}
			// No subscription exists in this ready full view. Persist the exact input
			// with an empty set instead of dropping the record from the partition proof.
			e = w.work.Accept(ctx, pricework.Input{Key: key, ProjectionID: id, C: cp.C, Quote: u.Quote, Position: &pos})
			if e == nil {
				e = w.work.Complete(ctx, key, nil, prefix, nil)
			}
		}
	}
	if e != nil {
		return e
	}
	if e = a.Confirm(ctx, u.Position); e != nil {
		return fmt.Errorf("replica: confirm durable prefix: %w", e)
	}
	w.next = prefix.Next
	return nil
}
