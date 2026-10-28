package stream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/url"
	"slices"
	"sync"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/catalog"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/streamplan"
)

// ReconcileConfig requires a bare /ws endpoint (optional timeUnit query), so no
// URL subscription can escape ownership of the catalog plan. AckTimeout includes
// scheduler queueing and must leave room for the shared command/pong budget.
type ReconcileConfig struct {
	Endpoint   string
	Limit      int
	Transport  Config
	Retry      RetryConfig
	AckTimeout time.Duration
}

// Reconciler owns a single feed's connections. Reconcile calls are serialized.
// handle and disconnected may run concurrently across groups and must honor ctx.
// A terminal handle error stops all groups; transport/ACK errors retry indefinitely.
type Reconciler struct {
	ctx          context.Context
	cancel       context.CancelCauseFunc
	config       ReconcileConfig
	handle       func(context.Context, []byte) error
	disconnected func(int, error, time.Duration)
	gate         chan struct{}
	groups       []*planGroup
}

func NewReconciler(ctx context.Context, c ReconcileConfig, handle func(context.Context, []byte) error, disconnected func(int, error, time.Duration)) (*Reconciler, error) {
	u, err := url.Parse(c.Endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "ws" && u.Scheme != "wss") || u.User != nil || u.Fragment != "" || u.Path != "/ws" || u.RawPath != "" {
		return nil, errors.New("stream: reconciler requires a bare /ws endpoint")
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, errors.New("stream: invalid endpoint query")
	}
	for key, values := range query {
		if key != "timeUnit" || len(values) != 1 || (values[0] != "MICROSECOND" && values[0] != "microsecond") {
			return nil, errors.New("stream: unsupported endpoint query")
		}
	}
	if _, err := streamplan.Build(catalog.Snapshot{}, c.Limit); err != nil {
		return nil, err
	}
	if _, err := c.Transport.interval(); err != nil {
		return nil, err
	}
	if c.Retry.InitialBackoff <= 0 || c.Retry.MaxBackoff < c.Retry.InitialBackoff || c.AckTimeout <= 0 || handle == nil || disconnected == nil {
		return nil, errors.New("stream: invalid reconciler configuration")
	}
	ctx, cancel := context.WithCancelCause(ctx)
	r := &Reconciler{ctx: ctx, cancel: cancel, config: c, handle: handle, disconnected: disconnected, gate: make(chan struct{}, 1)}
	r.gate <- struct{}{}
	return r, nil
}

// Reconcile applies one successful catalog snapshot. All removals are confirmed
// before additions, including moves between groups. Return means subscription ACKs,
// not confirmed live data or gap recovery. On caller cancellation, partial progress
// remains safe; call again to converge. The lifetime context/Close stops workers.
func (r *Reconciler) Reconcile(ctx context.Context, snapshot catalog.Snapshot) error {
	plan, err := streamplan.Build(snapshot, r.config.Limit)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.ctx.Done():
		return context.Cause(r.ctx)
	case <-r.gate:
	}
	defer func() { r.gate <- struct{}{} }()
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.ctx.Err() != nil {
		return context.Cause(r.ctx)
	}
	// Each existing group first retains only streams that remain at its index.
	for i, g := range r.groups {
		var target []string
		if i < len(plan) {
			target = plan[i]
		}
		g.mu.Lock()
		var retained []string
		for _, name := range g.desired {
			if _, found := slices.BinarySearch(target, name); found {
				retained = append(retained, name)
			}
		}
		g.setLocked(retained)
		g.mu.Unlock()
	}
	for _, g := range r.groups {
		if err := g.wait(ctx); err != nil {
			return err
		}
	}
	for len(r.groups) > len(plan) {
		i := len(r.groups) - 1
		r.groups[i].stop()
		r.groups = r.groups[:i]
	}
	for i, names := range plan {
		if i == len(r.groups) {
			r.groups = append(r.groups, r.startGroup(i, names))
		} else {
			g := r.groups[i]
			g.mu.Lock()
			g.setLocked(names)
			g.mu.Unlock()
		}
	}
	for _, g := range r.groups {
		if err := g.wait(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Close cancels the owner and waits for every socket, writer and retry loop.
// Callbacks must cooperate with cancellation, as for Reconnect.
func (r *Reconciler) Close() {
	r.cancel(context.Canceled)
	<-r.gate
	defer func() { r.gate <- struct{}{} }()
	for _, g := range r.groups {
		g.stop()
	}
	r.groups = nil
}

type planGroup struct {
	owner     *Reconciler
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	mu        sync.Mutex
	desired   []string
	version   uint64
	confirmed uint64
	changed   chan struct{}
	wake      chan struct{}
}

func (r *Reconciler) startGroup(index int, names []string) *planGroup {
	ctx, cancel := context.WithCancel(r.ctx)
	g := &planGroup{owner: r, ctx: ctx, cancel: cancel, done: make(chan struct{}), changed: make(chan struct{}), wake: make(chan struct{}, 1)}
	g.setLocked(names)
	go func() {
		defer close(g.done)
		_ = retryConnections(ctx, r.config.Retry, realClock{}, rand.Int64N, func() (bool, error) {
			err := g.attempt()
			return r.ctx.Err() == nil, err
		}, func(err error, d time.Duration) { r.disconnected(index, err, d) })
	}()
	return g
}
func (g *planGroup) signalLocked() { close(g.changed); g.changed = make(chan struct{}) }
func (g *planGroup) setLocked(names []string) {
	if slices.Equal(g.desired, names) {
		return
	}
	g.desired = slices.Clone(names)
	g.version++
	g.signalLocked()
	select {
	case g.wake <- struct{}{}:
	default:
	}
}
func (g *planGroup) wait(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if g.ctx.Err() != nil {
			return context.Cause(g.ctx)
		}
		g.mu.Lock()
		ready := g.confirmed == g.version
		changed := g.changed
		g.mu.Unlock()
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-g.ctx.Done():
			return context.Cause(g.ctx)
		case <-changed:
		}
	}
}
func (g *planGroup) stop() { g.cancel(); <-g.done }

type subscriptionCommand struct {
	Method string   `json:"method"`
	Params []string `json:"params"`
	ID     int64    `json:"id"`
}
type commandReply struct {
	id  int64
	err error
}

func (g *planGroup) attempt() error {
	r := g.owner
	ctx, cancel := context.WithCancelCause(g.ctx)
	defer cancel(nil)
	commands := make(chan []byte)
	replies := make(chan commandReply, 1)
	commandDone := make(chan struct{})
	// Confirmation belongs to this connection only. Do not expose the old ACK
	// during backoff or on a new connection.
	g.mu.Lock()
	g.confirmed = 0
	g.signalLocked()
	g.mu.Unlock()
	go func() {
		defer close(commandDone)
		if err := g.applyCommands(ctx, commands, replies); err != nil {
			cancel(err)
		}
	}()
	err := readWithCommands(ctx, r.config.Endpoint, r.config.Transport, commands, func(ctx context.Context, b []byte) error {
		if isServerShutdown(b) {
			return ErrRotation
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal(b, &fields) == nil {
			if id, ok := fields["id"]; ok {
				reply := commandReply{}
				if err := json.Unmarshal(id, &reply.id); err != nil || bytes.Equal(bytes.TrimSpace(id), []byte("null")) {
					return errors.New("stream: invalid command reply id")
				}
				result, ok := fields["result"]
				if _, failed := fields["code"]; failed || !ok || !bytes.Equal(bytes.TrimSpace(result), []byte("null")) {
					reply.err = errors.New("stream: subscription command rejected")
				}
				select {
				case replies <- reply:
					return nil
				case <-ctx.Done():
					return context.Cause(ctx)
				default:
					return errors.New("stream: unsolicited command replies overflow")
				}
			}
			if _, failed := fields["code"]; failed {
				return errors.New("stream: command error without id")
			}
		}
		err := r.handle(ctx, b)
		if err != nil {
			if ctx.Err() != nil {
				return context.Cause(ctx)
			}
			if g.ctx.Err() == nil {
				r.cancel(err)
			}
		}
		return err
	}, newRotationTimer)
	cancel(err)
	<-commandDone
	g.mu.Lock()
	g.confirmed = 0
	g.signalLocked()
	g.mu.Unlock()
	// A command timeout/rejection cancels the attempt context, retaining its cause.
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return err
}

func (g *planGroup) applyCommands(ctx context.Context, commands chan<- []byte, replies <-chan commandReply) error {
	var applied []string
	var id int64
	for {
		g.mu.Lock()
		desired := slices.Clone(g.desired)
		version := g.version
		g.mu.Unlock()
		for _, step := range []struct {
			method string
			names  []string
		}{{"UNSUBSCRIBE", difference(applied, desired)}, {"SUBSCRIBE", difference(desired, applied)}} {
			if len(step.names) == 0 {
				continue
			}
			id++
			b, err := json.Marshal(subscriptionCommand{step.method, step.names, id})
			if err != nil {
				return err
			}
			timer := time.NewTimer(g.owner.config.AckTimeout)
			err = sendAndConfirm(ctx, commands, replies, b, id, timer.C)
			timer.Stop()
			if err != nil {
				return err
			}
		}
		applied = desired
		g.mu.Lock()
		if g.version == version {
			g.confirmed = version
			g.signalLocked()
		}
		changed := g.version != version
		g.mu.Unlock()
		if changed {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-g.wake:
		}
	}
}
func sendAndConfirm(ctx context.Context, commands chan<- []byte, replies <-chan commandReply, b []byte, id int64, timeout <-chan time.Time) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timeout:
		return errors.New("stream: command queue timeout")
	case commands <- b:
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			return errors.New("stream: command ACK timeout")
		case reply := <-replies:
			if reply.id != id {
				return fmt.Errorf("stream: unexpected command reply id %d, want %d", reply.id, id)
			}
			return reply.err
		}
	}
}
func difference(a, b []string) []string {
	var result []string
	for _, name := range a {
		if _, found := slices.BinarySearch(b, name); !found {
			result = append(result, name)
		}
	}
	return result
}
