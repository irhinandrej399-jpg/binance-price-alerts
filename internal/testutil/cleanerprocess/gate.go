package cleanerprocess

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Barrier coordinates test actions (including future registry mutations). It
// contains no inventory, registration or retirement policy. One arrival only.
type Barrier struct {
	Entered        chan struct{}
	done           chan struct{}
	enter, release sync.Once
	mu             sync.Mutex
	failed         bool
}

func NewBarrier() *Barrier  { return &Barrier{Entered: make(chan struct{}), done: make(chan struct{})} }
func (p *Barrier) Release() { p.release.Do(func() { close(p.done) }) }

// Fail drops the held RPC response. Release is safe before/after Fail.
func (p *Barrier) Fail() { p.mu.Lock(); p.failed = true; p.mu.Unlock(); p.Release() }
func (p *Barrier) Arrive(ctx context.Context) error {
	p.enter.Do(func() { close(p.Entered) })
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	select {
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	case <-p.done:
	}
	if ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failed {
		return status.Error(codes.Unavailable, "fixture response lost")
	}
	return nil
}

type gate struct {
	after bool
	p     *Barrier
}
type gates struct {
	mu    sync.Mutex
	queue []gate
	all   []*Barrier
}

func (g *gates) pause(after bool) *Barrier {
	p := NewBarrier()
	g.mu.Lock()
	defer g.mu.Unlock()
	g.queue = append(g.queue, gate{after, p})
	g.all = append(g.all, p)
	return p
}
func (g *gates) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, p := range g.all {
		p.Release()
	}
}
func (g *gates) intercept(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	var item *gate
	g.mu.Lock()
	if len(g.queue) > 0 {
		v := g.queue[0]
		g.queue = g.queue[1:]
		item = &v
	}
	g.mu.Unlock()
	if item != nil && !item.after {
		if e := item.p.Arrive(ctx); e != nil {
			return nil, e
		}
	}
	out, e := next(ctx, req)
	if item != nil && item.after && e == nil {
		if e = item.p.Arrive(ctx); e != nil {
			return nil, e
		}
	}
	return out, e
}
