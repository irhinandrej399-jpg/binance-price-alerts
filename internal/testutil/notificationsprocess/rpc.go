package notificationsprocess

import (
	"context"
	"google.golang.org/grpc"
	"sync"
	"time"
)

// Pause is a single-use gate. Entered closes at the requested boundary; Release
// is idempotent. Context/deadline and cleanup always unblock the server.
type Pause struct {
	Entered chan struct{}
	done    chan struct{}
	once    sync.Once
}

func (p *Pause) Release() { p.once.Do(func() { close(p.done) }) }
func (p *Pause) wait(ctx context.Context) {
	close(p.Entered)
	timer := time.NewTimer(20 * time.Second)
	defer timer.Stop()
	select {
	case <-p.done:
	case <-ctx.Done():
	case <-timer.C:
	}
}

type gate struct {
	method string
	after  bool
	p      *Pause
	used   bool
}
type rpcGates struct {
	mu    sync.Mutex
	items []*gate
}

// PauseRPC gates one matching call before storage, or after a successful adapter
// response (therefore after durable commit for mutations), before wire response.
func (f *Fixture) PauseRPC(method string, after bool) *Pause {
	p := &Pause{Entered: make(chan struct{}), done: make(chan struct{})}
	f.gates.mu.Lock()
	defer f.gates.mu.Unlock()
	f.gates.items = append(f.gates.items, &gate{method: method, after: after, p: p})
	return p
}
func (g *rpcGates) intercept(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	var selected *gate
	g.mu.Lock()
	for _, item := range g.items {
		if item.method == info.FullMethod && !item.used {
			item.used = true
			selected = item
			break
		}
	}
	g.mu.Unlock()
	if selected != nil && !selected.after {
		selected.p.wait(ctx)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	response, e := next(ctx, req)
	if selected != nil && selected.after && e == nil {
		selected.p.wait(ctx)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return response, e
}
func (g *rpcGates) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, item := range g.items {
		item.p.Release()
	}
}
