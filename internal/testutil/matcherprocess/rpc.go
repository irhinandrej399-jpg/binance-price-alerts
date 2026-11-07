package matcherprocess

import (
	"context"
	"sync"
	"time"

	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	mpb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/latest"
	"google.golang.org/grpc"
)

func registerLatest(g *grpc.Server, q *quotes) {
	mpb.RegisterQuotesServer(g, latest.NewServer(q, time.Now))
}

// DeliveryPause is single-use. Committed is signalled only AFTER the API has
// durably accepted the response; Release remains safe after child cancellation.
type DeliveryPause struct {
	Committed chan *pb.RequestDeliveryResponse
	done      chan struct{}
	once      sync.Once
}

func (p *DeliveryPause) Release() { p.once.Do(func() { close(p.done) }) }

type deliveryGate struct {
	pb.UnimplementedDeliveryServer
	base  pb.DeliveryServer
	mu    sync.Mutex
	id    string
	pause *DeliveryPause
}

func (f *Fixture) PauseDelivery(id string) *DeliveryPause {
	f.delivery.mu.Lock()
	defer f.delivery.mu.Unlock()
	if f.delivery.pause != nil {
		panic("fixture delivery pause already installed")
	}
	p := &DeliveryPause{Committed: make(chan *pb.RequestDeliveryResponse, 1), done: make(chan struct{})}
	f.delivery.id, f.delivery.pause = id, p
	return p
}
func (g *deliveryGate) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.pause != nil {
		g.pause.Release()
	}
}
func (g *deliveryGate) RequestDelivery(ctx context.Context, r *pb.RequestDeliveryRequest) (*pb.RequestDeliveryResponse, error) {
	response, err := g.base.RequestDelivery(ctx, r)
	g.mu.Lock()
	p := g.pause
	hold := p != nil && r.SubscriptionId == g.id
	g.mu.Unlock()
	if hold && err == nil && response.Workflow != nil {
		select {
		case p.Committed <- response:
		default:
		}
		// Bound the deliberate hold even if the test forgets to release. Server.Stop
		// and client cancellation also unblock; no goroutine survives teardown.
		timer := time.NewTimer(20 * time.Second)
		defer timer.Stop()
		select {
		case <-p.done:
		case <-ctx.Done():
		case <-timer.C:
		}
	}
	return response, err
}
