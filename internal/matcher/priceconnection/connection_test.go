package priceconnection

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricefeed"
)

var now = time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)

type quoteFunc func(context.Context, string) (*pb.LatestResponse, error)

func (f quoteFunc) Latest(ctx context.Context, pair string) (*pb.LatestResponse, error) {
	return f(ctx, pair)
}

type fakeReg struct {
	mu        sync.Mutex
	err       error
	done      chan struct{}
	queue     chan pricefeed.Update
	afterNext func()
}

func newReg() *fakeReg {
	return &fakeReg{done: make(chan struct{}), queue: make(chan pricefeed.Update, 512)}
}
func (r *fakeReg) Done() <-chan struct{} { return r.done }
func (r *fakeReg) Err() error            { r.mu.Lock(); defer r.mu.Unlock(); return r.err }
func (r *fakeReg) fail(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err == nil {
		r.err = err
		close(r.done)
	}
}
func (r *fakeReg) Close() { r.fail(pricefeed.ErrClosed) }
func (r *fakeReg) TryNext() (pricefeed.Update, bool, error) {
	if err := r.Err(); err != nil {
		return pricefeed.Update{}, false, err
	}
	select {
	case u := <-r.queue:
		return u, true, nil
	default:
		return pricefeed.Update{}, false, nil
	}
}
func (r *fakeReg) Next(ctx context.Context) (pricefeed.Update, error) {
	if err := r.Err(); err != nil {
		return pricefeed.Update{}, err
	}
	select {
	case <-ctx.Done():
		return pricefeed.Update{}, ctx.Err()
	case <-r.done:
		return pricefeed.Update{}, r.Err()
	case u := <-r.queue:
		if r.afterNext != nil {
			r.afterNext()
		}
		return u, nil
	}
}
func observed(seq uint64, value string) priceobserved.Observed {
	p, _ := price.Parse(value)
	return priceobserved.Observed{SchemaVersion: 1, Pair: "BTCUSDT", Price: &p, SourceID: value, Revision: priceobserved.Revision{StreamID: "durable", Generation: 1, Sequence: seq}, TradeTime: now.Add(-time.Hour), ReceivedAt: now.Add(-time.Minute), StreamState: priceobserved.Unconfirmed, GapDetected: true}
}
func response(q priceobserved.Observed) *pb.LatestResponse {
	return &pb.LatestResponse{Freshness: pb.Freshness_FRESHNESS_STALE, Quote: &pb.Quote{Pair: q.Pair, Price: q.Price.String(), SourceId: q.SourceID, Revision: &pb.Revision{StreamId: q.Revision.StreamID, Generation: q.Revision.Generation, Sequence: q.Revision.Sequence}, TradeTime: q.TradeTime.Format(time.RFC3339Nano), ReceivedAt: q.ReceivedAt.Format(time.RFC3339Nano), StreamState: string(q.StreamState), GapDetected: q.GapDetected}}
}
func (r *fakeReg) push(q priceobserved.Observed) { r.queue <- pricefeed.Update{Quote: q} }
func connect(t *testing.T, r *fakeReg, read quoteFunc) *Connection {
	t.Helper()
	c, err := Connect(context.Background(), "BTCUSDT", 0, func(context.Context, string, int32) (Registration, error) { return r, nil }, read, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}
func assertQuote(t *testing.T, q priceobserved.Observed, err error, seq uint64, value string) {
	t.Helper()
	if err != nil || q.Revision.Sequence != seq || q.Price == nil || q.Price.String() != value || q.SourceID != value || !q.TradeTime.Equal(now.Add(-time.Hour)) || q.StreamState != priceobserved.Unconfirmed || !q.GapDetected {
		t.Fatalf("quote=%+v err=%v", q, err)
	}
}

func TestBootstrapAndLiveMerge(t *testing.T) {
	for _, phase := range []string{"before", "during", "snapshot-newer", "transient"} {
		t.Run(phase, func(t *testing.T) {
			r := newReg()
			snapshot := observed(1, "99")
			if phase == "before" {
				r.push(observed(2, "101"))
			}
			c := connect(t, r, func(context.Context, string) (*pb.LatestResponse, error) {
				if phase == "during" || phase == "transient" {
					r.push(observed(2, "101"))
				}
				if phase == "transient" {
					r.push(observed(3, "99"))
				}
				if phase == "snapshot-newer" {
					r.push(observed(1, "101"))
					snapshot = observed(3, "99")
				}
				return response(snapshot), nil
			})
			seq, value := uint64(2), "101"
			if phase == "snapshot-newer" || phase == "transient" {
				seq, value = 3, "99"
			}
			q, err := c.Current()
			assertQuote(t, q, err, seq, value)
			// Replay cannot roll back or generate work; only the next greater revision is returned.
			r.push(observed(1, "105"))
			r.push(observed(seq, value))
			r.push(observed(4, "102"))
			q, err = c.Next(context.Background())
			assertQuote(t, q, err, 4, "102")
			// Returned pointers must not let a consumer corrupt future comparisons.
			_ = q.Price.UnmarshalText([]byte("777"))
			q, err = c.Current()
			assertQuote(t, q, err, 4, "102")
		})
	}
}

func TestRegistrationPrecedesLatestAndFailureCancelsBootstrap(t *testing.T) {
	r := newReg()
	registered := false
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := Connect(ctx, "BTCUSDT", 0, func(context.Context, string, int32) (Registration, error) { registered = true; return r, nil }, quoteFunc(func(ctx context.Context, _ string) (*pb.LatestResponse, error) {
		if !registered {
			t.Fatal("Latest before registration")
		}
		r.fail(pricefeed.ErrRevoked)
		<-ctx.Done()
		return response(observed(1, "99")), nil
	}), func() time.Time { return now })
	if c != nil || !errors.Is(err, pricefeed.ErrRevoked) {
		t.Fatalf("c=%v err=%v", c, err)
	}
}

func TestUnavailableAndReconnect(t *testing.T) {
	for _, mode := range []string{"rpc", "missing", "invalid-time", "future", "malformed", "wrong-pair"} {
		t.Run(mode, func(t *testing.T) {
			r := newReg()
			r.push(observed(2, "101"))
			c, err := Connect(context.Background(), "BTCUSDT", 0, func(context.Context, string, int32) (Registration, error) { return r, nil }, quoteFunc(func(context.Context, string) (*pb.LatestResponse, error) {
				res := response(observed(1, "99"))
				switch mode {
				case "rpc":
					return nil, errors.New("unavailable")
				case "missing":
					res.Freshness = pb.Freshness_FRESHNESS_MISSING
				case "invalid-time":
					res.Freshness = pb.Freshness_FRESHNESS_INVALID_TIME
				case "future":
					res.Quote.TradeTime = now.Add(time.Second).Format(time.RFC3339Nano)
				case "malformed":
					res.Quote.Revision = nil
				case "wrong-pair":
					res.Quote.Pair = "ETHUSDT"
				}
				return res, nil
			}), func() time.Time { return now })
			if c != nil || err == nil || r.Err() == nil {
				t.Fatalf("unavailable became ready: %v %v", c, err)
			}
			// A retry uses a new registration and a new Latest, not the abandoned queue.
			fresh := newReg()
			c = connect(t, fresh, func(context.Context, string) (*pb.LatestResponse, error) { return response(observed(3, "99")), nil })
			q, err := c.Current()
			assertQuote(t, q, err, 3, "99")
		})
	}
}

func TestLiveLossAndWaitCancellation(t *testing.T) {
	r := newReg()
	c := connect(t, r, func(context.Context, string) (*pb.LatestResponse, error) { return response(observed(3, "99")), nil })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	q, err := c.Current()
	assertQuote(t, q, err, 3, "99")
	r.push(observed(4, "101"))
	r.fail(pricefeed.ErrContinuity)
	if _, err = c.Current(); !errors.Is(err, pricefeed.ErrContinuity) {
		t.Fatal("still ready", err)
	}
	if _, err = c.Next(context.Background()); !errors.Is(err, pricefeed.ErrContinuity) {
		t.Fatal(err)
	}
}

func TestGenerationAndIncomparableStream(t *testing.T) {
	r := newReg()
	c := connect(t, r, func(context.Context, string) (*pb.LatestResponse, error) { return response(observed(10, "99")), nil })
	q := observed(1, "102")
	q.Revision.Generation = 2
	r.push(q)
	got, err := c.Next(context.Background())
	assertQuote(t, got, err, 1, "102")
	if got.Revision.Generation != 2 {
		t.Fatal(got.Revision)
	}
	q.Revision.StreamID = "other"
	r.push(q)
	if _, err = c.Next(context.Background()); !errors.Is(err, priceobserved.ErrIncomparableRevision) {
		t.Fatal(err)
	}
	if _, err = c.Current(); err == nil {
		t.Fatal("ready after incomparable stream")
	}
}

func TestCancellationAfterConsumptionDoesNotLoseRevision(t *testing.T) {
	r := newReg()
	c := connect(t, r, func(context.Context, string) (*pb.LatestResponse, error) { return response(observed(1, "99")), nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.afterNext = cancel
	r.push(observed(2, "101"))
	q, err := c.Next(ctx)
	assertQuote(t, q, err, 2, "101")
	q, err = c.Current()
	assertQuote(t, q, err, 2, "101")
}

func TestLifetimeAndConcurrentReadiness(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r := newReg()
	c, err := Connect(ctx, "BTCUSDT", 0, func(context.Context, string, int32) (Registration, error) { return r, nil }, quoteFunc(func(context.Context, string) (*pb.LatestResponse, error) { return response(observed(1, "99")), nil }), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for range 1000 {
			_, _ = c.Current()
		}
	}()
	cancel()
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("lifetime not observed")
	}
	if _, err = c.Current(); err == nil {
		t.Fatal("ready after lifetime cancellation")
	}
	<-stopped
}

func TestRegistrationFailureNeverReadsLatest(t *testing.T) {
	_, err := Connect(context.Background(), "BTCUSDT", 0, func(context.Context, string, int32) (Registration, error) { return nil, pricefeed.ErrPosition }, quoteFunc(func(context.Context, string) (*pb.LatestResponse, error) {
		t.Fatal("Latest after registration failure")
		return nil, nil
	}), func() time.Time { return now })
	if !errors.Is(err, pricefeed.ErrPosition) {
		t.Fatal(err)
	}
}
