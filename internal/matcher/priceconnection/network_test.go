package priceconnection

import (
	"context"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/latest"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/grpctest"
	"google.golang.org/grpc"
)

type readerFunc func(context.Context, string) (priceobserved.Observed, error)

func (f readerFunc) Latest(ctx context.Context, pair string) (priceobserved.Observed, error) {
	return f(ctx, pair)
}

func TestLatestClientBootstrapOverLoopback(t *testing.T) {
	r := newReg()
	entered, release := make(chan struct{}), make(chan struct{})
	conn := grpctest.Start(t, func(s *grpc.Server) {
		pb.RegisterQuotesServer(s, latest.NewServer(readerFunc(func(ctx context.Context, pair string) (priceobserved.Observed, error) {
			close(entered)
			select {
			case <-release:
				return observed(1, "99"), nil
			case <-ctx.Done():
				return priceobserved.Observed{}, ctx.Err()
			}
		}), func() time.Time { return now }))
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := make(chan *Connection, 1)
	failed := make(chan error, 1)
	go func() {
		c, err := Connect(ctx, "BTCUSDT", 0, func(context.Context, string, int32) (Registration, error) { return r, nil }, latest.NewClient(conn), func() time.Time { return now })
		if err != nil {
			failed <- err
			return
		}
		result <- c
	}()
	select {
	case <-entered:
	case err := <-failed:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	r.push(observed(2, "101"))
	r.push(observed(3, "99"))
	select {
	case <-result:
		t.Fatal("ready before Latest response")
	default:
	}
	close(release)
	select {
	case c := <-result:
		defer c.Close()
		q, err := c.Current()
		assertQuote(t, q, err, 3, "99")
	case err := <-failed:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
