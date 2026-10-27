package latest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/latest"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/storage"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/trade"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type reader func(context.Context, string) (priceobserved.Observed, error)

func (f reader) Latest(ctx context.Context, pair string) (priceobserved.Observed, error) {
	return f(ctx, pair)
}

var now = time.Date(2026, 10, 27, 12, 0, 0, 123456789, time.UTC)

func quote() priceobserved.Observed {
	p, _ := price.Parse("123.000000000000000001")
	return priceobserved.Observed{SchemaVersion: 1, Pair: "BTCUSDT", Price: &p, SourceID: "42", Revision: priceobserved.Revision{StreamID: trade.StreamID, Generation: 9, Sequence: 8}, TradeTime: now.Add(-time.Second), ReceivedAt: now, StreamState: priceobserved.Live}
}
func TestFreshnessAndStreamWarnings(t *testing.T) {
	for _, tc := range []struct {
		name     string
		age      time.Duration
		state    priceobserved.StreamState
		gap      bool
		fresh    pb.Freshness
		warnings []pb.Warning
	}{
		{"new", 0, priceobserved.Live, false, pb.Freshness_FRESHNESS_FRESH, nil},
		{"five seconds", 5 * time.Second, priceobserved.Live, false, pb.Freshness_FRESHNESS_FRESH, nil},
		{"stale by nanosecond", 5*time.Second + 1, priceobserved.Live, false, pb.Freshness_FRESHNESS_STALE, []pb.Warning{pb.Warning_WARNING_STALE}},
		{"late reception", time.Minute, priceobserved.Live, false, pb.Freshness_FRESHNESS_STALE, []pb.Warning{pb.Warning_WARNING_STALE}},
		{"restart or disconnect", time.Second, priceobserved.Unconfirmed, false, pb.Freshness_FRESHNESS_FRESH, []pb.Warning{pb.Warning_WARNING_STREAM_UNCONFIRMED}},
		{"old and unconfirmed", time.Minute, priceobserved.Unconfirmed, true, pb.Freshness_FRESHNESS_STALE, []pb.Warning{pb.Warning_WARNING_STALE, pb.Warning_WARNING_STREAM_UNCONFIRMED, pb.Warning_WARNING_GAP_DETECTED}},
		{"historical gap while live", time.Second, priceobserved.Live, true, pb.Freshness_FRESHNESS_FRESH, []pb.Warning{pb.Warning_WARNING_GAP_DETECTED}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := quote()
			q.TradeTime = now.Add(-tc.age)
			q.StreamState = tc.state
			q.GapDetected = tc.gap
			s := latest.NewServer(reader(func(ctx context.Context, pair string) (priceobserved.Observed, error) {
				if pair != "BTCUSDT" {
					t.Fatal(pair)
				}
				if _, ok := ctx.Deadline(); !ok {
					t.Error("missing storage deadline")
				}
				return q, nil
			}), func() time.Time { return now })
			r, err := s.Latest(context.Background(), &pb.LatestRequest{Pair: "BTCUSDT"})
			if err != nil {
				t.Fatal(err)
			}
			if r.Freshness != tc.fresh || len(r.Warnings) != len(tc.warnings) {
				t.Fatalf("%v", r)
			}
			for i, w := range tc.warnings {
				if r.Warnings[i] != w {
					t.Fatalf("%v", r.Warnings)
				}
			}
			if r.Quote.Price != "123.000000000000000001" || r.Quote.SourceId != "42" || r.Quote.Revision.Generation != 9 || r.Quote.Revision.Sequence != 8 || r.Quote.Revision.StreamId != trade.StreamID || r.Quote.TradeTime != q.TradeTime.Format(time.RFC3339Nano) || r.Quote.ReceivedAt != now.Format(time.RFC3339Nano) || r.CheckedAt != now.Format(time.RFC3339Nano) {
				t.Fatalf("lost snapshot: %v", r)
			}
			if r.Age.Seconds != int64(tc.age/time.Second) || r.Age.Nanos != int32(tc.age%time.Second) {
				t.Fatalf("age: %v", r.Age)
			}
		})
	}
}
func TestUnavailableQuotesAndErrors(t *testing.T) {
	for _, tc := range []struct {
		name  string
		q     priceobserved.Observed
		err   error
		fresh pb.Freshness
		code  codes.Code
	}{
		{"missing", priceobserved.Observed{}, storage.ErrNotFound, pb.Freshness_FRESHNESS_MISSING, codes.OK},
		{"future", func() priceobserved.Observed { q := quote(); q.TradeTime = now.Add(time.Nanosecond); return q }(), nil, pb.Freshness_FRESHNESS_INVALID_TIME, codes.OK},
		{"absent time", func() priceobserved.Observed { q := quote(); q.TradeTime = time.Time{}; return q }(), nil, pb.Freshness_FRESHNESS_INVALID_TIME, codes.OK},
		{"out of range", func() priceobserved.Observed {
			q := quote()
			q.TradeTime = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
			return q
		}(), nil, pb.Freshness_FRESHNESS_INVALID_TIME, codes.OK},
		{"database down", priceobserved.Observed{}, errors.New("private DSN"), 0, codes.Unavailable},
		{"deadline", priceobserved.Observed{}, context.DeadlineExceeded, 0, codes.DeadlineExceeded},
		{"cancel", priceobserved.Observed{}, context.Canceled, 0, codes.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := latest.NewServer(reader(func(context.Context, string) (priceobserved.Observed, error) { return tc.q, tc.err }), func() time.Time { return now })
			r, err := s.Latest(context.Background(), &pb.LatestRequest{Pair: "BTCUSDT"})
			if status.Code(err) != tc.code {
				t.Fatalf("%v", err)
			}
			if err != nil {
				if status.Convert(err).Message() == "private DSN" {
					t.Fatal("error leaked")
				}
				return
			}
			if r.Freshness != tc.fresh || r.Quote != nil || r.Age != nil {
				t.Fatalf("%v", r)
			}
		})
	}
}
func TestAgeChangesWithoutNewTrade(t *testing.T) {
	q := quote()
	clock := now
	s := latest.NewServer(reader(func(context.Context, string) (priceobserved.Observed, error) { return q, nil }), func() time.Time { return clock })
	a, _ := s.Latest(context.Background(), &pb.LatestRequest{Pair: "BTCUSDT"})
	clock = clock.Add(10 * time.Second)
	b, _ := s.Latest(context.Background(), &pb.LatestRequest{Pair: "BTCUSDT"})
	if a.Freshness != pb.Freshness_FRESHNESS_FRESH || b.Freshness != pb.Freshness_FRESHNESS_STALE || a.Quote.TradeTime != b.Quote.TradeTime || a.Quote.Revision.Sequence != b.Quote.Revision.Sequence {
		t.Fatalf("%v %v", a, b)
	}
}
func TestInvalidRequestDoesNotReadStorage(t *testing.T) {
	s := latest.NewServer(reader(func(context.Context, string) (priceobserved.Observed, error) {
		t.Fatal("storage called")
		return quote(), nil
	}), time.Now)
	for _, req := range []*pb.LatestRequest{nil, {}, {Pair: "btcusdt"}, {Pair: "BTC-USDT"}, {Pair: "BTCUSDT\n"}} {
		if _, err := s.Latest(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("%v", err)
		}
	}
}

func TestServerCapsUnboundedCaller(t *testing.T) {
	s := latest.NewServer(reader(func(ctx context.Context, _ string) (priceobserved.Observed, error) {
		<-ctx.Done()
		return priceobserved.Observed{}, ctx.Err()
	}), time.Now)
	started := time.Now()
	_, err := s.Latest(context.Background(), &pb.LatestRequest{Pair: "BTCUSDT"})
	if status.Code(err) != codes.DeadlineExceeded || time.Since(started) > 3*time.Second {
		t.Fatalf("unbounded server: %v", err)
	}
}
func TestVeryOldQuoteAgeDoesNotOverflow(t *testing.T) {
	q := quote()
	q.TradeTime = time.Date(1700, 1, 1, 0, 0, 0, 999999999, time.UTC)
	s := latest.NewServer(reader(func(context.Context, string) (priceobserved.Observed, error) { return q, nil }), func() time.Time { return now })
	r, err := s.Latest(context.Background(), &pb.LatestRequest{Pair: "BTCUSDT"})
	if err != nil || r.Age.Seconds != now.Unix()-q.TradeTime.Unix()-1 || r.Age.Nanos != 123456790 || r.Freshness != pb.Freshness_FRESHNESS_STALE {
		t.Fatalf("age overflow: %v %v", r, err)
	}
}
