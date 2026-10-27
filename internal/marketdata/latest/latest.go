// Package latest exposes the durable market-data quote as a bounded unary RPC.
package latest

import (
	"context"
	"errors"
	"regexp"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/storage"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const Budget = time.Second

var pairPattern = regexp.MustCompile(`^[A-Z0-9]+$`)

// Reader is implemented by storage.Repository; no client reads market-data SQL.
type Reader interface {
	Latest(context.Context, string) (priceobserved.Observed, error)
}
type Server struct {
	pb.UnimplementedQuotesServer
	reader Reader
	now    func() time.Time
}

// NewServer requires a durable reader and a clock. Before serving after restart,
// ingest must call BeginGeneration for each pair; see storage's lifecycle contract.
func NewServer(reader Reader, now func() time.Time) *Server { return &Server{reader: reader, now: now} }

// ServerOptions grants the configured API and matcher identities this RPC.
// Install the complete options together; the URI comes from trusted configuration.
func ServerOptions(cert, key, clientCA []byte, apiURI, matcherURI string) ([]grpc.ServerOption, error) {
	return serviceauth.ServerOptions(cert, key, clientCA, map[string][]string{apiURI: {pb.Quotes_Latest_FullMethodName}, matcherURI: {pb.Quotes_Latest_FullMethodName}})
}

func (s *Server) Latest(ctx context.Context, req *pb.LatestRequest) (*pb.LatestResponse, error) {
	if req == nil || !pairPattern.MatchString(req.Pair) {
		return nil, status.Error(codes.InvalidArgument, "canonical pair required")
	}
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	q, err := s.reader.Latest(ctx, req.Pair)
	if ctx.Err() != nil {
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil, status.FromContextError(err).Err()
	}
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return nil, status.Error(codes.Unavailable, "quote storage unavailable")
	}
	checked := s.now().UTC()
	fresh := price.CheckFreshness(err == nil && q.Price != nil, q.TradeTime, checked)
	r := &pb.LatestResponse{CheckedAt: checked.Format(time.RFC3339Nano)}
	switch fresh {
	case price.Missing:
		r.Freshness = pb.Freshness_FRESHNESS_MISSING
		return r, nil
	case price.InvalidTime:
		r.Freshness = pb.Freshness_FRESHNESS_INVALID_TIME
		return r, nil
	case price.Fresh:
		r.Freshness = pb.Freshness_FRESHNESS_FRESH
	case price.Stale:
		r.Freshness = pb.Freshness_FRESHNESS_STALE
		r.Warnings = append(r.Warnings, pb.Warning_WARNING_STALE)
	}
	if q.StreamState != priceobserved.Live {
		r.Warnings = append(r.Warnings, pb.Warning_WARNING_STREAM_UNCONFIRMED)
	}
	if q.GapDetected {
		r.Warnings = append(r.Warnings, pb.Warning_WARNING_GAP_DETECTED)
	}
	r.Quote = &pb.Quote{Pair: q.Pair, Price: q.Price.String(), SourceId: q.SourceID, Revision: &pb.Revision{StreamId: q.Revision.StreamID, Generation: q.Revision.Generation, Sequence: q.Revision.Sequence}, TradeTime: q.TradeTime.UTC().Format(time.RFC3339Nano), ReceivedAt: q.ReceivedAt.UTC().Format(time.RFC3339Nano), StreamState: string(q.StreamState), GapDetected: q.GapDetected}
	// Split seconds/nanos avoids time.Duration saturation for very old quotes.
	seconds := checked.Unix() - q.TradeTime.Unix()
	nanos := checked.Nanosecond() - q.TradeTime.Nanosecond()
	if nanos < 0 {
		seconds--
		nanos += 1000000000
	}
	r.Age = &pb.Age{Seconds: seconds, Nanos: int32(nanos)}
	return r, nil
}

type Client struct{ rpc pb.QuotesClient }

// NewClient uses an authenticated connection configured by the caller.
func NewClient(conn grpc.ClientConnInterface) *Client { return &Client{rpc: pb.NewQuotesClient(conn)} }

// Latest preserves earlier caller deadlines and never adds application retries.
func (c *Client) Latest(ctx context.Context, pair string) (*pb.LatestResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	return c.rpc.Latest(ctx, &pb.LatestRequest{Pair: pair})
}
