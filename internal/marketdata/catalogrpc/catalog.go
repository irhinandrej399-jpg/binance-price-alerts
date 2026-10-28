// Package catalogrpc exposes the market-data catalog through a bounded unary RPC.
package catalogrpc

import (
	"context"
	"errors"
	"time"

	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/catalog"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const Budget = time.Second
const maxResponseBytes = 16 << 20

// Source is implemented by catalog.Client. Filtering belongs to the source.
type Source interface {
	Fetch(context.Context) (catalog.Snapshot, error)
}

type Server struct {
	pb.UnimplementedCatalogServer
	source Source
}

func NewServer(source Source) *Server { return &Server{source: source} }

// ServerOptions grants only the configured API certificate URI access to Fetch.
// Install all returned options together; for multiple services compose one policy
// with serviceauth.ServerOptions rather than overriding authorization options.
func ServerOptions(cert, key, clientCA []byte, apiURI string) ([]grpc.ServerOption, error) {
	return serviceauth.ServerOptions(cert, key, clientCA, map[string][]string{apiURI: {pb.Catalog_Fetch_FullMethodName}})
}

func (s *Server) Fetch(ctx context.Context, _ *pb.CatalogRequest) (*pb.CatalogResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	snapshot, err := s.source.Fetch(ctx)
	if ctx.Err() != nil {
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil, status.FromContextError(err).Err()
	}
	if err != nil {
		return nil, status.Error(codes.Unavailable, "catalog source unavailable")
	}
	response := &pb.CatalogResponse{UpdatedAt: snapshot.UpdatedAt.UTC().Format(time.RFC3339Nano), Pairs: make([]*pb.CatalogPair, 0, len(snapshot.Pairs))}
	for _, p := range snapshot.Pairs {
		response.Pairs = append(response.Pairs, &pb.CatalogPair{Symbol: p.Symbol, BaseAsset: p.BaseAsset, QuoteAsset: p.QuoteAsset})
	}
	return response, nil
}

type Client struct{ rpc pb.CatalogClient }

// NewClient requires a caller-owned authenticated connection. It is safe for
// concurrent use and holds no cache or connection lifecycle responsibilities.
func NewClient(conn grpc.ClientConnInterface) *Client { return &Client{rpc: pb.NewCatalogClient(conn)} }

// Fetch preserves earlier deadlines, never retries, and returns a zero snapshot
// on failure. A successful empty catalog has its original UpdatedAt.
func (c *Client) Fetch(ctx context.Context) (catalog.Snapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	response, err := c.rpc.Fetch(ctx, &pb.CatalogRequest{}, grpc.MaxCallRecvMsgSize(maxResponseBytes))
	if err != nil {
		return catalog.Snapshot{}, err
	}
	updated, err := time.Parse(time.RFC3339Nano, response.UpdatedAt)
	if err != nil {
		return catalog.Snapshot{}, status.Error(codes.Internal, "invalid catalog response")
	}
	snapshot := catalog.Snapshot{UpdatedAt: updated.UTC(), Pairs: make([]catalog.Pair, 0, len(response.Pairs))}
	for _, p := range response.Pairs {
		snapshot.Pairs = append(snapshot.Pairs, catalog.Pair{Symbol: p.Symbol, BaseAsset: p.BaseAsset, QuoteAsset: p.QuoteAsset})
	}
	return snapshot, nil
}
