// Package progressrpc exports only SQL-confirmed progress for an exact consumer.
package progressrpc

import (
	"context"
	"errors"
	"strings"
	"time"

	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/matcher/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricework"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/projection"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/sourcebinding"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const Budget = time.Second
const MaxMessageBytes = 4096

type Checkpoint struct {
	Source     *kafkasource.Identity
	Topic      string
	Partition  int32
	NextOffset int64
}
type Reader interface {
	Read(context.Context, pb.ConsumerKind, string, string, int32) (Checkpoint, error)
}
type Postgres struct{ pool *pgxpool.Pool }

func NewPostgres(pool *pgxpool.Pool) *Postgres { return &Postgres{pool} }

// Read uses one statement snapshot per request. No assignment, pin or binding
// is created, and revoked/historical generations remain explicitly addressable.
func (p *Postgres) Read(ctx context.Context, kind pb.ConsumerKind, id, topic string, partition int32) (Checkpoint, error) {
	switch kind {
	case pb.ConsumerKind_CONSUMER_KIND_PROJECTION:
		r, err := projection.New(p.pool, id).Recover(ctx)
		return Checkpoint{r.Checkpoint.Source, r.Topic, r.Partition, r.Checkpoint.NextOffset}, err
	case pb.ConsumerKind_CONSUMER_KIND_PRICES:
		r, err := pricework.New(p.pool, id).ReadCheckpoint(ctx, topic, partition)
		return Checkpoint{r.Source, r.Topic, r.Partition, r.NextOffset}, err
	default:
		return Checkpoint{}, projection.ErrRecovery
	}
}

type Server struct {
	pb.UnimplementedMatcherProgressServer
	reader Reader
}

func NewServer(r Reader) *Server { return &Server{reader: r} }
func ServerOptions(cert, key, ca []byte, cleanerURI string) ([]grpc.ServerOption, error) {
	opts, err := serviceauth.ServerOptions(cert, key, ca, map[string][]string{cleanerURI: {pb.MatcherProgress_ReadProgress_FullMethodName}})
	if err != nil {
		return nil, err
	}
	return append(opts, grpc.WaitForHandlers(true), grpc.MaxRecvMsgSize(MaxMessageBytes), grpc.MaxSendMsgSize(MaxMessageBytes)), nil
}
func bounded(s string) bool { return strings.TrimSpace(s) != "" && len(s) <= 256 }
func unconfirmed() error {
	return status.Error(codes.FailedPrecondition, "consumer checkpoint is not confirmed for this source")
}
func (s *Server) ReadProgress(ctx context.Context, q *pb.ReadProgressRequest) (*pb.ReadProgressResponse, error) {
	if q == nil || !bounded(q.ConsumerId) || !bounded(q.ClusterId) || !bounded(q.Topic) || len(q.TopicId) != 16 || q.Partition < 0 || (q.Kind != pb.ConsumerKind_CONSUMER_KIND_PROJECTION && q.Kind != pb.ConsumerKind_CONSUMER_KIND_PRICES) {
		return nil, status.Error(codes.InvalidArgument, "exact consumer and source required")
	}
	expected := kafkasource.Identity{ClusterID: q.ClusterId, Topic: q.Topic}
	copy(expected.TopicID[:], q.TopicId)
	if expected.Validate() != nil {
		return nil, status.Error(codes.InvalidArgument, "valid source required")
	}
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	cp, err := s.reader.Read(ctx, q.Kind, q.ConsumerId, q.Topic, q.Partition)
	if ctx.Err() != nil {
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return nil, status.FromContextError(err).Err()
		}
		if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, projection.ErrRecovery) || errors.Is(err, sourcebinding.ErrUnbound) || errors.Is(err, kafkasource.ErrIdentity) || errors.Is(err, kafkasource.ErrMismatch) {
			return nil, unconfirmed()
		}
		return nil, status.Error(codes.Unavailable, "progress storage unavailable")
	}
	if cp.Source == nil || *cp.Source != expected || cp.Topic != q.Topic || cp.Partition != q.Partition || cp.NextOffset < 0 {
		return nil, unconfirmed()
	}
	return &pb.ReadProgressResponse{Kind: q.Kind, ConsumerId: q.ConsumerId, ClusterId: cp.Source.ClusterID, TopicId: append([]byte(nil), cp.Source.TopicID[:]...), Topic: cp.Topic, Partition: cp.Partition, NextOffset: &cp.NextOffset}, nil
}

type Client struct{ rpc pb.MatcherProgressClient }

func NewClient(c grpc.ClientConnInterface) *Client { return &Client{pb.NewMatcherProgressClient(c)} }

// ReadProgress adds no retries; an error never authorizes deletion.
func (c *Client) ReadProgress(ctx context.Context, q *pb.ReadProgressRequest) (*pb.ReadProgressResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	return c.rpc.ReadProgress(ctx, q, grpc.MaxCallRecvMsgSize(MaxMessageBytes), grpc.MaxCallSendMsgSize(MaxMessageBytes))
}
