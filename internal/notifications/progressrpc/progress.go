// Package progressrpc exports only SQL-confirmed intake progress.
package progressrpc

import (
	"context"
	"errors"
	"strings"
	"time"

	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/notifications/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/intake"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const Budget = time.Second
const MaxMessageBytes = 4096

type Reader interface {
	ReadProgress(context.Context, intake.Binding, int32) (intake.Progress, error)
}

func NewPostgres(pool *pgxpool.Pool) *intake.Store { return intake.NewStore(pool) }

type Server struct {
	pb.UnimplementedNotificationsProgressServer
	reader Reader
}

func NewServer(r Reader) *Server { return &Server{reader: r} }
func ServerOptions(cert, key, ca []byte, cleanerURI string) ([]grpc.ServerOption, error) {
	opts, err := serviceauth.ServerOptions(cert, key, ca, map[string][]string{cleanerURI: {pb.NotificationsProgress_ReadProgress_FullMethodName}})
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
	if q == nil || !bounded(q.ConsumerId) || !bounded(q.Generation) || !bounded(q.ClusterId) || !bounded(q.Topic) || len(q.TopicId) != 16 || q.Partition < 0 {
		return nil, status.Error(codes.InvalidArgument, "exact consumer, generation and source required")
	}
	b := intake.Binding{ConsumerID: q.ConsumerId, Generation: q.Generation, Source: kafkasource.Identity{ClusterID: q.ClusterId, Topic: q.Topic}}
	copy(b.Source.TopicID[:], q.TopicId)
	if b.Validate() != nil {
		return nil, status.Error(codes.InvalidArgument, "valid source required")
	}
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	cp, err := s.reader.ReadProgress(ctx, b, q.Partition)
	if ctx.Err() != nil {
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return nil, status.FromContextError(err).Err()
		}
		if errors.Is(err, intake.ErrMissing) || errors.Is(err, intake.ErrUnbound) || errors.Is(err, kafkasource.ErrIdentity) || errors.Is(err, kafkasource.ErrMismatch) {
			return nil, unconfirmed()
		}
		return nil, status.Error(codes.Unavailable, "progress storage unavailable")
	}
	if cp.Binding != b || cp.Partition != q.Partition || cp.Next < 0 {
		return nil, unconfirmed()
	}
	return &pb.ReadProgressResponse{ConsumerId: cp.Binding.ConsumerID, Generation: cp.Binding.Generation, ClusterId: cp.Binding.Source.ClusterID, TopicId: append([]byte(nil), cp.Binding.Source.TopicID[:]...), Topic: cp.Binding.Source.Topic, Partition: cp.Partition, NextOffset: &cp.Next}, nil
}

type Client struct {
	rpc pb.NotificationsProgressClient
}

func NewClient(c grpc.ClientConnInterface) *Client {
	return &Client{pb.NewNotificationsProgressClient(c)}
}

// ReadProgress never retries; any failure forbids cleanup.
func (c *Client) ReadProgress(ctx context.Context, q *pb.ReadProgressRequest) (*pb.ReadProgressResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	return c.rpc.ReadProgress(ctx, q, grpc.MaxCallRecvMsgSize(MaxMessageBytes), grpc.MaxCallSendMsgSize(MaxMessageBytes))
}
