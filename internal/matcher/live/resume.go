package live

import (
	"context"
	"errors"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/changefeed"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/projection"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrBootstrap = errors.New("live: new snapshot generation required")

type ResumeStore interface {
	Store
	Recover(context.Context) (projection.Recovery, error)
}

// NewResume restores an explicitly selected stable projection ID. SnapshotID
// must be empty: SQL supplies its origin. register(nil) means no Kafka prefix
// has ever committed (register at LSO); otherwise register exactly at *position.
// Config.PricePartitions restricts the restored view, as with New. Caller must
// bind Run to the assignment lifetime and enforce transport/SQL ownership.
func NewResume(store ResumeStore, replay Replay, register func(context.Context, *changefeed.Position) (Registration, error), connect func(context.Context, string) (QuoteConnection, error), cfg Config) (*Coordinator, error) {
	if store == nil || register == nil || cfg.SnapshotID != "" {
		return nil, ErrConfig
	}
	cfg.SnapshotID = "restore-from-sql"
	var position *changefeed.Position
	c, err := New(store, replay, func(ctx context.Context) (Registration, error) {
		r, err := register(ctx, position)
		if err != nil {
			return nil, err
		}
		if r != nil && position != nil && r.Start() != *position {
			r.Close()
			return nil, ErrBootstrap
		}
		return r, nil
	}, connect, cfg)
	if err != nil {
		return nil, err
	}
	c.restore = func(ctx context.Context) error {
		r, err := store.Recover(ctx)
		if err != nil {
			return err
		}
		if r.SnapshotID == "" {
			return ErrBootstrap
		}
		c.cfg.SnapshotID = r.SnapshotID
		if r.Topic != "" {
			position = &changefeed.Position{Topic: r.Topic, Partition: r.Partition, Offset: r.Checkpoint.NextOffset}
		}
		return nil
	}
	return c, nil
}

func bootstrapError(err error) error {
	if errors.Is(err, projection.ErrOwnerMissing) || errors.Is(err, projection.ErrRecovery) || errors.Is(err, projection.ErrSequence) || errors.Is(err, ErrPin) || errors.Is(err, ErrProtocol) || errors.Is(err, changefeed.ErrPosition) || status.Code(err) == codes.FailedPrecondition || status.Code(err) == codes.NotFound {
		return errors.Join(ErrBootstrap, err)
	}
	return err
}
