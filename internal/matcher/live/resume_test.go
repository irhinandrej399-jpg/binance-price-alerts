package live

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/changefeed"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/projection"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type recoveryStore struct {
	*memoryStore
	recovery projection.Recovery
	err      error
}

func (s recoveryStore) Recover(context.Context) (projection.Recovery, error) {
	return s.recovery, s.err
}
func (s recoveryStore) Initialize(context.Context, string) error {
	panic("restart must not initialize snapshot")
}

type resumeFeed struct {
	*lostRegistration
	start changefeed.Position
}

func (r resumeFeed) Start() changefeed.Position { return r.start }

func TestResumeUsesDurableIdentityBeforeReplay(t *testing.T) {
	for _, bad := range []bool{false, true} {
		t.Run(map[bool]string{false: "pin-lost", true: "wrong-offset"}[bad], func(t *testing.T) {
			s := recoveryStore{memoryStore: &memoryStore{c: 7}, recovery: projection.Recovery{SnapshotID: "snapshot", Checkpoint: projection.Checkpoint{C: 7, NextOffset: 12}, Topic: "changes"}}
			reg := resumeFeed{lostRegistration: &lostRegistration{done: make(chan struct{})}, start: changefeed.Position{Topic: "changes", Offset: 12}}
			c, err := NewResume(s, failedReplay{}, func(_ context.Context, p *changefeed.Position) (Registration, error) {
				if p == nil || p.Topic != "changes" || p.Offset != 12 {
					t.Fatal(p)
				}
				if bad {
					reg.start.Offset++
				}
				return reg, nil
			}, func(context.Context, string) (QuoteConnection, error) { panic("must not activate") }, Config{ReplayTTL: time.Minute})
			if err != nil {
				t.Fatal(err)
			}
			if err = c.Run(context.Background()); !errors.Is(err, ErrBootstrap) {
				t.Fatal(err)
			}
			if c.Ready() {
				t.Fatal("ready without pin")
			}
		})
	}
}

type failedReplay struct{ Replay }

func (failedReplay) BeginReplay(context.Context, *pb.BeginReplayRequest) (*pb.ReplayState, error) {
	return nil, status.Error(codes.FailedPrecondition, "pin expired")
}

func TestMissingRecoveryDoesNotRegisterOrInitialize(t *testing.T) {
	s := recoveryStore{memoryStore: &memoryStore{}, err: projection.ErrRecovery}
	c, err := NewResume(s, failedReplay{}, func(context.Context, *changefeed.Position) (Registration, error) {
		panic("no registration before recovery")
	}, func(context.Context, string) (QuoteConnection, error) { panic("no prices") }, Config{ReplayTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Run(context.Background()); !errors.Is(err, ErrBootstrap) {
		t.Fatal(err)
	}
}

func TestSQLCheckpointAheadOfAPIHeadCannotActivate(t *testing.T) {
	s := &memoryStore{c: 8}
	r := &replayFixture{store: s, progress: 7, head: 7}
	c := &Coordinator{cfg: Config{SnapshotID: "snapshot", ReplayTTL: time.Minute, PageSize: 2, Now: time.Now}, store: s, replay: r}
	if err := c.sync(context.Background()); !errors.Is(err, ErrProtocol) {
		t.Fatalf("invalid checkpoint accepted: %v", err)
	}
}
