package replayrpc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/journal"
	rpc "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/replayrpc"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/changefeed"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/live"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/projection"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/snapshotload"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
)

type interruptedReplayStore struct {
	*projection.Store
	after bool
}

var errRestartCrash = errors.New("process interrupted around SQL commit")

func (s interruptedReplayStore) ApplyReplay(c context.Context, events [][]byte) error {
	if s.after {
		if err := s.Store.ApplyReplay(c, events); err != nil {
			return err
		}
	}
	return errRestartCrash
}

// Real SQL plus the real protected Replay client/server; process-local objects
// are recreated around a failed write and an uncertain successful commit.
func TestMatcherRestartAroundReplayCommit(t *testing.T) {
	api := postgrestest.Open(t)
	matcher := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	second := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pins := sub.NewSnapshots(api)
	client := rpc.NewClient(connect(t, rpc.NewServer(pins, api), matcherURI))
	for _, after := range []bool{false, true} {
		name := "before-commit"
		if after {
			name = "after-commit"
		}
		t.Run(name, func(t *testing.T) {
			snap, err := pins.Build(ctx, matcherURI, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			defer pins.Abandon(context.Background(), matcherURI, snap.ID)
			src := frozenLiveSnapshot{page: &pb.SnapshotPageResponse{SnapshotId: snap.ID, BoundaryC: snap.C, ExpiresAt: snap.ExpiresAt.Format(time.RFC3339Nano)}, during: func() {}}
			if _, err = snapshotload.New(matcher, src).Load(ctx, &pb.BuildSnapshotRequest{RequestId: name, TtlMillis: 60000, PageSize: 1000}); err != nil {
				t.Fatal(err)
			}
			store := projection.New(matcher, t.Name())
			if err = store.Initialize(ctx, snap.ID); err != nil {
				t.Fatal(err)
			}
			tx, err := api.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			e := entry("restart-"+name, domain.Cancelled) // terminal avoids any price dependency
			last, err := journal.Append(ctx, tx, []journal.Entry{e})
			if err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			register := func(context.Context, *changefeed.Position) (live.Registration, error) { return newLiveFeed(), nil }
			noPrices := func(context.Context, string) (live.QuoteConnection, error) {
				t.Fatal("terminal row connected prices")
				return nil, nil
			}
			cfg := live.Config{ReplayTTL: time.Minute, PageSize: 1, HeadInterval: 10 * time.Millisecond}
			broken, err := live.NewResume(interruptedReplayStore{Store: store, after: after}, client, register, noPrices, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err = broken.Run(ctx); !errors.Is(err, errRestartCrash) {
				t.Fatal(err)
			}
			durable := projection.New(second, t.Name())
			r, err := durable.Recover(ctx)
			want := snap.C
			if after {
				want = last
			}
			if err != nil || r.Checkpoint.C != want {
				t.Fatal(r, err, want)
			}
			pin, err := pins.CheckPin(ctx, matcherURI, snap.ID)
			if err != nil || pin.Progress != snap.C {
				t.Fatal(pin, err)
			}
			// New coordinator, another SQL pool; repeated page must finish exactly once.
			resumed, err := live.NewResume(durable, client, register, noPrices, cfg)
			if err != nil {
				t.Fatal(err)
			}
			runCtx, stop := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- resumed.Run(runCtx) }()
			defer func() {
				stop()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("restart leaked")
				}
			}()
			for !resumed.Ready() {
				select {
				case e := <-done:
					done <- e
					t.Fatalf("resume stopped: %v", e)
				case <-resumed.Changed():
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			pin, err = pins.CheckPin(ctx, matcherURI, snap.ID)
			if err != nil || pin.Progress != last {
				t.Fatal(pin, err)
			}
			im, err := durable.ReadImage(ctx, e.Change.SubscriptionID)
			if err != nil || im.Change == nil || im.Change.State != domain.Cancelled {
				t.Fatal(im, err)
			}
			// Losing the resource is an explicit bootstrap boundary. No new snapshot
			// is created and no old generation is reset or published as ready.
			if err = pins.Abandon(ctx, matcherURI, snap.ID); err != nil {
				t.Fatal(err)
			}
			select {
			case err = <-done:
				done <- err
				if !errors.Is(err, live.ErrBootstrap) {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if resumed.Ready() {
				t.Fatal("ready after pin loss")
			}
			invalid, err := live.NewResume(durable, client, register, noPrices, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err = invalid.Run(ctx); !errors.Is(err, live.ErrBootstrap) {
				t.Fatal(err)
			}
		})
	}
}

func TestMatcherRestartExpiredPin(t *testing.T) {
	api := postgrestest.Open(t)
	matcher := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pins := sub.NewSnapshots(api)
	snap, err := pins.Build(ctx, matcherURI, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer pins.Abandon(context.Background(), matcherURI, snap.ID)
	src := frozenLiveSnapshot{page: &pb.SnapshotPageResponse{SnapshotId: snap.ID, BoundaryC: snap.C, ExpiresAt: snap.ExpiresAt.Format(time.RFC3339Nano)}, during: func() {}}
	if _, err = snapshotload.New(matcher, src).Load(ctx, &pb.BuildSnapshotRequest{RequestId: t.Name(), TtlMillis: 60000, PageSize: 1000}); err != nil {
		t.Fatal(err)
	}
	store := projection.New(matcher, t.Name())
	if err = store.Initialize(ctx, snap.ID); err != nil {
		t.Fatal(err)
	}
	pin, err := pins.Handoff(ctx, matcherURI, snap.ID, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(max(time.Duration(0), time.Until(pin.ExpiresAt)) + time.Millisecond)
	client := rpc.NewClient(connect(t, rpc.NewServer(pins, api), matcherURI))
	c, err := live.NewResume(store, client, func(context.Context, *changefeed.Position) (live.Registration, error) { return newLiveFeed(), nil }, func(context.Context, string) (live.QuoteConnection, error) {
		t.Fatal("expired pin activated prices")
		return nil, nil
	}, live.Config{ReplayTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Run(ctx); !errors.Is(err, live.ErrBootstrap) {
		t.Fatal(err)
	}
	if c.Ready() {
		t.Fatal("expired pin ready")
	}
	r, err := store.Recover(ctx)
	if err != nil || r.SnapshotID != snap.ID || r.Checkpoint.C != snap.C {
		t.Fatal("old generation modified", r, err)
	}
}
