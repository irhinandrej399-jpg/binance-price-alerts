package live

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/changefeed"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricefeed"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/projection"
)

type assignedStore struct {
	*memoryStore
	images []projection.Image
}

func (s assignedStore) Walk(_ context.Context, visit func(projection.Image) error) (projection.Checkpoint, error) {
	for _, im := range s.images {
		if err := visit(im); err != nil {
			return projection.Checkpoint{}, err
		}
	}
	return projection.Checkpoint{C: s.c}, nil
}

type idleFeed struct{ *lostRegistration }

func (r idleFeed) Start() changefeed.Position { return changefeed.Position{Topic: "changes"} }
func (r idleFeed) Next(ctx context.Context) (changefeed.Transaction, error) {
	<-ctx.Done()
	return changefeed.Transaction{}, ctx.Err()
}

type idleQuote struct {
	*lostRegistration
	pair string
}

func (q idleQuote) Current() (priceobserved.Observed, error) {
	p, _ := price.Parse("101")
	return priceobserved.Observed{SchemaVersion: 1, Pair: q.pair, Price: &p, SourceID: "1", Revision: priceobserved.Revision{StreamID: "s", Generation: 1, Sequence: 1}, TradeTime: time.Now(), ReceivedAt: time.Now(), StreamState: priceobserved.Live}, nil
}
func (q idleQuote) Next(ctx context.Context) (priceobserved.Observed, error) {
	<-ctx.Done()
	return priceobserved.Observed{}, ctx.Err()
}
func assignedImage(id, pair string) projection.Image {
	return projection.Image{Snapshot: &pb.SnapshotSubscription{OwnerId: "a4444444-4444-4444-8444-444444444444", SubscriptionId: id, Version: 1, Pair: pair, Threshold: "100", Direction: "up", State: "active"}}
}
func otherPair() string {
	for _, p := range []string{"ETHUSDT", "SOLUSDT", "BNBUSDT"} {
		if pricefeed.PairPartition(p) != pricefeed.PairPartition("BTCUSDT") {
			return p
		}
	}
	panic("no foreign pair")
}

func TestAssignedBootstrapRejectsForeignConnection(t *testing.T) {
	foreign := otherPair()
	s := assignedStore{memoryStore: &memoryStore{c: 7}, images: []projection.Image{assignedImage("own", "BTCUSDT"), assignedImage("foreign", foreign)}}
	r := &replayFixture{store: s.memoryStore, progress: 7, head: 7}
	reg := idleFeed{&lostRegistration{done: make(chan struct{})}}
	own := idleQuote{&lostRegistration{done: make(chan struct{})}, "BTCUSDT"}
	cfg := Config{SnapshotID: "snapshot", ReplayTTL: time.Minute, PricePartitions: []int32{pricefeed.PairPartition("BTCUSDT")}}
	c, err := New(s, r, func(context.Context) (Registration, error) { return reg, nil }, func(_ context.Context, pair string) (QuoteConnection, error) {
		if pair != own.pair {
			return nil, pricefeed.ErrConfig
		}
		return own, nil
	}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- c.Run(ctx) }()
	for !c.Ready() {
		select {
		case err := <-stopped:
			t.Fatalf("bootstrap attempted foreign connection: %v", err)
		case <-ctx.Done():
			t.Fatal("not ready")
		case <-c.Changed():
		}
	}
	if _, err = c.Current(foreign); !errors.Is(err, ErrNotReady) {
		t.Fatal(err)
	}
	if view, e := c.Current(own.pair); e != nil || view.C != 7 || len(view.Candidates) != 1 {
		t.Fatal(view, e)
	}
	cancel()
	if err = <-stopped; err == nil {
		t.Fatal("cancel succeeded")
	}
	select {
	case <-own.Done():
	default:
		t.Fatal("connection leaked")
	}
	if c.Ready() {
		t.Fatal("ready after cancellation")
	}
}

func TestAssignmentModesAndResume(t *testing.T) {
	ownPart := pricefeed.PairPartition("BTCUSDT")
	for _, resume := range []bool{false, true} {
		for _, mode := range []string{"unrestricted", "own", "empty", "no-active-owned"} {
			t.Run(fmt.Sprintf("%s/resume=%t", mode, resume), func(t *testing.T) {
				s := assignedStore{memoryStore: &memoryStore{c: 7}, images: []projection.Image{assignedImage("own", "BTCUSDT"), assignedImage("foreign", otherPair())}}
				var parts []int32
				want := []string{"BTCUSDT", otherPair()}
				switch mode {
				case "own":
					parts = []int32{ownPart}
					want = want[:1]
				case "empty":
					parts = []int32{}
					want = nil
				case "no-active-owned":
					parts = []int32{(ownPart + 1) % 16}
					for parts[0] == pricefeed.PairPartition(otherPair()) {
						parts[0] = (parts[0] + 1) % 16
					}
					want = nil
				}
				sort.Strings(want)
				reg := idleFeed{&lostRegistration{done: make(chan struct{})}}
				quotes := map[string]idleQuote{}
				connect := func(_ context.Context, pair string) (QuoteConnection, error) {
					allowed := false
					for _, p := range want {
						allowed = allowed || p == pair
					}
					if !allowed {
						return nil, pricefeed.ErrConfig
					}
					q := idleQuote{&lostRegistration{done: make(chan struct{})}, pair}
					quotes[pair] = q
					return q, nil
				}
				cfg := Config{SnapshotID: "snapshot", ReplayTTL: time.Minute, PricePartitions: parts}
				r := &replayFixture{store: s.memoryStore, progress: 7, head: 7}
				var c *Coordinator
				var err error
				if resume {
					cfg.SnapshotID = ""
					c, err = NewResume(assignedRecovery{s}, r, func(_ context.Context, p *changefeed.Position) (Registration, error) {
						if p == nil || p.Topic != "changes" || p.Offset != 12 {
							t.Error("lost cursor", p)
						}
						return assignedResumeFeed{idleFeed: reg}, nil
					}, connect, cfg)
				} else {
					c, err = New(s, r, func(context.Context) (Registration, error) { return reg, nil }, connect, cfg)
				}
				if err != nil {
					t.Fatal(err)
				}
				if len(parts) > 0 {
					parts[0] = pricefeed.PairPartition(otherPair())
				}
				if _, err = c.ActivePairs(); !errors.Is(err, ErrNotReady) {
					t.Fatal("pairs before Run", err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				stopped := make(chan error, 1)
				go func() { stopped <- c.Run(ctx) }()
				defer func() {
					cancel()
					select {
					case <-stopped:
					case <-time.After(time.Second):
						t.Error("leaked Run")
					}
				}()
				for !c.Ready() {
					select {
					case err := <-stopped:
						stopped <- err
						t.Fatal(err)
					case <-ctx.Done():
						t.Fatal("not ready")
					case <-c.Changed():
					}
				}
				pairs, err := c.ActivePairs()
				if err != nil || !slices.Equal(pairs, want) {
					t.Fatal(pairs, want, err)
				}
				if len(pairs) > 0 {
					pairs[0] = "mutated"
					again, _ := c.ActivePairs()
					if !slices.Equal(again, want) {
						t.Fatal("list aliased", again)
					}
				}
				for _, pair := range []string{"BTCUSDT", otherPair()} {
					view, e := c.Current(pair)
					if slices.Contains(want, pair) {
						if e != nil || view.C != 7 || len(view.Candidates) != 1 {
							t.Fatal(view, e)
						}
					} else if !errors.Is(e, ErrNotReady) {
						t.Fatal("foreign view", pair, e)
					}
				}
				cancel()
				if c.Ready() {
					t.Fatal("ready after lifetime cancellation")
				}
				if _, err = c.ActivePairs(); !errors.Is(err, ErrNotReady) {
					t.Fatal("pairs after cancellation", err)
				}
				if _, err = c.Current("BTCUSDT"); !errors.Is(err, ErrNotReady) {
					t.Fatal("view after cancellation", err)
				}
				<-stopped
				stopped <- context.Canceled
				for _, q := range quotes {
					select {
					case <-q.Done():
					default:
						t.Error("quote leaked")
					}
				}
			})
		}
	}
}

type assignedRecovery struct{ assignedStore }

func (s assignedRecovery) Initialize(context.Context, string) error { panic("must recover") }
func (s assignedRecovery) Recover(context.Context) (projection.Recovery, error) {
	return projection.Recovery{SnapshotID: "snapshot", Topic: "changes", Checkpoint: projection.Checkpoint{C: 7, NextOffset: 12}}, nil
}

type assignedResumeFeed struct{ idleFeed }

func (assignedResumeFeed) Start() changefeed.Position {
	return changefeed.Position{Topic: "changes", Offset: 12}
}

func TestInvalidPriceAssignment(t *testing.T) {
	for _, parts := range [][]int32{{-1}, {16}, {0, 0}, {15, 2, 15}} {
		_, err := New(&memoryStore{}, &replayFixture{}, func(context.Context) (Registration, error) { panic("unused") }, func(context.Context, string) (QuoteConnection, error) { panic("unused") }, Config{SnapshotID: "snapshot", ReplayTTL: time.Minute, PricePartitions: parts})
		if !errors.Is(err, ErrConfig) {
			t.Fatal(parts, err)
		}
	}
}
