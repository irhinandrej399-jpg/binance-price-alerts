package live

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	changed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/subscriptionchanged"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/projection"
)

// The pin's issued boundary can lag Kafka and the local durable C.
func TestReplayAcknowledgesOnlyIssuedDurablePrefix(t *testing.T) {
	ctx := context.Background()
	store := &memoryStore{c: 10}
	rpc := &replayFixture{store: store, progress: 4, head: 10, events: [][]byte{wire(5, 5, "active"), wire(6, 6, "cancelled"), wire(7, 7, "active"), wire(8, 8, "active"), wire(9, 9, "active"), wire(10, 10, "active")}}
	c := &Coordinator{cfg: Config{SnapshotID: "snapshot", ReplayTTL: time.Minute, PageSize: 2, Now: time.Now}, store: store, replay: rpc}
	if err := c.sync(ctx); err != nil {
		t.Fatal(err)
	}
	if rpc.progress != 10 || len(rpc.acks) != 3 || store.c != 10 {
		t.Fatal(rpc.progress, rpc.acks, store.c)
	}
}
func TestNoAPIAckAfterStorageFailure(t *testing.T) {
	store := &memoryStore{c: 4, fail: errors.New("commit unknown")}
	rpc := &replayFixture{store: store, progress: 4, head: 5, events: [][]byte{wire(5, 5, "active")}}
	c := &Coordinator{cfg: Config{SnapshotID: "snapshot", ReplayTTL: time.Minute, PageSize: 2, Now: time.Now}, store: store, replay: rpc}
	if err := c.sync(context.Background()); err == nil || len(rpc.acks) != 0 {
		t.Fatal(err, rpc.acks)
	}
}

type memoryStore struct {
	c    uint64
	fail error
}

func (s *memoryStore) Initialize(context.Context, string) error { return nil }
func (s *memoryStore) Checkpoint(context.Context) (projection.Checkpoint, error) {
	return projection.Checkpoint{C: s.c}, nil
}
func (s *memoryStore) ApplyReplay(_ context.Context, events [][]byte) error {
	if s.fail != nil {
		return s.fail
	}
	_, ch, err := changed.Decode(events[len(events)-1])
	if err == nil && ch.Position > s.c {
		s.c = ch.Position
	}
	return err
}
func (s *memoryStore) Handle(context.Context, projection.Batch, func(context.Context, int64) error) error {
	panic("unused")
}
func (s *memoryStore) Walk(context.Context, func(projection.Image) error) (projection.Checkpoint, error) {
	panic("unused")
}
func (s *memoryStore) ReadImage(context.Context, string) (projection.Image, error) { panic("unused") }

type replayFixture struct {
	store          *memoryStore
	progress, head uint64
	events         [][]byte
	acks           []uint64
	expiry         string
}

func (r *replayFixture) state() *pb.ReplayState {
	if r.expiry == "" {
		r.expiry = time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	}
	return &pb.ReplayState{ResourceId: "snapshot", Progress: r.progress, Issued: r.head, Head: r.head, ExpiresAt: r.expiry}
}
func (r *replayFixture) BeginReplay(context.Context, *pb.BeginReplayRequest) (*pb.ReplayState, error) {
	return r.state(), nil
}
func (r *replayFixture) ReadReplayPage(_ context.Context, q *pb.ReadReplayPageRequest) (*pb.ReplayPage, error) {
	p := &pb.ReplayPage{State: r.state(), Next: q.After}
	for _, b := range r.events {
		_, ch, _ := changed.Decode(b)
		if ch.Position > q.After {
			p.Events = append(p.Events, b)
			p.Next = ch.Position
			if len(p.Events) == int(q.MaxTransactions) {
				break
			}
		}
	}
	return p, nil
}
func (r *replayFixture) AcknowledgeReplay(_ context.Context, q *pb.AcknowledgeReplayRequest) (*pb.ReplayState, error) {
	if q.Boundary > r.store.c {
		return nil, errors.New("ack before durable")
	}
	r.progress = q.Boundary
	r.acks = append(r.acks, q.Boundary)
	return r.state(), nil
}

func wire(pos, end uint64, state string) []byte {
	return []byte(fmt.Sprintf(`{"event_id":"e-%d","type":"subscription.changed","version":1,"occurred_at":"2026-10-29T10:00:00Z","correlation_id":"test","ordering_key":"sub","payload":{"schema_version":1,"position":%d,"transaction_end":%d,"subscription_id":"sub","subscription_version":%d,"state":"%s","pair":"BTCUSDT","threshold":"100","direction":"up","activation":"latest_price","creation_snapshot":{"price":"101","trade_time":"2026-10-29T09:00:00Z"}}}`, pos, pos, end, pos, state))
}

func TestWholePagesAndMalformedBoundaries(t *testing.T) {
	valid := [][]byte{wire(5, 6, "active"), wire(6, 6, "cancelled"), wire(7, 7, "active")}
	batches, err := transactions(valid, 4, 7)
	if err != nil || len(batches) != 2 || len(batches[0]) != 2 {
		t.Fatal(batches, err)
	}
	for _, tc := range []struct {
		events      [][]byte
		after, next uint64
	}{
		{nil, 4, 4}, {valid[:1], 4, 5}, {valid, 4, 6}, {valid, 3, 7},
		{[][]byte{wire(5, 6, "active"), wire(6, 7, "active")}, 4, 6},
		{[][]byte{[]byte(`{}`)}, 4, 5}, {valid, ^uint64(0), 7},
	} {
		if _, err := transactions(tc.events, tc.after, tc.next); err == nil {
			t.Fatal("accepted incomplete or noncontiguous page", tc)
		}
	}
}
func TestPinIdentityExpiryAndProgress(t *testing.T) {
	now := time.Now()
	for _, test := range []string{"missing", "foreign", "expired", "bad-time", "regressed-issued", "head-before-issued"} {
		t.Run(test, func(t *testing.T) {
			c := &Coordinator{cfg: Config{SnapshotID: "snapshot", Now: func() time.Time { return now }}}
			s := &pb.ReplayState{ResourceId: "snapshot", Progress: 4, Issued: 5, Head: 6, ExpiresAt: now.Add(time.Minute).UTC().Format(time.RFC3339Nano)}
			switch test {
			case "missing":
				s = nil
			case "foreign":
				s.ResourceId = "other"
			case "expired":
				s.ExpiresAt = now.Format(time.RFC3339Nano)
			case "bad-time":
				s.ExpiresAt = "?"
			case "regressed-issued":
				s.Progress = 6
			case "head-before-issued":
				s.Head = 4
			}
			if err := c.state(s); err == nil {
				t.Fatal("invalid pin accepted")
			}
		})
	}
	c := &Coordinator{cfg: Config{SnapshotID: "snapshot", Now: func() time.Time { return now }}}
	s := &pb.ReplayState{ResourceId: "snapshot", ExpiresAt: now.Add(time.Minute).UTC().Format(time.RFC3339Nano)}
	if err := c.state(s); err != nil {
		t.Fatal(err)
	}
	s.ExpiresAt = now.Add(2 * time.Minute).UTC().Format(time.RFC3339Nano)
	if err := c.state(s); !errors.Is(err, ErrProtocol) {
		t.Fatal("pin silently renewed", err)
	}
}
func TestRegisterFailureLeavesGenerationUnreadyAndRunSingleUse(t *testing.T) {
	lost := errors.New("registration failed")
	c, err := New(&memoryStore{}, &replayFixture{}, func(context.Context) (Registration, error) { return nil, lost }, func(context.Context, string) (QuoteConnection, error) { panic("must not connect") }, Config{SnapshotID: "snapshot", ReplayTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if c.Ready() {
		t.Fatal("ready before Run")
	}
	if err = c.Run(context.Background()); !errors.Is(err, lost) {
		t.Fatal(err)
	}
	if c.Ready() {
		t.Fatal("ready after failure")
	}
	if err = c.Run(context.Background()); !errors.Is(err, ErrConfig) {
		t.Fatal(err)
	}
}

type lostRegistration struct {
	Registration
	done chan struct{}
	once sync.Once
}

func (r *lostRegistration) Done() <-chan struct{} { return r.done }
func (r *lostRegistration) Err() error            { return errors.New("registration lost") }
func (r *lostRegistration) Close()                { r.once.Do(func() { close(r.done) }) }

type blockingReplay struct {
	Replay
	entered chan struct{}
}

func (r blockingReplay) BeginReplay(c context.Context, _ *pb.BeginReplayRequest) (*pb.ReplayState, error) {
	close(r.entered)
	<-c.Done()
	return nil, c.Err()
}
func TestRegistrationLossCancelsReplayBeforeActivation(t *testing.T) {
	reg := &lostRegistration{done: make(chan struct{})}
	replay := blockingReplay{entered: make(chan struct{})}
	c, err := New(&memoryStore{}, replay, func(context.Context) (Registration, error) { return reg, nil }, func(context.Context, string) (QuoteConnection, error) { panic("must not activate") }, Config{SnapshotID: "snapshot", ReplayTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- c.Run(ctx) }()
	<-replay.entered
	if c.Ready() {
		t.Fatal("ready during replay")
	}
	reg.Close()
	select {
	case err := <-stopped:
		if err == nil {
			t.Fatal("lost registration succeeded")
		}
	case <-ctx.Done():
		t.Fatal("replay ignored loss")
	}
	if c.Ready() {
		t.Fatal("ready after lost registration")
	}
}
