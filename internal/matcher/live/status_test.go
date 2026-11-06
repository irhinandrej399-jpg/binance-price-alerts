package live

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestStatusTracksReplayLagAndDurableFailure(t *testing.T) {
	store := &memoryStore{c: 4}
	rpc := &replayFixture{store: store, progress: 4, head: 5, events: [][]byte{wire(5, 5, "active")}}
	c := &Coordinator{cfg: Config{SnapshotID: "snapshot", ReplayTTL: time.Minute, PageSize: 2, Now: time.Now}, store: store, replay: rpc}
	if s := c.Status(); s.Ready || s.Lag != nil {
		t.Fatal(s)
	}
	if err := c.sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	s := c.Status()
	if s.Ready || s.AppliedC != 5 || s.Head != 5 || s.Lag == nil || *s.Lag != 0 {
		t.Fatal(s)
	}
	// Kafka can be ahead of the last sampled SQL head; subtraction must not wrap.
	c.mu.Lock()
	c.appliedC = 6
	c.mu.Unlock()
	if s = c.Status(); s.Lag == nil || *s.Lag != 0 {
		t.Fatal(s)
	}
}

func TestStatusFailedReplayPreservesLag(t *testing.T) {
	store := &memoryStore{c: 4, fail: errors.New("commit failed")}
	rpc := &replayFixture{store: store, progress: 4, head: 5, events: [][]byte{wire(5, 5, "active")}}
	c := &Coordinator{cfg: Config{SnapshotID: "snapshot", ReplayTTL: time.Minute, PageSize: 2, Now: time.Now}, store: store, replay: rpc}
	if err := c.sync(context.Background()); err == nil {
		t.Fatal("failure hidden")
	}
	s := c.Status()
	if s.Ready || s.AppliedC != 4 || s.Head != 5 || s.Lag == nil || *s.Lag != 1 {
		t.Fatal(s)
	}
}

type pausedSnapshot struct {
	assignedStore
	entered, release chan struct{}
}

func (s pausedSnapshot) Initialize(ctx context.Context, _ string) error {
	close(s.entered)
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func TestStatusEmptySnapshotLifecycle(t *testing.T) {
	s := pausedSnapshot{assignedStore: assignedStore{memoryStore: &memoryStore{}}, entered: make(chan struct{}), release: make(chan struct{})}
	reg := idleFeed{&lostRegistration{done: make(chan struct{})}}
	c, err := New(s, &replayFixture{store: s.memoryStore}, func(context.Context) (Registration, error) { return reg, nil }, func(context.Context, string) (QuoteConnection, error) { panic("empty projection") }, Config{SnapshotID: "snapshot", ReplayTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if c.Status().Ready {
		t.Fatal("ready before run")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- c.Run(ctx) }()
	<-s.entered
	if v := c.Status(); v.Ready || v.Lag != nil {
		t.Fatal(v)
	}
	close(s.release)
	for !c.Status().Ready {
		select {
		case <-c.Changed():
		case e := <-stopped:
			t.Fatal(e)
		case <-ctx.Done():
			t.Fatal("not ready")
		}
	}
	v := c.Status()
	if v.Lag == nil || *v.Lag != 0 || v.AppliedC != 0 {
		t.Fatal(v)
	}
	reg.Close()
	if c.Status().Ready {
		t.Fatal("registration loss ready")
	}
	<-stopped
	if c.Status().Ready {
		t.Fatal("stopped ready")
	}
}
