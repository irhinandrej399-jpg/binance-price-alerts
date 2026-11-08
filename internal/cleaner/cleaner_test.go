package cleaner

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
)

type fakeBroker struct {
	start, end int64
	deleted    []int64
	fail       bool
	onDelete   func(int64)
}

func (b *fakeBroker) Bounds(context.Context, kafkasource.Identity, int32) (int64, int64, error) {
	return b.start, b.end, nil
}
func (b *fakeBroker) Delete(_ context.Context, _ kafkasource.Identity, _ int32, n int64) (int64, error) {
	b.deleted = append(b.deleted, n)
	if b.onDelete != nil {
		b.onDelete(n)
	}
	if b.fail {
		return 0, errors.New("lost response")
	}
	b.start = n
	return n, nil
}

type proofs map[string]int64

func (p proofs) Next(_ context.Context, c Consumer, _ kafkasource.Identity, _ int32) (int64, error) {
	n, ok := p[c.Name]
	if !ok {
		return 0, ErrProof
	}
	return n, nil
}
func plan() Inventory {
	return Inventory{Source: kafkasource.Identity{ClusterID: "cluster", TopicID: [16]byte{1}, Topic: "prices"}, Consumers: []Consumer{{Name: "fast", Kind: "prices", ID: "scope-fast", Endpoint: "matcher"}, {Name: "slow", Kind: "prices", ID: "scope-slow", Endpoint: "matcher"}}}
}
func TestDurableReservationAndProofMinimum(t *testing.T) {
	ctx := context.Background()
	s := NewStore(t.TempDir())
	b := &fakeBroker{end: 20}
	p := proofs{"fast": 10, "slow": 4}
	e := New(s, b, p)
	if err := e.Init(ctx, plan()); err != nil {
		t.Fatal(err)
	}
	b.onDelete = func(n int64) {
		state, err := s.load()
		if err != nil || state.Floor < n {
			t.Fatal("delete before durable floor", state, err)
		}
	}
	r, err := e.Clean(ctx, 2)
	if err != nil || r.Target != 2 || b.start != 2 {
		t.Fatal(r, err)
	}
	b.fail = true
	r, err = e.Clean(ctx, 10)
	if !errors.Is(err, ErrDelete) || r.Target != 4 || r.Status != "unknown" {
		t.Fatal(r, err)
	}
	// Reopen after lost response. New reader may never require reserved history.
	e = New(NewStore(s.dir), b, p)
	p["new"] = 3
	c := Consumer{Name: "new", Kind: "prices", ID: "new", Endpoint: "matcher"}
	if err = e.Register(ctx, c); !errors.Is(err, ErrProof) {
		t.Fatal("registered below reservation", err)
	}
	p["new"] = 4
	if err = e.Register(ctx, c); err != nil {
		t.Fatal(err)
	}
	delete(p, "slow")
	calls := len(b.deleted)
	if _, err = e.Clean(ctx, 10); !errors.Is(err, ErrProof) || len(b.deleted) != calls {
		t.Fatal("unknown consumer allowed delete", err)
	}
	if err = e.Retire(ctx, plan().Consumers[1]); err != nil {
		t.Fatal(err)
	}
	b.fail = false
	if _, err = e.Clean(ctx, 10); err != nil {
		t.Fatal(err)
	}
	if err = e.Register(ctx, plan().Consumers[1]); !errors.Is(err, ErrState) {
		t.Fatal("retired identity resurrected", err)
	}
}
func TestMissingCorruptAndSaveFailure(t *testing.T) {
	ctx := context.Background()
	s := NewStore(t.TempDir())
	b := &fakeBroker{end: 20}
	e := New(s, b, proofs{"fast": 8, "slow": 5})
	if _, err := e.Clean(ctx, 5); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
	if err := e.Init(ctx, plan()); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(s.dir, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(s.dir, 0700) })
	if _, err := e.Clean(ctx, 5); err == nil || len(b.deleted) != 0 {
		t.Fatal("save fault allowed delete", err)
	}
	_ = os.Chmod(s.dir, 0700)
	if err := os.WriteFile(s.path(), []byte(`{"Version":1`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Clean(ctx, 5); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
}

func TestZeroEmptyAndInvalidProgress(t *testing.T) {
	for _, p := range []proofs{{"fast": 0, "slow": 0}, {"fast": 21, "slow": 2}, {"fast": -1, "slow": 2}, {"fast": 5}} {
		e := New(NewStore(t.TempDir()), &fakeBroker{end: 20}, p)
		if err := e.Init(context.Background(), plan()); err != nil {
			t.Fatal(err)
		}
		r, err := e.Clean(context.Background(), 5)
		if p["fast"] == 0 {
			if err != nil || r.Status != "noop" || r.Attempted {
				t.Fatal(r, err)
			}
		} else if !errors.Is(err, ErrProof) || r.Attempted {
			t.Fatal("invalid proof authorized delete", r, err)
		}
	}
	e := New(NewStore(t.TempDir()), &fakeBroker{end: 20}, proofs{"fast": 8, "slow": 5})
	if err := e.Init(context.Background(), plan()); err != nil {
		t.Fatal(err)
	}
	wrong := plan().Consumers[0]
	wrong.ID = "different-generation"
	if err := e.Retire(context.Background(), wrong); !errors.Is(err, ErrState) {
		t.Fatal("wrong identity retired", err)
	}
	for _, c := range plan().Consumers {
		if err := e.Retire(context.Background(), c); err != nil {
			t.Fatal(err)
		}
		if err := e.Retire(context.Background(), c); err != nil {
			t.Fatal("retirement retry", err)
		}
	}
	if r, err := e.Clean(context.Background(), 5); !errors.Is(err, ErrProof) || r.Attempted {
		t.Fatal("empty inventory authorized delete", r, err)
	}
}
func TestLimitNearMaxOffset(t *testing.T) {
	const end = int64(1<<63 - 1)
	b := &fakeBroker{start: end - 5, end: end}
	e := New(NewStore(t.TempDir()), b, proofs{"fast": end, "slow": end})
	if err := e.Init(context.Background(), plan()); err != nil {
		t.Fatal(err)
	}
	r, err := e.Clean(context.Background(), 2)
	if err != nil || r.Target != end-3 {
		t.Fatal("offset arithmetic overflow", r, err)
	}
	for _, n := range []int64{-1, 0, 1_000_001} {
		if _, err = e.Clean(context.Background(), n); !errors.Is(err, ErrConfig) {
			t.Fatal(err)
		}
	}
}
func TestInvalidInventoryAndSymlinks(t *testing.T) {
	ctx := context.Background()
	s := NewStore(t.TempDir())
	e := New(s, &fakeBroker{end: 20}, proofs{})
	inv := plan()
	inv.Consumers = append(inv.Consumers, inv.Consumers[0])
	if err := e.Init(ctx, inv); !errors.Is(err, ErrConfig) {
		t.Fatal(err)
	}
	if err := e.Init(ctx, plan()); err != nil {
		t.Fatal(err)
	}
	if err := e.Init(ctx, plan()); !errors.Is(err, ErrState) {
		t.Fatal("init overwrote existing registry", err)
	}
	target := s.path() + ".real"
	if err := os.Rename(s.path(), target); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, s.path()); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Clean(ctx, 5); !errors.Is(err, ErrState) {
		t.Fatal("symlinked registry trusted", err)
	}
}

func TestRepeatedTargetMustStillSyncState(t *testing.T) {
	ctx := context.Background()
	s := NewStore(t.TempDir())
	b := &fakeBroker{end: 20, fail: true}
	engine := New(s, b, proofs{"fast": 8, "slow": 5})
	if e := engine.Init(ctx, plan()); e != nil {
		t.Fatal(e)
	}
	if _, e := engine.Clean(ctx, 5); !errors.Is(e, ErrDelete) {
		t.Fatal(e)
	}
	state, e := s.Inspect(ctx)
	if e != nil || state.Floor != 5 {
		t.Fatal(state, e)
	}
	calls := len(b.deleted)
	b.fail = false
	if e = os.Chmod(s.dir, 0500); e != nil {
		t.Fatal(e)
	}
	defer os.Chmod(s.dir, 0700)
	if _, e = engine.Clean(ctx, 5); e == nil || len(b.deleted) != calls {
		t.Fatal("retry skipped durable state sync", e)
	}
}

type rejectingBroker struct{ fakeBroker }

func (b *rejectingBroker) Delete(context.Context, kafkasource.Identity, int32, int64) (int64, error) {
	return 0, &BrokerRejection{Code: 1}
}
func TestReportBrokerRejection(t *testing.T) {
	ctx := context.Background()
	engine := New(NewStore(t.TempDir()), &rejectingBroker{fakeBroker{end: 20}}, proofs{"fast": 8, "slow": 5})
	if e := engine.Init(ctx, plan()); e != nil {
		t.Fatal(e)
	}
	r, e := engine.Clean(ctx, 5)
	if !errors.Is(e, ErrDelete) || r.Status != "rejected" || r.BrokerCode == nil || *r.BrokerCode != 1 || r.Floor != 5 {
		t.Fatal(r, e)
	}
}
