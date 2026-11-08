package sourceprovision

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/intake"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
)

type fakeBroker struct {
	nonempty, bad bool
	changed       bool
}

func (b *fakeBroker) Metadata(_ context.Context, topic string) (kafkasource.Identity, []int32, error) {
	n := 16
	if topic == "test-subscription-changes" {
		n = 1
	}
	parts := partitions(n)
	if b.bad {
		parts[len(parts)-1] = 99
	}
	id := byte(1)
	if n == 1 {
		id = 2
	}
	if topic == "test-delivery-requests" {
		id = 3
	}
	if b.changed {
		id += 10
	}
	return kafkasource.Identity{ClusterID: "cluster", Topic: topic, TopicID: [16]byte{id}}, parts, nil
}
func (b *fakeBroker) Empty(context.Context, string, []int32) error {
	if b.nonempty {
		return errors.New("not empty")
	}
	return nil
}

type fakeStore struct {
	b       intake.Binding
	present bool
	next    int64
	calls   int
	fail    bool
}

func (s *fakeStore) Bootstrap(_ context.Context, b intake.Binding, _ []int32) error {
	s.calls++
	if s.fail {
		return errors.New("uncertain")
	}
	s.b = b
	s.present = true
	return nil
}
func (s *fakeStore) ReadProgress(_ context.Context, b intake.Binding, p int32) (intake.Progress, error) {
	if !s.present {
		return intake.Progress{}, intake.ErrMissing
	}
	if b != s.b {
		return intake.Progress{}, kafkasource.ErrMismatch
	}
	return intake.Progress{Binding: s.b, Partition: p, Next: s.next}, nil
}
func TestProvisionLifecycle(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "inventory.json")
	b := &fakeBroker{}
	s := &fakeStore{}
	if err := Run(ctx, "init", path, "test", b, s); err != nil {
		t.Fatal(err)
	}
	inv, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	original := inv
	if s.calls != 1 || len(inv.Env()["PRICES_BOOTSTRAP"]) == 0 {
		t.Fatal("missing bootstrap")
	}
	b.nonempty = true
	s.next = 42
	for _, mode := range []string{"verify", "resume"} {
		if err = Run(ctx, mode, path, "", b, s); err != nil {
			t.Fatal(err)
		}
	}
	inv, err = Load(path)
	if err != nil || !reflect.DeepEqual(inv, original) || s.next != 42 || s.calls != 1 {
		t.Fatal("changed existing inventory/progress")
	}
	b.changed = true
	if Run(ctx, "verify", path, "", b, s) == nil {
		t.Fatal("accepted new source")
	}
	b.changed = false
	s.present = false
	if Run(ctx, "verify", path, "", b, s) == nil {
		t.Fatal("accepted missing bootstrap")
	}
	if Run(ctx, "resume", path, "", b, s) == nil {
		t.Fatal("bootstrapped nonempty source")
	}
	if Run(ctx, "init", path, "test", b, s) == nil {
		t.Fatal("overwrote inventory")
	}
}
func TestInterruptedProvisionKeepsIDs(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "inventory.json")
	b := &fakeBroker{}
	s := &fakeStore{fail: true}
	if Run(ctx, "init", path, "test", b, s) == nil {
		t.Fatal("ignored failure")
	}
	inv, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	s.fail = false
	if err = Run(ctx, "resume", path, "", b, s); err != nil {
		t.Fatal(err)
	}
	if s.b != inv.Binding {
		t.Fatal("regenerated identity")
	}
}
func TestInvalidFreshDataset(t *testing.T) {
	for _, b := range []*fakeBroker{{nonempty: true}, {bad: true}} {
		path := filepath.Join(t.TempDir(), "inventory.json")
		s := &fakeStore{}
		if Run(context.Background(), "init", path, "test", b, s) == nil {
			t.Fatal("accepted invalid dataset")
		}
		if _, err := Load(path); err == nil || s.calls != 0 {
			t.Fatal("wrote invalid bootstrap")
		}
	}
}

type partialStore struct{ fakeStore }

func (s *partialStore) ReadProgress(ctx context.Context, b intake.Binding, p int32) (intake.Progress, error) {
	if p == 15 {
		return intake.Progress{}, intake.ErrMissing
	}
	return s.fakeStore.ReadProgress(ctx, b, p)
}

type legacyStore struct{ fakeStore }

func (s *legacyStore) ReadProgress(context.Context, intake.Binding, int32) (intake.Progress, error) {
	return intake.Progress{}, intake.ErrUnbound
}
func TestPartialAndLegacyRefuseBootstrap(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "inventory.json")
	b := &fakeBroker{}
	s := &fakeStore{}
	if err := Run(ctx, "init", path, "test", b, s); err != nil {
		t.Fatal(err)
	}
	partial := &partialStore{fakeStore: *s}
	legacy := &legacyStore{}
	if Run(ctx, "resume", path, "", b, partial) == nil || partial.calls != 1 {
		t.Fatal("repaired partial checkpoint set")
	}
	if Run(ctx, "resume", path, "", b, legacy) == nil || legacy.calls != 0 {
		t.Fatal("bound legacy SQL")
	}
}
func TestInventoryRejectsInvalidData(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "inventory.json")
	if err := Run(ctx, "init", path, "test", &fakeBroker{}, &fakeStore{}); err != nil {
		t.Fatal(err)
	}
	inv, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	delete(inv.PricesBootstrap, 15)
	data, _ := json.Marshal(inv)
	if os.WriteFile(path, data, 0600) != nil {
		t.Fatal("write")
	}
	if _, err = Load(path); err == nil {
		t.Fatal("accepted incomplete vector")
	}
	for _, data := range []string{`{}`, `null`, `{"Version":1,"unexpected":1}`, `{} {}`} {
		if os.WriteFile(path, []byte(data), 0600) != nil {
			t.Fatal("write")
		}
		if _, err = Load(path); err == nil {
			t.Fatal("accepted invalid inventory")
		}
	}
}
