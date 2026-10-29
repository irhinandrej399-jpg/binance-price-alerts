package projection_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/projection"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
)

func wire(pos, end, version uint64, state string) []byte {
	return []byte(fmt.Sprintf(`{"event_id":"e-%d","type":"subscription.changed","version":1,"occurred_at":"2026-10-29T10:00:00Z","correlation_id":"test","ordering_key":"sub","payload":{"schema_version":1,"position":%d,"transaction_end":%d,"subscription_id":"sub","subscription_version":%d,"state":"%s","pair":"BTCUSDT","threshold":"999999999999999999.123456789012345678","direction":"up","activation":"latest_price","creation_snapshot":{"price":"0.000000000000000001","trade_time":"2026-10-29T09:00:00Z"},"workflow":{"trigger_id":"trigger","version":%d}}}`, pos, pos, end, version, state, version))
}

func TestDurableVersionsAndAck(t *testing.T) {
	pool := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	other := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx := context.Background()
	id := t.Name()
	store := projection.New(pool, id)
	reader := projection.New(other, id)
	var acked int64
	handle := func(pos, version uint64, state string) {
		t.Helper()
		batch := projection.Batch{Topic: "changes", Partition: 0, NextOffset: int64(pos), Events: [][]byte{wire(pos, pos, version, state)}}
		err := store.Handle(ctx, batch, func(ctx context.Context, next int64) error {
			cp, err := reader.Checkpoint(ctx)
			if err != nil {
				return err
			}
			c, err := reader.Get(ctx, "sub")
			if err != nil {
				return err
			}
			if cp.C != pos || cp.NextOffset != next || c.SubscriptionVersion < version {
				t.Fatal("ack before durable state")
			}
			acked = next
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	handle(1, 1, "active")
	handle(2, 1, "active")
	handle(3, 3, "pending_delivery")
	handle(4, 2, "active")
	c, err := reader.Get(ctx, "sub")
	if err != nil {
		t.Fatal(err)
	}
	if c.SubscriptionVersion != 3 || c.State != "pending_delivery" || c.Workflow.TriggerID != "trigger" || c.Threshold.String() != "999999999999999999.123456789012345678" {
		t.Fatalf("projection rolled back: %+v", c)
	}
	handle(5, 4, "sending")
	handle(6, 5, "delivery_blocked")
	handle(7, 6, "active")
	handle(8, 7, "cancelled")
	handle(9, 3, "active")
	c, err = reader.Get(ctx, "sub")
	if err != nil || c.State != "cancelled" || c.SubscriptionVersion != 7 || acked != 9 {
		t.Fatalf("terminal lost: %+v %v", c, err)
	}
	// Commit succeeds; broker acknowledgement is lost. New store/pool retries it.
	batch := projection.Batch{Topic: "changes", NextOffset: 10, Events: [][]byte{wire(10, 10, 8, "delivered")}}
	lost := errors.New("lost ack")
	if err := store.Handle(ctx, batch, func(context.Context, int64) error { return lost }); !errors.Is(err, lost) {
		t.Fatal(err)
	}
	if err := reader.Handle(ctx, batch, func(_ context.Context, next int64) error { acked = next; return nil }); err != nil {
		t.Fatal(err)
	}
	cp, err := reader.Checkpoint(ctx)
	if err != nil || cp.C != 10 || cp.NextOffset != 10 || acked != 10 {
		t.Fatalf("checkpoint: %+v %v", cp, err)
	}
	c, err = reader.Get(ctx, "sub")
	if err != nil || c.State != "delivered" {
		t.Fatalf("delivered: %+v %v", c, err)
	}
}

func TestWholeTransactionRollbackAndInvalidInput(t *testing.T) {
	pool := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx := context.Background()
	store := projection.New(pool, t.Name())
	called := false
	ack := func(context.Context, int64) error { called = true; return nil }
	// SQL trigger fails at checkpoint UPDATE, after the projection INSERT.
	// Fixture installs it only for this named test stream.
	batch := projection.Batch{Topic: "changes", NextOffset: 2, Events: [][]byte{wire(1, 2, 1, "active"), wire(2, 2, 2, "cancelled")}}
	if err := store.Handle(ctx, batch, ack); err == nil || called {
		t.Fatalf("rollback/ack: %v %v", err, called)
	}
	var rows int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM app.matcher_subscriptions WHERE projection_id=$1", t.Name()).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("rollback rows %d %v", rows, err)
	}
	cp, err := store.Checkpoint(ctx)
	if err != nil || cp.C != 0 || cp.NextOffset != 0 {
		t.Fatalf("rollback checkpoint %+v %v", cp, err)
	}
	good := projection.New(pool, t.Name()+"-valid")
	for _, invalid := range []projection.Batch{
		{Topic: "changes", NextOffset: 1, Events: [][]byte{wire(1, 2, 1, "active")}},
		{Topic: "changes", NextOffset: 2, Events: [][]byte{wire(2, 2, 2, "active")}},
		{Topic: "changes", NextOffset: 2, Events: [][]byte{wire(1, 2, 1, "active"), wire(2, 3, 2, "active")}},
		{Topic: "changes", NextOffset: 1, Events: [][]byte{[]byte(`{"version":99}`)}},
	} {
		called = false
		if err := good.Handle(ctx, invalid, ack); err == nil || called {
			t.Fatalf("accepted invalid batch: %v", err)
		}
	}
	if err := good.Handle(ctx, batch, ack); err != nil || !called {
		t.Fatalf("valid transaction: %v", err)
	}
	cp, err = good.Checkpoint(ctx)
	if err != nil || cp.C != 2 {
		t.Fatalf("whole C %+v %v", cp, err)
	}
	// Transport identity cannot silently change for the same projection.
	batch.Topic = "other"
	called = false
	if err := good.Handle(ctx, batch, ack); err == nil || called {
		t.Fatal("accepted different transport")
	}
}

func TestConcurrentDuplicate(t *testing.T) {
	pool := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	other := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx := context.Background()
	id := t.Name()
	batch := projection.Batch{Topic: "changes", NextOffset: 1, Events: [][]byte{wire(1, 1, 1, "active")}}
	var wg sync.WaitGroup
	for _, s := range []*projection.Store{projection.New(pool, id), projection.New(other, id)} {
		wg.Go(func() {
			if err := s.Handle(ctx, batch, func(context.Context, int64) error { return nil }); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	cp, err := projection.New(pool, id).Checkpoint(ctx)
	if err != nil || cp.C != 1 || cp.NextOffset != 1 {
		t.Fatalf("concurrent checkpoint %+v %v", cp, err)
	}
}

func TestInvalidContractStopsBeforeStorage(t *testing.T) {
	store := projection.New(nil, "validation")
	acked := false
	ack := func(context.Context, int64) error { acked = true; return nil }
	for _, b := range []projection.Batch{
		{},
		{Topic: "changes", NextOffset: 1, Events: [][]byte{[]byte(`{"version":99}`)}},
		{Topic: "changes", NextOffset: 1, Events: [][]byte{wire(1, 2, 1, "active")}},
		{Topic: "changes", NextOffset: 2, Events: [][]byte{wire(1, 2, 1, "active"), wire(3, 3, 2, "active")}},
	} {
		if err := store.Handle(context.Background(), b, ack); err == nil || acked {
			t.Fatalf("invalid contract accepted: %v", err)
		}
	}
}

func TestUint64CheckpointAndProjectionIsolation(t *testing.T) {
	pool := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	ctx := context.Background()
	id := t.Name()
	store := projection.New(pool, id)
	// A future snapshot loader can establish C above int64 without loss.
	_, err := pool.Exec(ctx, `INSERT INTO app.matcher_checkpoints(projection_id,topic,partition,journal_c,next_offset) VALUES($1,'changes',0,18446744073709551614,40)`, id)
	if err != nil {
		t.Fatal(err)
	}
	batch := projection.Batch{Topic: "changes", NextOffset: 43, Events: [][]byte{wire(^uint64(0), ^uint64(0), ^uint64(0), "active")}}
	if err := store.Handle(ctx, batch, func(context.Context, int64) error { return nil }); err != nil {
		t.Fatal(err)
	}
	cp, err := store.Checkpoint(ctx)
	if err != nil || cp.C != ^uint64(0) || cp.NextOffset != 43 {
		t.Fatalf("uint64 C: %+v %v", cp, err)
	}
	c, err := store.Get(ctx, "sub")
	if err != nil || c.SubscriptionVersion != ^uint64(0) {
		t.Fatalf("uint64 version %+v %v", c, err)
	}
	isolated := projection.New(pool, id+"-other")
	if _, err := isolated.Get(ctx, "sub"); err == nil {
		t.Fatal("projection identity leaked")
	}
	// Replaying an old transport offset cannot regress either checkpoint.
	batch.NextOffset = 41
	if err := store.Handle(ctx, batch, func(_ context.Context, next int64) error {
		if next != 43 {
			t.Fatal("offset regressed")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
