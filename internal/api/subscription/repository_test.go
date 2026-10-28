package subscription_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	store "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestRoundTripAndOwner(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	owner, err := account.New(tx).Create(ctx, fmt.Sprintf("sub-%d@example.com", time.Now().UnixNano()), "test-hash")
	if err != nil {
		t.Fatal(err)
	}
	repo := store.New(tx)
	threshold, _ := price.Parse("999999999999999999.123456789123456789")
	base, _ := price.Parse("0.000000000000000001")
	percent := "-0.000000000000000001"
	for i, state := range []domain.State{domain.Active, domain.PendingDelivery, domain.Sending, domain.DeliveryBlocked, domain.Delivered, domain.Cancelled} {
		t.Run(string(state), func(t *testing.T) {
			in := store.Record{ID: fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1), OwnerID: owner.ID, Pair: "BTCUSDT", Threshold: threshold, Direction: price.Down, CreationPrice: base, CreationTradeTime: time.Date(2026, 10, 28, 10, 0, 0, 123456000, time.UTC), Percent: &percent, State: state, Version: 9223372036854775807, Activation: store.LatestPrice, Stale: true, StreamUnconfirmed: true, GapDetected: true}
			if state != domain.Active && state != domain.Cancelled {
				in.Workflow = &store.Workflow{TriggerID: fmt.Sprintf("trigger-%d", i), Version: 7}
			}
			if state == domain.DeliveryBlocked {
				in.Blocked = &store.Blocked{Reason: domain.ChannelUnavailable, BindingVersion: 3}
			}
			if i%2 == 0 {
				in.Percent = nil
				in.Direction = price.Up
			}
			if err := repo.Insert(ctx, in); err != nil {
				t.Fatal(err)
			}
			got, err := repo.ByID(ctx, owner.ID, in.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, in) {
				t.Fatalf("round trip differs: got %+v want %+v", got, in)
			}
			if _, err := repo.ByID(ctx, "ffffffff-ffff-4fff-8fff-ffffffffffff", in.ID); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("other owner: %v", err)
			}
			duplicate, err := tx.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.New(duplicate).Insert(ctx, in); !errors.Is(err, store.ErrExists) {
				t.Fatalf("duplicate: %v", err)
			}
			if err := duplicate.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
	if _, err := repo.ByID(ctx, owner.ID, "ffffffff-ffff-4fff-8fff-ffffffffffff"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	// Rollback is owned by the caller, including an otherwise successful Insert.
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.New(pool).ByID(ctx, owner.ID, "00000000-0000-4000-8000-000000000001"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("rollback leaked: %v", err)
	}
}

func TestDatabaseConstraints(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// Independent statements must reject invalid values, including extra precision
	// rather than rounding it into a valid Price.
	for _, tc := range []struct{ name, column, value string }{
		{"threshold precision", "threshold", "0.1234567890123456789"},
		{"threshold overflow", "threshold", "1000000000000000000"},
		{"threshold negative", "threshold", "-1"},
		{"threshold nan", "threshold", "'NaN'"},
		{"base zero", "creation_price", "0"},
		{"base precision", "creation_price", "0.1234567890123456789"},
		{"pair", "pair", "'btc usdt'"}, {"direction", "direction", "'sideways'"},
		{"state", "state", "'triggered'"}, {"version", "version", "0"},
		{"activation", "activation", "'historical_replay'"},
		{"workflow missing", "state", "'sending'"},
		{"blocked reason missing", "state", "'delivery_blocked'"},
		{"percent zero", "percent", "'0'"}, {"percent range", "percent", "'-100'"},
		{"percent precision", "percent", "'1.1234567890123456789'"},
		{"workflow version missing", "workflow_id", "'job'"},
		{"orphan workflow version", "workflow_version", "1"},
		{"orphan block reason", "block_reason", "'channel_unavailable'"},
		{"null state", "state", "NULL"},
		{"time infinite", "creation_trade_time", "'infinity'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			owner, err := account.New(tx).Create(ctx, fmt.Sprintf("constraint-%d@example.com", time.Now().UnixNano()), "hash")
			if err != nil {
				t.Fatal(err)
			}
			_, err = tx.Exec(ctx, `INSERT INTO app.subscriptions(owner_id,pair,threshold,direction,creation_price,creation_trade_time,state,version,activation) VALUES ($1,'BTCUSDT',100,'up',99,'2026-10-28','active',1,'latest_price')`, owner.ID)
			if err != nil {
				t.Fatal(err)
			}
			_, err = tx.Exec(ctx, "UPDATE app.subscriptions SET "+tc.column+"="+tc.value+" WHERE owner_id=$1", owner.ID)
			var pe *pgconn.PgError
			if !errors.As(err, &pe) || pe.Code != "23514" && !(tc.name == "null state" && pe.Code == "23502") {
				t.Fatalf("constraint: %v", err)
			}
		})
	}
}

func TestCommittedRecordAndErrors(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	owner, err := account.New(tx).Create(ctx, fmt.Sprintf("committed-%d@example.com", time.Now().UnixNano()), "hash")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, err := pool.Exec(ctx, "DELETE FROM app.subscriptions WHERE owner_id=$1", owner.ID); err != nil {
			t.Error(err)
		}
		if _, err := pool.Exec(ctx, "DELETE FROM app.users WHERE id=$1", owner.ID); err != nil {
			t.Error(err)
		}
	})
	base, _ := price.Parse("100")
	threshold, _ := price.Parse("105")
	percent := "+5"
	v := store.Record{ID: "00000000-0000-4000-8000-100000000001", OwnerID: owner.ID, Pair: "ETHUSDT", Threshold: threshold, Direction: price.Up, CreationPrice: base, CreationTradeTime: time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC), Percent: &percent, State: domain.Active, Version: 1, Activation: store.LatestPrice}
	if err := store.New(tx).Insert(ctx, v); err != nil {
		t.Fatal(err)
	}
	if _, err := store.New(pool).ByID(ctx, owner.ID, v.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("uncommitted visible: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := store.New(pool).ByID(ctx, owner.ID, v.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, v) {
		t.Fatalf("commit round trip: %+v", got)
	}
	invalid := v
	invalid.ID = "invalid UUID"
	if err := store.New(pool).Insert(ctx, invalid); !errors.Is(err, store.ErrInvalidInput) {
		t.Fatalf("invalid: %v", err)
	}
	cancelled, stop := context.WithCancel(ctx)
	stop()
	if _, err := store.New(pool).ByID(cancelled, owner.ID, v.ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
}
