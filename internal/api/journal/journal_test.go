package journal_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/journal"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	changed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/subscriptionchanged"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5"
)

func input(id string) journal.Entry {
	p, _ := price.Parse("999999999999999999.123456789012345678")
	return journal.Entry{Metadata: event.Envelope{EventID: id, Type: changed.Type, Version: 1, OccurredAt: time.Date(2026, 10, 28, 10, 0, 0, 123456789, time.UTC), CorrelationID: "command-1", OrderingKey: "sub-1"}, Change: changed.Changed{SchemaVersion: 1, SubscriptionID: "sub-1", SubscriptionVersion: 1, State: domain.Active, Pair: "BTCUSDT", Threshold: &p, Direction: price.Up, Activation: changed.LatestPrice, CreationSnapshot: &changed.Snapshot{Price: &p, TradeTime: time.Date(2026, 10, 28, 9, 0, 0, 0, time.UTC)}}}
}

func TestJournalTransactions(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start, err := journal.Current(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	email := fmt.Sprintf("journal-%d@example.com", time.Now().UnixNano())
	owner, err := account.New(tx).Create(ctx, email, "test-hash")
	if err != nil {
		t.Fatal(err)
	}
	a, b := input(email+"-a"), input(email+"-b")
	end, err := journal.Append(ctx, tx, []journal.Entry{a, b})
	if err != nil || end != start+2 {
		t.Fatalf("append end=%d err=%v", end, err)
	}
	if c, err := journal.Current(ctx, pool); err != nil || c != start {
		t.Fatalf("uncommitted counter %d %v", c, err)
	}
	if got, err := journal.ReadAfter(ctx, pool, start, 1); err != nil || len(got) != 0 {
		t.Fatalf("uncommitted journal %+v %v", got, err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	var exists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.users WHERE id=$1)`, owner.ID).Scan(&exists); err != nil || exists {
		t.Fatalf("business rollback %v %v", exists, err)
	}
	if c, err := journal.Current(ctx, pool); err != nil || c != start {
		t.Fatalf("rollback counter %d %v", c, err)
	}
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	end, err = journal.Append(ctx, tx, []journal.Entry{a, b})
	if err != nil || end != start+2 {
		t.Fatalf("reused range %d %v", end, err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := journal.ReadAfter(ctx, pool, start, 1)
	if err != nil || len(got) != 2 {
		t.Fatalf("whole transaction len=%d err=%v", len(got), err)
	}
	for i, want := range []journal.Entry{a, b} {
		want.Change.Position = start + uint64(i) + 1
		want.Change.TransactionEnd = end
		encoded, e := changed.Encode(want.Metadata, want.Change)
		if e != nil {
			t.Fatal(e)
		}
		meta, change, e := changed.Decode(encoded)
		if e != nil {
			t.Fatal(e)
		}
		if !reflect.DeepEqual(got[i], journal.Entry{Metadata: meta, Change: change}) {
			t.Fatalf("replay differs: %+v", got[i])
		}
	}
	if _, err = journal.ReadAfter(ctx, pool, start+1, 1); !errors.Is(err, journal.ErrInvalidInput) {
		t.Fatalf("partial cursor %v", err)
	}
	if got, err = journal.ReadAfter(ctx, pool, end, 1); err != nil || len(got) != 0 {
		t.Fatalf("after end %+v %v", got, err)
	}
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = journal.Append(ctx, tx, []journal.Entry{input(email + "-new"), a}); !errors.Is(err, journal.ErrExists) {
		t.Fatalf("duplicate %v", err)
	}
	// A failed batch must not leave even its first event/counter if caller commits.
	if err = tx.Commit(ctx); err == nil {
		t.Fatal("failed append transaction committed")
	}
	if c, err := journal.Current(ctx, pool); err != nil || c != end {
		t.Fatalf("failed append advanced %d %v", c, err)
	}
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `UPDATE app.api_journal SET body=body WHERE position=$1`, end); err == nil {
		t.Fatal("runtime can mutate journal")
	}
	tx.Rollback(ctx)
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `DELETE FROM app.api_journal WHERE position=$1`, end); err == nil {
		t.Fatal("runtime can delete replay")
	}
}

func TestCommitOrderingAndRollback(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, rollback := range []bool{false, true} {
		t.Run(fmt.Sprint(rollback), func(t *testing.T) {
			start, err := journal.Current(ctx, pool)
			if err != nil {
				t.Fatal(err)
			}
			tx1, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx1.Rollback(ctx)
			id := fmt.Sprintf("ordering-%d", time.Now().UnixNano())
			if _, err = journal.Append(ctx, tx1, []journal.Entry{input(id + "-1"), input(id + "-2")}); err != nil {
				t.Fatal(err)
			}
			conn, err := pool.Acquire(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Release()
			tx2, err := conn.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx2.Rollback(ctx)
			pid := conn.Conn().PgConn().PID()
			result := make(chan error, 1)
			go func() { _, e := journal.Append(ctx, tx2, []journal.Entry{input(id + "-3")}); result <- e }()
			// Observe a real PostgreSQL lock wait, not just a scheduling delay.
			deadline := time.Now().Add(5 * time.Second)
			for {
				var waiting bool
				err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')`, pid).Scan(&waiting)
				if err != nil {
					t.Fatal(err)
				}
				if waiting {
					break
				}
				select {
				case e := <-result:
					t.Fatalf("T2 did not wait: %v", e)
				default:
				}
				if time.Now().After(deadline) {
					t.Fatal("T2 never waited on counter")
				}
				time.Sleep(10 * time.Millisecond)
			}
			if rows, e := journal.ReadAfter(ctx, pool, start, 1); e != nil || len(rows) != 0 {
				t.Fatalf("incomplete T1 visible: %v %v", rows, e)
			}
			if rollback {
				err = tx1.Rollback(ctx)
			} else {
				err = tx1.Commit(ctx)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = <-result; err != nil {
				t.Fatal(err)
			}
			boundary := start
			if !rollback {
				boundary += 2
			}
			if c, e := journal.Current(ctx, pool); e != nil || c != boundary {
				t.Fatalf("C before T2 commit %d %v", c, e)
			}
			if err = tx2.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			rows, e := journal.ReadAfter(ctx, pool, boundary, 1)
			if e != nil || len(rows) != 1 || rows[0].Change.Position != boundary+1 || rows[0].Metadata.EventID != id+"-3" {
				t.Fatalf("lost late commit %+v %v", rows, e)
			}
		})
	}
}

func TestAppendValidation(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	bad := input("bad")
	bad.Change.Pair = "bad pair"
	for _, batch := range [][]journal.Entry{nil, {bad}} {
		if _, err = journal.Append(ctx, tx, batch); !errors.Is(err, journal.ErrInvalidInput) {
			t.Fatalf("invalid batch %v", err)
		}
	}
	if _, err = journal.ReadAfter(ctx, pool, 0, 0); !errors.Is(err, journal.ErrInvalidInput) {
		t.Fatalf("invalid limit %v", err)
	}
	if _, err = journal.ReadAfter(ctx, pool, 1<<63, 1); !errors.Is(err, journal.ErrInvalidInput) {
		t.Fatalf("overflow cursor %v", err)
	}
	if _, err = journal.ReadAfter(ctx, pool, 1<<62, 1); !errors.Is(err, journal.ErrInvalidInput) {
		t.Fatalf("future cursor %v", err)
	}
	if _, err = journal.Append(ctx, tx, []journal.Entry{input(fmt.Sprintf("once-%d", time.Now().UnixNano()))}); err != nil {
		t.Fatal(err)
	}
	// One complete batch is required: a second call would invalidate its first C.
	if _, err = journal.Append(ctx, tx, []journal.Entry{input("twice")}); !errors.Is(err, journal.ErrExists) {
		t.Fatalf("second batch %v", err)
	}
	if err = tx.Commit(ctx); !errors.Is(err, pgx.ErrTxCommitRollback) {
		t.Fatalf("second append did not abort transaction %v", err)
	}
}

func TestSnapshotBoundary(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	snapshot, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Rollback(ctx)
	start, err := journal.Current(ctx, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback(ctx)
	email := fmt.Sprintf("snapshot-journal-%d@example.com", time.Now().UnixNano())
	owner, err := account.New(writer).Create(ctx, email, "test-hash")
	if err != nil {
		t.Fatal(err)
	}
	end, err := journal.Append(ctx, writer, []journal.Entry{input(email)})
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var visible bool
	if err = snapshot.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.users WHERE id=$1)`, owner.ID).Scan(&visible); err != nil || visible {
		t.Fatalf("snapshot business row visible %v %v", visible, err)
	}
	if c, e := journal.Current(ctx, snapshot); e != nil || c != start {
		t.Fatalf("snapshot C changed %d %v", c, e)
	}
	if rows, e := journal.ReadAfter(ctx, snapshot, start, 1); e != nil || len(rows) != 0 {
		t.Fatalf("snapshot tail changed %+v %v", rows, e)
	}
	if err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.users WHERE id=$1)`, owner.ID).Scan(&visible); err != nil || !visible {
		t.Fatalf("committed business row missing %v %v", visible, err)
	}
	rows, err := journal.ReadAfter(ctx, pool, start, 1)
	if err != nil || len(rows) != 1 || rows[0].Change.TransactionEnd != end {
		t.Fatalf("replay after snapshot lost commit %+v %v", rows, err)
	}
}
