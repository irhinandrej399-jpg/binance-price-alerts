package storage_test

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5/pgconn"
	"sync"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/storage"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/trade"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
)

func quote(id string) priceobserved.Observed {
	p, _ := price.Parse("123.000000000000000001")
	tm := time.Date(2026, 10, 27, 10, 0, 0, 123456789, time.UTC)
	return priceobserved.Observed{SchemaVersion: 1, Pair: "BTCUSDT", Price: &p, SourceID: id, Revision: priceobserved.Revision{StreamID: trade.StreamID, Generation: 1, Sequence: 1}, TradeTime: tm, ReceivedAt: tm, SourceEventTime: &tm, StreamState: priceobserved.Live}
}
func meta(id string) event.Envelope {
	return event.Envelope{EventID: id, Type: priceobserved.Type, Version: 1, OccurredAt: time.Now().UTC(), CorrelationID: "test", OrderingKey: "BTCUSDT"}
}

func TestDurableAcceptanceAndClaims(t *testing.T) {
	pool := postgrestest.OpenWithDSN(t, "MARKET_DATA_DATABASE_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	r := storage.New(pool)
	gen, err := r.BeginGeneration(ctx, "BTCUSDT")
	if err != nil || gen != 1 {
		t.Fatalf("begin %d %v", gen, err)
	}
	if err := r.SetStatus(ctx, "BTCUSDT", gen, priceobserved.Live, false); err != nil {
		t.Fatal(err)
	}
	accept := func(id, eid string) bool {
		t.Helper()
		ok, err := r.Accept(ctx, gen, quote(id), meta(eid))
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	if !accept("10", "event1") {
		t.Fatal("not accepted")
	}
	before, err := r.Latest(ctx, "BTCUSDT")
	if err != nil {
		t.Fatal(err)
	}
	// A real UNIQUE violation in the outbox occurs after updating latest in SQL.
	if ok, err := r.Accept(ctx, gen, quote("11"), meta("event1")); err == nil || ok {
		t.Fatal("outbox failure acknowledged")
	}
	after, err := r.Latest(ctx, "BTCUSDT")
	if err != nil || after.SourceID != before.SourceID || after.Revision != before.Revision {
		t.Fatalf("rollback: %v %v", after, err)
	}
	if accept("10", "duplicate") || accept("9", "old") {
		t.Fatal("accepted old source ID")
	}
	if !accept("12", "event2") {
		t.Fatal("not accepted")
	}
	after, err = r.Latest(ctx, "BTCUSDT")
	if err != nil || !after.GapDetected || after.Revision.Sequence != 2 || !after.TradeTime.Equal(quote("12").TradeTime) {
		t.Fatalf("quote %v %v", after, err)
	}
	// New repository has no process-local queue or checkpoint.
	r = storage.New(pool)
	var wg sync.WaitGroup
	claims := make(chan storage.Work, 2)
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w, e := r.Claim(ctx, time.Minute)
			if e == nil {
				claims <- w
			}
			errs <- e
		}()
	}
	wg.Wait()
	close(claims)
	n := 0
	var first storage.Work
	for w := range claims {
		n++
		first = w
	}
	if n != 1 || first.EventID != "event1" {
		t.Fatalf("claims %d %v", n, first)
	}
	for i := 0; i < 2; i++ {
		e := <-errs
		if e != nil && !errors.Is(e, storage.ErrNotFound) {
			t.Fatal(e)
		}
	}
	_, q, err := priceobserved.Decode(first.Data)
	if err != nil || q.SourceID != "10" || !q.TradeTime.Equal(before.TradeTime) || !q.ReceivedAt.Equal(before.ReceivedAt) || !q.SourceEventTime.Equal(*before.SourceEventTime) || q.Price.Cmp(*before.Price) != 0 {
		t.Fatalf("payload %v %v", q, err)
	}
	// Force expiry using DB time, without a timing-dependent sleep.
	if _, err := pool.Exec(ctx, "UPDATE app.price_outbox SET lease_until=clock_timestamp()-interval '1 second' WHERE event_id=$1", first.EventID); err != nil {
		t.Fatal(err)
	}
	if err := r.MarkPublished(ctx, first.EventID, first.Token); !errors.Is(err, storage.ErrStale) {
		t.Fatalf("expired mark %v", err)
	}
	second, err := storage.New(pool).Claim(ctx, time.Minute)
	if err != nil || second.EventID != first.EventID || second.Token <= first.Token || string(second.Data) != string(first.Data) {
		t.Fatalf("reclaim %v %v", second, err)
	}
	if err := r.MarkPublished(ctx, first.EventID, first.Token); !errors.Is(err, storage.ErrStale) {
		t.Fatalf("stale mark %v", err)
	}
	if err := r.MarkPublished(ctx, second.EventID, second.Token); err != nil {
		t.Fatal(err)
	}
	next, err := r.Claim(ctx, time.Minute)
	if err != nil || next.EventID != "event2" {
		t.Fatalf("next %v %v", next, err)
	}
	if err := r.MarkPublished(ctx, next.EventID, next.Token); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Claim(ctx, time.Minute); !errors.Is(err, storage.ErrNotFound) {
		t.Fatal(err)
	}
	gen2, err := r.BeginGeneration(ctx, "BTCUSDT")
	if err != nil || gen2 != 2 {
		t.Fatalf("generation %d %v", gen2, err)
	}
	after, err = r.Latest(ctx, "BTCUSDT")
	if err != nil || after.StreamState != priceobserved.Unconfirmed || !after.GapDetected || after.SourceID != "12" {
		t.Fatalf("restart %v %v", after, err)
	}
	if _, err := r.Accept(ctx, gen, quote("13"), meta("stale")); !errors.Is(err, storage.ErrStale) {
		t.Fatal(err)
	}
	if err := r.SetStatus(ctx, "BTCUSDT", gen, priceobserved.Live, false); !errors.Is(err, storage.ErrStale) {
		t.Fatal(err)
	}
	gen = gen2
	if accept("12", "replayed") {
		t.Fatal("reconnect reset source boundary")
	}
	if !accept("13", "event3") {
		t.Fatal("new generation acceptance")
	}
	after, err = r.Latest(ctx, "BTCUSDT")
	if err != nil || after.Revision.Generation != 2 || after.Revision.Sequence != 1 || !after.GapDetected {
		t.Fatalf("revision %v %v", after, err)
	}
	if err := r.SetStatus(ctx, "BTCUSDT", gen, priceobserved.Unconfirmed, true); err != nil {
		t.Fatal(err)
	}
	after, err = storage.New(pool).Latest(ctx, "BTCUSDT")
	if err != nil || after.StreamState != priceobserved.Unconfirmed || !after.GapDetected {
		t.Fatal("lost status", err)
	}
	// Hold the pair lock until both independent acceptance transactions wait.
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(ctx)
	if _, err = blocker.Exec(ctx, "SELECT pair FROM app.market_streams WHERE pair='BTCUSDT' FOR UPDATE"); err != nil {
		t.Fatal(err)
	}
	// Concurrent acceptance of the same source ID must create exactly one job.
	accepted := make(chan bool, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := "concurrentA"
			if i == 1 {
				id = "concurrentB"
			}
			ok, e := r.Accept(ctx, gen, quote("14"), meta(id))
			accepted <- ok
			errs <- e
		}(i)
	}
	for {
		var waiting int
		err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE usename=current_user AND wait_event_type='Lock' AND query LIKE 'SELECT latest,generation,%'`).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting == 2 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("acceptance did not contend")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	count := 0
	for i := 0; i < 2; i++ {
		if <-accepted {
			count++
		}
		if e := <-errs; e != nil {
			t.Fatal(e)
		}
	}
	if count != 1 {
		t.Fatalf("concurrent accepted %d", count)
	}
	var jobs int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM app.price_outbox WHERE event_id IN ('concurrentA','concurrentB')").Scan(&jobs); err != nil || jobs != 1 {
		t.Fatalf("jobs %d %v", jobs, err)
	}
	for _, sql := range []string{
		"UPDATE app.price_outbox SET data=data WHERE event_id='event1'",
		"UPDATE app.price_outbox SET event_id=event_id WHERE event_id='event1'",
		"DELETE FROM app.price_outbox WHERE false",
	} {
		_, err := pool.Exec(ctx, sql)
		var pgerr *pgconn.PgError
		if !errors.As(err, &pgerr) || pgerr.Code != "42501" {
			t.Fatalf("immutable outbox: %v", err)
		}
	}
	pool.Close()
	if ok, err := r.Accept(ctx, gen, quote("14"), meta("db-down")); ok || err == nil {
		t.Fatal("database failure acknowledged")
	}
}
