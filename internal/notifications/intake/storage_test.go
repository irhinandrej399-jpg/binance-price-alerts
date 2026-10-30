package intake

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
)

const message = `{"event_id":"outbox-1","type":"alert.triggered","version":1,"occurred_at":"2026-10-27T10:00:01Z","correlation_id":"request-1","ordering_key":"sub-1","payload":{"schema_version":1,"intent":"delivery_requested","trigger_id":"workflow-1","workflow_version":9007199254740993,"user_id":"user-1","subscription_id":"sub-1","subscription_version":9007199254740994,"pair":"BTCUSDT","threshold":"999999999999999999.123456789012345677","direction":"up","candidate_quote":{"schema_version":1,"pair":"BTCUSDT","price":"999999999999999999.123456789012345678","source_id":"9007199254740993","revision":{"stream_id":"binance-spot-trade","generation":2,"sequence":9007199254740993},"trade_time":"2026-10-27T10:00:00.123456Z","received_at":"2026-10-27T10:00:01.123456Z","stream_state":"unconfirmed","gap_detected":true}}}`

func bounded(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func wire(id string) []byte {
	return bytes.ReplaceAll([]byte(message), []byte("workflow-1"), []byte(id))
}
func TestDurableDuplicatesAndConcurrentInsert(t *testing.T) {
	p := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
	ctx := bounded(t)
	s := NewStore(p)
	topic := t.Name()
	id := topic
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := range 20 {
		wg.Go(func() {
			part := int32(i)
			if _, e := s.Checkpoint(ctx, topic, part); e != nil {
				errs <- e
				return
			}
			errs <- s.Accept(ctx, topic, part, 0, 0, []byte("sub-1"), wire(id))
		})
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	var count int
	var body []byte
	if e := p.QueryRow(ctx, "SELECT count(*) FROM app.delivery_jobs WHERE trigger_id=$1", id).Scan(&count); e != nil || count != 1 {
		t.Fatal(count, e)
	}
	if _, e := p.Exec(ctx, "UPDATE app.delivery_jobs SET state='wait_condition' WHERE trigger_id=$1", id); e != nil {
		t.Fatal(e)
	}
	// A new event/workflow version for this trigger cannot reset existing work.
	newer := strings.Replace(string(wire(id)), `"workflow_version":9007199254740993`, `"workflow_version":9007199254740994`, 1)
	newer = strings.Replace(newer, `"event_id":"outbox-1"`, `"event_id":"resumed-event"`, 1)
	if e := s.Accept(ctx, topic, 0, 1, 3, []byte("sub-1"), []byte(newer)); e != nil {
		t.Fatal(e)
	}
	if e := s.Accept(ctx, topic, 0, 0, 0, []byte("sub-1"), wire(id)); e != nil {
		t.Fatal(e)
	}
	var state string
	if e := p.QueryRow(ctx, "SELECT state,body FROM app.delivery_jobs WHERE trigger_id=$1", id).Scan(&state, &body); e != nil || state != "wait_condition" || !bytes.Equal(body, wire(id)) {
		t.Fatal(state, e)
	}
	if n, e := s.Checkpoint(ctx, topic, 0); e != nil || n != 4 {
		t.Fatal(n, e)
	}
	if e := s.Accept(ctx, topic, 0, 1, 5, []byte("sub-1"), wire(id+"-gap")); !errors.Is(e, ErrCheckpoint) {
		t.Fatal(e)
	}
}
func TestRollbackAndInvalid(t *testing.T) {
	p := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
	ctx := bounded(t)
	s := NewStore(p)
	for _, topic := range []string{"fail-checkpoint", "fail-commit"} {
		if _, e := s.Checkpoint(ctx, topic, 0); e != nil {
			t.Fatal(e)
		}
		if e := s.Accept(ctx, topic, 0, 0, 0, []byte("sub-1"), wire(topic)); e == nil {
			t.Fatal("injected failure accepted")
		}
		n, e := s.Checkpoint(ctx, topic, 0)
		if e != nil || n != 0 {
			t.Fatal(n, e)
		}
		var count int
		if e = p.QueryRow(ctx, "SELECT count(*) FROM app.delivery_jobs WHERE trigger_id=$1", topic).Scan(&count); e != nil || count != 0 {
			t.Fatal(count, e)
		}
	}
	topic := t.Name()
	if _, e := s.Checkpoint(ctx, topic, 0); e != nil {
		t.Fatal(e)
	}
	for i, raw := range [][]byte{[]byte("null"), bytes.Replace(wire(topic), []byte(`"version":1`), []byte(`"version":2`), 1), wire(topic)} {
		key := []byte("sub-1")
		if i == 2 {
			key = []byte("wrong")
		}
		if e := s.Accept(ctx, topic, 0, 0, 0, key, raw); !errors.Is(e, ErrContract) {
			t.Fatal(i, e)
		}
	}
	if n, e := s.Checkpoint(ctx, topic, 0); e != nil || n != 0 {
		t.Fatal(n, e)
	}
	// Existing role policy also denies CONNECT to every foreign database.
	for _, db := range []string{"api", "market_data", "matcher"} {
		var allowed bool
		if e := p.QueryRow(ctx, "SELECT has_database_privilege(current_user,$1,'CONNECT')", db).Scan(&allowed); e != nil || allowed {
			t.Fatal(db, allowed, e)
		}
	}
}
