package jobs

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/retry"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/telegram"
)

func TestUncertainDisconnectRestartAndWaitCondition(t *testing.T) {
	ctx, p, a, b, clock, id := fixture(t)
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
		if calls.Add(1) > 1 {
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"ok":true,"result":{"message_id":88}}`)
			return
		}
		c, _, e := w.(http.Hijacker).Hijack()
		if e != nil {
			t.Error(e)
			return
		}
		c.Close()
	}))
	defer server.Close()
	client, e := telegram.New(telegram.Config{Token: "fixture", Timeout: time.Second, BaseURL: server.URL})
	if e != nil {
		t.Fatal(e)
	}
	l, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	attempt := "api-" + id
	if e = a.RegisterAttempt(ctx, l, attempt); e != nil {
		t.Fatal(e)
	}
	_, e = client.Send(ctx, 1, "alert")
	var unknown *telegram.Error
	if !errors.As(e, &unknown) || unknown.Kind != telegram.Unknown {
		t.Fatal(e)
	}
	policy, _ := retry.New(retry.Config{BaseDelay: time.Second, MaxDelay: time.Second, ConditionInterval: 3 * time.Second, MaxFailures: 5})
	next := policy.WaitCondition(time.UnixMicro(clock.Load()), 0).NextAttemptAt
	if e = a.SaveUncertain(ctx, l, attempt, next); e != nil {
		t.Fatal(e)
	}
	// Lost successful DB response: restart and replay, retaining first deadline.
	restarted, _ := New(b.pool, time.Second)
	restarted.now = b.now
	b = restarted
	if e = b.SaveUncertain(ctx, l, attempt, next.Add(time.Hour)); e != nil {
		t.Fatal(e)
	}
	if _, e = b.Claim(ctx, id); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	var pending bool
	var saved time.Time
	if e = p.QueryRow(ctx, `SELECT r.pending,u.next_attempt_at FROM app.delivery_uncertainties u JOIN app.delivery_uncertain_reports r USING(attempt_id) WHERE attempt_id=$1`, attempt).Scan(&pending, &saved); e != nil || !pending || !saved.Equal(next) {
		t.Fatal(pending, saved, e)
	}
	clock.Store(next.UnixMicro())
	fresh, e := b.Claim(ctx, id)
	if e != nil || !fresh.PossibleDuplicate || fresh.TriggerID != id {
		t.Fatal(fresh, e)
	}
	// A fresh API wait_condition result is represented by Park, without another HTTP.
	if e = b.Park(ctx, fresh, WaitCondition, next.Add(3*time.Second)); e != nil {
		t.Fatal(e)
	}
	if calls.Load() != 1 {
		t.Fatal(calls.Load())
	}
	var state string
	if e = p.QueryRow(ctx, `SELECT state FROM app.delivery_jobs WHERE trigger_id=$1`, id).Scan(&state); e != nil || state != "wait_condition" {
		t.Fatal(state, e)
	}
	// Later an admitted true condition may produce a second external message.
	// This composes the storage/sender contract; actual API admission is NTF-012.
	clock.Store(next.Add(3 * time.Second).UnixMicro())
	ready, e := b.Claim(ctx, id)
	if e != nil || !ready.PossibleDuplicate || ready.TriggerID != id {
		t.Fatal(ready, e)
	}
	if e = b.RegisterAttempt(ctx, ready, "new-api-"+id); e != nil {
		t.Fatal(e)
	}
	message, e := client.Send(ctx, 1, "newly admitted alert")
	if e != nil || message != 88 {
		t.Fatal(message, e)
	}
	if e = b.SaveSuccess(ctx, ready, "new-api-"+id, message); e != nil {
		t.Fatal(e)
	}
	if calls.Load() != 2 {
		t.Fatal(calls.Load())
	}
	if _, e = b.Claim(ctx, id); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}

}

func TestUnresolvedAttemptRequiresDurableRecovery(t *testing.T) {
	ctx, _, a, b, clock, id := fixture(t)
	old, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.RegisterAttempt(ctx, old, "api-"+id); e != nil {
		t.Fatal(e)
	}
	clock.Add(int64(time.Second / time.Microsecond))
	_, e = b.Claim(ctx, id)
	var unresolved *UnresolvedAttempt
	if !errors.As(e, &unresolved) || unresolved.AttemptID != "api-"+id || unresolved.Lease.ID != old.ID {
		t.Fatal(e)
	}
	next := time.UnixMicro(clock.Load()).Add(time.Second)
	if e = b.SaveUncertain(ctx, unresolved.Lease, unresolved.AttemptID, next); e != nil {
		t.Fatal(e)
	}
	if _, e = b.Claim(ctx, id); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	clock.Store(next.UnixMicro())
	fresh, e := b.Claim(ctx, id)
	if e != nil || !fresh.PossibleDuplicate {
		t.Fatal(fresh, e)
	}
	if e = a.SaveSuccess(ctx, old, "api-"+id, 1); e != nil {
		t.Fatal(e)
	}
	if e = b.RegisterAttempt(ctx, fresh, "new-"+id); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
}

func TestUncertainConcurrentIdempotencyAndLateSuccess(t *testing.T) {
	ctx, p, a, b, clock, id := fixture(t)
	before, e := a.UncertainAttempts(ctx)
	if e != nil {
		t.Fatal(e)
	}
	old, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	attempt := "api-" + id
	if e = a.RegisterAttempt(ctx, old, attempt); e != nil {
		t.Fatal(e)
	}
	next := time.UnixMicro(clock.Load()).Add(time.Second)
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			s := a
			if i%2 == 0 {
				s = b
			}
			if e := s.SaveUncertain(ctx, old, attempt, next); e != nil {
				t.Error(e)
			}
		})
	}
	wg.Wait()
	var n int
	if e = p.QueryRow(ctx, `SELECT count(*) FROM app.delivery_uncertainties WHERE attempt_id=$1`, attempt).Scan(&n); e != nil || n != 1 {
		t.Fatal(n, e)
	}
	if _, e = p.Exec(ctx, `UPDATE app.delivery_uncertain_reports SET pending=false WHERE attempt_id=$1`, attempt); e != nil {
		t.Fatal(e)
	}
	clock.Store(next.UnixMicro())
	fresh, e := b.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.SaveUncertain(ctx, old, attempt, next.Add(time.Hour)); e != nil {
		t.Fatal(e)
	}
	if e = a.SaveSuccess(ctx, old, attempt, 123); e != nil {
		t.Fatal(e)
	}
	if e = b.SaveUncertain(ctx, old, attempt, next); e != nil {
		t.Fatal(e)
	}
	var active, pending bool
	if e = p.QueryRow(ctx, `SELECT lease_id=$2 AND state='leased' FROM app.delivery_jobs WHERE trigger_id=$1`, id, fresh.ID).Scan(&active); e != nil || !active {
		t.Fatal(active, e)
	}
	if e = p.QueryRow(ctx, `SELECT pending FROM app.delivery_uncertain_reports WHERE attempt_id=$1`, attempt).Scan(&pending); e != nil || pending {
		t.Fatal(pending, e)
	}
	assertSuccess(t, p, id, 1)
	metric, e := b.UncertainAttempts(ctx)
	if e != nil || metric != before+1 {
		t.Fatal(metric, e)
	}
}

func TestUncertainRollback(t *testing.T) {
	ctx, p, a, _, clock, id := fixture(t)
	l, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.RegisterAttempt(ctx, l, "api-"+id); e != nil {
		t.Fatal(e)
	}
	if e = a.SaveUncertain(ctx, l, "api-"+id, time.UnixMicro(clock.Load()).Add(time.Second)); e == nil {
		t.Fatal("commit failure accepted")
	}
	var n int
	var active bool
	if e = p.QueryRow(ctx, `SELECT (SELECT count(*) FROM app.delivery_uncertainties WHERE attempt_id=$2),state='leased' AND lease_id=$3 FROM app.delivery_jobs WHERE trigger_id=$1`, id, "api-"+id, l.ID).Scan(&n, &active); e != nil || n != 0 || !active {
		t.Fatal(n, active, e)
	}
}

func TestUncertainMetricCancellation(t *testing.T) {
	_, _, a, _, _, _ := fixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := a.UncertainAttempts(ctx); e == nil {
		t.Fatal("cancelled read succeeded")
	}
}

func TestUncertainValidationAndSuccessFirst(t *testing.T) {
	ctx, p, a, _, clock, id := fixture(t)
	l, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	next := time.UnixMicro(clock.Load()).Add(time.Second)
	if e = a.SaveUncertain(ctx, l, "missing", next); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	attempt := "api-" + id
	if e = a.RegisterAttempt(ctx, l, attempt); e != nil {
		t.Fatal(e)
	}
	forged := l
	forged.Fence++
	if e = a.SaveUncertain(ctx, forged, attempt, next); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	for _, deadline := range []time.Time{{}, time.UnixMicro(clock.Load())} {
		if e = a.SaveUncertain(ctx, l, attempt, deadline); e == nil {
			t.Fatal("bad deadline accepted")
		}
	}
	if e = a.SaveSuccess(ctx, l, attempt, 7); e != nil {
		t.Fatal(e)
	}
	if e = a.SaveUncertain(ctx, l, attempt, next); e != nil {
		t.Fatal(e)
	}
	var n int
	if e = p.QueryRow(ctx, `SELECT count(*) FROM app.delivery_uncertainties WHERE attempt_id=$1`, attempt).Scan(&n); e != nil || n != 0 {
		t.Fatal(n, e)
	}
	assertSuccess(t, p, id, 1)
}

func TestHistoricalUncertainPreservesCancelledJob(t *testing.T) {
	ctx, p, a, _, clock, id := fixture(t)
	l, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	attempt := "api-" + id
	if e = a.RegisterAttempt(ctx, l, attempt); e != nil {
		t.Fatal(e)
	}
	if _, e = p.Exec(ctx, `UPDATE app.delivery_jobs SET state='cancelled',lease_id=NULL,lease_expires_at=NULL WHERE trigger_id=$1`, id); e != nil {
		t.Fatal(e)
	}
	if e = a.SaveUncertain(ctx, l, attempt, time.UnixMicro(clock.Load()).Add(time.Second)); e != nil {
		t.Fatal(e)
	}
	var state string
	if e = p.QueryRow(ctx, `SELECT state FROM app.delivery_jobs WHERE trigger_id=$1`, id).Scan(&state); e != nil || state != "cancelled" {
		t.Fatal(state, e)
	}
	if _, e = a.Claim(ctx, id); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
}

func TestExpiredUncertainRollback(t *testing.T) {
	ctx, p, a, b, clock, id := fixture(t)
	l, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	attempt := "api-" + id
	if e = a.RegisterAttempt(ctx, l, attempt); e != nil {
		t.Fatal(e)
	}
	clock.Add(int64(time.Second / time.Microsecond))
	if e = a.SaveUncertain(ctx, l, attempt, time.UnixMicro(clock.Load()).Add(time.Second)); e == nil {
		t.Fatal("commit failure accepted")
	}
	var n int
	if e = p.QueryRow(ctx, `SELECT count(*) FROM app.delivery_uncertainties WHERE attempt_id=$1`, attempt).Scan(&n); e != nil || n != 0 {
		t.Fatal(n, e)
	}
	_, e = b.Claim(ctx, id)
	var unresolved *UnresolvedAttempt
	if !errors.As(e, &unresolved) {
		t.Fatal(e)
	}
}
