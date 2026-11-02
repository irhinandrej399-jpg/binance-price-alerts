package jobs

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5/pgxpool"
)

func fixture(t *testing.T) (context.Context, *pgxpool.Pool, *Store, *Store, *atomic.Int64, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	p := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
	p2 := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
	a, err := New(p, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(p2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var clock atomic.Int64
	clock.Store(time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC).UnixMicro())
	now := func(context.Context) (time.Time, error) { return time.UnixMicro(clock.Load()), nil }
	a.now, b.now = now, now
	id := t.Name()
	_, err = p.Exec(ctx, `INSERT INTO app.delivery_jobs(trigger_id,subscription_id,user_id,event_id,body) VALUES($1,'sub','user','event',$2)`, id, []byte("request"))
	if err != nil {
		t.Fatal(err)
	}
	return ctx, p, a, b, &clock, id
}

func TestConcurrentClaimExpiryRestartAndStaleWorker(t *testing.T) {
	ctx, p, a, b, clock, id := fixture(t)
	var wg sync.WaitGroup
	got := make(chan Lease, 20)
	for i := range 20 {
		wg.Go(func() {
			s := a
			if i%2 == 0 {
				s = b
			}
			l, e := s.Claim(ctx, id)
			if e == nil {
				got <- l
			} else if !errors.Is(e, ErrUnavailable) {
				t.Error(e)
			}
		})
	}
	wg.Wait()
	close(got)
	var first Lease
	n := 0
	for l := range got {
		first = l
		n++
	}
	if n != 1 || first.Fence != 1 || string(first.Body) != "request" {
		t.Fatalf("claims=%d lease=%+v", n, first)
	}
	clock.Add(int64(time.Second/time.Microsecond) - 1)
	if _, e := b.Claim(ctx, id); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	clock.Add(1) // equality expires, even if the old process is still running
	if e := a.Park(ctx, first, WaitCondition, time.UnixMicro(clock.Load()).Add(time.Second)); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	// A newly constructed store with a separate pool recovers without old memory.
	second, e := b.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if second.Fence != 2 || second.ID == first.ID || !second.ExpiresAt.Equal(first.ExpiresAt.Add(time.Second)) {
		t.Fatal(first, second)
	}
	if e = a.Park(ctx, first, WaitLimiter, time.UnixMicro(clock.Load()).Add(time.Hour)); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	forged := second
	forged.ID = first.ID
	if e = a.Park(ctx, forged, WaitLimiter, time.UnixMicro(clock.Load()).Add(time.Hour)); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	var status string
	if e = p.QueryRow(ctx, `SELECT status FROM app.delivery_claims WHERE trigger_id=$1 AND fence=1`, id).Scan(&status); e != nil || status != "expired" {
		t.Fatal(status, e)
	}
	if e = b.Park(ctx, second, WaitCondition, time.UnixMicro(clock.Load()).Add(time.Second)); e != nil {
		t.Fatal(e)
	}
}

func TestParkingReleasesLeaseAndPreservesWorkflow(t *testing.T) {
	ctx, p, a, b, clock, id := fixture(t)
	for _, reason := range []WaitReason{WaitCondition, WaitLimiter} {
		l, e := a.Claim(ctx, id)
		if e != nil {
			t.Fatal(e)
		}
		next := time.UnixMicro(clock.Load()).Add(10 * time.Second)
		if e = a.Park(ctx, l, reason, next); e != nil {
			t.Fatal(e)
		}
		var active bool
		var state string
		if e = p.QueryRow(ctx, `SELECT lease_id IS NOT NULL,state FROM app.delivery_jobs WHERE trigger_id=$1`, id).Scan(&active, &state); e != nil || active || state != string(reason) {
			t.Fatal(active, state, e)
		}
		if _, e = b.Claim(ctx, id); !errors.Is(e, ErrUnavailable) {
			t.Fatal(e)
		}
		if e = a.Park(ctx, l, reason, next); !errors.Is(e, ErrStale) {
			t.Fatal(e)
		}
		clock.Store(next.UnixMicro())
	}
	l, e := b.Claim(ctx, id)
	if e != nil || l.Fence != 3 || string(l.Body) != "request" {
		t.Fatal(l, e)
	}
}

func TestLockedJobDoesNotBlockAndTerminalIsUnavailable(t *testing.T) {
	ctx, p, a, _, _, id := fixture(t)
	tx, e := p.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(context.Background())
	if _, e = tx.Exec(ctx, `SELECT 1 FROM app.delivery_jobs WHERE trigger_id=$1 FOR UPDATE`, id); e != nil {
		t.Fatal(e)
	}
	short, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	if _, e = a.Claim(short, id); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	if e = tx.Rollback(ctx); e != nil {
		t.Fatal(e)
	}
	for _, state := range []string{"delivered", "cancelled", "blocked", "uncertain"} {
		if _, e = p.Exec(ctx, `UPDATE app.delivery_jobs SET state=$2 WHERE trigger_id=$1`, id, state); e != nil {
			t.Fatal(e)
		}
		if _, e = a.Claim(ctx, id); !errors.Is(e, ErrUnavailable) {
			t.Fatal(state, e)
		}
	}
	if _, e = a.Claim(ctx, "missing"); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
}

func TestSQLClockAndInputValidation(t *testing.T) {
	ctx, p, _, _, _, id := fixture(t)
	s, e := New(p, time.Second)
	if e != nil {
		t.Fatal(e)
	}
	var before, after time.Time
	if e = p.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&before); e != nil {
		t.Fatal(e)
	}
	l, e := s.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = p.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&after); e != nil {
		t.Fatal(e)
	}
	if l.ExpiresAt.Before(before.Add(time.Second)) || l.ExpiresAt.After(after.Add(time.Second)) {
		t.Fatal(l.ExpiresAt, before, after)
	}
	for _, ttl := range []time.Duration{0, -1, time.Nanosecond, time.Minute + time.Microsecond} {
		if _, e = New(p, ttl); e == nil {
			t.Fatal(ttl)
		}
	}
	if _, e = New(nil, time.Second); e == nil {
		t.Fatal("nil pool")
	}
	for _, next := range []time.Time{{}, before} {
		if e = s.Park(ctx, l, WaitCondition, next); e == nil {
			t.Fatal("bad deadline")
		}
	}
	if e = s.Park(ctx, l, WaitReason("sending"), after.Add(time.Second)); e == nil {
		t.Fatal("bad reason")
	}
}

func TestClaimRollback(t *testing.T) {
	ctx, p, a, _, _, id := fixture(t)
	if _, e := a.Claim(ctx, id); e == nil {
		t.Fatal("injected commit failure accepted")
	}
	var fence, count int64
	var unowned bool
	var state string
	if e := p.QueryRow(ctx, `SELECT lease_fence,lease_id IS NULL,state FROM app.delivery_jobs WHERE trigger_id=$1`, id).Scan(&fence, &unowned, &state); e != nil || fence != 0 || !unowned || state != "pending" {
		t.Fatal(fence, unowned, state, e)
	}
	if e := p.QueryRow(ctx, `SELECT count(*) FROM app.delivery_claims WHERE trigger_id=$1`, id).Scan(&count); e != nil || count != 0 {
		t.Fatal(count, e)
	}
}

func TestParkRollback(t *testing.T) {
	ctx, p, a, b, clock, id := fixture(t)
	l, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.Park(ctx, l, WaitLimiter, time.UnixMicro(clock.Load()).Add(time.Minute)); e == nil {
		t.Fatal("injected commit failure accepted")
	}
	var state, status string
	var active bool
	if e = p.QueryRow(ctx, `SELECT j.state,j.lease_id=$2,c.status FROM app.delivery_jobs j JOIN app.delivery_claims c ON c.claim_id=j.lease_id WHERE j.trigger_id=$1`, id, l.ID).Scan(&state, &active, &status); e != nil || state != "leased" || !active || status != "claimed" {
		t.Fatal(state, active, status, e)
	}
	if _, e = b.Claim(ctx, id); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	clock.Add(int64(time.Second / time.Microsecond))
	next, e := b.Claim(ctx, id)
	if e != nil || next.Fence != 2 {
		t.Fatal(next, e)
	}
}

func TestFenceOverflowFailsClosed(t *testing.T) {
	ctx, p, a, _, _, id := fixture(t)
	if _, e := p.Exec(ctx, `UPDATE app.delivery_jobs SET lease_fence=9223372036854775807 WHERE trigger_id=$1`, id); e != nil {
		t.Fatal(e)
	}
	if _, e := a.Claim(ctx, id); e == nil {
		t.Fatal("overflow accepted")
	}
	var unowned bool
	if e := p.QueryRow(ctx, `SELECT lease_id IS NULL FROM app.delivery_jobs WHERE trigger_id=$1`, id).Scan(&unowned); e != nil || !unowned {
		t.Fatal(unowned, e)
	}
}
