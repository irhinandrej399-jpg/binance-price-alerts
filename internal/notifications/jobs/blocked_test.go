package jobs

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/telegram"
)

func TestBlockedHTTPAndRestart(t *testing.T) {
	for _, response := range []struct {
		name, body string
		status     int
		reason     domain.BlockReason
	}{
		{"bot", `{"ok":false,"error_code":403,"description":"private secret"}`, 403, domain.ChannelUnavailable},
		{"chat", `{"ok":false,"error_code":400,"description":"Bad Request: chat not found"}`, 400, domain.ChannelUnavailable},
		{"other", `{"ok":false,"error_code":400,"description":"private secret"}`, 400, domain.PermanentFailure},
	} {
		t.Run(response.name, func(t *testing.T) {
			ctx, p, a, b, clock, id := fixture(t)
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.WriteHeader(response.status)
				w.Write([]byte(response.body))
			}))
			defer server.Close()
			client, e := telegram.New(telegram.Config{Token: "secret", Timeout: time.Second, BaseURL: server.URL})
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
			_, sendErr := client.Send(ctx, 1, "private message")
			reason, ok := PermanentReason(sendErr)
			if !ok || reason != response.reason {
				t.Fatal(reason, ok, sendErr)
			}
			if e = a.SaveBlocked(ctx, l, attempt, reason, 9223372036854775807); e != nil {
				t.Fatal(e)
			}
			if _, e = p.Exec(ctx, `UPDATE app.delivery_blocked_reports SET pending=false WHERE attempt_id=$1`, attempt); e != nil {
				t.Fatal(e)
			}
			clock.Add(int64(time.Hour / time.Microsecond))
			if e = b.SaveBlocked(ctx, l, attempt, reason, 9223372036854775807); e != nil {
				t.Fatal(e)
			}
			if _, e = b.Claim(ctx, id); !errors.Is(e, ErrUnavailable) {
				t.Fatal(e)
			}
			var state, saved string
			var version int64
			var pending, unowned bool
			if e = p.QueryRow(ctx, `SELECT j.state,j.blocked_reason,j.blocked_binding_version,j.lease_id IS NULL,r.pending FROM app.delivery_jobs j JOIN app.delivery_attempts a USING(trigger_id) JOIN app.delivery_blocked_reports r USING(attempt_id) WHERE j.trigger_id=$1`, id).Scan(&state, &saved, &version, &unowned, &pending); e != nil || state != "blocked" || saved != string(reason) || version != 9223372036854775807 || !unowned || pending {
				t.Fatal(state, saved, version, unowned, pending, e)
			}
			if calls != 1 {
				t.Fatal(calls)
			}
		})
	}
}

func TestBlockedConcurrencyConflictAndSuccess(t *testing.T) {
	ctx, p, a, b, _, id := fixture(t)
	l, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	attempt := "api-" + id
	if e = a.RegisterAttempt(ctx, l, attempt); e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			s := a
			if i%2 == 0 {
				s = b
			}
			if e := s.SaveBlocked(ctx, l, attempt, domain.AttemptsExhausted, 3); e != nil {
				t.Error(e)
			}
		})
	}
	wg.Wait()
	if e = b.SaveBlocked(ctx, l, attempt, domain.PermanentFailure, 3); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	var n int
	if e = p.QueryRow(ctx, `SELECT count(*) FROM app.delivery_blocks WHERE attempt_id=$1`, attempt).Scan(&n); e != nil || n != 1 {
		t.Fatal(n, e)
	}
	if e = a.SaveSuccess(ctx, l, attempt, 99); e != nil {
		t.Fatal(e)
	}
	if e = b.SaveBlocked(ctx, l, attempt, domain.AttemptsExhausted, 3); e != nil {
		t.Fatal(e)
	}
	if _, e = b.Claim(ctx, id); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	assertSuccess(t, p, id, 1)
}

func TestMissingBindingParksWithoutAttempt(t *testing.T) {
	ctx, p, a, b, _, id := fixture(t)
	l, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.ParkBlocked(ctx, l, domain.ChannelUnavailable, 4); e != nil {
		t.Fatal(e)
	}
	if e = b.ParkBlocked(ctx, l, domain.ChannelUnavailable, 4); e != nil {
		t.Fatal(e)
	}
	if _, e = b.Claim(ctx, id); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	var state string
	var n int
	if e = p.QueryRow(ctx, `SELECT state,(SELECT count(*) FROM app.delivery_attempts WHERE trigger_id=$1) FROM app.delivery_jobs WHERE trigger_id=$1`, id).Scan(&state, &n); e != nil || state != "blocked" || n != 0 {
		t.Fatal(state, n, e)
	}
}

func TestBlockedRollback(t *testing.T) {
	ctx, p, a, _, _, id := fixture(t)
	l, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	attempt := "api-" + id
	if e = a.RegisterAttempt(ctx, l, attempt); e != nil {
		t.Fatal(e)
	}
	if e = a.SaveBlocked(ctx, l, attempt, domain.PermanentFailure, 1); e == nil {
		t.Fatal("commit failure accepted")
	}
	var n int
	var active bool
	if e = p.QueryRow(ctx, `SELECT (SELECT count(*) FROM app.delivery_blocks WHERE attempt_id=$2),state='leased' AND lease_id=$3 AND blocked_reason IS NULL FROM app.delivery_jobs WHERE trigger_id=$1`, id, attempt, l.ID).Scan(&n, &active); e != nil || n != 0 || !active {
		t.Fatal(n, active, e)
	}
}

func TestHistoricalBlockedPreservesNewLeaseAndCancel(t *testing.T) {
	for _, mode := range []string{"expired", "takeover", "cancelled", "success"} {
		t.Run(mode, func(t *testing.T) {
			ctx, p, a, b, clock, id := fixture(t)
			old, e := a.Claim(ctx, id)
			if e != nil {
				t.Fatal(e)
			}
			attempt := "api-" + id
			if e = a.RegisterAttempt(ctx, old, attempt); e != nil {
				t.Fatal(e)
			}
			var fresh Lease
			switch mode {
			case "expired":
				clock.Add(int64(time.Second / time.Microsecond))
			case "takeover":
				next := time.UnixMicro(clock.Load()).Add(time.Second)
				if e = a.SaveUncertain(ctx, old, attempt, next); e != nil {
					t.Fatal(e)
				}
				clock.Store(next.UnixMicro())
				fresh, e = b.Claim(ctx, id)
				if e != nil {
					t.Fatal(e)
				}
			case "cancelled":
				if _, e = p.Exec(ctx, `UPDATE app.delivery_jobs SET state='cancelled',lease_id=NULL,lease_expires_at=NULL WHERE trigger_id=$1`, id); e != nil {
					t.Fatal(e)
				}
			case "success":
				if e = a.SaveSuccess(ctx, old, attempt, 7); e != nil {
					t.Fatal(e)
				}
			}
			var before, after string
			snapshot := `SELECT row_to_json(j)::text FROM app.delivery_jobs j WHERE trigger_id=$1`
			if e = p.QueryRow(ctx, snapshot, id).Scan(&before); e != nil {
				t.Fatal(e)
			}
			if e = a.SaveBlocked(ctx, old, attempt, domain.ChannelUnavailable, 1); e != nil {
				t.Fatal(e)
			}
			if e = p.QueryRow(ctx, snapshot, id).Scan(&after); e != nil || before != after {
				t.Fatal(before, after, e)
			}
			if mode == "expired" {
				if _, e = b.Claim(ctx, id); !errors.Is(e, ErrUnavailable) {
					t.Fatal(e)
				}
			}
			if mode == "takeover" {
				if e = b.RegisterAttempt(ctx, fresh, "fresh-"+id); e != nil {
					t.Fatal(e)
				}
			}
			if mode == "success" {
				var n int
				if e = p.QueryRow(ctx, `SELECT count(*) FROM app.delivery_blocks WHERE attempt_id=$1`, attempt).Scan(&n); e != nil || n != 0 {
					t.Fatal(n, e)
				}
			}
		})
	}
}

func TestBlockedValidationAndUncertainRecovery(t *testing.T) {
	ctx, p, a, b, clock, id := fixture(t)
	l, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	attempt := "api-" + id
	if e = a.SaveBlocked(ctx, l, attempt, domain.ChannelUnavailable, 1); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	if e = a.RegisterAttempt(ctx, l, attempt); e != nil {
		t.Fatal(e)
	}
	if e = a.ParkBlocked(ctx, l, domain.ChannelUnavailable, 1); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	forged := l
	forged.Fence++
	if e = a.SaveBlocked(ctx, forged, attempt, domain.ChannelUnavailable, 1); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	for _, v := range []struct {
		reason  domain.BlockReason
		version int64
	}{{"private secret", 1}, {domain.ChannelUnavailable, -1}} {
		if e = a.SaveBlocked(ctx, l, attempt, v.reason, v.version); e == nil {
			t.Fatal("invalid reason/version accepted")
		}
		if e = a.ParkBlocked(ctx, l, v.reason, v.version); e == nil {
			t.Fatal("invalid park accepted")
		}
	}
	if e = a.SaveBlocked(ctx, l, attempt, domain.AttemptsExhausted, 1); e != nil {
		t.Fatal(e)
	}
	if e = b.SaveUncertain(ctx, l, attempt, time.UnixMicro(clock.Load()).Add(time.Second)); e != nil {
		t.Fatal(e)
	}
	var n int
	if e = p.QueryRow(ctx, `SELECT count(*) FROM app.delivery_uncertainties WHERE attempt_id=$1`, attempt).Scan(&n); e != nil || n != 0 {
		t.Fatal(n, e)
	}
}

func TestParkBlockedStaleAndRollback(t *testing.T) {
	ctx, p, a, b, clock, id := fixture(t)
	l, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.ParkBlocked(ctx, l, domain.ChannelUnavailable, 1); e == nil {
		t.Fatal("commit failure accepted")
	}
	var n int
	var active bool
	if e = p.QueryRow(ctx, `SELECT (SELECT count(*) FROM app.delivery_block_parks WHERE claim_id=$2),state='leased' AND lease_id=$2 AND blocked_reason IS NULL FROM app.delivery_jobs WHERE trigger_id=$1`, id, l.ID).Scan(&n, &active); e != nil || n != 0 || !active {
		t.Fatal(n, active, e)
	}
	clock.Add(int64(time.Second / time.Microsecond))
	fresh, e := b.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.ParkBlocked(ctx, l, domain.ChannelUnavailable, 1); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	if e = b.Park(ctx, fresh, WaitCondition, time.UnixMicro(clock.Load()).Add(time.Second)); e != nil {
		t.Fatal(e)
	}
}

func TestPermanentReasonRejectsTemporaryAndUnknown(t *testing.T) {
	for _, cause := range []error{nil, errors.New("secret"), (*telegram.Error)(nil), &telegram.Error{Kind: telegram.Unknown}, &telegram.Error{Kind: telegram.Server}, &telegram.Error{Kind: telegram.RateLimited}, &telegram.Error{Kind: telegram.NotSent}} {
		if r, ok := PermanentReason(cause); ok || r != "" {
			t.Fatal(r, ok)
		}
	}
}

func TestBlockedAfterUncertainStopsFutureClaims(t *testing.T) {
	ctx, _, a, b, clock, id := fixture(t)
	l, e := a.Claim(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	attempt := "api-" + id
	if e = a.RegisterAttempt(ctx, l, attempt); e != nil {
		t.Fatal(e)
	}
	next := time.UnixMicro(clock.Load()).Add(time.Second)
	if e = a.SaveUncertain(ctx, l, attempt, next); e != nil {
		t.Fatal(e)
	}
	// Unknown evidence survives, but an exhausted budget must stop automatic sends.
	if e = b.SaveBlocked(ctx, l, attempt, domain.AttemptsExhausted, 1); e != nil {
		t.Fatal(e)
	}
	clock.Store(next.UnixMicro())
	if _, e = b.Claim(ctx, id); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
}
