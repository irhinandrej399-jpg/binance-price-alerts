package telegramlink_test

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/telegramcode"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/telegramlink"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/telegramwebhook"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
)

func TestLinkWithPostgres(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	codes := telegramcode.New(pool, func() time.Time { return now })
	links := telegramlink.New(pool, func() time.Time { return now })
	create := func() string {
		t.Helper()
		a, err := account.New(pool).Create(ctx, fmt.Sprintf("link-%d@example.com", time.Now().UnixNano()), "test-hash")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, err := pool.Exec(ctx, "DELETE FROM app.users WHERE id=$1", a.ID)
			if err != nil {
				t.Error(err)
			}
		})
		return a.ID
	}
	issue := func(id string) string {
		t.Helper()
		c, err := codes.Issue(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return c.Value
	}
	state := func(id string, chat, version int64) {
		t.Helper()
		var gotChat, gotVersion int64
		if err := pool.QueryRow(ctx, "SELECT COALESCE(telegram_chat_id,0),telegram_version FROM app.users WHERE id=$1", id).Scan(&gotChat, &gotVersion); err != nil {
			t.Fatal(err)
		}
		if gotChat != chat || gotVersion != version {
			t.Fatalf("state %d/%d, want %d/%d", gotChat, gotVersion, chat, version)
		}
	}
	a, b := create(), create()
	c := issue(a)
	if err := links.Consume(ctx, c, 101); err != nil {
		t.Fatal(err)
	}
	state(a, 101, 1)
	if err := links.Consume(ctx, c, 102); !errors.Is(err, telegramlink.ErrInvalidCode) {
		t.Fatal(err)
	}
	state(a, 101, 1)
	for _, chat := range []int64{102, 102} {
		if err := links.Consume(ctx, issue(a), chat); err != nil {
			t.Fatal(err)
		}
	}
	state(a, 102, 3)
	c = issue(b)
	if err := links.Consume(ctx, c, 102); !errors.Is(err, telegramlink.ErrChatInUse) {
		t.Fatal(err)
	}
	state(b, 0, 0)
	state(a, 102, 3)
	// A conflict must roll back consumption; retry on a free chat succeeds.
	if err := links.Consume(ctx, c, 103); err != nil {
		t.Fatal(err)
	}
	state(b, 103, 1)
	c = issue(a)
	for _, offset := range []time.Duration{0, time.Microsecond} {
		expired := telegramlink.New(pool, func() time.Time { return now.Add(telegramcode.TTL + offset) })
		if err := expired.Consume(ctx, c, 104); !errors.Is(err, telegramlink.ErrInvalidCode) {
			t.Fatal(err)
		}
	}
	state(a, 102, 3)
	before := telegramlink.New(pool, func() time.Time { return now.Add(telegramcode.TTL - time.Microsecond) })
	if err := before.Consume(ctx, c, 104); err != nil {
		t.Fatal(err)
	}
	old := issue(a)
	current := issue(a)
	if err := links.Consume(ctx, old, 105); !errors.Is(err, telegramlink.ErrInvalidCode) {
		t.Fatal(err)
	}
	if err := links.Consume(ctx, current, 105); err != nil {
		t.Fatal(err)
	}
	state(a, 105, 5)
	// Same code, competing destinations: exactly one commit and version increment.
	c = issue(a)
	start := make(chan struct{})
	results := make(chan error, 8)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() { <-start; results <- links.Consume(ctx, c, int64(200+i)) })
	}
	close(start)
	wg.Wait()
	close(results)
	success := 0
	for err := range results {
		if err == nil {
			success++
		} else if !errors.Is(err, telegramlink.ErrInvalidCode) {
			t.Fatal(err)
		}
	}
	if success != 1 {
		t.Fatalf("successes %d", success)
	}
	var version int64
	if err := pool.QueryRow(ctx, "SELECT telegram_version FROM app.users WHERE id=$1", a).Scan(&version); err != nil || version != 6 {
		t.Fatal(version, err)
	}
	// Different accounts competing for one chat: unique constraint arbitrates.
	c1, c2 := issue(a), issue(b)
	start = make(chan struct{})
	results = make(chan error, 2)
	for _, code := range []string{c1, c2} {
		wg.Go(func() { <-start; results <- links.Consume(ctx, code, 300) })
	}
	close(start)
	wg.Wait()
	close(results)
	success = 0
	conflicts := 0
	for err := range results {
		if err == nil {
			success++
		} else if errors.Is(err, telegramlink.ErrChatInUse) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || conflicts != 1 {
		t.Fatal(success, conflicts)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := links.Consume(canceled, issue(a), 400); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	broken := postgrestest.Open(t)
	broken.Close()
	if err := telegramlink.New(broken, nil).Consume(ctx, issue(a), 400); !errors.Is(err, telegramlink.ErrStorage) {
		t.Fatal(err)
	}
	// Exercise the complete protected HTTP path against committed storage.
	httpOwner := create()
	httpCode := issue(httpOwner)
	h, err := telegramwebhook.Handler("test-secret", telegramlink.Handler(links))
	if err != nil {
		t.Fatal(err)
	}
	body := `{"update_id":42,"message":{"chat":{"id":500,"type":"private"},"text":"/start ` + httpCode + `"}}`
	for range 2 {
		r := httptest.NewRequest("POST", "/v1/telegram/webhook", strings.NewReader(body))
		r.Header.Set("X-Telegram-Bot-Api-Secret-Token", "test-secret")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatal(w.Code)
		}
		state(httpOwner, 500, 1)
	}
	// Force consumption to discover a code and then wait on the owner lock.
	// Replace/expire it while blocked, proving the post-lock recheck is effective.
	for _, mode := range []string{"replace", "expire"} {
		t.Run(mode+" while locked", func(t *testing.T) {
			id := create()
			code := issue(id)
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			var pid int32
			if err := tx.QueryRow(ctx, "SELECT pg_backend_pid() FROM app.users WHERE id=$1 FOR UPDATE", id).Scan(&pid); err != nil {
				t.Fatal(err)
			}
			var expired atomic.Bool
			waiting := telegramlink.New(pool, func() time.Time {
				if expired.Load() {
					return now.Add(telegramcode.TTL)
				}
				return now
			})
			done := make(chan error, 1)
			go func() { done <- waiting.Consume(ctx, code, 600) }()
			deadline := time.NewTimer(5 * time.Second)
			defer deadline.Stop()
			ticker := time.NewTicker(5 * time.Millisecond)
			defer ticker.Stop()
		wait:
			for {
				var blocked bool
				if err := pool.QueryRow(ctx, "SELECT EXISTS (SELECT FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid)))", pid).Scan(&blocked); err != nil {
					t.Fatal(err)
				}
				if blocked {
					break wait
				}
				select {
				case err := <-done:
					t.Fatalf("consume did not wait: %v", err)
				case <-deadline.C:
					t.Fatal("owner lock was not reached")
				case <-ticker.C:
				}
			}
			if mode == "replace" {
				if _, err := telegramcode.New(tx, func() time.Time { return now }).Issue(ctx, id); err != nil {
					t.Fatal(err)
				}
			} else {
				expired.Store(true)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if !errors.Is(err, telegramlink.ErrInvalidCode) {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("consume stuck")
			}
			state(id, 0, 0)
		})
	}

}
