package e2escenario_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/postgres"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/binancefake"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/e2estack"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/telegramfake"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestUserJourney(t *testing.T) {
	if os.Getenv("E2E_PROCESS_INTEGRATION") != "1" {
		t.Skip("run scripts/check_e2e_environment.py --package ./internal/testutil/e2escenario")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	s := e2estack.New(t, ctx)
	api, err := postgres.Open(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer api.Close()
	ntf, err := postgres.Open(ctx, os.Getenv("NOTIFICATIONS_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer ntf.Close()
	transport := &http.Transport{Proxy: nil}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 9 * time.Second}
	err = s.With(ctx, func(s *e2estack.Stack) error {
		// Each observable phase has its own deadline. Polls only pace observations;
		// elapsed time alone never proves that the asynchronous path completed.
		wait := func(label string, check func(context.Context) (bool, error)) error {
			c, done := context.WithTimeout(ctx, 20*time.Second)
			defer done()
			for {
				ok, e := check(c)
				if e != nil {
					return fmt.Errorf("%s: %w", label, e)
				}
				if ok {
					return nil
				}
				select {
				case <-c.Done():
					return fmt.Errorf("%s: deadline exceeded", label)
				case <-time.After(20 * time.Millisecond):
				}
			}
		}
		request := func(method, path, body, token string, want int, dst any) error {
			req, e := http.NewRequestWithContext(ctx, method, s.API.URL+path, strings.NewReader(body))
			if e != nil {
				return e
			}
			req.Header.Set("Content-Type", "application/json")
			if token != "" {
				req.Header.Set("Authorization", "Bearer "+token)
			}
			res, e := client.Do(req)
			if e != nil {
				return fmt.Errorf("%s %s failed", method, path)
			}
			defer res.Body.Close()
			if res.StatusCode != want {
				return fmt.Errorf("%s %s: status %d want %d", method, path, res.StatusCode, want)
			}
			if dst != nil {
				return json.NewDecoder(io.LimitReader(res.Body, 65536)).Decode(dst)
			}
			_, e = io.Copy(io.Discard, res.Body)
			return e
		}
		conn, e := s.NextFeed(ctx)
		if e != nil {
			return e
		}
		var seq uint64
		trade := func(value string) error {
			seq++
			now := time.Now().UnixMilli()
			if e := conn.TradePair(ctx, "BTCUSDT", binancefake.Trade{ID: seq, Price: value, TradeTime: now, EventTime: now}); e != nil {
				return e
			}
			c, done := context.WithTimeout(ctx, 10*time.Second)
			defer done()
			return s.CheckQuote(c, fmt.Sprint(seq), value)
		}
		if e = trade("99"); e != nil {
			return e
		}
		if e = s.Ready(ctx); e != nil {
			return e
		}
		credentials := `{"email":"journey@example.test","password":"test-only-password"}`
		var account struct {
			ID string `json:"id"`
		}
		var session struct {
			Token string `json:"access_token"`
		}
		var code struct {
			Code string `json:"code"`
		}
		if e = request("POST", "/v1/accounts", credentials, "", 201, &account); e != nil {
			return e
		}
		if e = request("POST", "/v1/sessions", credentials, "", 200, &session); e != nil {
			return e
		}
		if e = request("POST", "/v1/telegram/link-codes", "", session.Token, 201, &code); e != nil {
			return e
		}
		if status, e := telegramfake.Link(ctx, s.API.URL, 1, 90001, code.Code); e != nil || status != 200 {
			return fmt.Errorf("Telegram link failed: status %d", status)
		}
		var chat int64
		if e = api.QueryRow(ctx, `SELECT telegram_chat_id FROM app.users WHERE id=$1`, account.ID).Scan(&chat); e != nil || chat != 90001 {
			return fmt.Errorf("binding not persisted")
		}
		create := func() (string, error) {
			var v struct {
				ID       string   `json:"id"`
				Price    string   `json:"creation_price"`
				Warnings []string `json:"warnings"`
			}
			e := request("POST", "/v1/subscriptions", `{"pair":"BTCUSDT","price":"100","direction":"up"}`, session.Token, 201, &v)
			if e != nil {
				return "", e
			}
			if v.ID == "" || v.Price != "99" || !contains(v.Warnings, "stream_unconfirmed") {
				return "", fmt.Errorf("creation snapshot/warning changed")
			}
			return v.ID, nil
		}
		slots := func(want int) error {
			var n int
			if e := api.QueryRow(ctx, `SELECT active_subscriptions FROM app.users WHERE id=$1`, account.ID).Scan(&n); e != nil {
				return e
			}
			if n != want {
				return fmt.Errorf("slots %d want %d", n, want)
			}
			return nil
		}
		state := func(id, want string) error {
			return wait("subscription "+want, func(c context.Context) (bool, error) {
				var st string
				e := api.QueryRow(c, `SELECT state FROM app.subscriptions WHERE id=$1`, id).Scan(&st)
				return st == want, e
			})
		}
		pending := func(id string) (string, error) {
			if e := state(id, "pending_delivery"); e != nil {
				return "", e
			}
			var w string
			e := api.QueryRow(ctx, `SELECT workflow_id FROM app.subscriptions WHERE id=$1`, id).Scan(&w)
			return w, e
		}
		// Gate ONLY delivery intake, using an ordinary rollback-only SQL lock.
		// It cannot fabricate prices, jobs, permissions or outcomes. Release on error
		// precedes stack shutdown so SQL cancellation/drain can complete normally.
		gate := func() (func(), error) {
			tx, e := ntf.Begin(ctx)
			if e != nil {
				return nil, e
			}
			if _, e = tx.Exec(ctx, `LOCK TABLE app.delivery_jobs IN SHARE MODE`); e != nil {
				tx.Rollback(ctx)
				return nil, e
			}
			return func() {
				c, done := context.WithTimeout(context.Background(), time.Second)
				defer done()
				_ = tx.Rollback(c)
			}, nil
		}
		deliver := func(id, workflow, value string, messageID int64) error {
			c, done := context.WithTimeout(ctx, 10*time.Second)
			defer done()
			call, e := s.Telegram.Next(c)
			if e != nil {
				return fmt.Errorf("Telegram send: %w", e)
			}
			// HTTP arrival alone does not free the slot: fake is still withholding success.
			if e = slots(1); e != nil {
				return e
			}
			if call.Request.ChatID != 90001 || call.Request.ParseMode != "HTML" || !strings.Contains(call.Request.Text, "BTCUSDT") || !strings.Contains(call.Request.Text, "Порог: 100") || !strings.Contains(call.Request.Text, "Фактическая цена: "+value+"\n") || !strings.Contains(call.Request.Text, "Актуальность рыночных данных не подтверждена") {
				return fmt.Errorf("Telegram payload differs from admitted quote")
			}
			call.Reply(messageID)
			if e = state(id, "delivered"); e != nil {
				return e
			}
			if e = slots(0); e != nil {
				return e
			}
			var w, attempt string
			var mid int64
			var reserved bool
			if e = api.QueryRow(ctx, `SELECT workflow_id,delivered_attempt_id,delivered_message_id,quota_reserved FROM app.subscriptions WHERE id=$1`, id).Scan(&w, &attempt, &mid, &reserved); e != nil {
				return e
			}
			if w != workflow || attempt == "" || mid != messageID || reserved {
				return fmt.Errorf("canonical API receipt/workflow changed")
			}
			return wait("durable success/report", func(c context.Context) (bool, error) {
				var n int
				e := ntf.QueryRow(c, `SELECT count(*) FROM app.delivery_successes s JOIN app.delivery_success_reports r USING(attempt_id) JOIN app.delivery_attempts a USING(attempt_id) WHERE a.trigger_id=$1 AND s.attempt_id=$2 AND s.message_id=$3 AND NOT r.pending`, workflow, attempt, messageID).Scan(&n)
				return n == 1, e
			})
		}
		// Straight journey: registration/link -> subscription -> raw trade -> one
		// Telegram receipt -> API commit -> released quota.
		first, e := create()
		if e != nil {
			return e
		}
		if e = slots(1); e != nil {
			return e
		}
		if e = trade("101"); e != nil {
			return e
		}
		// Do not require pending_delivery here: a fast worker may already be sending.
		var workflow string
		if e = wait("first workflow", func(c context.Context) (bool, error) {
			e := api.QueryRow(c, `SELECT coalesce(workflow_id,'') FROM app.subscriptions WHERE id=$1`, first).Scan(&workflow)
			return workflow != "", e
		}); e != nil {
			return e
		}
		if e = deliver(first, workflow, "101", 501); e != nil {
			return e
		}
		t.Log("straight journey: exact send, durable receipt, quota released")
		// A real durable workflow exists at 101, but no intake can pass the gate
		// until Latest has actually observed 99. This checks admission, not timing.
		if e = trade("99"); e != nil {
			return e
		}
		second, e := create()
		if e != nil {
			return e
		}
		release, e := gate()
		if e != nil {
			return e
		}
		e = func() error {
			defer release()
			if e := trade("101"); e != nil {
				return e
			}
			var e error
			workflow, e = pending(second)
			if e != nil {
				return e
			}
			return trade("99")
		}()
		if e != nil {
			return e
		}
		if e = wait("false condition parked", func(c context.Context) (bool, error) {
			var n int
			e := ntf.QueryRow(c, `SELECT count(*) FROM app.delivery_jobs WHERE trigger_id=$1 AND state='wait_condition' AND lease_id IS NULL`, workflow).Scan(&n)
			return n == 1, e
		}); e != nil {
			return e
		}
		if e = state(second, "active"); e != nil {
			return e
		}
		if e = slots(1); e != nil {
			return e
		}
		if e = exactAttempts(ctx, api, ntf, second, 0); e != nil {
			return e
		}
		if len(s.Telegram.Requests()) != 1 {
			return fmt.Errorf("disappeared condition sent Telegram")
		}
		if e = trade("102"); e != nil {
			return e
		}
		if e = deliver(second, workflow, "102", 502); e != nil {
			return e
		}
		t.Log("101 -> 99: no HTTP, slot reserved, job parked; 102: same workflow, one confirmed send")
		// Cancel after durable request creation but before any admission.
		if e = trade("99"); e != nil {
			return e
		}
		third, e := create()
		if e != nil {
			return e
		}
		release, e = gate()
		if e != nil {
			return e
		}
		e = func() error {
			defer release()
			if e := trade("101"); e != nil {
				return e
			}
			if _, e := pending(third); e != nil {
				return e
			}
			return request("DELETE", "/v1/subscriptions/"+third, "", session.Token, 200, nil)
		}()
		if e != nil {
			return e
		}
		if e = state(third, "cancelled"); e != nil {
			return e
		}
		if e = slots(0); e != nil {
			return e
		}
		// Terminal PrepareRecipient allocates no admission and leaves its claim
		// to expire. Observe intake and re-claim after expiry, rather than infer
		// absence while Kafka is still catching up. No local stop-state is required.
		if e = wait("cancelled job processed", func(c context.Context) (bool, error) {
			var n int
			e := ntf.QueryRow(c, `SELECT count(*) FROM app.delivery_jobs j JOIN app.delivery_claims c USING(trigger_id) WHERE j.subscription_id=$1 AND c.status='expired'`, third).Scan(&n)
			return n > 0, e
		}); e != nil {
			return e
		}
		for _, id := range []string{first, second, third} {
			want := 1
			if id == third {
				want = 0
			}
			if e = exactAttempts(ctx, api, ntf, id, want); e != nil {
				return e
			}
		}
		var listed struct {
			Subscriptions []json.RawMessage `json:"subscriptions"`
		}
		if e = request("GET", "/v1/subscriptions", "", session.Token, 200, &listed); e != nil {
			return e
		}
		if len(listed.Subscriptions) != 0 {
			return fmt.Errorf("terminal subscriptions still listed")
		}
		if len(s.Telegram.Requests()) != 2 {
			return fmt.Errorf("duplicate/cancelled Telegram send")
		}
		t.Log("cancel before admission: real intake/claim, zero attempts/HTTP; two total confirmed deliveries")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.AssertStopped(ctx); err != nil {
		t.Fatal(err)
	}
}

func contains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

func exactAttempts(ctx context.Context, api, ntf *pgxpool.Pool, id string, want int) error {
	var admissions, results, jobs, attempts, successes int
	if e := api.QueryRow(ctx, `SELECT (SELECT count(*) FROM app.delivery_admissions WHERE subscription_id=$1 AND attempt_id IS NOT NULL),(SELECT count(*) FROM app.delivery_results WHERE subscription_id=$1 AND outcome='succeeded')`, id).Scan(&admissions, &results); e != nil {
		return e
	}
	if e := ntf.QueryRow(ctx, `SELECT count(DISTINCT j.trigger_id),count(DISTINCT a.attempt_id),count(DISTINCT s.attempt_id) FROM app.delivery_jobs j LEFT JOIN app.delivery_attempts a USING(trigger_id) LEFT JOIN app.delivery_successes s USING(attempt_id) WHERE j.subscription_id=$1`, id).Scan(&jobs, &attempts, &successes); e != nil {
		return e
	}
	if jobs != 1 || admissions != want || results != want || attempts != want || successes != want {
		return fmt.Errorf("exact outcomes: jobs=%d admissions=%d results=%d attempts=%d successes=%d want=%d", jobs, admissions, results, attempts, successes, want)
	}
	return nil
}
