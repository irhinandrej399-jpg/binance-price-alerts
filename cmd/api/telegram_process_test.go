package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/apiprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/telegramfake"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testTelegramProcess(t *testing.T, ctx context.Context, cfg apiprocess.Config, pool *pgxpool.Pool) {
	t.Helper()
	t.Log("Telegram HTTP runtime, durable binding, replay and shutdown")
	cfg.Env["TELEGRAM_WEBHOOK_SECRET"] = telegramfake.Secret
	var owner, code string
	const chat int64 = 9007199254740993
	checkBinding := func(wantChat int64, wantVersion int) error {
		var gotChat int64
		var version, codes int
		e := pool.QueryRow(ctx, `SELECT coalesce(telegram_chat_id,0),telegram_version,(SELECT count(*) FROM app.telegram_link_codes WHERE user_id=$1) FROM app.users WHERE id=$1`, owner).Scan(&gotChat, &version, &codes)
		if e != nil || gotChat != wantChat || version != wantVersion || (wantVersion > 0 && codes != 0) {
			return errors.New("unexpected durable binding/code")
		}
		return nil
	}
	err := withDrainedAPI(ctx, cfg, pool, func(p *apiprocess.Process) error {
		client := &http.Client{Timeout: 8 * time.Second}
		request := func(path, body, token, secret string, want int, dst any) error {
			req, e := http.NewRequestWithContext(ctx, "POST", p.URL+path, strings.NewReader(body))
			if e != nil {
				return e
			}
			req.Header.Set("Content-Type", "application/json")
			if token != "" {
				req.Header.Set("Authorization", "Bearer "+token)
			}
			if secret != "" {
				req.Header.Set("X-Telegram-Bot-Api-Secret-Token", secret)
			}
			resp, e := client.Do(req)
			if e != nil {
				return errors.New("Telegram HTTP failed")
			}
			defer resp.Body.Close()
			if resp.StatusCode != want {
				return fmt.Errorf("Telegram route %s: status %d, want %d", path, resp.StatusCode, want)
			}
			if dst != nil {
				if e = json.NewDecoder(resp.Body).Decode(dst); e != nil {
					return errors.New("invalid HTTP response")
				}
			} else {
				_, e = io.Copy(io.Discard, resp.Body)
			}
			return e
		}
		email := fmt.Sprintf("ops072-%d@example.test", time.Now().UnixNano())
		credentials := `{"email":"` + email + `","password":"test-only-password"}`
		var account struct {
			ID string `json:"id"`
		}
		if e := request("/v1/accounts", credentials, "", "", 201, &account); e != nil {
			return e
		}
		owner = account.ID
		var session struct {
			Token string `json:"access_token"`
		}
		if e := request("/v1/sessions", credentials, "", "", 200, &session); e != nil {
			return e
		}
		for _, token := range []string{"", "invalid-session"} {
			if e := request("/v1/telegram/link-codes", "", token, "", 401, nil); e != nil {
				return e
			}
		}
		if e := request("/v1/telegram/link-codes", "x", session.Token, "", 400, nil); e != nil {
			return e
		}
		if e := request("/v1/telegram/link-codes", strings.Repeat("x", 65537), session.Token, "", 413, nil); e != nil {
			return e
		}
		var issued struct {
			Code      string `json:"code"`
			ExpiresIn int    `json:"expires_in"`
		}
		if e := request("/v1/telegram/link-codes", "", session.Token, "", 201, &issued); e != nil {
			return e
		}
		if len(issued.Code) != 43 || issued.ExpiresIn != 600 {
			return errors.New("invalid issued code")
		}
		code = issued.Code
		payload := fmt.Sprintf(`{"update_id":72,"message":{"chat":{"id":%d,"type":"private"},"text":"/start %s"}}`, chat, code)
		for _, secret := range []string{"", "wrong-test-secret"} {
			if e := request("/v1/telegram/webhook", payload, "", secret, 403, nil); e != nil {
				return e
			}
		}
		if e := checkBinding(0, 0); e != nil {
			return e
		}
		if e := request("/v1/telegram/webhook", strings.Repeat("x", 65537), "", telegramfake.Secret, 413, nil); e != nil {
			return e
		}
		for range 2 {
			status, e := telegramfake.Link(ctx, p.URL, 72, chat, code)
			if e != nil || status != 200 {
				return errors.New("fake Link failed")
			}
		}
		if e := checkBinding(chat, 1); e != nil {
			return e
		}
		// A new code rebinds the same chat and increments the existing version.
		if e := request("/v1/telegram/link-codes", "", session.Token, "", 201, &issued); e != nil {
			return e
		}
		status, e := telegramfake.Link(ctx, p.URL, 73, chat, issued.Code)
		if e != nil || status != 200 {
			return errors.New("rebind failed")
		}
		if e = checkBinding(chat, 2); e != nil {
			return e
		}
		if e = syscall.Kill(p.PID(), syscall.SIGTERM); e != nil {
			return e
		}
		select {
		case <-p.Done():
		case <-time.After(9 * time.Second):
			return errors.New("Telegram API shutdown timed out")
		}
		if exit, ok := p.ExitCode(); !ok || exit != 0 {
			return errors.New("Telegram API shutdown failed")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = withDrainedAPI(ctx, cfg, pool, func(p *apiprocess.Process) error {
		status, e := telegramfake.Link(ctx, p.URL, 72, chat+1, code)
		if e != nil || status != 200 {
			return errors.New("restart replay failed")
		}
		return checkBinding(chat, 2)
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"", "invalid secret", strings.Repeat("x", 257)} {
		cfg.Env["TELEGRAM_WEBHOOK_SECRET"] = secret
		e := apiprocess.With(ctx, cfg, func(*apiprocess.Process) error { return errors.New("invalid secret reached readiness") })
		if !errors.Is(e, apiprocess.ErrEarlyExit) {
			t.Fatal("invalid webhook secret startup was accepted")
		}
	}
	cfg.Env["TELEGRAM_WEBHOOK_SECRET"] = telegramfake.Secret
}
