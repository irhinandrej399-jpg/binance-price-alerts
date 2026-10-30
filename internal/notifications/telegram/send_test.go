package telegram_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/telegram"
)

func client(t *testing.T, url string, timeout time.Duration) *telegram.Client {
	t.Helper()
	c, err := telegram.New(telegram.Config{Token: "123:test-secret", BaseURL: url, Timeout: timeout})
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func failure(t *testing.T, err error, kind telegram.Kind, timeout bool) *telegram.Error {
	t.Helper()
	var e *telegram.Error
	if !errors.As(err, &e) || e.Kind != kind || e.Timeout != timeout {
		t.Fatalf("error = %#v, want %s timeout=%v", err, kind, timeout)
	}
	if strings.Contains(err.Error(), "test-secret") || strings.Contains(err.Error(), "private-text") {
		t.Fatalf("unsafe error: %v", err)
	}
	return e
}
func TestSuccessRequest(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != "POST" || r.URL.Path != "/bot123:test-secret/sendMessage" || r.Header.Get("Content-Type") != "application/json" {
			t.Error("incorrect request")
		}
		var body struct {
			ChatID    int64  `json:"chat_id"`
			Text      string `json:"text"`
			ParseMode string `json:"parse_mode"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body.ChatID != 4503599627370495 || body.Text != "<b>BTC</b> &amp; цена" || body.ParseMode != "HTML" {
			t.Errorf("body=%+v", body)
		}
		io.WriteString(w, `{"ok":true,"result":{"message_id":123456789,"chat":{"id":4503599627370495}}}`)
	}))
	defer srv.Close()
	id, err := client(t, srv.URL, time.Second).Send(context.Background(), 4503599627370495, "<b>BTC</b> &amp; цена")
	if err != nil || id != 123456789 || calls.Load() != 1 {
		t.Fatalf("id=%d err=%v calls=%d", id, err, calls.Load())
	}
}
func TestResponses(t *testing.T) {
	for _, tt := range []struct {
		name   string
		status int
		body   string
		kind   telegram.Kind
		retry  int64
	}{
		{"flood", 429, `{"ok":false,"error_code":429,"parameters":{"retry_after":37},"description":"private-text test-secret"}`, telegram.RateLimited, 37},
		{"forbidden", 403, `{"ok":false,"error_code":403}`, telegram.Forbidden, 0},
		{"server", 503, `unavailable private-text`, telegram.Server, 0},
		{"bad chat", 400, `{"ok":false,"error_code":400}`, telegram.Rejected, 0},
		{"unauthorized", 401, `{"ok":false,"error_code":401}`, telegram.Rejected, 0},
		{"envelope error", 200, `{"ok":false,"error_code":429,"parameters":{"retry_after":9}}`, telegram.RateLimited, 9},
		{"malformed", 200, `{"ok":true`, telegram.Unknown, 0},
		{"missing id", 200, `{"ok":true,"result":{}}`, telegram.Unknown, 0},
		{"zero id", 200, `{"ok":true,"result":{"message_id":0}}`, telegram.Unknown, 0},
		{"missing ok", 200, `{"result":{"message_id":3}}`, telegram.Unknown, 0},
		{"trailing", 200, `{"ok":true,"result":{"message_id":3}} {}`, telegram.Unknown, 0},
		{"oversize", 200, strings.Repeat("x", (1<<20)+1), telegram.Unknown, 0},
		{"bad flood body", 429, `broken`, telegram.RateLimited, 0},
		{"negative retry", 429, `{"ok":false,"parameters":{"retry_after":-1}}`, telegram.RateLimited, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(tt.status)
				io.WriteString(w, tt.body)
			}))
			defer srv.Close()
			id, err := client(t, srv.URL, time.Second).Send(context.Background(), 42, "private-text")
			e := failure(t, err, tt.kind, false)
			if id != 0 || e.RetryAfterSeconds != tt.retry || e.HTTPStatus != tt.status || calls.Load() != 1 {
				t.Fatalf("id=%d error=%+v calls=%d", id, e, calls.Load())
			}
		})
	}
}
func TestLostResponseAndTimeout(t *testing.T) {
	for _, mode := range []string{"disconnect", "headers timeout", "body timeout", "context deadline", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				io.Copy(io.Discard, r.Body)
				if mode == "disconnect" {
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					conn.Close()
					return
				}
				if mode == "body timeout" {
					io.WriteString(w, `{"ok":true,"result":`)
					w.(http.Flusher).Flush()
				}
				<-r.Context().Done()
			}))
			defer srv.Close()
			ctx := context.Background()
			timeout := 40 * time.Millisecond
			if mode == "context deadline" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 40*time.Millisecond)
				defer cancel()
				timeout = time.Second
			}
			if mode == "cancel" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
				time.AfterFunc(40*time.Millisecond, cancel)
				timeout = time.Second
			}
			id, err := client(t, srv.URL, timeout).Send(ctx, 42, "private-text")
			failure(t, err, telegram.Unknown, mode != "disconnect" && mode != "cancel")
			if id != 0 || calls.Load() != 1 {
				t.Fatalf("id=%d calls=%d", id, calls.Load())
			}
		})
	}
}
func TestNoRedirect(t *testing.T) {
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1) }))
	defer target.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 307) }))
	defer srv.Close()
	_, err := client(t, srv.URL, time.Second).Send(context.Background(), 42, "private-text")
	failure(t, err, telegram.Unknown, false)
	if redirected.Load() != 0 {
		t.Fatal("redirect followed")
	}
}
func TestInvalidAndCanceledInput(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer srv.Close()
	c := client(t, srv.URL, time.Second)
	for _, tt := range []struct {
		chat int64
		text string
	}{{0, "x"}, {-1, "x"}, {42, ""}} {
		_, err := c.Send(context.Background(), tt.chat, tt.text)
		failure(t, err, telegram.NotSent, false)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.Send(ctx, 42, "private-text")
	failure(t, err, telegram.NotSent, false)
	if calls.Load() != 0 {
		t.Fatal("sent invalid input")
	}
	for _, cfg := range []telegram.Config{{}, {Token: "bad/token", Timeout: time.Second}, {Token: "abc", Timeout: 0}, {Token: "abc", Timeout: time.Second, BaseURL: "https://user:password@host"}, {Token: "abc", Timeout: time.Second, BaseURL: "http://example.com"}} {
		if _, err := telegram.New(cfg); err == nil {
			t.Fatal("accepted invalid config")
		}
	}
}
