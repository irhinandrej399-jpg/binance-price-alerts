// Package telegramfake provides a controlled loopback Telegram fixture.
package telegramfake

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"time"
)

// These credentials are deliberately fake. The fixture never accepts real tokens.
const Token = "123:e2e-test-token"
const Secret = "e2e_test_webhook_secret"

// Request is a snapshot of one accepted sendMessage wire payload.
type Request struct {
	ChatID    int64  `json:"chat_id"`
	Text      string `json:"text"`
	ParseMode string `json:"parse_mode"`
}

// Send controls one accepted request. The first disposition wins.
// Request is a caller-owned copy; changing it does not change the journal.
type Send struct {
	Request Request
	once    sync.Once
	action  chan response
}
type response struct {
	kind  string
	value int64
}

func (s *Send) Reply(id int64) {
	if id <= 0 {
		panic("message ID must be positive")
	}
	s.resolve(response{"success", id})
}
func (s *Send) RateLimit(seconds int64) {
	if seconds <= 0 {
		panic("retry_after must be positive")
	}
	s.resolve(response{"rate_limit", seconds})
}
func (s *Send) Forbid() { s.resolve(response{kind: "forbidden"}) }

// Disconnect drops TCP after reading the full request, before writing a response.
// It models an ambiguous outcome, not proof that delivery did not happen.
func (s *Send) Disconnect()        { s.resolve(response{kind: "disconnect"}) }
func (s *Send) resolve(r response) { s.once.Do(func() { s.action <- r }) }

type Fake struct {
	server *httptest.Server
	calls  chan *Send
	closed chan struct{}
	once   sync.Once
	mu     sync.Mutex
	log    []Request
}

// New starts a fresh loopback server. Close must be called by its owner.
func New() *Fake {
	f := &Fake{calls: make(chan *Send, 16), closed: make(chan struct{})}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	return f
}
func (f *Fake) URL() string { return f.server.URL }

// Close unblocks held requests and joins handlers. It is safe to repeat.
func (f *Fake) Close() {
	f.once.Do(func() {
		close(f.closed)
		f.server.CloseClientConnections()
		f.server.Close()
	})
}
func (f *Fake) Next(ctx context.Context) (*Send, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case <-f.closed:
		return nil, errors.New("fake Telegram closed")
	default:
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-f.closed:
		return nil, errors.New("fake Telegram closed")
	case call := <-f.calls:
		return call, nil
	}
}

// Requests returns an independent journal snapshot, in acceptance order.
func (f *Fake) Requests() []Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Request(nil), f.log...)
}
func (f *Fake) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/bot"+Token+"/sendMessage" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, "invalid content type", 400)
		return
	}
	var request Request
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<10))
	if err != nil || json.Unmarshal(body, &request) != nil || request.ChatID <= 0 || request.Text == "" || request.ParseMode != "HTML" {
		http.Error(w, "invalid payload", 400)
		return
	}
	call := &Send{Request: request, action: make(chan response, 1)}
	// Serialize acceptance and enqueue so journal order matches Next order.
	f.mu.Lock()
	select {
	case f.calls <- call:
		f.log = append(f.log, request)
	default:
		f.mu.Unlock()
		http.Error(w, "fixture queue full", 503)
		return
	}
	f.mu.Unlock()
	timer := time.NewTimer(20 * time.Second)
	defer timer.Stop()
	select {
	case a := <-call.action:
		if a.kind == "disconnect" {
			conn, _, e := w.(http.Hijacker).Hijack()
			if e == nil {
				_ = conn.Close()
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch a.kind {
		case "success":
			_, _ = fmt.Fprintf(w, `{"ok":true,"result":{"message_id":%d}}`, a.value)
		case "rate_limit":
			w.WriteHeader(429)
			_, _ = fmt.Fprintf(w, `{"ok":false,"error_code":429,"parameters":{"retry_after":%d}}`, a.value)
		case "forbidden":
			w.WriteHeader(403)
			_, _ = io.WriteString(w, `{"ok":false,"error_code":403,"description":"Forbidden: bot was blocked by the user"}`)
		}
	case <-f.closed:
	case <-r.Context().Done():
	case <-timer.C:
		w.WriteHeader(http.StatusGatewayTimeout)
	}
}

// Link posts a private /start update with the fixed test secret to the fixed
// webhook path. Origin must be an HTTP literal loopback address. DNS, proxies,
// redirects and arbitrary paths are disabled; no production bot API is called.
func Link(ctx context.Context, origin string, updateID, chatID int64, code string) (int, error) {
	invalid := errors.New("invalid test webhook configuration")
	u, err := url.Parse(origin)
	if err != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawFragment != "" || u.Opaque != "" || (u.Path != "" && u.Path != "/") {
		return 0, invalid
	}
	ip := net.ParseIP(u.Hostname())
	if ip == nil || !ip.IsLoopback() || chatID <= 0 || updateID < 0 || code == "" {
		return 0, invalid
	}
	u.Path = "/v1/telegram/webhook"
	payload := struct {
		UpdateID int64 `json:"update_id"`
		Message  struct {
			Chat struct {
				ID   int64  `json:"id"`
				Type string `json:"type"`
			} `json:"chat"`
			Text string `json:"text"`
		} `json:"message"`
	}{UpdateID: updateID}
	payload.Message.Chat.ID = chatID
	payload.Message.Chat.Type = "private"
	payload.Message.Text = "/start " + code
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return 0, invalid
	}
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Telegram-Bot-Api-Secret-Token", Secret)
	transport := &http.Transport{DialContext: (&net.Dialer{Timeout: time.Second}).DialContext}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		return 0, errors.New("test webhook request failed")
	}
	defer resp.Body.Close()
	_, err = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return resp.StatusCode, errors.New("test webhook response failed")
	}
	return resp.StatusCode, nil
}
