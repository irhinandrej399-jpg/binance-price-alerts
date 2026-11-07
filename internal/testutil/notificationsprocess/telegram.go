package notificationsprocess

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// TestToken is deliberately fake and valid only at the local fixture endpoint.
const TestToken = "123:process-test-token"

type Telegram struct {
	URL      string
	Requests <-chan *Send
}

// Send is one observed sendMessage request. The first disposition wins.
// Receipt means the server wrote a response, not that the client received it.
type Send struct {
	ChatID    int64  `json:"chat_id"`
	Text      string `json:"text"`
	ParseMode string `json:"parse_mode"`
	once      sync.Once
	action    chan int64
	done      <-chan struct{}
}

// Done observes the server request context closing (cancellation or completion).
func (s *Send) Done() <-chan struct{} { return s.done }

func (s *Send) Reply(messageID int64) {
	if messageID <= 0 {
		panic("fixture message ID must be positive")
	}
	s.once.Do(func() { s.action <- messageID })
}

// RateLimit replies with a deterministic retry_after refusal.
func (s *Send) RateLimit()  { s.once.Do(func() { s.action <- -1 }) }
func (s *Send) Disconnect() { s.once.Do(func() { s.action <- 0 }) }

// NewTelegram accepts only the production POST/JSON wire contract. Requests are
// bounded, and handlers unblock on cancellation, timeout and fixture cleanup.
func NewTelegram(t testing.TB) *Telegram {
	t.Helper()
	requests := make(chan *Send, 16)
	closed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/bot"+TestToken+"/sendMessage" || r.Header.Get("Content-Type") != "application/json" {
			http.Error(w, "invalid request", 400)
			return
		}
		var call Send
		call.action = make(chan int64, 1)
		call.done = r.Context().Done()
		body, err := io.ReadAll(io.LimitReader(r.Body, 8193))
		if err != nil || len(body) > 8192 || json.Unmarshal(body, &call) != nil || call.ChatID <= 0 || call.Text == "" || call.ParseMode != "HTML" {
			http.Error(w, "invalid payload", 400)
			return
		}
		timer := time.NewTimer(20 * time.Second)
		defer timer.Stop()
		select {
		case requests <- &call:
		case <-closed:
			return
		case <-r.Context().Done():
			return
		case <-timer.C:
			return
		}
		select {
		case id := <-call.action:
			if id == -1 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = io.WriteString(w, `{"ok":false,"error_code":429,"parameters":{"retry_after":1}}`)
				return
			}
			if id == 0 {
				conn, _, e := w.(http.Hijacker).Hijack()
				if e == nil {
					_ = conn.Close()
				}
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"ok":true,"result":{"message_id":%d}}`, id)
		case <-closed:
		case <-r.Context().Done():
		case <-timer.C:
		}
	}))
	t.Cleanup(func() { close(closed); server.Close() })
	return &Telegram{URL: server.URL, Requests: requests}
}
