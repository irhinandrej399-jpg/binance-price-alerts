package telegramfake_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/telegramlink"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/telegramwebhook"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/telegram"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/telegramfake"
)

func TestScriptedResponses(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%t", explicit), func(t *testing.T) { scriptedResponses(t, explicit) })
	}
}

func scriptedResponses(t *testing.T, explicit bool) {
	f := newFixture(t, explicit)
	defer f.Close()
	c, err := telegram.New(telegram.Config{Token: telegramfake.Token, BaseURL: f.URL(), Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, kind := range []telegram.Kind{"", telegram.RateLimited, telegram.Forbidden, telegram.Unknown} {
		result := make(chan error, 1)
		go func() {
			id, e := c.Send(ctx, 9007199254740993, "<b>fixture</b>")
			if e == nil && id != 73 {
				e = errors.New("wrong message ID")
			}
			result <- e
		}()
		call, e := f.Next(ctx)
		if e != nil {
			t.Fatal(e)
		}
		if call.Request.ChatID != 9007199254740993 || call.Request.Text != "<b>fixture</b>" || call.Request.ParseMode != "HTML" {
			t.Fatal("wire mismatch")
		}
		select {
		case <-result:
			t.Fatal("gate escaped")
		default:
		}
		switch kind {
		case "":
			call.Reply(73)
		case telegram.RateLimited:
			call.RateLimit(17)
		case telegram.Forbidden:
			call.Forbid()
		case telegram.Unknown:
			call.Disconnect()
		}
		call.Reply(99)
		select {
		case e = <-result:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if kind == "" {
			if e != nil {
				t.Fatal(e)
			}
		} else {
			var te *telegram.Error
			if !errors.As(e, &te) || te.Kind != kind {
				t.Fatalf("unexpected error %v", e)
			}
			if kind == telegram.RateLimited && te.RetryAfterSeconds != 17 {
				t.Fatal("retry_after")
			}
		}
	}
	log := f.Requests()
	if len(log) != 4 {
		t.Fatal(len(log))
	}
	log[0].Text = "mutated"
	if f.Requests()[0].Text != "<b>fixture</b>" {
		t.Fatal("mutable log")
	}
}

type consumer struct {
	code string
	chat int64
}

func (c *consumer) Consume(_ context.Context, code string, chat int64) error {
	c.code = code
	c.chat = chat
	return nil
}
func TestLinkUpdate(t *testing.T) {
	links := &consumer{}
	h, e := telegramwebhook.Handler(telegramfake.Secret, telegramlink.Handler(links))
	if e != nil {
		t.Fatal(e)
	}
	s := httptest.NewServer(h)
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	code := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" // 32 zero bytes in base64url
	status, e := telegramfake.Link(ctx, s.URL, 42, 9007199254740993, code)
	if e != nil || status != 200 || links.code != code || links.chat != 9007199254740993 {
		t.Fatalf("update: %d %v %+v", status, e, links)
	}
}
func TestUpdateBoundary(t *testing.T) {
	for _, endpoint := range []string{"https://api.telegram.org", "http://localhost:123", "http://192.0.2.1", "http://127.0.0.1@api.telegram.org", "http://127.0.0.1/a", "http://127.0.0.1?x=y"} {
		if _, e := telegramfake.Link(context.Background(), endpoint, 1, 1, "test"); e == nil {
			t.Fatalf("accepted %s", endpoint)
		}
	}
	hit := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hit = true }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 307) }))
	defer redirect.Close()
	status, e := telegramfake.Link(context.Background(), redirect.URL, 1, 1, "test")
	if e != nil || status != 307 || hit {
		t.Fatal("redirect followed", status, e)
	}
}
func TestCloseHeldAndNext(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%t", explicit), func(t *testing.T) { closeHeldAndNext(t, explicit) })
	}
}

func closeHeldAndNext(t *testing.T, explicit bool) {
	f := newFixture(t, explicit)
	c, e := telegram.New(telegram.Config{Token: telegramfake.Token, BaseURL: f.URL(), Timeout: time.Second})
	if e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { _, e := c.Send(context.Background(), 1, "held"); done <- e }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	call, e := f.Next(ctx)
	if e != nil {
		t.Fatal(e)
	}
	f.Close()
	f.Close()
	call.Reply(1)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("held send leaked")
	}
	if _, e = f.Next(ctx); e == nil {
		t.Fatal("closed Next")
	}
	g := newFixture(t, explicit)
	defer g.Close()
	cancelled, stop := context.WithCancel(context.Background())
	stop()
	if _, e = g.Next(cancelled); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}

func TestRejectedRequestsAndIsolation(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%t", explicit), func(t *testing.T) { rejectedRequestsAndIsolation(t, explicit) })
	}
}

func rejectedRequestsAndIsolation(t *testing.T, explicit bool) {
	f := newFixture(t, explicit)
	defer f.Close()
	other := newFixture(t, explicit)
	defer other.Close()
	for _, tc := range []struct {
		method, path, body string
		status             int
	}{
		{"GET", "/bot" + telegramfake.Token + "/sendMessage", "", 405},
		{"POST", "/botREAL/sendMessage", `{}`, 404},
		{"POST", "/bot" + telegramfake.Token + "/sendMessage", `{"chat_id":1}`, 400},
		{"POST", "/bot" + telegramfake.Token + "/sendMessage", `{"chat_id":1,"text":"x","parse_mode":"HTML"} {}`, 400},
	} {
		req, e := http.NewRequest(tc.method, f.URL()+tc.path, strings.NewReader(tc.body))
		if e != nil {
			t.Fatal(e)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, e := http.DefaultClient.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.status {
			t.Fatalf("%s: %d", tc.path, resp.StatusCode)
		}
	}
	if len(f.Requests()) != 0 || len(other.Requests()) != 0 {
		t.Fatal("invalid request journaled")
	}
}

func TestConcurrentRequests(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%t", explicit), func(t *testing.T) { concurrentRequests(t, explicit) })
	}
}

func concurrentRequests(t *testing.T, explicit bool) {
	f := newFixture(t, explicit)
	defer f.Close()
	client, e := telegram.New(telegram.Config{Token: telegramfake.Token, BaseURL: f.URL(), Timeout: 2 * time.Second})
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := make(chan error, 8)
	for i := int64(1); i <= 8; i++ {
		go func(chat int64) {
			id, e := client.Send(ctx, chat, "concurrent")
			if e == nil && id != chat+100 {
				e = errors.New("crossed response")
			}
			result <- e
		}(i)
	}
	seen := map[int64]bool{}
	for i := 0; i < 8; i++ {
		call, e := f.Next(ctx)
		if e != nil {
			t.Fatal(e)
		}
		if seen[call.Request.ChatID] {
			t.Fatal("duplicate")
		}
		seen[call.Request.ChatID] = true
		call.Reply(call.Request.ChatID + 100)
	}
	for i := 0; i < 8; i++ {
		select {
		case e := <-result:
			if e != nil {
				t.Fatal(e)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if len(f.Requests()) != 8 {
		t.Fatal("missing journal")
	}
}

func TestHeldCancellation(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%t", explicit), func(t *testing.T) { heldCancellation(t, explicit) })
	}
}

func heldCancellation(t *testing.T, explicit bool) {
	f := newFixture(t, explicit)
	defer f.Close()
	c, e := telegram.New(telegram.Config{Token: telegramfake.Token, BaseURL: f.URL(), Timeout: time.Second})
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, e := c.Send(ctx, 1, "cancel"); done <- e }()
	wait, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	call, e := f.Next(wait)
	if e != nil {
		t.Fatal(e)
	}
	cancel()
	select {
	case e := <-done:
		var te *telegram.Error
		if !errors.As(e, &te) || te.Kind != telegram.Unknown {
			t.Fatal(e)
		}
	case <-wait.Done():
		t.Fatal("cancel stuck")
	}
	call.Disconnect()
	if len(f.Requests()) != 1 {
		t.Fatal("unexpected replay")
	}
}

func TestUpdateWireAndSecret(t *testing.T) {
	called := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called++
		if r.Method != "POST" || r.URL.Path != "/v1/telegram/webhook" || r.Header.Get("Content-Type") != "application/json" || r.Header.Get("X-Telegram-Bot-Api-Secret-Token") != telegramfake.Secret {
			t.Error("webhook contract")
		}
		var update struct {
			ID      int64 `json:"update_id"`
			Message struct {
				Chat struct {
					ID   int64  `json:"id"`
					Type string `json:"type"`
				} `json:"chat"`
				Text string `json:"text"`
			} `json:"message"`
		}
		if e := json.NewDecoder(r.Body).Decode(&update); e != nil || update.ID != 123 || update.Message.Chat.ID != 456 || update.Message.Chat.Type != "private" || update.Message.Text != "/start code" {
			t.Error("update payload", e)
		}
		w.WriteHeader(204)
	}))
	defer s.Close()
	status, e := telegramfake.Link(context.Background(), s.URL, 123, 456, "code")
	if e != nil || status != 204 || called != 1 {
		t.Fatal(status, e)
	}
	h, e := telegramwebhook.Handler("different_test_secret", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("wrong secret reached handler") }))
	if e != nil {
		t.Fatal(e)
	}
	protected := httptest.NewServer(h)
	defer protected.Close()
	status, e = telegramfake.Link(context.Background(), protected.URL, 1, 1, "code")
	if e != nil || status != 403 {
		t.Fatal(status, e)
	}
}

func TestCloseIncompleteBody(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%t", explicit), func(t *testing.T) { closeIncompleteBody(t, explicit) })
	}
}

func closeIncompleteBody(t *testing.T, explicit bool) {
	f := newFixture(t, explicit)
	conn, e := net.DialTimeout("tcp", strings.TrimPrefix(f.URL(), "http://"), time.Second)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	_, e = fmt.Fprintf(conn, "POST /bot%s/sendMessage HTTP/1.1\r\nHost: local\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{", telegramfake.Token)
	if e != nil {
		t.Fatal(e)
	}
	// A read deadline proves no response escaped while the body is incomplete.
	conn.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
	buf := make([]byte, 1)
	if _, e = conn.Read(buf); e == nil {
		t.Fatal("unexpected response")
	}
	done := make(chan struct{})
	go func() { f.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		conn.Close()
		<-done
		t.Fatal("Close waited for incomplete body")
	}
}
