package telegramwebhook_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/telegramwebhook"
)

const secret = "test_webhook-secret_123"
const header = "X-Telegram-Bot-Api-Secret-Token"
const path = "/v1/telegram/webhook"

// Reading rejected updates would expose the untrusted body to processing.
type unreadBody struct{ t *testing.T }

func (b unreadBody) Read([]byte) (int, error) { b.t.Error("rejected body read"); return 0, io.EOF }
func (b unreadBody) Close() error             { return nil }

func TestAuthentication(t *testing.T) {
	for _, tc := range []struct {
		name    string
		values  []string
		allowed bool
	}{
		{"valid", []string{secret}, true},
		{"missing", nil, false},
		{"empty", []string{""}, false},
		{"wrong same length", []string{"Test_webhook-secret_123"}, false},
		{"short", []string{"wrong"}, false},
		{"whitespace", []string{secret + " "}, false},
		{"duplicate valid", []string{secret, secret}, false},
		{"duplicate mixed", []string{"wrong", secret}, false},
		{"combined", []string{secret + "," + secret}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			body := `{"update_id":123,"message":{"text":"hello"}}`
			h, err := telegramwebhook.Handler(secret, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				got, err := io.ReadAll(r.Body)
				if err != nil || string(got) != body || r.Method != "POST" || r.URL.Path != path {
					t.Errorf("update altered: %q, %v", got, err)
				}
				w.WriteHeader(http.StatusAccepted)
				_, _ = w.Write([]byte("processed"))
			}))
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest("POST", path+"?secret_token="+secret, strings.NewReader(body))
			if !tc.allowed {
				r.Body = unreadBody{t}
			}
			for _, value := range tc.values {
				r.Header.Add(header, value)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if tc.allowed {
				if w.Code != 202 || calls != 1 || w.Body.String() != "processed" {
					t.Fatalf("status=%d calls=%d body=%s", w.Code, calls, w.Body)
				}
			} else {
				if w.Code != 403 || calls != 0 {
					t.Fatalf("status=%d calls=%d", w.Code, calls)
				}
				if w.Body.String() != "{\"error\":{\"code\":\"invalid_webhook_secret\"}}\n" {
					t.Fatalf("unexpected error: %s", w.Body)
				}
				if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Content-Type") != "application/json" {
					t.Fatal("missing error headers")
				}
			}
		})
	}
}

func TestInvalidConfiguration(t *testing.T) {
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	for _, value := range []string{"", strings.Repeat("a", 257), "has space", "has/slash", "é", "line\nbreak"} {
		h, err := telegramwebhook.Handler(value, next)
		if err == nil || h != nil {
			t.Errorf("invalid configuration accepted")
		}
		if err != nil && strings.Contains(err.Error(), value) && value != "" {
			t.Error("secret leaked in error")
		}
	}
	if h, err := telegramwebhook.Handler(secret, nil); h != nil || err == nil {
		t.Fatal("nil handler accepted")
	}
	for _, value := range []string{"a", strings.Repeat("Z", 256), "AZaz09_-"} {
		if _, err := telegramwebhook.Handler(value, next); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRoute(t *testing.T) {
	calls := 0
	h, err := telegramwebhook.Handler(secret, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		method, path string
		status       int
	}{
		{"GET", path, 405}, {"HEAD", path, 405}, {"PUT", path, 405}, {"POST", "/other", 404}, {"POST", path + "/", 404},
	} {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		r.Header.Set(header, secret)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Errorf("%s %s: %d", tc.method, tc.path, w.Code)
		}
	}
	if calls != 0 {
		t.Fatal("incorrect route reached update handler")
	}
}
