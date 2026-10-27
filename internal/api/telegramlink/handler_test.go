package telegramlink_test

import (
	"context"
	"errors"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/telegramlink"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/telegramwebhook"
	"net/http/httptest"
	"strings"
	"testing"
)

type consumer struct {
	calls int
	code  string
	chat  int64
	err   error
}

func (c *consumer) Consume(_ context.Context, code string, chat int64) error {
	c.calls++
	c.code = code
	c.chat = chat
	return c.err
}
func TestUpdateHandler(t *testing.T) {
	code := strings.Repeat("A", 43)
	for _, tc := range []struct {
		name, body, secret string
		err                error
		status, calls      int
	}{
		{"valid", `{"update_id":1,"message":{"chat":{"id":123,"type":"private"},"text":"/start ` + code + `","from":{"id":123}}}`, "secret", nil, 200, 1},
		{"secret", `{`, "wrong", nil, 403, 0},
		{"group", `{"message":{"chat":{"id":123,"type":"group"},"text":"/start ` + code + `"}}`, "secret", nil, 200, 0},
		{"edited", `{"edited_message":{"chat":{"id":123,"type":"private"},"text":"/start ` + code + `"}}`, "secret", nil, 200, 0},
		{"business", `{"message":{"business_connection_id":"x","chat":{"id":123,"type":"private"},"text":"/start ` + code + `"}}`, "secret", nil, 200, 0},
		{"unknown", `{"update_id":1,"future_field":true}`, "secret", nil, 200, 0},
		{"missing code", `{"message":{"chat":{"id":123,"type":"private"},"text":"/start"}}`, "secret", nil, 200, 0},
		{"malformed", `{`, "secret", nil, 400, 0},
		{"trailing", `{} {}`, "secret", nil, 400, 0},
		{"oversize", `{"padding":"` + strings.Repeat("x", 65536) + `"}`, "secret", nil, 400, 0},
		{"replay", `{"message":{"chat":{"id":123,"type":"private"},"text":"/start ` + code + `"}}`, "secret", telegramlink.ErrInvalidCode, 200, 1},
		{"conflict", `{"message":{"chat":{"id":123,"type":"private"},"text":"/start ` + code + `"}}`, "secret", telegramlink.ErrChatInUse, 200, 1},
		{"storage", `{"message":{"chat":{"id":123,"type":"private"},"text":"/start ` + code + `"}}`, "secret", errors.New("private SQL details"), 500, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &consumer{err: tc.err}
			h, err := telegramwebhook.Handler("secret", telegramlink.Handler(c))
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest("POST", "/v1/telegram/webhook", strings.NewReader(tc.body))
			r.Header.Set("X-Telegram-Bot-Api-Secret-Token", tc.secret)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.status || c.calls != tc.calls {
				t.Fatal(w.Code, c.calls, w.Body.String())
			}
			if c.calls > 0 && (c.chat != 123 || c.code != code) {
				t.Fatal("wrong code/chat")
			}
			if strings.Contains(w.Body.String(), code) || strings.Contains(w.Body.String(), "SQL") {
				t.Fatal("secret leaked")
			}
		})
	}
}

func TestInvalidInputDoesNotReachStorage(t *testing.T) {
	s := telegramlink.New(nil, nil)
	for _, code := range []string{"", strings.Repeat("A", 42), strings.Repeat("A", 44), strings.Repeat("A", 42) + "B"} {
		if err := s.Consume(context.Background(), code, 123); !errors.Is(err, telegramlink.ErrInvalidCode) {
			t.Fatal(err)
		}
	}
	for _, chat := range []int64{0, -1} {
		if err := s.Consume(context.Background(), strings.Repeat("A", 43), chat); !errors.Is(err, telegramlink.ErrInvalidChat) {
			t.Fatal(err)
		}
	}
}
