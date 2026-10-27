package telegramlink

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

type Consumer interface {
	Consume(context.Context, string, int64) error
}

// Handler processes ordinary private /start <code> messages. Mount only behind
// telegramwebhook.Handler. Unsupported updates and terminal refusals are acked;
// storage failures return 500 so Telegram can retry. No outbound messages here.
func Handler(links Consumer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		var update struct {
			Message *struct {
				Chat struct {
					ID   int64  `json:"id"`
					Type string `json:"type"`
				} `json:"chat"`
				Text                 string `json:"text"`
				BusinessConnectionID string `json:"business_connection_id"`
			} `json:"message"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
		if err := decoder.Decode(&update); err != nil {
			w.WriteHeader(400)
			return
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			w.WriteHeader(400)
			return
		}
		m := update.Message
		if m == nil || m.Chat.Type != "private" || m.Chat.ID <= 0 || m.BusinessConnectionID != "" {
			w.WriteHeader(200)
			return
		}
		fields := strings.Fields(m.Text)
		if len(fields) != 2 || fields[0] != "/start" || !validCode(fields[1]) {
			w.WriteHeader(200)
			return
		}
		err := links.Consume(r.Context(), fields[1], m.Chat.ID)
		if err != nil && !errors.Is(err, ErrInvalidCode) && !errors.Is(err, ErrChatInUse) && !errors.Is(err, ErrInvalidChat) {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
	})
}
