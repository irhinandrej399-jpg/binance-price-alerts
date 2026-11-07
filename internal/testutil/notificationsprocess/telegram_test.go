package notificationsprocess

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/telegram"
)

func TestControlledTelegram(t *testing.T) {
	for _, disconnect := range []bool{false, true} {
		t.Run(map[bool]string{false: "receipt", true: "disconnect"}[disconnect], func(t *testing.T) {
			server := NewTelegram(t)
			client, err := telegram.New(telegram.Config{Token: TestToken, BaseURL: server.URL, Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			type result struct {
				id  int64
				err error
			}
			done := make(chan result, 1)
			go func() { id, e := client.Send(ctx, 123, "<b>fixture</b>"); done <- result{id, e} }()
			var call *Send
			select {
			case call = <-server.Requests:
			case <-ctx.Done():
				t.Fatal("missing HTTP entry")
			}
			if call.ChatID != 123 || call.Text != "<b>fixture</b>" || call.ParseMode != "HTML" {
				t.Fatal("wire mismatch")
			}
			select {
			case <-done:
				t.Fatal("response escaped gate")
			default:
			}
			if disconnect {
				call.Disconnect()
			} else {
				call.Reply(73)
			}
			call.Reply(74) // first disposition wins
			select {
			case r := <-done:
				var te *telegram.Error
				if disconnect {
					if !errors.As(r.err, &te) || te.Kind != telegram.Unknown {
						t.Fatal("disconnect must be ambiguous")
					}
				} else if r.err != nil || r.id != 73 {
					t.Fatal("receipt mismatch")
				}
			case <-ctx.Done():
				t.Fatal("HTTP did not finish")
			}
		})
	}
}

func TestTelegramCancellationWhileHeld(t *testing.T) {
	server := NewTelegram(t)
	client, e := telegram.New(telegram.Config{Token: TestToken, BaseURL: server.URL, Timeout: time.Second})
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, e := client.Send(ctx, 123, "held"); done <- e }()
	select {
	case <-server.Requests:
	case <-time.After(time.Second):
		t.Fatal("HTTP entry missing")
	}
	cancel()
	select {
	case e := <-done:
		var te *telegram.Error
		if !errors.As(e, &te) || te.Kind != telegram.Unknown {
			t.Fatal("cancelled send must be ambiguous")
		}
	case <-time.After(time.Second):
		t.Fatal("held Send did not cancel")
	}
}
