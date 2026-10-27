package stream_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/stream"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/trade"
)

func server(t *testing.T, serve func(*websocket.Conn)) string {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ws/btcusdt@trade" {
			t.Errorf("path = %s", r.URL.Path)
		}
		c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer c.Close()
		c.SetReadDeadline(time.Now().Add(3 * time.Second))
		serve(c)
	}))
	t.Cleanup(s.Close)
	return "ws" + strings.TrimPrefix(s.URL, "http") + "/ws/btcusdt@trade"
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestEventPingAndClose(t *testing.T) {
	payload, err := os.ReadFile("../trade/testdata/trade.json")
	if err != nil {
		t.Fatal(err)
	}
	pong := make(chan string, 1)
	ping := "\x00binance\xff"
	endpoint := server(t, func(c *websocket.Conn) {
		c.SetPongHandler(func(s string) error { pong <- s; return nil })
		if err := c.WriteMessage(websocket.TextMessage, payload); err != nil {
			t.Error(err)
			return
		}
		if err := c.WriteControl(websocket.PingMessage, []byte(ping), time.Now().Add(time.Second)); err != nil {
			t.Error(err)
			return
		}
		// Reading dispatches pong, then the client answers this close.
		go func() {
			select {
			case got := <-pong:
				if got != ping {
					t.Errorf("pong = %q", got)
				}
				if err := c.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(1000, "finished"), time.Now().Add(time.Second)); err != nil {
					t.Error(err)
				}
			case <-time.After(3 * time.Second):
				t.Error("missing pong")
				c.Close()
			}
		}()
		_, _, _ = c.ReadMessage()
	})
	count := 0
	err = stream.Read(testContext(t), endpoint, func(ctx context.Context, b []byte) error {
		count++
		if string(b) != string(payload) {
			t.Fatalf("changed payload: %s", b)
		}
		_, err := trade.Decode(b, trade.Metadata{Unit: priceobserved.Milliseconds, ReceivedAt: time.Now(), Generation: 1, Sequence: 1, StreamState: priceobserved.Live})
		return err
	})
	var closed *websocket.CloseError
	if !errors.As(err, &closed) || closed.Code != 1000 || closed.Text != "finished" {
		t.Fatalf("close: %v", err)
	}
	if count != 1 {
		t.Fatalf("events = %d", count)
	}
}

func TestCancelIdleReadClosesConnection(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "deadline"}[deadline], func(t *testing.T) {
			connected := make(chan struct{})
			closed := make(chan struct{})
			endpoint := server(t, func(c *websocket.Conn) { close(connected); c.ReadMessage(); close(closed) })
			ctx, cancel := context.WithCancel(testContext(t))
			defer cancel()
			if deadline {
				ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancel()
			}
			result := make(chan error, 1)
			go func() {
				result <- stream.Read(ctx, endpoint, func(context.Context, []byte) error { t.Error("unexpected event"); return nil })
			}()
			select {
			case <-connected:
			case <-time.After(3 * time.Second):
				t.Fatal("not connected")
			}
			want := context.Canceled
			if deadline {
				want = context.DeadlineExceeded
			} else {
				cancel()
			}
			select {
			case err := <-result:
				if !errors.Is(err, want) {
					t.Fatalf("error = %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("read stuck")
			}
			select {
			case <-closed:
			case <-time.After(3 * time.Second):
				t.Fatal("connection left open")
			}
		})
	}
}

func TestReadFailures(t *testing.T) {
	sentinel := errors.New("handler failed")
	for _, mode := range []string{"abrupt", "handler", "binary", "oversize"} {
		t.Run(mode, func(t *testing.T) {
			closed := make(chan struct{})
			endpoint := server(t, func(c *websocket.Conn) {
				defer close(closed)
				switch mode {
				case "abrupt":
					return
				case "binary":
					c.WriteMessage(websocket.BinaryMessage, []byte("data"))
				case "oversize":
					c.WriteMessage(websocket.TextMessage, make([]byte, (1<<20)+1))
				default:
					c.WriteMessage(websocket.TextMessage, []byte("data"))
				}
				c.ReadMessage()
			})
			count := 0
			err := stream.Read(testContext(t), endpoint, func(context.Context, []byte) error { count++; return sentinel })
			if err == nil {
				t.Fatal("missing error")
			}
			switch mode {
			case "handler":
				if !errors.Is(err, sentinel) || count != 1 {
					t.Fatalf("handler: %v, calls %d", err, count)
				}
			case "abrupt":
				var closed *websocket.CloseError
				if !errors.As(err, &closed) || closed.Code != websocket.CloseAbnormalClosure {
					t.Fatalf("abrupt close: %v", err)
				}
			case "binary":
				if !errors.Is(err, stream.ErrMessageType) {
					t.Fatalf("binary: %v", err)
				}
			case "oversize":
				if !errors.Is(err, websocket.ErrReadLimit) {
					t.Fatalf("size: %v", err)
				}
			}
			if mode != "handler" && count != 0 {
				t.Errorf("unexpected calls: %d", count)
			}
			select {
			case <-closed:
			case <-time.After(3 * time.Second):
				t.Fatal("connection left open")
			}
		})
	}
}

func TestHandshakeFailure(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer s.Close()
	err := stream.Read(testContext(t), "ws"+strings.TrimPrefix(s.URL, "http"), func(context.Context, []byte) error { t.Error("unexpected event"); return nil })
	if !errors.Is(err, websocket.ErrBadHandshake) {
		t.Fatalf("handshake: %v", err)
	}
}

func TestCanceledBeforeDial(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := stream.Read(ctx, "ws://127.0.0.1:1/ws/btcusdt@trade", func(context.Context, []byte) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled dial: %v", err)
	}
}

func TestCommandsAndPongDoNotBlockPrices(t *testing.T) {
	first := make(chan string, 1)
	pong := make(chan string, 1)
	endpoint := server(t, func(c *websocket.Conn) {
		kind, b, err := c.ReadMessage()
		if err != nil || kind != websocket.TextMessage {
			t.Errorf("command: %d %v", kind, err)
			return
		}
		first <- string(b)
		if err := c.WriteControl(websocket.PingMessage, []byte("budget"), time.Now().Add(time.Second)); err != nil {
			t.Error(err)
			return
		}
		if err := c.WriteMessage(websocket.TextMessage, []byte("price")); err != nil {
			t.Error(err)
			return
		}
		stop := errors.New("received pong")
		c.SetPongHandler(func(s string) error { pong <- s; return stop })
		// A queued pong must precede the second command.
		if _, _, err := c.ReadMessage(); !errors.Is(err, stop) {
			t.Errorf("expected pong before command: %v", err)
			return
		}
		c.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(1000, "done"), time.Now().Add(time.Second))
	})
	commands := make(chan []byte, 12)
	for i := 0; i < 12; i++ {
		commands <- []byte(`{"method":"SUBSCRIBE","params":["btcusdt@trade"],"id":1}`)
	}
	close(commands)
	config := stream.DefaultConfig()
	config.Messages = 1
	prices := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- stream.ReadWithCommands(testContext(t), endpoint, config, commands, func(_ context.Context, b []byte) error { prices <- string(b); return nil })
	}()
	select {
	case <-first:
	case <-time.After(3 * time.Second):
		t.Fatal("no command")
	}
	select {
	case b := <-prices:
		if b != "price" {
			t.Fatal(b)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("price blocked by outbound budget")
	}
	select {
	case s := <-pong:
		if s != "budget" {
			t.Fatal(s)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no pong")
	}
	select {
	case err := <-done:
		var closed *websocket.CloseError
		if !errors.As(err, &closed) || closed.Code != 1000 {
			t.Fatalf("close = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("writer did not stop")
	}
}

func TestPongQueueOverflowFailsConnection(t *testing.T) {
	endpoint := server(t, func(c *websocket.Conn) {
		if _, _, err := c.ReadMessage(); err != nil {
			t.Error(err)
			return
		}
		for i := 0; i < 10; i++ {
			if err := c.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(time.Second)); err != nil {
				return
			}
		}
		c.ReadMessage()
	})
	config := stream.DefaultConfig()
	config.Messages = 1
	config.ControlQueue = 1
	commands := make(chan []byte, 2)
	commands <- []byte("command")
	commands <- []byte("command")
	err := stream.ReadWithCommands(testContext(t), endpoint, config, commands, func(context.Context, []byte) error { return nil })
	if !errors.Is(err, stream.ErrControlQueueFull) {
		t.Fatalf("overflow: %v", err)
	}
}

func TestCancelPendingCommand(t *testing.T) {
	connected := make(chan struct{})
	closed := make(chan struct{})
	endpoint := server(t, func(c *websocket.Conn) {
		defer close(closed)
		if _, _, err := c.ReadMessage(); err != nil {
			t.Error(err)
			return
		}
		close(connected)
		c.ReadMessage()
	})
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	commands := make(chan []byte, 3)
	for i := 0; i < 3; i++ {
		commands <- []byte("command")
	}
	config := stream.DefaultConfig()
	config.Messages = 1
	done := make(chan error, 1)
	go func() {
		done <- stream.ReadWithCommands(ctx, endpoint, config, commands, func(context.Context, []byte) error { return nil })
	}()
	select {
	case <-connected:
	case <-time.After(3 * time.Second):
		t.Fatal("no command")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("cancel blocked by budget")
	}
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("socket open")
	}
}
