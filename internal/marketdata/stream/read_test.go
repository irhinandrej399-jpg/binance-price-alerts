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
