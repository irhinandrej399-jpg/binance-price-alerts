package stream

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/trade"
)

func TestShutdownRecognition(t *testing.T) {
	for _, data := range []string{`{"e":"serverShutdown","E":1770123456789}`, `{"stream":"!serverShutdown","data":{"e":"serverShutdown"}}`} {
		if !isServerShutdown([]byte(data)) {
			t.Fatal(data)
		}
	}
	for _, data := range []string{`{"E":"serverShutdown"}`, `{"e":"trade"}`, `{"stream":"btcusdt@trade","data":{"e":"serverShutdown"}}`, `{"stream":"!serverShutdown","data":{"e":"trade"}}`, `{"result":null,"id":1}`, `{"e":`, `null`} {
		if isServerShutdown([]byte(data)) {
			t.Fatal(data)
		}
	}
}

// The first handler holds the read loop while both the timer and shutdown
// message become ready. Only one reconnect may result, with the same plan.
func TestRotationRestoresPlanAndPreservesFilter(t *testing.T) {
	for _, trigger := range []string{"age", "age-handler", "raw", "combined", "both"} {
		t.Run(trigger, func(t *testing.T) {
			clock := newFakeClock()
			var connections atomic.Int32
			commands := make(chan string, 4)
			sendShutdown := make(chan struct{})
			shutdownSent := make(chan struct{})
			entered := make(chan struct{})
			release := make(chan struct{})
			timerArmed := make(chan *fakeClock, 4)
			var stopped atomic.Int32
			timer := func(age time.Duration) (<-chan time.Time, func()) {
				clock := newFakeClock()
				ch := clock.After(age)
				if d := <-clock.armed; d != 23*time.Hour {
					t.Errorf("rotation age = %v", d)
				}
				timerArmed <- clock
				return ch, func() { stopped.Add(1) }
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.RequestURI() != "/ws?streams=btcusdt@trade/ethusdt@trade" {
					t.Error(r.URL)
				}
				c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer c.Close()
				n := connections.Add(1)
				c.SetReadDeadline(time.Now().Add(3 * time.Second))
				_, b, err := c.ReadMessage()
				if err != nil {
					t.Error(err)
					return
				}
				commands <- string(b)
				data := []byte(`{"e":"trade","s":"BTCUSDT","t":10,"p":"1","T":1672515782136,"E":1672515782136}`)
				c.WriteMessage(websocket.TextMessage, data)
				if n == 1 {
					<-sendShutdown
					payload := `{"e":"serverShutdown"}`
					if trigger == "combined" {
						payload = `{"stream":"!serverShutdown","data":{"e":"serverShutdown"}}`
					}
					if trigger != "age" && trigger != "age-handler" {
						c.WriteMessage(websocket.TextMessage, []byte(payload))
					}
					close(shutdownSent)
				} else {
					c.WriteMessage(websocket.TextMessage, []byte(`{"e":"trade","s":"BTCUSDT","t":11,"p":"1","T":1672515782136,"E":1672515782136}`))
				}
				c.ReadMessage()
			}))
			defer server.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			gaps := make(chan error, 4)
			var previous *priceobserved.Observed
			accepted := []string{}
			messages := 0
			go func() {
				done <- readReconnectRotating(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/ws?streams=btcusdt@trade/ethusdt@trade", DefaultConfig(), DefaultRetryConfig(), [][]byte{[]byte(`{"method":"SUBSCRIBE","params":["btcusdt@trade","ethusdt@trade"],"id":1}`)}, func(handlerCtx context.Context, b []byte) error {
					messages++
					q, err := trade.Decode(b, trade.Metadata{Unit: priceobserved.Milliseconds, ReceivedAt: time.Now(), Generation: 1, Sequence: uint64(messages), StreamState: priceobserved.Live})
					if err != nil {
						return err
					}
					decision, err := trade.CheckNext(previous, q)
					if err != nil {
						return err
					}
					if decision.Accept {
						accepted = append(accepted, q.SourceID)
						previous = &q
					}
					if messages == 1 {
						close(entered)
						if trigger == "age-handler" {
							<-handlerCtx.Done()
							return handlerCtx.Err()
						}
						<-release
					}
					if messages == 3 {
						cancel()
					}
					return nil
				}, func(err error, _ time.Duration) { gaps <- err }, clock, func(int64) int64 { return 0 }, timer)
			}()
			first := receive(t, commands)
			age := receive(t, timerArmed)
			receive(t, entered)
			close(sendShutdown)
			receive(t, shutdownSent)
			if trigger == "age" || trigger == "age-handler" || trigger == "both" {
				age.advance(RotationAge)
			}
			close(release)
			if err := receive(t, gaps); !errors.Is(err, ErrRotation) {
				t.Fatal(err)
			}
			clock.advance(receive(t, clock.armed))
			if second := receive(t, commands); second != first {
				t.Fatal(second, first)
			}
			receive(t, timerArmed)
			if err := receive(t, done); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if connections.Load() != 2 || stopped.Load() != 2 {
				t.Fatal(connections.Load(), stopped.Load())
			}
			if strings.Join(accepted, ",") != "10,11" {
				t.Fatal(accepted)
			}
			select {
			case err := <-gaps:
				t.Fatalf("extra rotation: %v", err)
			default:
			}
		})
	}
}

func TestRotationTimerDeadlineAndCleanup(t *testing.T) {
	for _, cancelEarly := range []bool{false, true} {
		t.Run(map[bool]string{false: "deadline", true: "canceled"}[cancelEarly], func(t *testing.T) {
			clock := newFakeClock()
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			stopped := false
			stop := watchRotation(ctx, cancel, func(age time.Duration) (<-chan time.Time, func()) {
				return clock.After(age), func() { stopped = true }
			})
			if d := receive(t, clock.armed); d != 23*time.Hour {
				t.Fatal(d)
			}
			clock.advance(23*time.Hour - time.Nanosecond)
			if ctx.Err() != nil {
				t.Fatal("rotated too early")
			}
			if cancelEarly {
				cancel(context.Canceled)
				stop()
				// An expired timer from the closed connection must have no surviving watcher.
				clock.advance(time.Nanosecond)
				if !errors.Is(context.Cause(ctx), context.Canceled) {
					t.Fatal(context.Cause(ctx))
				}
			} else {
				clock.advance(time.Nanosecond)
				receive(t, ctx.Done())
				stop()
				if !errors.Is(context.Cause(ctx), ErrRotation) {
					t.Fatal(context.Cause(ctx))
				}
			}
			if !stopped {
				t.Fatal("timer not stopped")
			}
		})
	}
}
