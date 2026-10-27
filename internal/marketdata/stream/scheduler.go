package stream

import (
	"context"
	"errors"
	"time"

	"github.com/gorilla/websocket"
)

// Config sets the shared per-connection budget, bounded pong queue and write timeout.
// Messages must be 1..5 and Window at least one second; no burst tokens accrue.
type Config struct {
	Messages     int
	Window       time.Duration
	WriteTimeout time.Duration
	ControlQueue int
}

func DefaultConfig() Config {
	return Config{Messages: 5, Window: time.Second, WriteTimeout: time.Second, ControlQueue: 16}
}

func (c Config) interval() (time.Duration, error) {
	if c.Messages < 1 || c.Messages > 5 || c.Window < time.Second || c.WriteTimeout <= 0 || c.ControlQueue < 1 {
		return 0, errors.New("stream: invalid scheduler configuration")
	}
	d := c.Window / time.Duration(c.Messages)
	if c.Window%time.Duration(c.Messages) != 0 {
		d++
	}
	return d, nil
}

type writerClock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}
type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// runWriter owns every application write. A pending command never consumes the
// next slot ahead of an already queued pong. Time is charged after write completion
// so a stalled write or scheduler pause cannot cause a catch-up burst.
func runWriter(ctx context.Context, c Config, clock writerClock, commands <-chan []byte, pongs <-chan []byte, write func(int, []byte) error) error {
	interval, err := c.interval()
	if err != nil {
		return err
	}
	var command []byte
	pending := false
	var next time.Time
	for {
		var payload []byte
		kind := websocket.TextMessage
		select {
		case payload = <-pongs:
			kind = websocket.PongMessage
		default:
			if !pending {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case payload = <-pongs:
					kind = websocket.PongMessage
				case data, ok := <-commands:
					if !ok {
						commands = nil
						continue
					}
					command = data
					pending = true
				}
			}
		}
		if delay := next.Sub(clock.Now()); delay > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-clock.After(delay):
			}
		}
		if kind == websocket.TextMessage {
			select {
			case payload = <-pongs:
				kind = websocket.PongMessage
			default:
				payload = command
				pending = false
				command = nil
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := write(kind, payload); err != nil {
			return err
		}
		next = clock.Now().Add(interval)
	}
}
