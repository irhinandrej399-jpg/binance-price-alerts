// Package stream reads one Binance WebSocket connection without retries.
package stream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
)

var ErrMessageType = errors.New("stream: expected text message")

// ErrControlQueueFull terminates the connection instead of blocking price reads
// or silently losing a required pong when the peer floods the bounded queue.
var ErrControlQueueFull = errors.New("stream: pong queue full")

// Read opens endpoint once and delivers complete text messages in wire order.
// For raw trades use /ws/<lowercase-symbol>@trade. JSON and timestamp units are
// the caller's responsibility. handle runs synchronously and must return promptly
// (or honor ctx), because reading also processes ping and close control frames.
// Every exit closes the connection; even a normal peer close returns CloseError.
func Read(ctx context.Context, endpoint string, handle func(context.Context, []byte) error) error {
	return ReadWithCommands(ctx, endpoint, DefaultConfig(), nil, handle)
}

// ReadWithCommands adds paced JSON text commands to Read. The caller owns the
// commands channel and payload bytes; do not mutate bytes after sending them.
// Closing commands disables command input, without stopping prices or pongs.
// Responses/acknowledgements remain raw text messages delivered to handle.
func ReadWithCommands(ctx context.Context, endpoint string, config Config, commands <-chan []byte, handle func(context.Context, []byte) error) error {
	if _, err := config.interval(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	if err := ctx.Err(); err != nil {
		return err
	}
	if handle == nil {
		return errors.New("stream: handler required")
	}
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.DialContext(ctx, endpoint, nil)
	if err != nil {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		return fmt.Errorf("stream: dial: %w", err)
	}
	defer conn.Close()
	// Close is safe concurrently with reads and control writes. No read goroutine
	// is detached on cancellation; closing the socket wakes ReadMessage itself.
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	conn.SetReadLimit(1 << 20)
	pongs := make(chan []byte, config.ControlQueue)
	conn.SetPingHandler(func(payload string) error {
		select {
		case pongs <- []byte(payload):
			return nil
		default:
			return ErrControlQueueFull
		}
	})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		err := runWriter(ctx, config, realClock{}, commands, pongs, func(kind int, payload []byte) error {
			deadline := time.Now().Add(config.WriteTimeout)
			if kind == websocket.PongMessage {
				return conn.WriteControl(kind, payload, deadline)
			}
			if err := conn.SetWriteDeadline(deadline); err != nil {
				return err
			}
			return conn.WriteMessage(kind, payload)
		})
		cancel(fmt.Errorf("stream: write: %w", err))
	}()
	defer func() {
		cancel(nil)
		_ = conn.Close()
		<-writerDone
	}()
	for {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		kind, payload, err := conn.ReadMessage()
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		if err != nil {
			return fmt.Errorf("stream: read: %w", err)
		}
		if kind != websocket.TextMessage {
			return ErrMessageType
		}
		if err := handle(ctx, payload); err != nil {
			return fmt.Errorf("stream: handle: %w", err)
		}
	}
}
