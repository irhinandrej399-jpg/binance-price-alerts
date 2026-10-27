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

// Read opens endpoint once and delivers complete text messages in wire order.
// For raw trades use /ws/<lowercase-symbol>@trade. JSON and timestamp units are
// the caller's responsibility. handle runs synchronously and must return promptly
// (or honor ctx), because reading also processes ping and close control frames.
// Every exit closes the connection; even a normal peer close returns CloseError.
func Read(ctx context.Context, endpoint string, handle func(context.Context, []byte) error) error {
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
			return ctx.Err()
		}
		return fmt.Errorf("stream: dial: %w", err)
	}
	defer conn.Close()
	// Close is safe concurrently with reads and control writes. No read goroutine
	// is detached on cancellation; closing the socket wakes ReadMessage itself.
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	conn.SetReadLimit(1 << 20)
	conn.SetPingHandler(func(payload string) error {
		return conn.WriteControl(websocket.PongMessage, []byte(payload), time.Now().Add(time.Second))
	})
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		kind, payload, err := conn.ReadMessage()
		if ctx.Err() != nil {
			return ctx.Err()
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
