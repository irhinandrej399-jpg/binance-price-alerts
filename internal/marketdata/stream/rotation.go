package stream

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// RotationAge leaves one hour before Binance's 24-hour connection limit.
// Age is measured from a successful handshake; every connection gets a new timer.
const RotationAge = 23 * time.Hour

// ErrRotation reports a possible gap caused by age or Binance serverShutdown.
// Reconnect handles it through the same observer/backoff/replay as transport loss.
var ErrRotation = errors.New("stream: planned rotation")

type rotationTimer func(time.Duration) (<-chan time.Time, func())

func newRotationTimer(age time.Duration) (<-chan time.Time, func()) {
	timer := time.NewTimer(age)
	return timer.C, func() { timer.Stop() }
}

// The watcher belongs to one established connection. It is stopped and joined
// before retrying, so a stale deadline cannot rotate a replacement connection.
func watchRotation(ctx context.Context, cancel context.CancelCauseFunc, timer rotationTimer) func() {
	ticks, stop := timer(RotationAge)
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		select {
		case <-ctx.Done():
		case <-done:
		case <-ticks:
			cancel(ErrRotation)
		}
	}()
	return func() { close(done); stop(); <-finished }
}

// Exact keys are required: Binance distinguishes e (type) from E (timestamp).
// Other messages, including malformed JSON, are left to the caller's handler.
func isServerShutdown(data []byte) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil {
		return false
	}
	isEvent := func(fields map[string]json.RawMessage) bool {
		var event string
		return json.Unmarshal(fields["e"], &event) == nil && event == "serverShutdown"
	}
	if raw, combined := fields["stream"]; combined {
		var stream string
		if json.Unmarshal(raw, &stream) != nil || stream != "!serverShutdown" {
			return false
		}
		var inner map[string]json.RawMessage
		return json.Unmarshal(fields["data"], &inner) == nil && isEvent(inner)
	}
	return isEvent(fields)
}
