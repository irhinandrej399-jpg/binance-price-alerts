package stream

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

// RetryConfig bounds exponential backoff. Equal jitter selects an independent
// delay in [max(1ns, backoff/2), backoff], so even repeated immediate failures wait.
// Backoff grows across the entire call, including short-lived connections.
type RetryConfig struct {
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

func DefaultRetryConfig() RetryConfig {
	return RetryConfig{InitialBackoff: time.Second, MaxBackoff: 30 * time.Second}
}

// Reconnect rotates at RotationAge or serverShutdown and retries transport
// failures until ctx ends, replaying a private copy
// of initialCommands through the existing per-connection scheduler on every dial.
// endpoint (including URL streams/query) is unchanged. handle errors are terminal.
// Shutdown messages are consumed internally; other messages reach handle unchanged.
// disconnected is required and runs synchronously after each failure or rotation,
// before waiting the reported delay; it must return promptly or honor ctx.
// It reports a possible data gap, not recovered history or confirmed subscription.
func Reconnect(ctx context.Context, endpoint string, config Config, retry RetryConfig, initialCommands [][]byte, handle func(context.Context, []byte) error, disconnected func(error, time.Duration)) error {
	return readReconnect(ctx, endpoint, config, retry, initialCommands, handle, disconnected, realClock{}, rand.Int64N)
}

func readReconnect(ctx context.Context, endpoint string, config Config, retry RetryConfig, initialCommands [][]byte, handle func(context.Context, []byte) error, disconnected func(error, time.Duration), clock writerClock, random func(int64) int64) error {
	return readReconnectRotating(ctx, endpoint, config, retry, initialCommands, handle, disconnected, clock, random, newRotationTimer)
}

func readReconnectRotating(ctx context.Context, endpoint string, config Config, retry RetryConfig, initialCommands [][]byte, handle func(context.Context, []byte) error, disconnected func(error, time.Duration), clock writerClock, random func(int64) int64, timer rotationTimer) error {
	if _, err := config.interval(); err != nil {
		return err
	}
	if handle == nil || disconnected == nil {
		return errors.New("stream: handler and disconnect observer required")
	}
	commands := make([][]byte, len(initialCommands))
	for i, b := range initialCommands {
		commands[i] = bytes.Clone(b)
	}
	return retryConnections(ctx, retry, clock, random, func() (bool, error) {
		// A fresh channel for every connection also restores unsent pending commands.
		input := make(chan []byte, len(commands))
		for _, b := range commands {
			input <- b
		}
		close(input)
		handlerFailed := false
		err := readWithCommands(ctx, endpoint, config, input, func(ctx context.Context, b []byte) error {
			if isServerShutdown(b) {
				return ErrRotation
			}
			err := handle(ctx, b)
			// A cooperative handler may return the connection context cancellation
			// caused by age. That is a rotation, not a terminal application error.
			if errors.Is(err, context.Canceled) && errors.Is(context.Cause(ctx), ErrRotation) {
				return ErrRotation
			}
			handlerFailed = err != nil
			return err
		}, timer)
		return !handlerFailed, err
	}, disconnected)
}

func retryConnections(ctx context.Context, config RetryConfig, clock writerClock, random func(int64) int64, attempt func() (bool, error), disconnected func(error, time.Duration)) error {
	if config.InitialBackoff <= 0 || config.MaxBackoff < config.InitialBackoff {
		return errors.New("stream: invalid retry configuration")
	}
	backoff := config.InitialBackoff
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		retry, err := attempt()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !retry || err == nil {
			return err
		}
		lower := backoff / 2
		if lower < 1 {
			lower = 1
		}
		delay := lower + time.Duration(random(int64(backoff-lower)+1))
		disconnected(err, delay)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-clock.After(delay):
		}
		// Saturating multiplication avoids overflow even for MaxInt64 durations.
		if backoff > config.MaxBackoff-backoff {
			backoff = config.MaxBackoff
		} else {
			backoff *= 2
		}
	}
}
