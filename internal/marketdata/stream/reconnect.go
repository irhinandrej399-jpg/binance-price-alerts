package stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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

// AttemptCallbacks belong to one prepared dial, including a failed handshake.
// Both callbacks are required. Handle receives the connection context (also
// canceled on rotation); Disconnected receives the owner's context after cleanup.
// Callbacks run synchronously and must return promptly or honor their context.
type AttemptCallbacks struct {
	Handle       func(context.Context, []byte) error
	Disconnected func(context.Context, error, time.Duration) error
}

// PrepareAttempt runs before every dial and can bind callbacks to durable state.
// Its error is terminal and preserves errors.Is/As. No socket is opened on error.
type PrepareAttempt func(context.Context) (AttemptCallbacks, error)

// ReconnectPrepared is Reconnect with synchronous per-attempt preparation.
// Previous handlers, writer, socket and rotation watcher finish before the next
// preparation. Preparation and disconnect errors are terminal; transport failures
// and rotation use the same backoff, scheduler and command replay as Reconnect.
func ReconnectPrepared(ctx context.Context, endpoint string, config Config, retry RetryConfig, initialCommands [][]byte, prepare PrepareAttempt) error {
	return readReconnectPrepared(ctx, endpoint, config, retry, initialCommands, prepare, realClock{}, rand.Int64N, newRotationTimer)
}

func readReconnectRotating(ctx context.Context, endpoint string, config Config, retry RetryConfig, initialCommands [][]byte, handle func(context.Context, []byte) error, disconnected func(error, time.Duration), clock writerClock, random func(int64) int64, timer rotationTimer) error {
	if handle == nil || disconnected == nil {
		return errors.New("stream: handler and disconnect observer required")
	}
	return readReconnectPrepared(ctx, endpoint, config, retry, initialCommands, func(context.Context) (AttemptCallbacks, error) {
		return AttemptCallbacks{Handle: handle, Disconnected: func(_ context.Context, err error, delay time.Duration) error {
			disconnected(err, delay)
			return nil
		}}, nil
	}, clock, random, timer)
}

func readReconnectPrepared(ctx context.Context, endpoint string, config Config, retry RetryConfig, initialCommands [][]byte, prepare PrepareAttempt, clock writerClock, random func(int64) int64, timer rotationTimer) error {
	if _, err := config.interval(); err != nil {
		return err
	}
	if prepare == nil {
		return errors.New("stream: attempt preparation required")
	}
	commands := make([][]byte, len(initialCommands))
	for i, b := range initialCommands {
		commands[i] = bytes.Clone(b)
	}
	var callbacks AttemptCallbacks
	return retryConnectionsObserved(ctx, retry, clock, random, func() (bool, error) {
		var err error
		callbacks, err = prepare(ctx)
		if err != nil {
			return false, fmt.Errorf("stream: prepare: %w", err)
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if callbacks.Handle == nil || callbacks.Disconnected == nil {
			return false, errors.New("stream: attempt handler and disconnect observer required")
		}
		// A fresh channel for every connection also restores unsent pending commands.
		input := make(chan []byte, len(commands))
		for _, b := range commands {
			input <- b
		}
		close(input)
		handlerFailed := false
		err = readWithCommands(ctx, endpoint, config, input, func(ctx context.Context, b []byte) error {
			if isServerShutdown(b) {
				return ErrRotation
			}
			err := callbacks.Handle(ctx, b)
			// A cooperative handler may return the connection context cancellation
			// caused by age. That is a rotation, not a terminal application error.
			if errors.Is(err, context.Canceled) && errors.Is(context.Cause(ctx), ErrRotation) {
				return ErrRotation
			}
			handlerFailed = err != nil
			return err
		}, timer)
		return !handlerFailed, err
	}, func(err error, delay time.Duration) error {
		return callbacks.Disconnected(ctx, err, delay)
	})
}

func retryConnections(ctx context.Context, config RetryConfig, clock writerClock, random func(int64) int64, attempt func() (bool, error), disconnected func(error, time.Duration)) error {
	return retryConnectionsObserved(ctx, config, clock, random, attempt, func(err error, delay time.Duration) error {
		disconnected(err, delay)
		return nil
	})
}

func retryConnectionsObserved(ctx context.Context, config RetryConfig, clock writerClock, random func(int64) int64, attempt func() (bool, error), disconnected func(error, time.Duration) error) error {
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
		if err := disconnected(err, delay); err != nil {
			return fmt.Errorf("stream: disconnect: %w", err)
		}
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
