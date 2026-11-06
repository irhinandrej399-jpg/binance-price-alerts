//go:build darwin || linux

package latency

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// HostClock reads a process-independent kernel clock. It intentionally rechecks
// boot identity on each call and fails closed if identity cannot be obtained.
func HostClock() (Stamp, error) {
	scale, err := hostScale()
	if err != nil {
		return Stamp{}, fmt.Errorf("latency: clock identity: %w", err)
	}
	var ts unix.Timespec
	if err = unix.ClockGettime(rawClock, &ts); err != nil {
		return Stamp{}, fmt.Errorf("latency: kernel clock: %w", err)
	}
	return Stamp{Scale: scale, Nanos: ts.Nano()}, nil
}
