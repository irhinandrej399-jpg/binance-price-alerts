package latency

import (
	"errors"

	"golang.org/x/sys/unix"
)

const rawClock = unix.CLOCK_MONOTONIC_RAW

func hostScale() (string, error) {
	boot, e := unix.Sysctl("kern.bootsessionuuid")
	if e != nil {
		return "", e
	}
	if boot == "" {
		return "", errors.New("empty boot UUID")
	}
	return "darwin/monotonic_raw/" + boot, nil
}
