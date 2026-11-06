package latency

import (
	"errors"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

// RAW is not offset by Linux time namespaces (unlike MONOTONIC/BOOTTIME).
const rawClock = unix.CLOCK_MONOTONIC_RAW

func hostScale() (string, error) {
	b, e := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if e != nil {
		return "", e
	}
	boot := strings.TrimSpace(string(b))
	if boot == "" {
		return "", errors.New("empty boot ID")
	}
	return "linux/monotonic_raw/" + boot, nil
}
