//go:build !darwin && !linux

package latency

import "errors"

func HostClock() (Stamp, error) {
	return Stamp{}, errors.New("latency: host clock supported only on Darwin and Linux")
}
