package latency_test

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/latency"
)

func TestStampIntervalAndFailures(t *testing.T) {
	a := latency.Stamp{Scale: "boot-a/raw", Nanos: 100}
	b := latency.Stamp{Scale: a.Scale, Nanos: 100 + int64(200*time.Millisecond)}
	if d, e := latency.Between(a, b); e != nil || d != 200*time.Millisecond {
		t.Fatal(d, e)
	}
	for _, v := range []latency.Stamp{{}, {Scale: "boot-b/raw", Nanos: b.Nanos}, {Scale: a.Scale, Nanos: 99}} {
		if _, e := latency.Between(a, v); e == nil {
			t.Fatal("accepted missing/foreign/backward stamp", v)
		}
	}
	r, e := latency.New(func() (latency.Stamp, error) { return a, nil }, func(latency.Observation) error { return errors.New("sink lost") })
	if e != nil {
		t.Fatal(e)
	}
	r.Receive("BTCUSDT", "1", "event", 1)
	if n, e := r.Failures(); n != 1 || e == nil {
		t.Fatal(n, e)
	}
	r, e = latency.New(func() (latency.Stamp, error) { return latency.Stamp{}, errors.New("clock lost") }, func(latency.Observation) error { t.Fatal("invalid stamp sent"); return nil })
	if e != nil {
		t.Fatal(e)
	}
	r.Receive("BTCUSDT", "1", "event", 1)
	if n, e := r.Failures(); n != 1 || e == nil {
		t.Fatal(n, e)
	}
}

func TestHostClockAcrossProcesses(t *testing.T) {
	if os.Getenv("LATENCY_CLOCK_CHILD") == "1" {
		s, e := latency.HostClock()
		if e != nil {
			t.Fatal(e)
		}
		if e = json.NewEncoder(os.Stdout).Encode(s); e != nil {
			t.Fatal(e)
		}
		os.Exit(0)
	}
	a, e := latency.HostClock()
	if e != nil {
		t.Fatal(e)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestHostClockAcrossProcesses$")
	cmd.Env = append(os.Environ(), "LATENCY_CLOCK_CHILD=1")
	raw, e := cmd.Output()
	if e != nil {
		t.Fatal(e)
	}
	var b latency.Stamp
	if e = json.Unmarshal(raw, &b); e != nil {
		t.Fatal(e, string(raw))
	}
	c, e := latency.HostClock()
	if e != nil {
		t.Fatal(e)
	}
	if _, e = latency.Between(a, b); e != nil {
		t.Fatal(e)
	}
	if _, e = latency.Between(b, c); e != nil {
		t.Fatal(e)
	}
}
