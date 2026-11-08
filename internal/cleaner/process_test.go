package cleaner

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestCrashAfterReservation(t *testing.T) {
	if os.Getenv("CLEANER_CRASH_CHILD") == "1" {
		s := NewStore(os.Getenv("CLEANER_STATE_DIR"))
		b := &fakeBroker{end: 20, onDelete: func(int64) { os.Exit(19) }}
		_, _ = New(s, b, proofs{"fast": 10, "slow": 5}).Clean(context.Background(), 10)
		os.Exit(20)
	}
	dir := t.TempDir()
	s := NewStore(dir)
	b := &fakeBroker{end: 20}
	p := proofs{"fast": 10, "slow": 5}
	if e := New(s, b, p).Init(context.Background(), plan()); e != nil {
		t.Fatal(e)
	}
	exe, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestCrashAfterReservation$")
	cmd.Env = []string{"CLEANER_CRASH_CHILD=1", "CLEANER_STATE_DIR=" + dir}
	e = cmd.Run()
	var exit *exec.ExitError
	if !errors.As(e, &exit) || exit.ExitCode() != 19 {
		t.Fatal("child did not crash at reserved delete", e)
	}
	st, e := s.Inspect(ctx)
	if e != nil || st.Floor != 5 {
		t.Fatal(st, e)
	}
	p["new"] = 4
	if e = New(s, b, p).Register(ctx, Consumer{Name: "new", ID: "new", Kind: "prices", Endpoint: "matcher"}); !errors.Is(e, ErrProof) {
		t.Fatal("crash forgot floor", e)
	}
	// The killed process released its flock, so retry can now recover normally.
	if _, e = New(s, b, p).Clean(ctx, 10); e != nil || b.start != 5 {
		t.Fatal(b.start, e)
	}
}
func TestInterprocessLock(t *testing.T) {
	if os.Getenv("CLEANER_LOCK_CHILD") == "1" {
		e := NewStore(os.Getenv("CLEANER_STATE_DIR")).locked(context.Background(), func(context.Context) error {
			if os.WriteFile(os.Getenv("CLEANER_ENTERED"), []byte("locked"), 0600) != nil {
				os.Exit(22)
			}
			select {}
		})
		if e != nil {
			os.Exit(21)
		}
		return
	}
	dir := t.TempDir()
	marker := filepath.Join(t.TempDir(), "entered")
	s := NewStore(dir)
	if e := New(s, &fakeBroker{end: 20}, proofs{}).Init(context.Background(), plan()); e != nil {
		t.Fatal(e)
	}
	exe, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestInterprocessLock$")
	cmd.Env = []string{"CLEANER_LOCK_CHILD=1", "CLEANER_STATE_DIR=" + dir, "CLEANER_ENTERED=" + marker}
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	for {
		if _, e = os.Stat(marker); e == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	short, stop := context.WithTimeout(ctx, 60*time.Millisecond)
	defer stop()
	if _, e = s.Inspect(short); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal("different process escaped flock", e)
	}
}
