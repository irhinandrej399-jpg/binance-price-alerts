package apiprocess

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/postgres"
)

// This is a test executable, never cmd/api or an application endpoint.
func TestChild(t *testing.T) {
	mode := os.Getenv("API_PROCESS_CHILD")
	if mode == "" {
		return
	}
	if file := os.Getenv("CHILD_PID_FILE"); file != "" {
		_ = os.WriteFile(file, []byte(fmt.Sprint(os.Getpid())), 0600)
	}
	if mode == "exit" {
		os.Exit(23)
	}
	if mode == "hang" {
		signal.Ignore(syscall.SIGTERM)
		for {
			time.Sleep(time.Hour)
		}
	}
	if os.Getenv("MARKET_DATA_DATABASE_URL") != "" || os.Getenv("PARENT_SECRET") != "" {
		os.Exit(24)
	}
	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		pool, err := postgres.Open(ctx, dsn)
		cancel()
		if err != nil {
			os.Exit(25)
		}
		defer pool.Close()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	if http.ListenAndServe(os.Getenv("HTTP_ADDR"), mux) != nil {
		os.Exit(26)
	}
	os.Exit(0)
}

func childConfig(t *testing.T, mode string) Config {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return Config{Executable: exe, Args: []string{"-test.run=^TestChild$"}, Env: map[string]string{"API_PROCESS_CHILD": mode}, StartupTimeout: 2 * time.Second, StopTimeout: 100 * time.Millisecond}
}

func assertDead(t *testing.T, pid int) {
	t.Helper()
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("child %d still exists: %v", pid, err)
	}
}

func TestLifecycle(t *testing.T) {
	registry := t.TempDir()
	t.Setenv("API_PROCESS_PID_DIR", registry)
	t.Setenv("PARENT_SECRET", "must-not-leak")
	t.Setenv("MARKET_DATA_DATABASE_URL", "must-not-leak")
	sentinel := errors.New("verification failed")
	for _, fail := range []bool{false, true} {
		pid := 0
		err := With(context.Background(), childConfig(t, "serve"), func(p *Process) error {
			pid = p.PID()
			if _, err := os.Stat(filepath.Join(registry, strconv.Itoa(pid))); err != nil {
				t.Fatal("missing child registry entry")
			}
			resp, err := (&http.Client{Timeout: time.Second}).Get(p.URL + "/readyz")
			if err != nil {
				return err
			}
			resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Fatal(resp.StatusCode)
			}
			if fail {
				return sentinel
			}
			return nil
		})
		if fail && !errors.Is(err, sentinel) || !fail && err != nil {
			t.Fatal(err)
		}
		assertDead(t, pid)
		entries, e := os.ReadDir(registry)
		if e != nil || len(entries) != 0 {
			t.Fatal("stale child registry entry")
		}
	}
}
func TestStartupFailures(t *testing.T) {
	for _, mode := range []string{"exit", "hang"} {
		cfg := childConfig(t, mode)
		cfg.StartupTimeout = 250 * time.Millisecond
		pidFile := filepath.Join(t.TempDir(), "pid")
		cfg.Env["CHILD_PID_FILE"] = pidFile
		start := time.Now()
		called := false
		err := With(context.Background(), cfg, func(*Process) error { called = true; return nil })
		if err == nil || called {
			t.Fatalf("mode=%s err=%v called=%v", mode, err, called)
		}
		if mode == "exit" && !errors.Is(err, ErrEarlyExit) {
			t.Fatal(err)
		}
		if mode == "hang" && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		raw, e := os.ReadFile(pidFile)
		if e != nil {
			t.Fatal(e)
		}
		pid, e := strconv.Atoi(string(raw))
		if e != nil {
			t.Fatal(e)
		}
		assertDead(t, pid)
		if time.Since(start) > 3*time.Second {
			t.Fatal("unbounded startup/cleanup")
		}
	}
	cfg := childConfig(t, "serve")
	cfg.Executable = "/no/such/executable"
	if err := With(context.Background(), cfg, func(*Process) error { return nil }); !errors.Is(err, ErrStart) {
		t.Fatal(err)
	}
}
func TestCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pid := 0
	err := With(ctx, childConfig(t, "serve"), func(p *Process) error {
		pid = p.PID()
		cancel()
		select {
		case <-p.Done():
		case <-time.After(2 * time.Second):
			t.Fatal("child not stopped on cancellation")
		}
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	assertDead(t, pid)
}
