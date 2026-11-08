// Intentionally failing fixture for the disposable runner's cleanup proof.
// testdata is excluded from go test ./...; run only using check_api_process.py.
package ownerexit

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/apiprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/cleanerprocess"
)

func TestAbruptOwner(t *testing.T) {
	if os.Getenv("CLEANER_FIXTURE_ROLE") != "" {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
		defer stop()
		if cleanerprocess.ServeOwner(ctx) != nil {
			os.Exit(23)
		}
		return
	}
	if os.Getenv("CLEANER_PROCESS_INTEGRATION") == "1" {
		f := cleanerprocess.New(t, os.Getenv("KAFKA_TEST_BROKER"))
		exe, e := os.Executable()
		if e != nil {
			t.Fatal(e)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		m := f.OwnerConfig("matcher", os.Getenv("MATCHER_DATABASE_URL"), exe, "-test.run=^TestAbruptOwner$")
		n := f.OwnerConfig("notifications", os.Getenv("NOTIFICATIONS_DATABASE_URL"), exe, "-test.run=^TestAbruptOwner$")
		e = apiprocess.With(ctx, m, func(*apiprocess.Process) error {
			return apiprocess.With(ctx, n, func(*apiprocess.Process) error { os.Exit(19); return nil })
		})
		t.Fatal("expected abrupt owner exit with two exporters", e)
	}

	if os.Getenv("API_PROCESS_OWNEREXIT_CHILD") == "1" {
		mux := http.NewServeMux()
		mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
		_ = http.ListenAndServe(os.Getenv("HTTP_ADDR"), mux)
		os.Exit(22)
	}
	if os.Getenv("E2E_PROCESS_INTEGRATION") != "1" && os.Getenv("API_PROCESS_INTEGRATION") != "1" && os.Getenv("MARKET_DATA_PROCESS_INTEGRATION") != "1" && os.Getenv("MATCHER_PROCESS_INTEGRATION") != "1" && os.Getenv("NOTIFICATIONS_PROCESS_INTEGRATION") != "1" && os.Getenv("CLEANER_PROCESS_INTEGRATION") != "1" {
		t.Skip("disposable runner only")
	}
	if os.Getenv("E2E_PROCESS_INTEGRATION") == "1" {
		// A credential-like temporary artifact must disappear even without defers.
		if err := os.WriteFile(t.TempDir()+"/test-key", []byte("test-only private material"), 0600); err != nil {
			t.Fatal("cannot create temporary cleanup probe")
		}
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	err = apiprocess.With(context.Background(), apiprocess.Config{
		Executable: executable, Args: []string{"-test.run=^TestAbruptOwner$"},
		Env:            map[string]string{"API_PROCESS_OWNEREXIT_CHILD": "1"},
		StartupTimeout: 3 * time.Second, StopTimeout: time.Second,
	}, func(*apiprocess.Process) error { os.Exit(19); return nil })
	t.Fatal("expected abrupt exit after child readiness", err)
}
