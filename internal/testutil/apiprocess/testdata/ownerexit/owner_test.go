// Intentionally failing fixture for the disposable runner's cleanup proof.
// testdata is excluded from go test ./...; run only using check_api_process.py.
package ownerexit

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/apiprocess"
)

func TestAbruptOwner(t *testing.T) {
	if os.Getenv("API_PROCESS_OWNEREXIT_CHILD") == "1" {
		mux := http.NewServeMux()
		mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
		_ = http.ListenAndServe(os.Getenv("HTTP_ADDR"), mux)
		os.Exit(22)
	}
	if os.Getenv("API_PROCESS_INTEGRATION") != "1" {
		t.Skip("disposable runner only")
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
