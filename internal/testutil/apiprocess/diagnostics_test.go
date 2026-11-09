package apiprocess

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestEarlyExitDiagnostics(t *testing.T) {
	cfg := childConfig(t, "diagnostic-exit")
	cfg.DiagnosticLines = []string{"safe startup category"}
	err := With(context.Background(), cfg, func(*Process) error { t.Fatal("unexpected ready"); return nil })
	if !errors.Is(err, ErrEarlyExit) || !strings.Contains(err.Error(), "exit status 23") || strings.Count(err.Error(), "safe startup category") != 1 {
		t.Fatal("missing bounded diagnostic", err)
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatal("child output leaked")
	}
	cfg.DiagnosticLines = nil
	err = With(context.Background(), cfg, func(*Process) error { return nil })
	if strings.Contains(err.Error(), "safe startup category") || strings.Contains(err.Error(), "secret") {
		t.Fatal("default must discard output", err)
	}
}

func TestDiagnosticConfigurationBounds(t *testing.T) {
	for _, lines := range [][]string{{""}, {"unsafe\nline"}, {strings.Repeat("x", 257)}, make([]string, 33)} {
		cfg := childConfig(t, "exit")
		cfg.DiagnosticLines = lines
		if err := With(context.Background(), cfg, func(*Process) error { return nil }); !errors.Is(err, ErrConfig) {
			t.Fatal("invalid diagnostics accepted", err)
		}
	}
}
