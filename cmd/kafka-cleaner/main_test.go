package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRejectUnsafeCommandConfiguration(t *testing.T) {
	for _, args := range [][]string{{}, {"-operation", "unknown", "-state-dir", t.TempDir()}, {"-operation", "clean", "-state-dir", t.TempDir(), "-timeout", "1h"}, {"-operation", "clean", "-state-dir", t.TempDir(), "-max-offsets", "-1"}, {"-operation", "clean", "-state-dir", t.TempDir(), "-config", "secret-dsn-file"}} {
		var out, err bytes.Buffer
		if run(context.Background(), args, &out, &err) != 2 || strings.Contains(err.String(), "secret-dsn-file") || out.Len() != 0 {
			t.Fatal(out.String(), err.String())
		}
	}
}
