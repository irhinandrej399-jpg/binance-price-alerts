package main

import (
	"context"
	"errors"
	"testing"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/priceconnection"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/snapshotload"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestComponentDiagnostic(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{nil, ""}, {context.Canceled, ""},
		{errors.Join(context.Canceled, priceconnection.ErrUnavailable), priceconnection.ErrUnavailable.Error()},
		{snapshotload.ErrInvalidSnapshot, snapshotload.ErrInvalidSnapshot.Error()},
		{status.Error(codes.Unavailable, "secret"), "matcher: component RPC unavailable"},
		{status.Error(codes.DeadlineExceeded, "secret"), "matcher: component deadline exceeded"},
		{&pgconn.PgError{Code: "42501", Message: "secret"}, "matcher: component SQL permission denied"},
		{&pgconn.PgError{Code: "23505", Message: "secret"}, "matcher: component SQL failure"},
		{errors.New("postgres://user:secret@localhost/db"), "matcher: component failure"},
	} {
		got := componentDiagnostic(tc.err)
		if got != tc.want {
			t.Fatalf("got %q want %q", got, tc.want)
		}
	}
}

func TestStartupDiagnosticAllowlist(t *testing.T) {
	lines := startupDiagnosticLines()
	if len(lines) > 32 {
		t.Fatal("process harness allowlist limit exceeded")
	}
	for _, line := range lines {
		if len(line) > 256 {
			t.Fatal("diagnostic line exceeds harness bound")
		}
	}
}
