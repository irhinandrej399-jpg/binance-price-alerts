package main

import (
	"context"
	"errors"
	"net/http"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/fencing"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/live"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/priceconnection"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/projection"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/snapshotload"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/sourcebinding"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var diagnosticKinds = []error{
	priceconnection.ErrUnavailable, priceconnection.ErrQuote,
	snapshotload.ErrInvalidSnapshot, snapshotload.ErrIncomplete,
	live.ErrBootstrap, live.ErrPin, live.ErrProtocol,
	projection.ErrSequence, fencing.ErrAuthority,
}

const (
	diagnosticDeadline       = "matcher: component deadline exceeded"
	diagnosticRPC            = "matcher: component RPC failure"
	diagnosticRPCUnavailable = "matcher: component RPC unavailable"
	diagnosticSQL            = "matcher: component SQL failure"
	diagnosticSQLPermission  = "matcher: component SQL permission denied"
	diagnosticUnknown        = "matcher: component failure"
)

// Never return dependency error text: RPC descriptions and SQL messages may
// contain credentials. Recognize joined business errors before cancellation.
func componentDiagnostic(err error) string {
	if err == nil {
		return ""
	}
	for _, kind := range diagnosticKinds {
		if errors.Is(err, kind) {
			return kind.Error()
		}
	}
	var sql *pgconn.PgError
	if errors.As(err, &sql) {
		if sql.Code == "42501" {
			return diagnosticSQLPermission
		}
		return diagnosticSQL
	}
	if errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded {
		return diagnosticDeadline
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, http.ErrServerClosed) || errors.Is(err, grpc.ErrServerStopped) || status.Code(err) == codes.Canceled {
		return ""
	}
	if status.Code(err) == codes.Unavailable {
		return diagnosticRPCUnavailable
	}
	if _, ok := status.FromError(err); ok {
		return diagnosticRPC
	}
	return diagnosticUnknown
}

// The process fixture opts into only these exact lines. Raw stderr remains
// discarded, including dependency descriptions, panic text and partial lines.
func startupDiagnosticLines() []string {
	lines := []string{
		"matcher: invalid configuration", "matcher: credentials unavailable",
		"matcher: invalid service credentials", "matcher: database startup failed",
		"matcher: API client startup failed", "matcher: Latest client startup failed",
		"matcher: HTTP bind failed", "matcher: RPC bind failed",
		"matcher: invalid exporter credentials", "matcher: replica startup failed",
		"matcher: required component stopped",
		sourcebinding.ErrUnbound.Error(),
		kafkasource.ErrMismatch.Error(), kafkasource.ErrIdentity.Error(), kafkasource.ErrMetadata.Error(),
		diagnosticDeadline, diagnosticRPC, diagnosticRPCUnavailable,
		diagnosticSQL, diagnosticSQLPermission, diagnosticUnknown,
	}
	for _, kind := range diagnosticKinds {
		lines = append(lines, kind.Error())
	}
	return lines
}
