// Package health provides the API's local-profile HTTP health checks.
package health

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"
)

// Handler starts uninitialized. Construct once and share across requests.
type Handler struct {
	initialized atomic.Bool
	storage     func(context.Context) error
	timeout     time.Duration
	mux         *http.ServeMux
}

// New checks only API-owned storage (for example, its pgxpool.Pool.Ping).
// The probe must honor context cancellation and be safe for concurrent calls.
// A nil probe fails readiness; a nonpositive timeout defaults to one second.
func New(storage func(context.Context) error, timeout time.Duration) *Handler {
	if timeout <= 0 {
		timeout = time.Second
	}
	h := &Handler{storage: storage, timeout: timeout, mux: http.NewServeMux()}
	h.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		respond(w, http.StatusOK, "ok")
	})
	h.mux.HandleFunc("GET /readyz", h.ready)
	return h
}

// SetInitialized publishes completion of startup, or withdraws readiness when
// initialization is no longer valid. Storage recovery is checked per request.
func (h *Handler) SetInitialized(value bool) { h.initialized.Store(value) }

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) ready(w http.ResponseWriter, r *http.Request) {
	if !h.initialized.Load() || h.storage == nil {
		respond(w, http.StatusServiceUnavailable, "not_ready")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	err := h.storage(ctx)
	if err != nil || ctx.Err() != nil || !h.initialized.Load() {
		respond(w, http.StatusServiceUnavailable, "not_ready")
		return
	}
	respond(w, http.StatusOK, "ready")
}

func respond(w http.ResponseWriter, code int, status string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_, _ = w.Write([]byte("{\"status\":\"" + status + "\"}\n"))
}
