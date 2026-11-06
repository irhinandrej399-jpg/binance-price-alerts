// Package health provides matcher HTTP liveness and readiness observations.
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/live"
)

type Handler struct {
	initialized atomic.Bool
	storage     func(context.Context) error
	snapshot    func() live.Status
	timeout     time.Duration
	mux         *http.ServeMux
}

// New starts uninitialized. storage probes only matcher-owned storage and must
// honor context; snapshot is a fast, concurrent-safe local observation, e.g.
// replica.Status. Nil callbacks fail closed. Default timeout is one second.
func New(storage func(context.Context) error, snapshot func() live.Status, timeout time.Duration) *Handler {
	if timeout <= 0 {
		timeout = time.Second
	}
	h := &Handler{storage: storage, snapshot: snapshot, timeout: timeout, mux: http.NewServeMux()}
	h.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { respond(w, 200, "ok", nil) })
	h.mux.HandleFunc("GET /readyz", h.ready)
	return h
}
func (h *Handler) SetInitialized(v bool)                            { h.initialized.Store(v) }
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.mux.ServeHTTP(w, r) }
func (h *Handler) ready(w http.ResponseWriter, r *http.Request) {
	s := live.Status{}
	if h.initialized.Load() && h.storage != nil && h.snapshot != nil {
		ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
		defer cancel()
		if err := h.storage(ctx); err == nil && ctx.Err() == nil {
			s = h.snapshot()
			if ctx.Err() != nil {
				s = live.Status{}
			}
		}
	}
	if !h.initialized.Load() {
		s = live.Status{}
	}
	if s.Ready {
		respond(w, 200, "ready", &s)
	} else {
		respond(w, 503, "not_ready", &s)
	}
}
func respond(w http.ResponseWriter, code int, status string, s *live.Status) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(struct {
		Status     string       `json:"status"`
		Projection *live.Status `json:"projection,omitempty"`
	}{status, s})
}
