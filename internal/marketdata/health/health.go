// Package health provides market-data's local-profile HTTP health checks.
package health

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"
)

// FeedState describes the current feed, independently of quote age and historical gaps.
type FeedState string

const (
	Starting     FeedState = "starting"
	Unconfirmed  FeedState = "unconfirmed"
	Disconnected FeedState = "disconnected"
	Live         FeedState = "live"
)

type Handler struct {
	initialized atomic.Bool
	storage     func(context.Context) error
	feed        func(context.Context) (FeedState, error)
	timeout     time.Duration
	mux         *http.ServeMux
}

// New starts uninitialized. storage probes only the service-owned database.
// feed returns a current aggregate over the runtime's desired stream plan:
// Live requires explicit confirmation of all required streams, never just a
// recent quote or subscription ACK. Both callbacks must honor cancellation and
// support concurrent calls. Nil callbacks fail closed. A nonpositive timeout
// defaults to one second; one budget covers both checks.
func New(storage func(context.Context) error, feed func(context.Context) (FeedState, error), timeout time.Duration) *Handler {
	if timeout <= 0 {
		timeout = time.Second
	}
	h := &Handler{storage: storage, feed: feed, timeout: timeout, mux: http.NewServeMux()}
	h.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		respond(w, http.StatusOK, "ok", "")
	})
	h.mux.HandleFunc("GET /readyz", h.ready)
	return h
}

func (h *Handler) SetInitialized(value bool) { h.initialized.Store(value) }

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.mux.ServeHTTP(w, r) }

func (h *Handler) ready(w http.ResponseWriter, r *http.Request) {
	state := Starting
	ready := false
	if h.initialized.Load() {
		state = Unconfirmed
		if h.storage != nil && h.feed != nil {
			ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
			defer cancel()
			if err := h.storage(ctx); err == nil && ctx.Err() == nil {
				observed, err := h.feed(ctx)
				if err == nil && ctx.Err() == nil {
					switch observed {
					case Starting, Unconfirmed, Disconnected, Live:
						state = observed
					}
					ready = state == Live
				}
			}
		}
	}
	if !h.initialized.Load() {
		state, ready = Starting, false
	}
	if ready {
		respond(w, http.StatusOK, "ready", state)
	} else {
		respond(w, http.StatusServiceUnavailable, "not_ready", state)
	}
}

// All strings supplied here are internal constants; callback errors and unknown
// state values are never serialized.
func respond(w http.ResponseWriter, code int, status string, feed FeedState) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	body := "{\"status\":\"" + status + "\""
	if feed != "" {
		body += ",\"feed\":\"" + string(feed) + "\""
	}
	_, _ = w.Write([]byte(body + "}\n"))
}
