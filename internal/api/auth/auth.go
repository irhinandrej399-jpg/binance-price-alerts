// Package auth authenticates protected API handlers with opaque sessions.
package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/session"
)

type ownerKey struct{}

// UserID returns the session owner set by this middleware, never client input.
func UserID(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(ownerKey{}).(string)
	return id, ok && id != ""
}

// New wraps a protected handler. Construct it once with the API session store.
func New(store *session.Store, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers := r.Header.Values("Authorization")
		if len(headers) != 1 {
			reject(w, 401, "invalid_session")
			return
		}
		scheme, token, ok := strings.Cut(headers[0], " ")
		if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" || strings.ContainsAny(token, " \t\r\n,") {
			reject(w, 401, "invalid_session")
			return
		}
		id, err := store.Verify(r.Context(), token)
		if errors.Is(err, session.ErrInvalid) {
			reject(w, 401, "invalid_session")
			return
		}
		if err != nil || id == "" || r.Context().Err() != nil {
			reject(w, 500, "internal_error")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ownerKey{}, id)))
	})
}

func reject(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if status == 401 {
		w.Header().Set("WWW-Authenticate", "Bearer")
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]map[string]string{"error": {"code": code}})
}
