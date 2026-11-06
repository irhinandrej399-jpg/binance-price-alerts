// Package bodylimit bounds public HTTP bodies before invoking application code.
package bodylimit

import (
	"bytes"
	"errors"
	"io"
	"net/http"
)

// PublicLimit is the current public API body budget in bytes (64 KiB).
const PublicLimit int64 = 64 << 10

// New buffers at most maxBytes plus one probe byte before calling next. Mount
// outside authentication and JSON handlers so no SQL/RPC/password work starts
// on an oversized or incomplete body. The HTTP server must set a read timeout.
// Invalid construction is a programmer error.
func New(maxBytes int64, next http.Handler) http.Handler {
	if maxBytes <= 0 || next == nil {
		panic("bodylimit: invalid configuration")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > maxBytes {
			reject(w, http.StatusRequestEntityTooLarge, "request_body_too_large")
			return
		}
		if r.Body != nil {
			limited := http.MaxBytesReader(w, r.Body, maxBytes)
			defer limited.Close()
			body, err := io.ReadAll(limited)
			if err != nil {
				var tooLarge *http.MaxBytesError
				if errors.As(err, &tooLarge) {
					reject(w, http.StatusRequestEntityTooLarge, "request_body_too_large")
				} else {
					reject(w, http.StatusBadRequest, "invalid_request")
				}
				return
			}
			// Preserve context, headers and framing metadata on the original request.
			r = r.Clone(r.Context())
			r.Body = io.NopCloser(bytes.NewReader(body))
		}
		next.ServeHTTP(w, r)
	})
}

func reject(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, "{\"error\":{\"code\":\""+code+"\"}}\n")
}
