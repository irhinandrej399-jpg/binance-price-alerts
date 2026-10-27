// Package login provides the public REST session issuance endpoint.
package login

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/password"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/session"
)

// New returns a handler to mount at /v1/sessions. Both repositories must commit
// their writes before returning success; normally they share an API pgx pool.
func New(repo *account.Repository, sessions *session.Store) http.Handler {
	// An unknown email still runs the same Argon2id profile as a known account.
	// This generated dummy credential is never usable as a login.
	dummyHash, err := password.Hash("dummy-login-password")
	if err != nil {
		panic("initialize login password verifier")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/sessions", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || media != "application/json" {
			writeError(w, 415, "unsupported_media_type")
			return
		}
		var input struct {
			Email    string `json:"email"`
			Password string `json:"password"`
		}
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			writeError(w, 400, "invalid_request")
			return
		}
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			writeError(w, 400, "invalid_request")
			return
		}
		email := account.NormalizeEmail(input.Email)
		if email == "" || input.Password == "" {
			writeError(w, 400, "invalid_request")
			return
		}
		a, lookupErr := repo.ByEmail(r.Context(), email)
		missing := errors.Is(lookupErr, account.ErrNotFound)
		if lookupErr != nil && !missing {
			writeError(w, 500, "internal_error")
			return
		}
		hash := a.PasswordHash
		if missing {
			hash = dummyHash
		}
		verifyErr := password.Verify(hash, input.Password)
		if missing || verifyErr != nil {
			writeError(w, 401, "invalid_credentials")
			return
		}
		credentials, err := sessions.Issue(r.Context(), a.ID)
		if err != nil {
			writeError(w, 500, "internal_error")
			return
		}
		writeJSON(w, 200, credentials)
	})
	return mux
}

type errorCode struct {
	Code string `json:"code"`
}
type errorResponse struct {
	Error errorCode `json:"error"`
}

func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, errorResponse{Error: errorCode{Code: code}})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
