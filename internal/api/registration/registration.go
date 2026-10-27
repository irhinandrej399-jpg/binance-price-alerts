// Package registration provides the public REST account registration endpoint.
package registration

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/mail"
	"strings"
	"unicode"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/password"
)

// New returns a handler ready to mount at /v1/accounts. The repository must
// commit its own inserts (normally account.New(pool)), not use an outer Tx.
func New(repo *account.Repository) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/accounts", func(w http.ResponseWriter, r *http.Request) {
		mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || mediaType != "application/json" {
			writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type")
			return
		}
		var input struct {
			Email    string `json:"email"`
			Password string `json:"password"`
		}
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request")
			return
		}
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			writeError(w, http.StatusBadRequest, "invalid_request")
			return
		}
		email := account.NormalizeEmail(input.Email)
		if !validEmail(email) || input.Password == "" {
			writeError(w, http.StatusBadRequest, "invalid_request")
			return
		}
		hash, err := password.Hash(input.Password)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "internal_error")
			return
		}
		saved, err := repo.Create(r.Context(), email, hash)
		if errors.Is(err, account.ErrEmailExists) {
			writeError(w, http.StatusConflict, "email_exists")
			return
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, "internal_error")
			return
		}
		// Explicit public DTO: never encode the internal credential record.
		writeJSON(w, http.StatusCreated, struct {
			ID    string `json:"id"`
			Email string `json:"email"`
		}{saved.ID, saved.Email})
	})
	return mux
}

func validEmail(email string) bool {
	if strings.ContainsAny(email, "()<>") || strings.ContainsFunc(email, unicode.IsSpace) {
		return false
	}
	address, err := mail.ParseAddress(email)
	return err == nil && address.Name == "" && address.Address == email
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
