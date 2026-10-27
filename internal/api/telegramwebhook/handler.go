// Package telegramwebhook protects incoming Telegram updates.
package telegramwebhook

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"
)

// Handler serves POST /v1/telegram/webhook, authenticating before next reads an
// update. Construct once with the same secret_token used to register the webhook.
// The caller owns update processing and its response, including retry semantics.
func Handler(secret string, next http.Handler) (http.Handler, error) {
	if !validSecret(secret) {
		return nil, errors.New("telegramwebhook: invalid secret configuration")
	}
	if next == nil {
		return nil, errors.New("telegramwebhook: update handler required")
	}
	expected := sha256.Sum256([]byte(secret))
	mux := http.NewServeMux()
	mux.Handle("POST /v1/telegram/webhook", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		values := r.Header.Values("X-Telegram-Bot-Api-Secret-Token")
		if len(values) != 1 || !validSecret(values[0]) {
			reject(w)
			return
		}
		actual := sha256.Sum256([]byte(values[0]))
		if subtle.ConstantTimeCompare(actual[:], expected[:]) != 1 {
			reject(w)
			return
		}
		next.ServeHTTP(w, r)
	}))
	return mux, nil
}

func validSecret(secret string) bool {
	if len(secret) < 1 || len(secret) > 256 {
		return false
	}
	for i := 0; i < len(secret); i++ {
		c := secret[i]
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

func reject(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusForbidden)
	_, _ = w.Write([]byte("{\"error\":{\"code\":\"invalid_webhook_secret\"}}\n"))
}
