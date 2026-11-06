// Package authlimit implements the API-owned shared authentication quota.
package authlimit

import (
	"context"
	"errors"
	"math"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrStorage = errors.New("authentication quota unavailable")
var ErrInvalid = errors.New("invalid authentication quota key")

const Limit = 20

// Store uses a committing API pool. No transaction spans credential processing.
// now must be nil in production: PostgreSQL supplies the common clock. An
// injected clock is exclusively for deterministic integration tests.
type Store struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

func New(pool *pgxpool.Pool, now func() time.Time) *Store {
	return &Store{pool: pool, now: now}
}

// Allow commits one attempt, or returns the seconds until the current window
// ends. Errors never authorize work, including an ambiguous commit result.
func (s *Store) Allow(ctx context.Context, ip string) (bool, int, error) {
	address, err := netip.ParseAddr(ip)
	if err != nil || address.Zone() != "" {
		return false, 0, ErrInvalid
	}
	ip = address.Unmap().String()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, 0, ErrStorage
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	// The unique IP row serializes both first insertion and later windows. Read
	// the shared clock AFTER acquiring its lock, so a waiter crossing a minute
	// cannot reset the new window to an old timestamp captured before waiting.
	_, err = tx.Exec(ctx, `INSERT INTO app.auth_limits(ip,window_start,used) VALUES ($1,'epoch',0) ON CONFLICT DO NOTHING`, ip)
	if err != nil {
		return false, 0, ErrStorage
	}
	var start time.Time
	var used int
	if err = tx.QueryRow(ctx, `SELECT window_start,used FROM app.auth_limits WHERE ip=$1 FOR UPDATE`, ip).Scan(&start, &used); err != nil {
		return false, 0, ErrStorage
	}
	var now time.Time
	if s.now != nil {
		now = s.now().UTC()
	} else if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return false, 0, ErrStorage
	}
	window := now.Truncate(time.Minute)
	if window.After(start) {
		start = window
		used = 0
	}
	retry := int(math.Ceil(start.Add(time.Minute).Sub(now).Seconds()))
	if used >= Limit {
		return false, retry, nil
	}
	_, err = tx.Exec(ctx, `UPDATE app.auth_limits SET window_start=$2,used=$3 WHERE ip=$1`, ip, start, used+1)
	if err != nil {
		return false, 0, ErrStorage
	}
	if err = tx.Commit(ctx); err != nil {
		return false, 0, ErrStorage
	}
	return true, 0, nil
}

// Handler wraps a POST route; method/path dispatch belongs to the outer mux.
// Mount after bodylimit and before JSON/password processing. Forwarded headers
// are intentionally ignored: the direct TCP peer defines the quota key.
func (s *Store) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			reject(w, 400, "invalid_request")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		ok, retry, err := s.Allow(ctx, host)
		cancel()
		if errors.Is(err, ErrInvalid) {
			reject(w, 400, "invalid_request")
			return
		}
		if err != nil {
			reject(w, 503, "auth_limit_unavailable")
			return
		}
		if !ok {
			w.Header().Set("Retry-After", strconv.Itoa(retry))
			reject(w, 429, "rate_limited")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func reject(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write([]byte("{\"error\":{\"code\":\"" + code + "\"}}\n"))
}
