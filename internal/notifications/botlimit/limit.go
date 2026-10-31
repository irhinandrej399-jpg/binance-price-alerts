// Package botlimit provides durable admission pacing shared by notifications replicas.
package botlimit

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultRate is below Telegram's approximate free broadcast ceiling of 30/s.
const DefaultRate = 25

var ErrConfiguration = errors.New("bot limiter configuration mismatch")

// Limiter is safe for concurrent use. All replicas of a bot must use the same
// notifications database, stable bot ID and rate. It does not send or sleep.
type Limiter struct {
	pool     *pgxpool.Pool
	botID    string
	interval time.Duration
	// Test clock; production always reads the database clock after obtaining the lock.
	now func(context.Context) (time.Time, error)
}

// New accepts the numeric ID of the bot (stable across token rotation), not the
// secret bot token. Rate is admissions per second; zero selects DefaultRate.
// Microsecond rounding is conservative. No paid broadcast is enabled here.
func New(pool *pgxpool.Pool, botID string, rate int) (*Limiter, error) {
	id, err := strconv.ParseInt(botID, 10, 64)
	if pool == nil || err != nil || id <= 0 || strconv.FormatInt(id, 10) != botID || rate < 0 || rate > 1000000 {
		return nil, errors.New("invalid bot limiter configuration")
	}
	if rate == 0 {
		rate = DefaultRate
	}
	us := (1000000 + int64(rate) - 1) / int64(rate)
	return &Limiter{pool: pool, botID: botID, interval: time.Duration(us) * time.Microsecond}, nil
}

// TryAcquire returns zero wait ONLY with nil error after a committed grant.
// Positive wait is advisory: wait with context, then compete again. No future
// grant is reserved and no unused quota accumulates. A grant is never refunded,
// including after cancellation, failed send or ambiguous database commit.
func (l *Limiter) TryAcquire(ctx context.Context) (time.Duration, error) {
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return 0, errors.New("begin bot limiter transaction")
	}
	defer tx.Rollback(context.Background())
	_, err = tx.Exec(ctx, `INSERT INTO app.bot_limits(bot_id,interval_us,next_allowed_at)
 VALUES($1,$2,'-infinity') ON CONFLICT(bot_id) DO NOTHING`, l.botID, l.interval.Microseconds())
	if err != nil {
		return 0, errors.New("initialize bot limiter")
	}
	var interval int64
	var next time.Time
	// Read finite initial value without decoding PostgreSQL infinity as time.Time.
	err = tx.QueryRow(ctx, `SELECT interval_us, GREATEST(next_allowed_at,'0001-01-01'::timestamptz)
 FROM app.bot_limits WHERE bot_id=$1 FOR UPDATE`, l.botID).Scan(&interval, &next)
	if err != nil {
		return 0, errors.New("lock bot limiter")
	}
	if interval != l.interval.Microseconds() {
		return 0, ErrConfiguration
	}
	var now time.Time
	if l.now != nil {
		now, err = l.now(ctx)
	} else {
		err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now)
	}
	if err != nil {
		return 0, errors.New("read bot limiter clock")
	}
	if now.Before(next) {
		return next.Sub(now), nil
	}
	_, err = tx.Exec(ctx, `UPDATE app.bot_limits SET next_allowed_at=$2 WHERE bot_id=$1`, l.botID, now.Add(l.interval))
	if err != nil {
		return 0, errors.New("persist bot limiter grant")
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, errors.New("commit bot limiter grant")
	}
	return 0, nil
}
