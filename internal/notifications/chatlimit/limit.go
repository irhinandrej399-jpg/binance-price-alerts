// Package chatlimit schedules admissions independently for each Telegram chat.
package chatlimit

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const DefaultInterval = time.Second

// A busy transaction has no known completion time. Retry without holding a job.
const contentionWait = time.Millisecond

var ErrConfiguration = errors.New("chat limiter configuration mismatch")

// Limiter shares durable state across replicas in the notifications database.
// It neither sleeps nor owns jobs. Its methods are safe for concurrent use.
type Limiter struct {
	pool     *pgxpool.Pool
	botID    string
	interval time.Duration
	now      func(context.Context) (time.Time, error) // test-only; production uses SQL clock
}

// New uses the stable numeric bot ID, never its secret token. Zero interval
// selects one second; positive intervals must be whole microseconds.
func New(pool *pgxpool.Pool, botID string, interval time.Duration) (*Limiter, error) {
	id, err := strconv.ParseInt(botID, 10, 64)
	if pool == nil || err != nil || id <= 0 || strconv.FormatInt(id, 10) != botID || interval < 0 || interval%time.Microsecond != 0 {
		return nil, errors.New("invalid chat limiter configuration")
	}
	if interval == 0 {
		interval = DefaultInterval
	}
	return &Limiter{pool: pool, botID: botID, interval: interval}, nil
}

// Next grants the first available chat in caller order, skipping delayed/busy
// chats and duplicate chat IDs. It returns its original slice index and zero
// wait only after commit. Otherwise index is -1 and wait is the minimum retry
// delay (zero for an empty slice). Errors always prohibit sending.
// Caller retains all jobs, removes only a selected job, and competes again
// after waiting; this method reserves no future grants.
func (l *Limiter) Next(ctx context.Context, chats []int64) (index int, wait time.Duration, err error) {
	// Validate the whole batch before consuming any admission.
	for _, chat := range chats {
		if chat == 0 {
			return -1, 0, errors.New("invalid chat ID")
		}
	}
	seen := make(map[int64]bool, len(chats))
	for i, chat := range chats {
		if seen[chat] {
			continue
		}
		seen[chat] = true
		delay, err := l.TryAcquire(ctx, chat)
		if err != nil {
			return -1, 0, err
		}
		if delay == 0 {
			return i, 0, nil
		}
		if wait == 0 || delay < wait {
			wait = delay
		}
	}
	return -1, wait, nil
}

// TryAcquire grants only when err==nil and wait==0. A positive delay does not
// reserve capacity. Grants are consumed even if the caller later cannot send.
func (l *Limiter) TryAcquire(ctx context.Context, chatID int64) (time.Duration, error) {
	if chatID == 0 {
		return 0, errors.New("invalid chat ID")
	}
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return 0, errors.New("begin chat limiter transaction")
	}
	defer tx.Rollback(context.Background())
	// Try-lock before INSERT also avoids waiting on an uncommitted first row.
	// Hash collisions cause a conservative retry, never an extra admission.
	var locked bool
	err = tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, lockKey(l.botID, chatID)).Scan(&locked)
	if err != nil {
		return 0, errors.New("lock chat limiter")
	}
	if !locked {
		return contentionWait, nil
	}
	_, err = tx.Exec(ctx, `INSERT INTO app.chat_limits(bot_id,chat_id,interval_us,next_allowed_at)
 VALUES($1,$2,$3,'-infinity') ON CONFLICT(bot_id,chat_id) DO NOTHING`, l.botID, chatID, l.interval.Microseconds())
	if err != nil {
		return 0, errors.New("initialize chat limiter")
	}
	var interval int64
	var next time.Time
	err = tx.QueryRow(ctx, `SELECT interval_us,GREATEST(next_allowed_at,'0001-01-01'::timestamptz)
 FROM app.chat_limits WHERE bot_id=$1 AND chat_id=$2`, l.botID, chatID).Scan(&interval, &next)
	if err != nil {
		return 0, errors.New("read chat limiter")
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
		return 0, errors.New("read chat limiter clock")
	}
	if now.Before(next) {
		return next.Sub(now), nil
	}
	_, err = tx.Exec(ctx, `UPDATE app.chat_limits SET next_allowed_at=$3 WHERE bot_id=$1 AND chat_id=$2`, l.botID, chatID, now.Add(l.interval))
	if err != nil {
		return 0, errors.New("persist chat limiter grant")
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, errors.New("commit chat limiter grant")
	}
	return 0, nil
}

func lockKey(botID string, chatID int64) string {
	return "notifications/chatlimit/" + botID + "/" + strconv.FormatInt(chatID, 10)
}
