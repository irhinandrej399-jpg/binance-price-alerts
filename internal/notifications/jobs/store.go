// Package jobs owns bounded local claims and durable delivery evidence in the notifications database.
package jobs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrUnavailable = errors.New("job not ready or already claimed")
	ErrStale       = errors.New("expired or superseded job lease")
)

type Store struct {
	pool *pgxpool.Pool
	ttl  time.Duration
	now  func(context.Context) (time.Time, error) // test-only; production uses SQL clock
}

// New accepts a positive lease TTL up to one minute, in whole microseconds.
// No renewal: work exceeding the bound must stop and obtain a fresh claim and
// API admission. Different replicas may use different TTLs without overlap.
func New(pool *pgxpool.Pool, ttl time.Duration) (*Store, error) {
	if pool == nil || ttl <= 0 || ttl > time.Minute || ttl%time.Microsecond != 0 {
		return nil, errors.New("invalid job lease configuration")
	}
	return &Store{pool: pool, ttl: ttl}, nil
}

// Lease is local ownership, never permission to call Telegram. ID is a durable
// random claim identity; Fence increases per workflow and is never reset.
type Lease struct {
	TriggerID, ID     string
	Fence             int64
	ExpiresAt         time.Time
	Body              []byte
	PossibleDuplicate bool
}

func (s *Store) clock(ctx context.Context, tx pgx.Tx) (time.Time, error) {
	if s.now != nil {
		return s.now(ctx)
	}
	var now time.Time
	err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now)
	return now, err
}

// Claim atomically claims a scheduler-selected workflow if due and unowned or
// expired. Row contention returns ErrUnavailable without waiting on another
// worker. Only a successful commit returns a lease. On an ambiguous commit,
// never send: recover by claiming again after expiry, with new API admission.
func (s *Store) Claim(ctx context.Context, triggerID string) (Lease, error) {
	if strings.TrimSpace(triggerID) == "" {
		return Lease{}, errors.New("empty trigger ID")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Lease{}, err
	}
	defer tx.Rollback(context.Background())
	var l Lease
	var state string
	var oldID *string
	var expires *time.Time
	var next pgtype.Timestamptz
	// Read time after taking the row lock; a blocked/stale clock cannot extend
	// old ownership. next_attempt_at may be PostgreSQL -infinity.
	err = tx.QueryRow(ctx, `SELECT state,lease_fence,lease_id,lease_expires_at,next_attempt_at,body
 FROM app.delivery_jobs WHERE trigger_id=$1 FOR UPDATE SKIP LOCKED`, triggerID).Scan(&state, &l.Fence, &oldID, &expires, &next, &l.Body)
	if errors.Is(err, pgx.ErrNoRows) {
		return Lease{}, ErrUnavailable
	}
	if err != nil {
		return Lease{}, err
	}
	succeeded, err := hasSuccess(ctx, tx, triggerID)
	if err != nil {
		return Lease{}, err
	}
	if succeeded {
		return Lease{}, ErrUnavailable
	}
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.delivery_uncertainties u JOIN app.delivery_attempts a USING(attempt_id) WHERE a.trigger_id=$1)`, triggerID).Scan(&l.PossibleDuplicate)
	if err != nil {
		return Lease{}, err
	}
	now, err := s.clock(ctx, tx)
	if err != nil {
		return Lease{}, err
	}
	if (state != "pending" && state != "leased" && state != string(WaitCondition) && state != string(WaitLimiter) && !(state == "uncertain" && l.PossibleDuplicate)) ||
		(next.InfinityModifier == 0 && now.Before(next.Time)) || next.InfinityModifier > 0 ||
		(expires != nil && now.Before(*expires)) {
		return Lease{}, ErrUnavailable
	}
	l.TriggerID = triggerID
	if expires != nil {
		l.ExpiresAt = *expires
	}
	if err = checkPreviousAttempt(ctx, tx, l, oldID, now); err != nil {
		return Lease{}, err
	}
	if oldID != nil {
		if _, err = tx.Exec(ctx, `UPDATE app.delivery_claims SET status='expired' WHERE claim_id=$1 AND status='claimed'`, *oldID); err != nil {
			return Lease{}, err
		}
	}
	var nonce [32]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return Lease{}, err
	}
	l.TriggerID = triggerID
	l.ID = hex.EncodeToString(nonce[:])
	l.ExpiresAt = now.Add(s.ttl)
	// PostgreSQL rejects bigint overflow and rolls back both updates.
	err = tx.QueryRow(ctx, `UPDATE app.delivery_jobs SET state='leased',lease_fence=lease_fence+1,
 lease_id=$2,lease_expires_at=$3 WHERE trigger_id=$1 RETURNING lease_fence`, triggerID, l.ID, l.ExpiresAt).Scan(&l.Fence)
	if err != nil {
		return Lease{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO app.delivery_claims(claim_id,trigger_id,fence,claimed_at,expires_at,status)
 VALUES($1,$2,$3,$4,$5,'claimed')`, l.ID, triggerID, l.Fence, now, l.ExpiresAt)
	if err != nil {
		return Lease{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Lease{}, err
	}
	return l, nil
}

type WaitReason string

const (
	WaitCondition WaitReason = "wait_condition"
	WaitLimiter   WaitReason = "wait_limiter"
)

// Park durably schedules a not-sent check and releases ownership in one commit.
// Caller uses retry.Policy.WaitCondition or the limiter deadline. It must not
// park an HTTP outcome through this method: outcomes require their own durable
// evidence/report transaction. Repeats and stale leases return ErrStale.
func (s *Store) Park(ctx context.Context, l Lease, reason WaitReason, next time.Time) error {
	if l.TriggerID == "" || l.ID == "" || l.Fence <= 0 || (reason != WaitCondition && reason != WaitLimiter) || next.IsZero() {
		return errors.New("invalid park request")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	var id *string
	var fence int64
	var expires *time.Time
	var state string
	err = tx.QueryRow(ctx, `SELECT lease_id,lease_fence,lease_expires_at,state FROM app.delivery_jobs WHERE trigger_id=$1 FOR UPDATE`, l.TriggerID).Scan(&id, &fence, &expires, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrStale
	}
	if err != nil {
		return err
	}
	now, err := s.clock(ctx, tx)
	if err != nil {
		return err
	}
	if id == nil || *id != l.ID || fence != l.Fence || expires == nil || !now.Before(*expires) || state != "leased" {
		return ErrStale
	}
	succeeded, err := hasSuccess(ctx, tx, l.TriggerID)
	if err != nil {
		return err
	}
	if succeeded {
		return ErrUnavailable
	}
	var attempted bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.delivery_attempts WHERE claim_id=$1)`, l.ID).Scan(&attempted); err != nil {
		return err
	}
	if attempted {
		return ErrConflict
	}
	if !next.After(now) {
		return errors.New("park deadline must be in the future")
	}
	// PostgreSQL has microsecond precision; round up so work never becomes due
	// before the caller's deadline.
	if next.Nanosecond()%1000 != 0 {
		next = next.Truncate(time.Microsecond).Add(time.Microsecond)
	}
	if _, err = tx.Exec(ctx, `UPDATE app.delivery_jobs SET state=$2,next_attempt_at=$3,lease_id=NULL,lease_expires_at=NULL WHERE trigger_id=$1`, l.TriggerID, string(reason), next); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE app.delivery_claims SET status='released' WHERE claim_id=$1`, l.ID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
