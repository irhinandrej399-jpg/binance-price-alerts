// Package deliveryrelay publishes immutable delivery work requests serially.
package deliveryrelay

import (
	"context"
	"errors"
	"strings"
	"time"

	alert "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/alerttriggered"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafka"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrLeaseLost = errors.New("delivery relay: lease lost")
	ErrStorage   = errors.New("delivery relay: storage failed")
	ErrOutbox    = errors.New("delivery relay: invalid outbox")
	ErrBusy      = errors.New("delivery relay: shard already leased")
)

type publisher interface {
	Publish(context.Context, ...kafka.Record) error
	Close()
}

// Relay has no in-memory retry queue. Every Step reads unpublished durable work
// and initializes the same Kafka transactional ID. All replicas MUST use that ID
// and the environment's delivery-requests topic.
type Relay struct {
	pool  *pgxpool.Pool
	topic string
	open  func(context.Context) (publisher, error)
}

func New(pool *pgxpool.Pool, cfg kafka.Config, topic string) (*Relay, error) {
	if pool == nil || strings.TrimSpace(topic) == "" || cfg.TransactionalID == "" || len(cfg.Brokers) == 0 {
		return nil, kafka.ErrConfig
	}
	cfg.SingleRecord = true // One immutable work request per Kafka transaction.
	return &Relay{pool: pool, topic: topic, open: func(c context.Context) (publisher, error) { return kafka.New(c, cfg) }}, nil
}

// Step attempts one work request. false,nil means no pending work.
// The SQL lease lasts 30s; all work has a 5s budget. No database transaction or
// row lock is held during network calls. A crash leaves the lease for recovery.
// Any error retains the outbox; only confirmed Kafka commit permits a fenced SQL
// acknowledgement. Every retry initializes a fresh epoch before publishing.
func (r *Relay) Step(parent context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	var token int64
	err := r.pool.QueryRow(ctx, `UPDATE app.delivery_relay SET token=token+1,lease_until=clock_timestamp()+interval '30 seconds'
 WHERE singleton AND (lease_until IS NULL OR lease_until<=clock_timestamp()) RETURNING token`).Scan(&token)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrBusy
	}
	if err != nil {
		return false, storageError(ctx)
	}
	defer func() {
		// This conditional cleanup cannot release a replacement owner's lease.
		c, done := context.WithTimeout(context.Background(), time.Second)
		defer done()
		_, _ = r.pool.Exec(c, `UPDATE app.delivery_relay SET lease_until=NULL WHERE singleton AND token=$1`, token)
	}()
	var sub, trigger, id string
	var body []byte
	err = r.pool.QueryRow(ctx, `SELECT subscription_id::text,trigger_id,event_id,body FROM app.delivery_outbox o
 WHERE NOT EXISTS (SELECT 1 FROM app.delivery_published p WHERE p.subscription_id=o.subscription_id)
 ORDER BY subscription_id LIMIT 1`).Scan(&sub, &trigger, &id, &body)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, storageError(ctx)
	}
	meta, work, e := alert.Decode(body)
	if e != nil || meta.EventID != id || work.SubscriptionID != sub || work.TriggerID != trigger {
		return false, ErrOutbox
	}
	records := []kafka.Record{{Topic: r.topic, Key: []byte(sub), Value: body}}
	p, err := r.open(ctx)
	if err != nil {
		return false, err
	}
	defer p.Close()
	// Initialization may have been delayed past our lease. A late initializer
	// can fence a newer producer, but MUST NOT publish a stale batch afterwards.
	var owns bool
	if err = r.pool.QueryRow(ctx, `SELECT token=$1 AND lease_until>clock_timestamp() FROM app.delivery_relay WHERE singleton`, token).Scan(&owns); err != nil {
		return false, storageError(ctx)
	}
	if !owns {
		return false, ErrLeaseLost
	}
	if err = p.Publish(ctx, records...); err != nil {
		return false, err
	}
	tag, err := r.pool.Exec(ctx, `WITH owned AS (
 UPDATE app.delivery_relay SET lease_until=NULL
 WHERE singleton AND token=$2 AND lease_until>clock_timestamp() RETURNING singleton
 ) INSERT INTO app.delivery_published (subscription_id) SELECT $1::uuid FROM owned
 ON CONFLICT DO NOTHING`, sub, token)
	if err != nil {
		return false, storageError(ctx)
	}
	if tag.RowsAffected() != 1 {
		return false, ErrLeaseLost
	}
	return true, nil
}
func storageError(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return ErrStorage
}

// Run polls the SQL queue and retries transient failures with capped backoff.
// ErrFenced and ErrLeaseLost are terminal for this run. The caller owns cancellation and reporting;
// observe is synchronous and must return promptly. Runtime wiring is separate.
func (r *Relay) Run(ctx context.Context, observe func(error)) error {
	delay := 250 * time.Millisecond
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		worked, err := r.Step(ctx)
		if err != nil && observe != nil {
			observe(err)
		}
		if errors.Is(err, kafka.ErrFenced) || errors.Is(err, ErrLeaseLost) || errors.Is(err, kafka.ErrConfig) || errors.Is(err, ErrOutbox) {
			return err
		}
		if err == nil && worked {
			delay = 250 * time.Millisecond
			continue
		}
		pause := 250 * time.Millisecond
		if err != nil && !errors.Is(err, ErrBusy) {
			pause = delay
			delay = min(2*delay, 5*time.Second)
		} else {
			delay = 250 * time.Millisecond
		}
		timer := time.NewTimer(pause)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
