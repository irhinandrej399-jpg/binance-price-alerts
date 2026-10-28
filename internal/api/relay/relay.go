// Package relay publishes whole API journal transactions serially.
package relay

import (
	"context"
	"errors"
	"strings"
	"time"

	changed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/subscriptionchanged"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafka"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrLeaseLost = errors.New("subscription relay: lease lost")
	ErrStorage   = errors.New("subscription relay: storage failed")
	ErrJournal   = errors.New("subscription relay: invalid journal")
	ErrBusy      = errors.New("subscription relay: shard already leased")
)

type publisher interface {
	Publish(context.Context, ...kafka.Record) error
	Close()
}

// Relay has no in-memory retry queue. Every Step reclaims the durable prefix
// and initializes the same Kafka transactional ID. All replicas MUST use that ID
// and the environment's single-partition subscription-changes topic.
type Relay struct {
	pool  *pgxpool.Pool
	topic string
	open  func(context.Context) (publisher, error)
}

func New(pool *pgxpool.Pool, cfg kafka.Config, topic string) (*Relay, error) {
	if pool == nil || strings.TrimSpace(topic) == "" || cfg.TransactionalID == "" || len(cfg.Brokers) == 0 {
		return nil, kafka.ErrConfig
	}
	cfg.SingleRecord = false // A SQL transaction may contain multiple events.
	return &Relay{pool: pool, topic: topic, open: func(c context.Context) (publisher, error) { return kafka.New(c, cfg) }}, nil
}

// Step attempts one whole SQL transaction. false,nil means no pending work.
// The SQL lease lasts 30s; all work has a 5s budget. No database transaction or
// row lock is held during network calls. A crash leaves the lease for recovery.
// Any error retains the journal/cursor; only confirmed Kafka commit permits CAS
// advancement. Every retry initializes a fresh producer epoch before publishing.
func (r *Relay) Step(parent context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	var after, token int64
	err := r.pool.QueryRow(ctx, `UPDATE app.subscription_relay SET token=token+1,lease_until=clock_timestamp()+interval '30 seconds'
 WHERE singleton AND (lease_until IS NULL OR lease_until<=clock_timestamp()) RETURNING position,token`).Scan(&after, &token)
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
		_, _ = r.pool.Exec(c, `UPDATE app.subscription_relay SET lease_until=NULL WHERE singleton AND token=$1`, token)
	}()
	rows, err := r.pool.Query(ctx, `SELECT position,transaction_end,event_id,body FROM app.api_journal
 WHERE transaction_end=(SELECT transaction_end FROM app.api_journal_batches WHERE transaction_end>$1 ORDER BY transaction_end LIMIT 1) ORDER BY position`, after)
	if err != nil {
		return false, storageError(ctx)
	}
	var records []kafka.Record
	next, end := after+1, int64(0)
	for rows.Next() {
		var pos, batchEnd int64
		var id string
		var body []byte
		if err = rows.Scan(&pos, &batchEnd, &id, &body); err != nil {
			rows.Close()
			return false, storageError(ctx)
		}
		meta, change, e := changed.Decode(body)
		if e != nil || pos != next || (end != 0 && end != batchEnd) || change.Position != uint64(pos) || change.TransactionEnd != uint64(batchEnd) || meta.EventID != id {
			rows.Close()
			return false, ErrJournal
		}
		records = append(records, kafka.Record{Topic: r.topic, Key: []byte(meta.OrderingKey), Value: body})
		next++
		end = batchEnd
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return false, storageError(ctx)
	}
	if len(records) == 0 {
		return false, nil
	}
	if next-1 != end {
		return false, ErrJournal
	}
	p, err := r.open(ctx)
	if err != nil {
		return false, err
	}
	defer p.Close()
	// Initialization may have been delayed past our lease. A late initializer
	// can fence a newer producer, but MUST NOT publish a stale batch afterwards.
	var owns bool
	if err = r.pool.QueryRow(ctx, `SELECT token=$1 AND lease_until>clock_timestamp() FROM app.subscription_relay WHERE singleton`, token).Scan(&owns); err != nil {
		return false, storageError(ctx)
	}
	if !owns {
		return false, ErrLeaseLost
	}
	if err = p.Publish(ctx, records...); err != nil {
		return false, err
	}
	tag, err := r.pool.Exec(ctx, `UPDATE app.subscription_relay SET position=$1,lease_until=NULL
 WHERE singleton AND token=$2 AND position=$3 AND lease_until>clock_timestamp()`, end, token, after)
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
		if errors.Is(err, kafka.ErrFenced) || errors.Is(err, ErrLeaseLost) || errors.Is(err, kafka.ErrConfig) || errors.Is(err, ErrJournal) {
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
