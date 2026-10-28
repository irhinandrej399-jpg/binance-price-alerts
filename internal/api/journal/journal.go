// Package journal stores the API subscription journal in caller-owned transactions.
package journal

import (
	"context"
	"errors"
	"math"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	changed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/subscriptionchanged"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrInvalidInput = errors.New("invalid journal input or cursor")
	ErrExists       = errors.New("journal event or transaction already exists")
	ErrStorage      = errors.New("journal storage operation failed")
)

type Entry struct {
	Metadata event.Envelope
	Change   changed.Changed
}

type Reader interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

// Current returns the commit-consistent C, never MAX(position). For snapshots,
// call with the same REPEATABLE READ transaction used to read business rows.
func Current(ctx context.Context, db Reader) (uint64, error) {
	var c int64
	err := db.QueryRow(ctx, `SELECT position FROM app.api_journal_counter WHERE singleton`).Scan(&c)
	if err != nil {
		return 0, storageError(ctx, err)
	}
	return uint64(c), nil
}

// Append must be called exactly once with the whole transaction's changes,
// after all business locks/writes. Do not acquire business locks or wait on a
// network after it. The counter row stays locked until the caller ends tx.
// IDs/times are caller-assigned and preserved. Position/TransactionEnd are
// assigned here; inputs are not modified. A DB error aborts tx; always rollback
// on any error. No commit, retry, publication or deletion occurs here.
func Append(ctx context.Context, tx pgx.Tx, entries []Entry) (uint64, error) {
	if len(entries) == 0 {
		return 0, ErrInvalidInput
	}
	// Validate the complete batch before touching the counter. Placeholder
	// positions allow callers to provide after-images without allocating C.
	for _, e := range entries {
		e.Change.Position = 1
		e.Change.TransactionEnd = 1
		if _, err := changed.Encode(e.Metadata, e.Change); err != nil {
			return 0, ErrInvalidInput
		}
	}
	var end int64
	err := tx.QueryRow(ctx, `UPDATE app.api_journal_counter SET position=position+$1
 WHERE singleton RETURNING position`, int64(len(entries))).Scan(&end)
	if err != nil {
		return 0, storageError(ctx, err)
	}
	// PostgreSQL transaction IDs identify a batch, not journal ordering. A second
	// Append in this tx must abort, otherwise previously stored ends would split C.
	_, err = tx.Exec(ctx, `INSERT INTO app.api_journal_batches (transaction_id,transaction_end) VALUES (pg_current_xact_id(),$1)`, end)
	if err != nil {
		return 0, storageError(ctx, err)
	}
	for i, e := range entries {
		pos := end - int64(len(entries)) + int64(i) + 1
		e.Change.Position = uint64(pos)
		e.Change.TransactionEnd = uint64(end)
		body, err := changed.Encode(e.Metadata, e.Change)
		if err != nil {
			return 0, ErrInvalidInput
		}
		_, err = tx.Exec(ctx, `INSERT INTO app.api_journal (position,transaction_end,event_id,body) VALUES ($1,$2,$3,$4)`, pos, end, e.Metadata.EventID, body)
		if err != nil {
			return 0, storageError(ctx, err)
		}
	}
	return uint64(end), nil
}

// ReadAfter returns every record of up to maxTransactions whole transactions,
// strictly after a validated C. Zero denotes the beginning. A cursor inside a
// transaction or ahead of the journal is rejected. Advance only to the final
// TransactionEnd after durable application. Limits count transactions, not rows.
func ReadAfter(ctx context.Context, db Reader, after uint64, maxTransactions int) ([]Entry, error) {
	if after > math.MaxInt64 || maxTransactions < 1 || maxTransactions > 1000 {
		return nil, ErrInvalidInput
	}
	if after != 0 {
		var valid bool
		err := db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.api_journal_batches WHERE transaction_end=$1)`, int64(after)).Scan(&valid)
		if err != nil {
			return nil, storageError(ctx, err)
		}
		if !valid {
			return nil, ErrInvalidInput
		}
	}
	rows, err := db.Query(ctx, `SELECT position,transaction_end,event_id,body FROM app.api_journal
 WHERE transaction_end IN (SELECT transaction_end FROM app.api_journal_batches
 WHERE transaction_end>$1 ORDER BY transaction_end LIMIT $2) ORDER BY position`, int64(after), maxTransactions)
	if err != nil {
		return nil, storageError(ctx, err)
	}
	defer rows.Close()
	result := make([]Entry, 0)
	for rows.Next() {
		var pos, end int64
		var id string
		var body []byte
		if err = rows.Scan(&pos, &end, &id, &body); err != nil {
			return nil, storageError(ctx, err)
		}
		meta, change, e := changed.Decode(body)
		if e != nil || change.Position != uint64(pos) || change.TransactionEnd != uint64(end) || meta.EventID != id {
			return nil, ErrStorage
		}
		result = append(result, Entry{Metadata: meta, Change: change})
	}
	if err = rows.Err(); err != nil {
		return nil, storageError(ctx, err)
	}
	return result, nil
}

func storageError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		switch pe.Code {
		case "23505":
			return ErrExists
		case "22003", "23514":
			return ErrInvalidInput
		}
	}
	return ErrStorage
}
