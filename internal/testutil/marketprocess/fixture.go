// Package marketprocess supplies fixtures for disposable market-data process
// tests. Process ownership, readiness and cleanup use apiprocess.With unchanged.
package marketprocess

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/storage"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/trade"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Fixture struct {
	Generation uint64
	EventID    string
	Key, Value []byte
}

// Seed requires a fresh disposable market_data DB and a valid raw trade. It
// leaves one pending, unclaimed outbox record. On failure discard the dataset;
// generations and durable partial writes are intentionally not rolled back here.
func Seed(ctx context.Context, pool *pgxpool.Pool, raw []byte) (Fixture, error) {
	var f Fixture
	now := time.Now().UTC()
	q, err := trade.Decode(raw, trade.Metadata{Unit: priceobserved.Milliseconds, ReceivedAt: now, Generation: 1, Sequence: 1, StreamState: priceobserved.Unconfirmed})
	if err != nil {
		return f, err
	}
	repo := storage.New(pool)
	f.Generation, err = repo.BeginGeneration(ctx, q.Pair)
	if err != nil {
		return f, err
	}
	var id [16]byte
	if _, err = rand.Read(id[:]); err != nil {
		return f, err
	}
	f.EventID = "market-process-" + hex.EncodeToString(id[:])
	meta := event.Envelope{EventID: f.EventID, Type: priceobserved.Type, Version: 1, OccurredAt: now, CorrelationID: f.EventID, OrderingKey: q.Pair}
	ok, err := repo.Accept(ctx, f.Generation, q, meta)
	if err != nil {
		return f, err
	}
	if !ok {
		return f, fmt.Errorf("market fixture: trade was not accepted")
	}
	var pair string
	err = pool.QueryRow(ctx, `SELECT pair,data FROM app.price_outbox WHERE event_id=$1`, f.EventID).Scan(&pair, &f.Value)
	f.Key = []byte(pair)
	return f, err
}
