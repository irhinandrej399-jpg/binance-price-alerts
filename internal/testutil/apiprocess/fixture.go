package apiprocess

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/session"
	store "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Fixture is private test data; do not log it (Token is a bearer credential).
// Active remains active for HTTP cancellation. Delivery has canonical workflow
// and outbox created by the existing API command, not handcrafted business SQL.
// The disposable volume owns cleanup, including immutable journal rows.
type Fixture struct {
	OwnerID, Token             string
	Active, Delivery           store.Record
	DeliveryEventID            string
	DeliveryKey, DeliveryValue []byte
}

func uuid() string {
	var b [16]byte
	rand.Read(b[:])
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// Seed uses API runtime privileges on a fresh disposable API database only.
// Binding setup is the same test SQL used by quota fixtures; no Telegram call.
func Seed(ctx context.Context, pool *pgxpool.Pool) (Fixture, error) {
	var f Fixture
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return f, err
	}
	defer tx.Rollback(context.Background())
	owner, err := account.New(tx).Create(ctx, uuid()+"@example.test", "test-only-hash")
	if err != nil {
		return f, err
	}
	f.OwnerID = owner.ID
	if _, err = tx.Exec(ctx, `UPDATE app.users SET telegram_chat_id=123456,telegram_version=1 WHERE id=$1`, owner.ID); err != nil {
		return f, err
	}
	credentials, err := session.New(tx, nil).Issue(ctx, owner.ID)
	if err != nil {
		return f, err
	}
	f.Token = credentials.AccessToken
	quota, err := store.LockQuota(ctx, tx, owner.ID)
	if err != nil {
		return f, err
	}
	p, _ := price.Parse("100")
	record := func() store.Record {
		return store.Record{ID: uuid(), OwnerID: owner.ID, Pair: "BTCUSDT", Threshold: p, Direction: price.Up, CreationPrice: p, CreationTradeTime: time.Now().UTC().Truncate(time.Microsecond), State: domain.Active, Version: 1, Activation: store.LatestPrice}
	}
	f.Active = record()
	f.Delivery = record()
	for _, v := range []store.Record{f.Active, f.Delivery} {
		if err = quota.Create(ctx, v); err != nil {
			return f, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return f, err
	}
	q := priceobserved.Observed{SchemaVersion: 1, Pair: "BTCUSDT", Price: &p, SourceID: "fixture-1", Revision: priceobserved.Revision{StreamID: "fixture", Generation: 1, Sequence: 1}, TradeTime: f.Delivery.CreationTradeTime, ReceivedAt: f.Delivery.CreationTradeTime, StreamState: priceobserved.Unconfirmed}
	f.Delivery, err = store.RequestDelivery(ctx, pool, store.DeliveryCandidate{OwnerID: f.OwnerID, SubscriptionID: f.Delivery.ID, ExpectedVersion: 1, Quote: q})
	if err != nil {
		return f, err
	}
	f.DeliveryKey = []byte(f.Delivery.ID)
	err = pool.QueryRow(ctx, `SELECT event_id,body FROM app.delivery_outbox WHERE subscription_id=$1`, f.Delivery.ID).Scan(&f.DeliveryEventID, &f.DeliveryValue)
	return f, err
}
