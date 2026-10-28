package deliveryrpc_test

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/deliveryrpc"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/alerttriggered"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/latest"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/storage"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPostgresDeliveryOverTLS(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	a, e := account.New(pool).Create(ctx, fmt.Sprintf("rpc-%d@example.com", time.Now().UnixNano()), "hash")
	if e != nil {
		t.Fatal(e)
	}
	_, e = pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=123,telegram_version=1 WHERE id=$1`, a.ID)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		_, e := pool.Exec(ctx, `DELETE FROM app.subscriptions WHERE owner_id=$1`, a.ID)
		if e != nil {
			t.Error(e)
		}
		_, e = pool.Exec(ctx, `DELETE FROM app.users WHERE id=$1`, a.ID)
		if e != nil {
			t.Error(e)
		}
	})
	var n atomic.Int64
	newRecord := func() sub.Record {
		r := record()
		r.OwnerID = a.ID
		r.ID = fmt.Sprintf("00000000-0000-4000-8000-%012d", 1980000+n.Add(1))
		r.CreationPrice = r.Threshold
		r.CreationTradeTime = now
		r.Activation = sub.LatestPrice
		tx, e := pool.Begin(ctx)
		if e != nil {
			t.Fatal(e)
		}
		defer tx.Rollback(ctx)
		q, e := sub.LockQuota(ctx, tx, a.ID)
		if e != nil {
			t.Fatal(e)
		}
		if e = q.Create(ctx, r); e != nil {
			t.Fatal(e)
		}
		if e = tx.Commit(ctx); e != nil {
			t.Fatal(e)
		}
		return r
	}
	var value atomic.Int64
	value.Store(99)
	var missing atomic.Bool
	marketConn := marketConnection(t, reader(func(ctx context.Context, pair string) (priceobserved.Observed, error) {
		if missing.Load() {
			return priceobserved.Observed{}, storage.ErrNotFound
		}
		p, _ := price.Parse(fmt.Sprint(value.Load()))
		return priceobserved.Observed{SchemaVersion: 1, Pair: pair, Price: &p, SourceID: "canonical-source", Revision: priceobserved.Revision{StreamID: "canonical-stream", Generation: 7, Sequence: 42}, TradeTime: now.Add(-time.Hour), ReceivedAt: now.Add(-time.Minute), StreamState: priceobserved.Unconfirmed, GapDetected: true}, nil
	}))
	api := connect(t, deliveryrpc.NewServer(deliveryrpc.NewPostgres(pool), latest.NewClient(marketConn), func() time.Time { return now }), matcherURI)
	client := deliveryrpc.NewClient(api)
	r := newRecord()
	assertUnchanged := func(r sub.Record) {
		got, e := sub.New(pool).ByID(ctx, r.OwnerID, r.ID)
		if e != nil || got.State != domain.Active || got.Version != 1 || got.Workflow != nil {
			t.Fatal(got, e)
		}
		var count int
		e = pool.QueryRow(ctx, `SELECT count(*) FROM app.delivery_outbox WHERE subscription_id=$1`, r.ID).Scan(&count)
		if e != nil || count != 0 {
			t.Fatal(count, e)
		}
	}
	// Historical/candidate 101 cannot be supplied: the command has no price field.
	if _, e = client.RequestDelivery(ctx, a.ID, r.ID, 1); status.Code(e) != codes.FailedPrecondition {
		t.Fatal(e)
	}
	assertUnchanged(r)
	value.Store(101)
	if _, e = client.RequestDelivery(ctx, a.ID, r.ID, 2); status.Code(e) != codes.Aborted {
		t.Fatal(e)
	}
	assertUnchanged(r)
	missing.Store(true)
	if _, e = client.RequestDelivery(ctx, a.ID, r.ID, 1); status.Code(e) != codes.Unavailable {
		t.Fatal(e)
	}
	assertUnchanged(r)
	missing.Store(false)
	// Concurrent authenticated commands all converge to one persisted identity.
	results := make(chan *pb.RequestDeliveryResponse, 10)
	errs := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func() { v, e := client.RequestDelivery(ctx, a.ID, r.ID, 1); results <- v; errs <- e }()
	}
	var first *pb.RequestDeliveryResponse
	for i := 0; i < 10; i++ {
		v := <-results
		if e := <-errs; e != nil {
			t.Fatal(e)
		}
		if first == nil {
			first = v
		}
		if v.Workflow.TriggerId != first.Workflow.TriggerId || v.SubscriptionVersion != 2 || v.State != "pending_delivery" {
			t.Fatal(v)
		}
	}
	var body, again []byte
	var count, quota int
	e = pool.QueryRow(ctx, `SELECT body FROM app.delivery_outbox WHERE subscription_id=$1`, r.ID).Scan(&body)
	if e != nil {
		t.Fatal(e)
	}
	_, work, e := alerttriggered.Decode(body)
	if e != nil || work.CandidateQuote.Price.String() != "101" || work.CandidateQuote.SourceID != "canonical-source" || work.CandidateQuote.Revision.Generation != 7 || !work.CandidateQuote.TradeTime.Equal(now.Add(-time.Hour)) {
		t.Fatal(work, e)
	}
	missing.Store(true)
	dup, e := client.RequestDelivery(ctx, a.ID, r.ID, 1)
	if e != nil || dup.Workflow.TriggerId != first.Workflow.TriggerId {
		t.Fatal(dup, e)
	}
	e = pool.QueryRow(ctx, `SELECT body FROM app.delivery_outbox WHERE subscription_id=$1`, r.ID).Scan(&again)
	if e != nil || !bytes.Equal(body, again) {
		t.Fatal(e)
	}
	e = pool.QueryRow(ctx, `SELECT count(*) FROM app.delivery_outbox WHERE subscription_id=$1`, r.ID).Scan(&count)
	if e != nil || count != 1 {
		t.Fatal(count, e)
	}
	e = pool.QueryRow(ctx, `SELECT active_subscriptions FROM app.users WHERE id=$1`, a.ID).Scan(&quota)
	if e != nil || quota != 1 {
		t.Fatal(quota, e)
	}
	// A wrong owner cannot select a subscription even with matcher credentials.
	if _, e = client.RequestDelivery(ctx, ownerID, r.ID, 1); status.Code(e) != codes.NotFound {
		t.Fatal(e)
	}
	missing.Store(false)
	// API deadline cancels SQL lock wait without durable work; retry can commit.
	waiting := newRecord()
	tx, e := pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	if _, e = sub.LockQuota(ctx, tx, a.ID); e != nil {
		t.Fatal(e)
	}
	short, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	if _, e = client.RequestDelivery(short, a.ID, waiting.ID, 1); status.Code(e) != codes.DeadlineExceeded {
		t.Fatal(e)
	}
	if e = tx.Rollback(ctx); e != nil {
		t.Fatal(e)
	}
	assertUnchanged(waiting)
	if v, e := client.RequestDelivery(ctx, a.ID, waiting.ID, 1); e != nil || v.Workflow == nil {
		t.Fatal(v, e)
	}
}
