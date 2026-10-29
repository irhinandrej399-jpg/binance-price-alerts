package deliveryrpc_test

import (
	"context"
	"fmt"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/deliveryrpc"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	market "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/live"
	handler "github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricehandler"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricework"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/projection"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/thresholdindex"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
	"time"
)

type handlerView struct {
	sel   live.Selection
	image projection.Image
}

func (v *handlerView) Current(string) (live.Selection, error) { return v.sel, nil }
func (v *handlerView) ReadImage(context.Context, string) (projection.Image, error) {
	return v.image, nil
}
func TestPostgresPriceHandlerOverTLS(t *testing.T) {
	ctx := context.Background()
	pool := postgrestest.Open(t)
	mp := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	mp2 := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	a, err := account.New(pool).Create(ctx, fmt.Sprintf("price-handler-%d@example.com", time.Now().UnixNano()), "hash")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=123,telegram_version=1 WHERE id=$1`, a.ID); err != nil {
		t.Fatal(err)
	}
	r := record()
	r.ID = "00000000-0000-4000-8000-000001990001"
	r.OwnerID = a.ID
	r.CreationPrice = r.Threshold
	r.CreationTradeTime = now
	r.Activation = sub.LatestPrice
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	quota, err := sub.LockQuota(ctx, tx, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = quota.Create(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, e := pool.Exec(ctx, `DELETE FROM app.subscriptions WHERE owner_id=$1`, a.ID); e != nil {
			t.Error(e)
		}
		if _, e := pool.Exec(ctx, `DELETE FROM app.users WHERE id=$1`, a.ID); e != nil {
			t.Error(e)
		}
	})
	p, _ := price.Parse("101")
	q := priceobserved.Observed{SchemaVersion: 1, Pair: r.Pair, Price: &p, SourceID: "matcher-price", Revision: priceobserved.Revision{StreamID: "stream", Generation: 1, Sequence: 1}, TradeTime: now, ReceivedAt: now, StreamState: priceobserved.Live}
	view := &handlerView{sel: live.Selection{C: 10, Quote: q, Candidates: []thresholdindex.Entry{{ID: r.ID, Version: 1, Pair: r.Pair, Threshold: r.Threshold, Direction: r.Direction}}}, image: projection.Image{Snapshot: &pb.SnapshotSubscription{OwnerId: a.ID, SubscriptionId: r.ID, Version: 1, Pair: r.Pair, Threshold: "100", Direction: "up", State: "active"}}}
	value := "99"
	conn := connect(t, deliveryrpc.NewServer(deliveryrpc.NewPostgres(pool), quotes(func(context.Context, string) (*market.LatestResponse, error) { return quote(value), nil }), func() time.Time { return now }), matcherURI)
	store, check := pricework.New(mp, t.Name()), pricework.New(mp2, t.Name())
	h := handler.New(store, view, view, deliveryrpc.NewClient(conn), "generation")
	request := handler.Request{Key: "latest", Pair: r.Pair}
	acked := 0
	ack := func(context.Context) error {
		w, e := check.Get(ctx, request.Key)
		if e != nil || w.State != "completed" || w.Entries[0].Result.TriggerID == "" {
			t.Fatal(w, e)
		}
		acked++
		return nil
	}
	// API's latest 99 overrides the candidate 101. Its opaque rejection stays
	// pending while the local view lags, without inventing a terminal outcome.
	if err = h.Handle(ctx, request, ack); status.Code(err) != codes.FailedPrecondition || acked != 0 {
		t.Fatal(err, acked)
	}
	canonical, err := sub.New(pool).ByID(ctx, a.ID, r.ID)
	if err != nil || canonical.Workflow != nil {
		t.Fatal(canonical, err)
	}
	value = "101"
	if err = h.Handle(ctx, request, ack); err != nil {
		t.Fatal(err)
	}
	w, err := check.Get(ctx, request.Key)
	if err != nil {
		t.Fatal(err)
	}
	trigger := w.Entries[0].Result.TriggerID
	// A distinct evaluation recovers the same canonical workflow even with the
	// old expected version, and never asks API to use the supplied event price.
	request.Key = "duplicate-view"
	view.sel.C++
	if err = h.Handle(ctx, request, ack); err != nil {
		t.Fatal(err)
	}
	w, err = check.Get(ctx, request.Key)
	if err != nil || w.Entries[0].Result.TriggerID != trigger || acked != 2 {
		t.Fatal(w, err, acked)
	}
	var count int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM app.delivery_outbox WHERE subscription_id=$1`, r.ID).Scan(&count); err != nil || count != 1 {
		t.Fatal(count, err)
	}
}
