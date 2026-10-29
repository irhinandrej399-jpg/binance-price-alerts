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

type recoveryView struct {
	handlerView
	images map[string]projection.Image
}

func (v *recoveryView) ReadImage(_ context.Context, id string) (projection.Image, error) {
	return v.images[id], nil
}

type recoveryClient struct {
	client *deliveryrpc.Client
	calls  int
	fail   bool
}

func (c *recoveryClient) RequestDelivery(ctx context.Context, owner, id string, version int64) (*pb.RequestDeliveryResponse, error) {
	response, err := c.client.RequestDelivery(ctx, owner, id, version)
	c.calls++
	if err == nil && c.fail && c.calls == 38 {
		return nil, status.Error(codes.Unavailable, "response lost after canonical commit")
	}
	return response, err
}

func TestPostgresPriceRecovery100OverTLS(t *testing.T) {
	ctx := context.Background()
	pool := postgrestest.Open(t)
	mp := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	mp2 := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	r := record()
	v := &recoveryView{images: map[string]projection.Image{}}
	p, _ := price.Parse("101")
	v.sel = live.Selection{C: 10, Quote: priceobserved.Observed{SchemaVersion: 1, Pair: r.Pair, Price: &p, SourceID: "recovery-price", Revision: priceobserved.Revision{StreamID: "stream", Generation: 1, Sequence: 1}, TradeTime: now, ReceivedAt: now, StreamState: priceobserved.Live}}
	ids := []string{}
	owners := []string{}
	t.Cleanup(func() {
		for _, owner := range owners {
			if _, e := pool.Exec(ctx, `DELETE FROM app.subscriptions WHERE owner_id=$1`, owner); e != nil {
				t.Error(e)
			}
			if _, e := pool.Exec(ctx, `DELETE FROM app.users WHERE id=$1`, owner); e != nil {
				t.Error(e)
			}
		}
	})
	for i := 0; i < 100; i++ {
		if i%15 == 0 {
			a, e := account.New(pool).Create(ctx, fmt.Sprintf("price-recovery-%d-%d@example.com", time.Now().UnixNano(), i), "hash")
			if e != nil {
				t.Fatal(e)
			}
			owners = append(owners, a.ID)
			if _, e = pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=$2,telegram_version=1 WHERE id=$1`, a.ID, 900000+i); e != nil {
				t.Fatal(e)
			}
		}
		r.ID = fmt.Sprintf("00000000-0000-4000-8000-00000299%04d", i)
		r.OwnerID = owners[len(owners)-1]
		r.CreationPrice = r.Threshold
		r.CreationTradeTime = now
		r.Activation = sub.LatestPrice
		tx, e := pool.Begin(ctx)
		if e != nil {
			t.Fatal(e)
		}
		quota, e := sub.LockQuota(ctx, tx, r.OwnerID)
		if e == nil {
			e = quota.Create(ctx, r)
		}
		if e != nil {
			tx.Rollback(ctx)
			t.Fatal(e)
		}
		if e = tx.Commit(ctx); e != nil {
			t.Fatal(e)
		}
		ids = append(ids, r.ID)
		v.sel.Candidates = append(v.sel.Candidates, thresholdindex.Entry{ID: r.ID, Version: 1, Pair: r.Pair, Threshold: r.Threshold, Direction: r.Direction})
		v.images[r.ID] = projection.Image{Snapshot: &pb.SnapshotSubscription{OwnerId: r.OwnerID, SubscriptionId: r.ID, Version: 1, Pair: r.Pair, Threshold: "100", Direction: "up", State: "active"}}
	}
	conn := connect(t, deliveryrpc.NewServer(deliveryrpc.NewPostgres(pool), quotes(func(context.Context, string) (*market.LatestResponse, error) { return quote("101"), nil }), func() time.Time { return now }), matcherURI)
	client := &recoveryClient{client: deliveryrpc.NewClient(conn), fail: true}
	store := pricework.New(mp, t.Name())
	h := handler.New(store, v, v, client, "old-generation")
	if e := h.Handle(ctx, handler.Request{Key: "stable", Pair: r.Pair}, nil); status.Code(e) != codes.Unavailable {
		t.Fatal(e)
	}
	w, e := store.Get(ctx, "stable")
	if e != nil || w.Completed != 37 {
		t.Fatal(w, e)
	}
	mp.Close()
	store = pricework.New(mp2, t.Name())
	h = handler.New(store, v, v, client, "new-generation")
	client.fail = false
	if e = h.Recover(ctx); e != nil {
		t.Fatal(e)
	}
	if e = h.Recover(ctx); e != nil {
		t.Fatal(e)
	}
	w, e = store.Get(ctx, "stable")
	if e != nil || w.State != "completed" || w.Completed != 100 || client.calls != 101 {
		t.Fatal(w, e, client.calls)
	}
	triggers := map[string]bool{}
	for _, entry := range w.Entries {
		if entry.Result == nil || entry.Result.TriggerID == "" || triggers[entry.Result.TriggerID] {
			t.Fatal(entry)
		}
		triggers[entry.Result.TriggerID] = true
		var count int
		var trigger string
		if e = pool.QueryRow(ctx, `SELECT count(*),min(trigger_id::text) FROM app.delivery_outbox WHERE subscription_id=$1`, entry.Candidate.SubscriptionID).Scan(&count, &trigger); e != nil || count != 1 || trigger != entry.Result.TriggerID {
			t.Fatal(count, trigger, e)
		}
		canonical, e := sub.New(pool).ByID(ctx, entry.Candidate.OwnerID, entry.Candidate.SubscriptionID)
		if e != nil || canonical.Workflow == nil || canonical.Workflow.TriggerID != trigger {
			t.Fatal(canonical, e)
		}
	}
	if len(triggers) != len(ids) {
		t.Fatal(len(triggers), len(ids))
	}
}
