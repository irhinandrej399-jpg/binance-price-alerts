package latest_test

import (
	"context"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/latest"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/storage"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
)

func TestDurableLatestOverTLS(t *testing.T) {
	pool := postgrestest.OpenWithDSN(t, "MARKET_DATA_DATABASE_URL")
	repo := storage.New(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// Dedicated pair keeps this test independent of existing storage scenarios.
	q := quote()
	q.Pair = "ETHUSDT"
	gen, err := repo.BeginGeneration(ctx, q.Pair)
	if err != nil {
		t.Fatal(err)
	}
	conn := connect(t, repo, apiURI)
	client := latest.NewClient(conn)
	read := func() *pb.LatestResponse {
		t.Helper()
		r, err := client.Latest(ctx, q.Pair)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	if r := read(); r.Freshness != pb.Freshness_FRESHNESS_MISSING || r.Quote != nil {
		t.Fatal(r)
	}
	if err := repo.SetStatus(ctx, q.Pair, gen, priceobserved.Live, false); err != nil {
		t.Fatal(err)
	}
	accept := func(id, eid string) bool {
		t.Helper()
		candidate := q
		candidate.SourceID = id
		ok, err := repo.Accept(ctx, gen, candidate, event.Envelope{EventID: eid, Type: priceobserved.Type, Version: 1, OccurredAt: now, CorrelationID: "latest-rpc-test", OrderingKey: q.Pair})
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	if !accept("42", "latest-rpc-42") {
		t.Fatal("not accepted")
	}
	first := read()
	if first.Quote.SourceId != "42" || first.Quote.Revision.Generation != gen || first.Quote.Revision.Sequence != 1 || len(first.Warnings) != 0 {
		t.Fatal(first)
	}
	if accept("41", "latest-rpc-41") || accept("42", "latest-rpc-repeat") {
		t.Fatal("old/duplicate accepted")
	}
	repeated := read()
	if repeated.Quote.SourceId != first.Quote.SourceId || repeated.Quote.Revision.Sequence != first.Quote.Revision.Sequence || repeated.Quote.TradeTime != first.Quote.TradeTime {
		t.Fatal(repeated)
	}
	// Recreate the repository and fence the previous ingest process before serving.
	repo = storage.New(pool)
	newGen, err := repo.BeginGeneration(ctx, q.Pair)
	if err != nil || newGen <= gen {
		t.Fatalf("%d %v", newGen, err)
	}
	conn2 := connect(t, repo, apiURI)
	client = latest.NewClient(conn2)
	restarted := read()
	if restarted.Freshness != pb.Freshness_FRESHNESS_FRESH || restarted.Quote.Revision.Generation != gen || restarted.Quote.TradeTime != first.Quote.TradeTime || restarted.Quote.ReceivedAt != first.Quote.ReceivedAt || restarted.Quote.StreamState != "unconfirmed" || !restarted.Quote.GapDetected || len(restarted.Warnings) != 2 {
		t.Fatal(restarted)
	}
	// Explicit confirmation by ingest preserves timestamps and the historical gap.
	if err := repo.SetStatus(ctx, q.Pair, newGen, priceobserved.Live, false); err != nil {
		t.Fatal(err)
	}
	live := read()
	if live.Quote.TradeTime != first.Quote.TradeTime || len(live.Warnings) != 1 || live.Warnings[0] != pb.Warning_WARNING_GAP_DETECTED {
		t.Fatal(live)
	}
	gen = newGen
	if !accept("43", "latest-rpc-43") {
		t.Fatal("new trade rejected")
	}
	newer := read()
	if newer.Quote.Revision.Generation != newGen || newer.Quote.Revision.Sequence != 1 || newer.Quote.SourceId != "43" {
		t.Fatal(newer)
	}
}
