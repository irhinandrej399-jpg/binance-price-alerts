package subscription_test

import (
	"context"
	"errors"
	"testing"

	store "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/latency"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
)

func TestDeliveryCommitObservations(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	owner := quotaOwner(t, pool)
	for i, mode := range []string{"success", "rollback", "unknown", "sink_failure"} {
		t.Run(mode, func(t *testing.T) {
			v := quotaRecord(owner, 945100+i)
			if e := quotaTx(ctx, pool, owner, func(q *store.OwnerQuota) error { return q.Create(ctx, v) }); e != nil {
				t.Fatal(e)
			}
			var observations []latency.Observation
			r, e := latency.New(func() (latency.Stamp, error) { return latency.Stamp{Scale: "test", Nanos: 200000100}, nil }, func(o latency.Observation) error {
				// Independent connection proves observation is after committed outbox visibility.
				var n int
				if e := pool.QueryRow(ctx, `SELECT count(*) FROM app.delivery_outbox WHERE subscription_id=$1 AND trigger_id=$2 AND event_id=$3`, v.ID, o.TriggerID, o.EventID).Scan(&n); e != nil || n != 1 {
					t.Fatalf("before commit: %d %v", n, e)
				}
				observations = append(observations, o)
				if mode == "sink_failure" {
					return errors.New("sink lost")
				}
				return nil
			})
			if e != nil {
				t.Fatal(e)
			}
			var db store.Beginner = pool
			if mode == "rollback" {
				db = failedCommitDB{pool}
			}
			if mode == "unknown" {
				db = lostDeliveryCommitDB{pool}
			}
			got, e := store.RequestDeliveryObserved(ctx, db, deliveryCandidate(v), r)
			if mode == "rollback" || mode == "unknown" {
				if e == nil || len(observations) != 0 {
					t.Fatal("failure became confirmation", e, observations)
				}
			} else {
				if e != nil || len(observations) != 1 {
					t.Fatal(got, e, observations)
				}
				o := observations[0]
				if o.Kind != latency.WorkflowCommitted || o.SubscriptionID != v.ID || o.OwnerID != owner || o.TriggerID != got.Workflow.TriggerID || o.Revision != deliveryCandidate(v).Quote.Revision || o.At.Nanos != 200000100 {
					t.Fatal(o)
				}
			}
			n, _ := r.Failures()
			if (mode == "sink_failure") != (n == 1) {
				t.Fatal("failure hidden", n)
			}
			before := len(observations)
			if _, e = store.RequestDeliveryObserved(ctx, pool, deliveryCandidate(v), r); e != nil {
				t.Fatal(e)
			}
			want := before
			if mode == "rollback" {
				want++
			}
			if len(observations) != want {
				t.Fatal("retry fabricated commit", observations)
			}
		})
	}
}
