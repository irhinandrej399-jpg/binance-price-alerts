package subscription_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/auth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/journal"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/session"
	store "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5"
)

func deleteSubscription(h http.Handler, token, id string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("DELETE", "/v1/subscriptions/"+id+"?user_id=forged", strings.NewReader(`{"user_id":"forged"}`))
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("X-User-ID", "forged")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

type cancelledResponse struct {
	ID       string       `json:"id"`
	State    domain.State `json:"state"`
	Version  int64        `json:"version"`
	InFlight bool         `json:"message_may_be_in_flight"`
}

func decodeCancel(t *testing.T, w *httptest.ResponseRecorder) cancelledResponse {
	t.Helper()
	if w.Code != 200 {
		t.Fatalf("cancel: %d %s", w.Code, w.Body.String())
	}
	if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Content-Type") != "application/json" {
		t.Fatal(w.Header())
	}
	var v cancelledResponse
	if err := json.Unmarshal(w.Body.Bytes(), &v); err != nil {
		t.Fatal(err)
	}
	return v
}
func TestCancelHTTP(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	owner, other := quotaOwner(t, pool), quotaOwner(t, pool)
	sessions := session.New(pool, nil)
	credentials, err := sessions.Issue(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	raw := store.NewCancelHandler(pool)
	h := auth.New(sessions, raw)
	for _, handler := range []http.Handler{raw, h} {
		if w := deleteSubscription(handler, "", quotaRecord(owner, 900001).ID); w.Code != 401 {
			t.Fatal(w.Code)
		}
	}
	foreign := quotaRecord(other, 900000)
	if err := quotaTx(ctx, pool, other, func(q *store.OwnerQuota) error { return q.Create(ctx, foreign) }); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{foreign.ID, quotaRecord(owner, 999999).ID} {
		w := deleteSubscription(h, credentials.AccessToken, id)
		if w.Code != 404 || strings.TrimSpace(w.Body.String()) != `{"error":{"code":"subscription_not_found"}}` {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	assertQuota(t, pool, other, 1)
	if w := deleteSubscription(h, credentials.AccessToken, "bad-id"); w.Code != 400 {
		t.Fatal(w.Code, w.Body.String())
	}
	states := []domain.State{domain.Active, domain.PendingDelivery, domain.Sending, domain.DeliveryBlocked, domain.Delivered, domain.Cancelled, domain.Active}
	for i, state := range states {
		t.Run(fmt.Sprintf("%s-%d", state, i), func(t *testing.T) {
			v := quotaRecord(owner, 900001+i)
			if err := quotaTx(ctx, pool, owner, func(q *store.OwnerQuota) error { return q.Create(ctx, v) }); err != nil {
				t.Fatal(err)
			}
			// Prepare persisted states using the existing writer protocol; admission,
			// blocking and result endpoints belong to their own tickets.
			tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(context.Background())
			q, err := store.LockQuota(ctx, tx, owner)
			if err != nil {
				t.Fatal(err)
			}
			var workflow, reason, binding any
			if i > 0 {
				workflow = "cancel-" + v.ID
			}
			if state == domain.DeliveryBlocked {
				reason = "channel_unavailable"
				binding = int64(1)
			}
			_, err = tx.Exec(ctx, `UPDATE app.subscriptions SET state=$2,version=3,workflow_id=$3,workflow_version=CASE WHEN $3::text IS NULL THEN NULL ELSE 2 END,block_reason=$4,blocked_binding_version=$5 WHERE id=$1`, v.ID, state, workflow, reason, binding)
			if err != nil {
				t.Fatal(err)
			}
			if err = q.ReleaseTerminal(ctx, v.ID); err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			wantState, wantVersion := domain.Cancelled, int64(4)
			if state == domain.Delivered || state == domain.Cancelled {
				wantState = state
				wantVersion = 3
			}
			before, err := journal.Current(ctx, pool)
			if err != nil {
				t.Fatal(err)
			}
			var first string
			for repeat := 0; repeat < 2; repeat++ {
				w := deleteSubscription(h, credentials.AccessToken, v.ID)
				got := decodeCancel(t, w)
				if got.ID != v.ID || got.State != wantState || got.Version != wantVersion || got.InFlight != (i > 0 && state != domain.Delivered) {
					t.Fatalf("got %+v", got)
				}
				if repeat == 0 {
					first = w.Body.String()
				} else if first != w.Body.String() {
					t.Fatal("unstable retry")
				}
				assertQuota(t, pool, owner, 0)
			}
			saved, err := store.New(pool).ByID(ctx, owner, v.ID)
			if err != nil || saved.State != wantState || saved.Version != wantVersion || saved.Blocked != nil || saved.Threshold.String() != "100" {
				t.Fatalf("saved %+v %v", saved, err)
			}
			if i > 0 && (saved.Workflow == nil || saved.Workflow.Version != 2) {
				t.Fatal("lost workflow")
			}

			entries, err := journal.ReadAfter(ctx, pool, before, 10)
			if err != nil {
				t.Fatal(err)
			}
			if state == domain.Delivered || state == domain.Cancelled {
				if len(entries) != 0 {
					t.Fatal("terminal retry emitted event")
				}
			} else {
				if len(entries) != 1 {
					t.Fatalf("cancel journal len=%d", len(entries))
				}
				e := entries[0]
				c := e.Change
				if c.SubscriptionID != saved.ID || c.SubscriptionVersion != uint64(saved.Version) || c.State != saved.State || c.Pair != saved.Pair || c.Threshold.String() != saved.Threshold.String() || c.Direction != saved.Direction || c.Activation != saved.Activation || c.CreationSnapshot.Price.String() != saved.CreationPrice.String() || !c.CreationSnapshot.TradeTime.Equal(saved.CreationTradeTime) || c.Position != before+1 || c.TransactionEnd != before+1 {
					t.Fatalf("cancel after-image differs: %+v vs %+v", c, saved)
				}
				if (c.Workflow == nil) != (saved.Workflow == nil) {
					t.Fatal("workflow presence")
				}
				if saved.Workflow != nil && (c.Workflow.TriggerID != saved.Workflow.TriggerID || c.Workflow.Version != uint64(saved.Workflow.Version)) {
					t.Fatal("workflow differs")
				}
				if e.Metadata.EventID == "" || e.Metadata.CorrelationID != e.Metadata.EventID || e.Metadata.OrderingKey != saved.ID || e.Metadata.OccurredAt.IsZero() {
					t.Fatal(e.Metadata)
				}
				replay, err := journal.ReadAfter(ctx, pool, before, 10)
				if err != nil || !reflect.DeepEqual(entries, replay) {
					t.Fatal("unstable replay", err)
				}
				var first, second []byte
				if err := pool.QueryRow(ctx, `SELECT body FROM app.api_journal WHERE event_id=$1`, e.Metadata.EventID).Scan(&first); err != nil {
					t.Fatal(err)
				}
				if err := pool.QueryRow(ctx, `SELECT body FROM app.api_journal WHERE event_id=$1`, e.Metadata.EventID).Scan(&second); err != nil || !bytes.Equal(first, second) {
					t.Fatal("unstable bytes", err)
				}
			}
			// Late-result slot reconciliation cannot free a second slot.
			if err := quotaTx(ctx, pool, owner, func(q *store.OwnerQuota) error { return q.ReleaseTerminal(ctx, v.ID) }); err != nil {
				t.Fatal(err)
			}
			assertQuota(t, pool, owner, 0)
		})
	}
}
func TestCancelConcurrentAndRollback(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	owner := quotaOwner(t, pool)
	v := quotaRecord(owner, 910001)
	if err := quotaTx(ctx, pool, owner, func(q *store.OwnerQuota) error { return q.Create(ctx, v) }); err != nil {
		t.Fatal(err)
	}
	sessions := session.New(pool, nil)
	credentials, err := sessions.Issue(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	before, err := journal.Current(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	failing := auth.New(sessions, store.NewCancelHandler(failedCommitDB{pool}))
	w := deleteSubscription(failing, credentials.AccessToken, v.ID)
	if w.Code != 500 || strings.Contains(w.Body.String(), "private") {
		t.Fatal(w.Code, w.Body.String())
	}
	assertQuota(t, pool, owner, 1)
	saved, err := store.New(pool).ByID(ctx, owner, v.ID)
	if err != nil || saved.State != domain.Active || saved.Version != 1 {
		t.Fatal(saved, err)
	}
	if c, err := journal.Current(ctx, pool); err != nil || c != before {
		t.Fatal("commit failure advanced C", c, err)
	}
	h := auth.New(sessions, store.NewCancelHandler(pool))
	start := make(chan struct{})
	results := make(chan *httptest.ResponseRecorder, 20)
	for i := 0; i < 20; i++ {
		go func() { <-start; results <- deleteSubscription(h, credentials.AccessToken, v.ID) }()
	}
	close(start)
	for i := 0; i < 20; i++ {
		got := decodeCancel(t, <-results)
		if got.State != domain.Cancelled || got.Version != 2 || got.InFlight {
			t.Fatal(got)
		}
	}
	assertQuota(t, pool, owner, 0)
	entries, err := journal.ReadAfter(ctx, pool, before, 10)
	if err != nil || len(entries) != 1 || entries[0].Change.SubscriptionID != v.ID || entries[0].Change.SubscriptionVersion != 2 {
		t.Fatalf("concurrent journal: %+v %v", entries, err)
	}
	// Freed slot is usable, without a Telegram requirement for cancellation itself.
	if err := quotaTx(ctx, pool, owner, func(q *store.OwnerQuota) error { return q.Create(ctx, quotaRecord(owner, 910002)) }); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=NULL WHERE id=$1`, owner); err != nil {
		t.Fatal(err)
	}
	decodeCancel(t, deleteSubscription(h, credentials.AccessToken, quotaRecord(owner, 910002).ID))
	assertQuota(t, pool, owner, 0)
	// Overflow must roll back, never wrap a version or release the slot.
	max := quotaRecord(owner, 910003)
	max.Version = 9223372036854775807
	if _, err := pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=123456789 WHERE id=$1`, owner); err != nil {
		t.Fatal(err)
	}
	initial := max
	initial.Version = 1
	if err := quotaTx(ctx, pool, owner, func(q *store.OwnerQuota) error { return q.Create(ctx, initial) }); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE app.subscriptions SET version=$2 WHERE id=$1`, max.ID, max.Version); err != nil {
		t.Fatal(err)
	}
	w = deleteSubscription(h, credentials.AccessToken, max.ID)
	if w.Code != 500 {
		t.Fatal(fmt.Sprint(w.Code), w.Body.String())
	}
	saved, err = store.New(pool).ByID(ctx, owner, max.ID)
	if err != nil || saved.State != domain.Active || saved.Version != max.Version {
		t.Fatal(saved, err)
	}
	assertQuota(t, pool, owner, 1)
}

func TestCancelWaitsForNewVersion(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	owner := quotaOwner(t, pool)
	v := quotaRecord(owner, 920001)
	if err := quotaTx(ctx, pool, owner, func(q *store.OwnerQuota) error { return q.Create(ctx, v) }); err != nil {
		t.Fatal(err)
	}
	sessions := session.New(pool, nil)
	credentials, err := sessions.Issue(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	h := auth.New(sessions, store.NewCancelHandler(pool))
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = store.LockQuota(ctx, tx, owner); err != nil {
		t.Fatal(err)
	}
	// Fixture of admission winning the user lock: cancellation must observe this
	// commit and bump version 8, never overwrite it with stale version 2.
	if _, err = tx.Exec(ctx, `UPDATE app.subscriptions SET state='sending',version=8,workflow_id=$2,workflow_version=2 WHERE id=$1`, v.ID, "admit-"+v.ID); err != nil {
		t.Fatal(err)
	}
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() { result <- deleteSubscription(h, credentials.AccessToken, v.ID) }()
	for {
		var waiting bool
		err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT id::text FROM app.users%')`).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case <-time.After(5 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case w := <-result:
		got := decodeCancel(t, w)
		if got.Version != 9 || got.State != domain.Cancelled || !got.InFlight {
			t.Fatal(got)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	assertQuota(t, pool, owner, 0)
	// Cancellation winning the lock leaves a terminal canonical record: future
	// admission's state/version predicate cannot authorize a subsequent attempt.
	tag, err := pool.Exec(ctx, `UPDATE app.subscriptions SET state='sending',version=version+1 WHERE id=$1 AND state IN ('active','pending_delivery') AND version=8`, v.ID)
	if err != nil || tag.RowsAffected() != 0 {
		t.Fatal(tag, err)
	}
}
