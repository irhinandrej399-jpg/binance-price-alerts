package subscription_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/auth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/session"
	store "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5"
)

func getList(h http.Handler, token, foreign string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("GET", "/v1/subscriptions?user_id="+foreign, nil)
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("X-User-ID", foreign)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

type listedSubscription struct {
	ID        string       `json:"id"`
	Pair      string       `json:"pair"`
	Threshold string       `json:"threshold"`
	Direction string       `json:"direction"`
	Percent   *string      `json:"percent"`
	State     domain.State `json:"state"`
	Version   int64        `json:"version"`
	Workflow  *struct {
		TriggerID string `json:"trigger_id"`
		Version   int64  `json:"version"`
	} `json:"workflow"`
	Blocked *struct {
		Reason         string `json:"reason"`
		BindingVersion int64  `json:"binding_version"`
	} `json:"blocked"`
}

func decodeList(t *testing.T, w *httptest.ResponseRecorder) []listedSubscription {
	t.Helper()
	if w.Code != 200 {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Content-Type") != "application/json" {
		t.Fatal(w.Header())
	}
	var response struct {
		Subscriptions []listedSubscription `json:"subscriptions"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Subscriptions == nil {
		t.Fatal("expected array, not null")
	}
	if strings.Contains(w.Body.String(), "owner_id") {
		t.Fatal("internal owner field exposed")
	}
	return response.Subscriptions
}
func TestListHTTP(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	owner, other := quotaOwner(t, pool), quotaOwner(t, pool)
	sessions := session.New(pool, nil)
	a, err := sessions.Issue(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	b, err := sessions.Issue(ctx, other)
	if err != nil {
		t.Fatal(err)
	}
	raw := store.NewListHandler(pool)
	h := auth.New(sessions, raw)
	if got := decodeList(t, getList(h, a.AccessToken, other)); len(got) != 0 {
		t.Fatal(got)
	}
	for _, handler := range []http.Handler{raw, h} {
		w := getList(handler, "", other)
		if w.Code != 401 {
			t.Fatalf("unauthenticated: %d", w.Code)
		}
	}
	if w := getList(h, "forged", other); w.Code != 401 {
		t.Fatalf("forged: %d", w.Code)
	}
	repo := store.New(pool)
	states := []domain.State{domain.Active, domain.PendingDelivery, domain.Sending, domain.DeliveryBlocked, domain.Delivered, domain.Cancelled}
	for i := len(states) - 1; i >= 0; i-- {
		v := quotaRecord(owner, 700000+i)
		v.State = states[i]
		v.Version = 3
		if i != 0 && i != 5 {
			v.Workflow = &store.Workflow{TriggerID: fmt.Sprintf("list-%s-%d", owner, i), Version: 2}
		}
		if i == 3 {
			v.Blocked = &store.Blocked{Reason: domain.ChannelUnavailable, BindingVersion: 4}
			p := "+5"
			v.Percent = &p
			v.Threshold, _ = price.Parse("105")
		}
		if err := repo.Insert(ctx, v); err != nil {
			t.Fatal(err)
		}
	}
	foreign := quotaRecord(other, 710000)
	if err := repo.Insert(ctx, foreign); err != nil {
		t.Fatal(err)
	}
	for repeat := 0; repeat < 2; repeat++ {
		got := decodeList(t, getList(h, a.AccessToken, other))
		if len(got) != 4 {
			t.Fatalf("states: %+v", got)
		}
		for i, v := range got {
			if v.ID != quotaRecord(owner, 700000+i).ID || v.State != states[i] || (i != 3 && v.Threshold != "100") || (i == 3 && v.Threshold != "105") || v.Direction != "up" || v.Pair != "BTCUSDT" || v.Version != 3 {
				t.Fatalf("item %d: %+v", i, v)
			}
			if i > 0 && (v.Workflow == nil || v.Workflow.Version != 2 || v.Workflow.TriggerID != fmt.Sprintf("list-%s-%d", owner, i)) {
				t.Fatalf("workflow: %+v", v)
			}
		}
		blocked := got[3]
		if blocked.Blocked == nil || blocked.Blocked.Reason != "channel_unavailable" || blocked.Blocked.BindingVersion != 4 || blocked.Percent == nil || *blocked.Percent != "+5" {
			t.Fatalf("blocked: %+v", blocked)
		}
	}
	if got := decodeList(t, getList(h, b.AccessToken, owner)); len(got) != 1 || got[0].ID != foreign.ID {
		t.Fatalf("other: %+v", got)
	}
	// Fixture for a future recovery writer: the reader must reflect the new stored
	// state, preserving threshold, percent and workflow rather than restoring itself.
	_, err = pool.Exec(ctx, `UPDATE app.subscriptions SET state='active',version=4,block_reason=NULL,blocked_binding_version=NULL WHERE owner_id=$1 AND state='delivery_blocked'`, owner)
	if err != nil {
		t.Fatal(err)
	}
	got := decodeList(t, getList(h, a.AccessToken, other))[3]
	if got.State != domain.Active || got.Version != 4 || got.Blocked != nil || got.Threshold != "105" || got.Percent == nil || *got.Percent != "+5" || got.Workflow == nil {
		t.Fatalf("recovered: %+v", got)
	}
	// Even malformed legacy data beyond the quota cannot produce an unbounded list.
	for i := 0; i < 12; i++ {
		if err := repo.Insert(ctx, quotaRecord(owner, 720000+i)); err != nil {
			t.Fatal(err)
		}
		if i == 10 {
			if got := decodeList(t, getList(h, a.AccessToken, other)); len(got) != 15 {
				t.Fatalf("at quota: %d", len(got))
			}
		}
	}
	gotAll := decodeList(t, getList(h, a.AccessToken, other))
	if len(gotAll) != 15 {
		t.Fatalf("limit: %d", len(gotAll))
	}
	for i := 1; i < len(gotAll); i++ {
		if gotAll[i-1].ID >= gotAll[i].ID {
			t.Fatal("unstable order")
		}
	}
}

// Inject a failure around real PostgreSQL rows to check that a late read error
// cannot expose a successful partial list or raw storage details.
type listDBFunc func(context.Context, string, ...any) (pgx.Rows, error)

func (f listDBFunc) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return f(ctx, sql, args...)
}

type failedListRows struct{ pgx.Rows }

func (r failedListRows) Err() error { return errors.New("private storage detail") }
func TestListStorageFailure(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	owner := quotaOwner(t, pool)
	if err := store.New(pool).Insert(ctx, quotaRecord(owner, 730000)); err != nil {
		t.Fatal(err)
	}
	sessions := session.New(pool, nil)
	credentials, err := sessions.Issue(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	for _, late := range []bool{false, true} {
		db := listDBFunc(func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
			if !late {
				return nil, errors.New("private storage detail")
			}
			rows, err := pool.Query(ctx, sql, args...)
			if err != nil {
				return nil, err
			}
			return failedListRows{rows}, nil
		})
		w := getList(auth.New(sessions, store.NewListHandler(db)), credentials.AccessToken, owner)
		if w.Code != 500 || strings.TrimSpace(w.Body.String()) != `{"error":{"code":"internal_error"}}` {
			t.Fatalf("failure: %d %s", w.Code, w.Body.String())
		}
	}
}
