package admissionrpc_test

import (
	"context"
	"fmt"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/auth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/session"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/admissionrpc"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/latest"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/storage"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var fixtureSeq atomic.Int64

func fixture(t *testing.T, pool *pgxpool.Pool) sub.Record {
	t.Helper()
	ctx := context.Background()
	a, e := account.New(pool).Create(ctx, fmt.Sprintf("admission-rpc-%d-%d@example.com", time.Now().UnixNano(), fixtureSeq.Add(1)), "hash")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=$2,telegram_version=1 WHERE id=$1`, a.ID, time.Now().UnixNano()); e != nil {
		t.Fatal(e)
	}
	r := record()
	r.OwnerID = a.ID
	r.ID = fmt.Sprintf("00000000-0000-4000-8000-%012d", 9980000+fixtureSeq.Add(1))
	r.State = domain.Active
	r.Version = 1
	r.Workflow = nil
	r.CreationPrice = r.Threshold
	r.CreationTradeTime = now
	r.Activation = sub.LatestPrice
	tx, e := pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	quota, e := sub.LockQuota(ctx, tx, a.ID)
	if e != nil {
		t.Fatal(e)
	}
	if e = quota.Create(ctx, r); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	candidate := observed("101")
	r, e = sub.RequestDelivery(ctx, pool, sub.DeliveryCandidate{OwnerID: r.OwnerID, SubscriptionID: r.ID, ExpectedVersion: 1, Quote: candidate})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if _, e := pool.Exec(ctx, `DELETE FROM app.subscriptions WHERE owner_id=$1`, a.ID); e != nil {
			t.Error(e)
		}
		if _, e := pool.Exec(ctx, `DELETE FROM app.users WHERE id=$1`, a.ID); e != nil {
			t.Error(e)
		}
	})
	return r
}
func observed(value string) priceobserved.Observed {
	p, _ := price.Parse(value)
	return priceobserved.Observed{SchemaVersion: 1, Pair: "BTCUSDT", Price: &p, SourceID: "checked-source", Revision: priceobserved.Revision{StreamID: "canonical", Generation: 7, Sequence: 42}, TradeTime: now.Add(-time.Hour), ReceivedAt: now.Add(-time.Minute), StreamState: priceobserved.Unconfirmed, GapDetected: true}
}
func assertCount(t *testing.T, pool *pgxpool.Pool, r sub.Record, want int) {
	t.Helper()
	var count int
	if e := pool.QueryRow(context.Background(), `SELECT count(*) FROM app.delivery_admissions WHERE subscription_id=$1`, r.ID).Scan(&count); e != nil || count != want {
		t.Fatal(count, e)
	}
}
func assertState(t *testing.T, pool *pgxpool.Pool, r sub.Record, want domain.State) {
	t.Helper()
	v, e := sub.New(pool).ByID(context.Background(), r.OwnerID, r.ID)
	if e != nil || v.State != want || v.Threshold.Cmp(r.Threshold) != 0 || v.Workflow.TriggerID != r.Workflow.TriggerID {
		t.Fatal(v, e)
	}
}
func TestPostgresAdmissionOverTLS(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	var value, calls atomic.Int64
	value.Store(99)
	var missing atomic.Bool
	conn := marketConnection(t, reader(func(ctx context.Context, pair string) (priceobserved.Observed, error) {
		calls.Add(1)
		if missing.Load() {
			return priceobserved.Observed{}, storage.ErrNotFound
		}
		return observed(fmt.Sprint(value.Load())), nil
	}))
	api := connect(t, newServer(admissionrpc.NewPostgres(pool), latest.NewClient(conn), func() time.Time { return now }), notificationsURI)
	client := admissionrpc.NewClient(api)
	r := fixture(t, pool)
	call := func(id string) (*pb.AdmitDeliveryResponse, error) {
		return client.AdmitDelivery(ctx, r.OwnerID, r.ID, r.Workflow.TriggerID, id)
	}
	// Candidate was 101; latest 99 must return to active without sending.
	wait, e := call("false")
	if e != nil || wait.Outcome != "wait_condition" || wait.Attempt != nil {
		t.Fatal(wait, e)
	}
	assertState(t, pool, r, domain.Active)
	missing.Store(true)
	dup, e := call("false")
	if e != nil || !proto.Equal(dup, wait) {
		t.Fatal(dup, e)
	}
	if _, e = call("unavailable"); status.Code(e) != codes.Unavailable {
		t.Fatal(e)
	}
	assertCount(t, pool, r, 1)
	missing.Store(false)
	value.Store(101)
	results := make(chan *pb.AdmitDeliveryResponse, 10)
	errs := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func() { v, e := call("permit"); results <- v; errs <- e }()
	}
	var first *pb.AdmitDeliveryResponse
	for i := 0; i < 10; i++ {
		v := <-results
		if e := <-errs; e != nil {
			t.Fatal(e)
		}
		if first == nil {
			first = v
		}
		if !proto.Equal(v, first) {
			t.Fatal(v, first)
		}
	}
	if first.Outcome != "admitted" || first.Attempt == nil || first.Attempt.Fence != 1 || first.Attempt.BindingVersion != 1 || first.Attempt.Quote.Price != "101" || first.Attempt.Quote.SourceId != "checked-source" || first.Attempt.Quote.Revision.Generation != 7 || first.Attempt.Quote.TradeTime != now.Add(-time.Hour).Format(time.RFC3339Nano) || !first.Attempt.Quote.GapDetected {
		t.Fatal(first)
	}
	assertCount(t, pool, r, 2)
	assertState(t, pool, r, domain.Sending)
	missing.Store(true)
	before := calls.Load()
	busy, e := call("other")
	if e != nil || busy.Outcome != "attempt_busy" || busy.Attempt != nil || calls.Load() != before {
		t.Fatal(busy, e)
	}
	if _, e = client.AdmitDelivery(ctx, ownerID, r.ID, r.Workflow.TriggerID, "foreign"); status.Code(e) != codes.NotFound {
		t.Fatal(e)
	}
	if _, e = client.AdmitDelivery(ctx, r.OwnerID, r.ID, workflowID, "foreign-workflow"); status.Code(e) != codes.FailedPrecondition {
		t.Fatal(e)
	}
	// Cancel after commit: replay stays historical, a new command is terminal,
	// both even if price is unavailable. No revival and no extra permission.
	sessions := session.New(pool, nil)
	cred, e := sessions.Issue(ctx, r.OwnerID)
	if e != nil {
		t.Fatal(e)
	}
	req := httptest.NewRequest(http.MethodDelete, "/v1/subscriptions/"+r.ID, nil)
	req.Header.Set("Authorization", "Bearer "+cred.AccessToken)
	w := httptest.NewRecorder()
	auth.New(sessions, sub.NewCancelHandler(pool)).ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	replay, e := call("permit")
	if e != nil || !proto.Equal(replay, first) {
		t.Fatal(replay, e)
	}
	terminal, e := call("terminal")
	if e != nil || terminal.Outcome != "terminal" || terminal.Attempt != nil || calls.Load() != before {
		t.Fatal(terminal, e)
	}
}

func TestPostgresBlockedAndRebinding(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	for _, mode := range []string{"true", "false", "unavailable", "race", "race_true"} {
		t.Run(mode, func(t *testing.T) {
			r := fixture(t, pool)
			if _, e := pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=NULL WHERE id=$1`, r.OwnerID); e != nil {
				t.Fatal(e)
			}
			var calls atomic.Int64
			var recovered atomic.Bool
			var unavailable atomic.Bool
			unavailable.Store(mode == "unavailable")
			conn := marketConnection(t, reader(func(ctx context.Context, _ string) (priceobserved.Observed, error) {
				n := calls.Add(1)
				if !recovered.Load() {
					t.Error("unchanged blocked channel read price")
				}
				if unavailable.Load() {
					return priceobserved.Observed{}, storage.ErrNotFound
				}
				if (mode == "race" || mode == "race_true") && n == 1 { // Also proves no user lock is held during market I/O.
					if _, e := pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=789,telegram_version=3 WHERE id=$1`, r.OwnerID); e != nil {
						return priceobserved.Observed{}, e
					}
					return observed("101"), nil
				}
				if mode == "false" || mode == "race" {
					return observed("99"), nil
				}
				return observed("101"), nil
			}))
			api := connect(t, newServer(admissionrpc.NewPostgres(pool), latest.NewClient(conn), func() time.Time { return now }), notificationsURI)
			client := admissionrpc.NewClient(api)
			call := func(id string) (*pb.AdmitDeliveryResponse, error) {
				return client.AdmitDelivery(ctx, r.OwnerID, r.ID, r.Workflow.TriggerID, id)
			}
			blocked, e := call("missing")
			if e != nil || blocked.Outcome != "blocked" || blocked.BlockReason != string(domain.ChannelUnavailable) || blocked.BlockedBindingVersion != 1 || calls.Load() != 0 {
				t.Fatal(blocked, e)
			}
			if _, e = pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=$2 WHERE id=$1`, r.OwnerID, time.Now().UnixNano()); e != nil {
				t.Fatal(e)
			}
			same, e := call("same-version")
			if e != nil || same.Outcome != "blocked" || calls.Load() != 0 {
				t.Fatal(same, e)
			}
			if _, e = pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=$2,telegram_version=2 WHERE id=$1`, r.OwnerID, time.Now().UnixNano()); e != nil {
				t.Fatal(e)
			}
			recovered.Store(true)
			v, e := call("recover")
			if mode == "unavailable" {
				if status.Code(e) != codes.Unavailable {
					t.Fatal(v, e)
				}
				assertState(t, pool, r, domain.DeliveryBlocked)
				assertCount(t, pool, r, 2)
				unavailable.Store(false)
				v, e = call("recover")
				if e != nil || v.Attempt == nil || v.Attempt.BindingVersion != 2 {
					t.Fatal(v, e)
				}
				assertCount(t, pool, r, 3)
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			if mode == "true" || mode == "race_true" {
				wantBinding := int64(2)
				if mode == "race_true" {
					wantBinding = 3
				}
				if v.Outcome != "admitted" || v.Attempt == nil || v.Attempt.BindingVersion != wantBinding || (mode == "race_true" && v.Attempt.ChatId != 789) {
					t.Fatal(v)
				}
				assertState(t, pool, r, domain.Sending)
			} else {
				if v.Outcome != "wait_condition" || v.Attempt != nil {
					t.Fatal(v)
				}
				assertState(t, pool, r, domain.Active)
			}
			if (mode == "race" || mode == "race_true") && calls.Load() != 2 {
				t.Fatal("price not reread", calls.Load())
			}
			assertCount(t, pool, r, 3)
		})
	}
}

type lostReplyDB struct {
	*pgxpool.Pool
	lost atomic.Bool
}
type lostReplyTx struct {
	pgx.Tx
	db *lostReplyDB
}

func (db *lostReplyDB) BeginTx(ctx context.Context, o pgx.TxOptions) (pgx.Tx, error) {
	tx, e := db.Pool.BeginTx(ctx, o)
	return &lostReplyTx{tx, db}, e
}
func (tx *lostReplyTx) Commit(ctx context.Context) error {
	if e := tx.Tx.Commit(ctx); e != nil {
		return e
	}
	if tx.db.lost.CompareAndSwap(false, true) {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}
func TestPostgresTimeoutAfterCommitAndLockWait(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	r := fixture(t, pool)
	var missing atomic.Bool
	conn := marketConnection(t, reader(func(context.Context, string) (priceobserved.Observed, error) {
		if missing.Load() {
			return priceobserved.Observed{}, storage.ErrNotFound
		}
		return observed("101"), nil
	}))
	db := &lostReplyDB{Pool: pool}
	api := connect(t, newServer(admissionrpc.NewPostgres(db), latest.NewClient(conn), func() time.Time { return now }), notificationsURI)
	client := admissionrpc.NewClient(api)
	short, stop := context.WithTimeout(ctx, 300*time.Millisecond)
	defer stop()
	if _, e := client.AdmitDelivery(short, r.OwnerID, r.ID, r.Workflow.TriggerID, "lost"); status.Code(e) != codes.DeadlineExceeded {
		t.Fatal(e)
	}
	if !db.lost.Load() {
		t.Fatal("did not commit before timeout")
	}
	assertCount(t, pool, r, 1)
	missing.Store(true)
	v, e := client.AdmitDelivery(ctx, r.OwnerID, r.ID, r.Workflow.TriggerID, "lost")
	if e != nil || v.Attempt == nil || v.Attempt.Fence != 1 {
		t.Fatal(v, e)
	}
	assertCount(t, pool, r, 1)
	// Real SQL lock wait must obey the incoming deadline, without a saved result.
	waiting := fixture(t, pool)
	tx, e := pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	if _, e = sub.LockQuota(ctx, tx, waiting.OwnerID); e != nil {
		t.Fatal(e)
	}
	short2, stop2 := context.WithTimeout(ctx, 150*time.Millisecond)
	defer stop2()
	if _, e = client.AdmitDelivery(short2, waiting.OwnerID, waiting.ID, waiting.Workflow.TriggerID, "lock"); status.Code(e) != codes.DeadlineExceeded {
		t.Fatal(e)
	}
	if e = tx.Rollback(ctx); e != nil {
		t.Fatal(e)
	}
	assertCount(t, pool, waiting, 0)
	missing.Store(false)
	if v, e = client.AdmitDelivery(ctx, waiting.OwnerID, waiting.ID, waiting.Workflow.TriggerID, "lock"); e != nil || v.Attempt == nil {
		t.Fatal(v, e)
	}
}

func TestPostgresCancellationDuringQuote(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	r := fixture(t, pool)
	sessions := session.New(pool, nil)
	cred, e := sessions.Issue(ctx, r.OwnerID)
	if e != nil {
		t.Fatal(e)
	}
	conn := marketConnection(t, reader(func(context.Context, string) (priceobserved.Observed, error) {
		req := httptest.NewRequest(http.MethodDelete, "/v1/subscriptions/"+r.ID, nil)
		req.Header.Set("Authorization", "Bearer "+cred.AccessToken)
		w := httptest.NewRecorder()
		auth.New(sessions, sub.NewCancelHandler(pool)).ServeHTTP(w, req)
		if w.Code != 200 {
			t.Error(w.Code)
		}
		return observed("101"), nil
	}))
	api := connect(t, newServer(admissionrpc.NewPostgres(pool), latest.NewClient(conn), func() time.Time { return now }), notificationsURI)
	v, e := admissionrpc.NewClient(api).AdmitDelivery(ctx, r.OwnerID, r.ID, r.Workflow.TriggerID, "cancel-during-price")
	if e != nil || v.Outcome != "terminal" || v.Attempt != nil {
		t.Fatal(v, e)
	}
	assertState(t, pool, r, domain.Cancelled)
	assertCount(t, pool, r, 1)
}
