package resultrpc_test

import (
	"context"
	"fmt"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/auth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/journal"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/resultrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/session"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

var now = time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
var fixtureSeq atomic.Int64

func fixture(t *testing.T, pool *pgxpool.Pool) sub.Record {
	t.Helper()
	ctx := context.Background()
	a, e := account.New(pool).Create(ctx, fmt.Sprintf("result-rpc-%d-%d@example.com", time.Now().UnixNano(), fixtureSeq.Add(1)), "hash")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=$2,telegram_version=1 WHERE id=$1`, a.ID, time.Now().UnixNano()); e != nil {
		t.Fatal(e)
	}
	r := record()
	r.Pair = "BTCUSDT"
	r.Threshold, _ = price.Parse("100")
	r.Direction = price.Up
	r.OwnerID = a.ID
	r.ID = fmt.Sprintf("00000000-0000-4000-8000-%012d", 9970000+fixtureSeq.Add(1))
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
func admitted(t *testing.T, pool *pgxpool.Pool) (sub.Record, sub.AdmissionResult, *pb.SaveDeliveryResultRequest) {
	t.Helper()
	r := fixture(t, pool)
	a, e := sub.AdmitDelivery(context.Background(), pool, sub.AdmissionCommand{OwnerID: r.OwnerID, SubscriptionID: r.ID, WorkflowID: r.Workflow.TriggerID, RequestID: "permit", PermitTTL: time.Microsecond, Quote: observed("101")})
	if e != nil || a.Attempt == nil {
		t.Fatal(a, e)
	}
	// Results intentionally arrive after this short permit has expired.
	if !time.Now().After(a.Attempt.ExpiresAt) {
		t.Fatal("fixture permit has not expired")
	}
	return r, a, &pb.SaveDeliveryResultRequest{OwnerId: r.OwnerID, SubscriptionId: r.ID, WorkflowId: r.Workflow.TriggerID, AttemptId: a.Attempt.ID, Outcome: "succeeded", MessageId: 123}
}
func assertSaved(t *testing.T, pool *pgxpool.Pool, r sub.Record, want domain.State, audits, slots int) {
	t.Helper()
	ctx := context.Background()
	v, e := sub.New(pool).ByID(ctx, r.OwnerID, r.ID)
	if e != nil || v.State != want || v.Workflow.TriggerID != r.Workflow.TriggerID || v.Threshold.Cmp(r.Threshold) != 0 {
		t.Fatal(v, e)
	}
	var n int
	if e = pool.QueryRow(ctx, `SELECT count(*) FROM app.delivery_results WHERE subscription_id=$1`, r.ID).Scan(&n); e != nil || n != audits {
		t.Fatal(n, e)
	}
	if e = pool.QueryRow(ctx, `SELECT active_subscriptions FROM app.users WHERE id=$1`, r.OwnerID).Scan(&n); e != nil || n != slots {
		t.Fatal(n, e)
	}
}
func TestPostgresResultsOverTLS(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	client := resultrpc.NewClient(connect(t, resultrpc.NewServer(resultrpc.NewPostgres(pool)), notificationsURI))
	for _, outcome := range []string{"succeeded", "retryable", "uncertain", "blocked"} {
		t.Run(outcome, func(t *testing.T) {
			r, a, req := admitted(t, pool)
			req.Outcome = outcome
			want := domain.Delivered
			slots := 0
			if outcome != "succeeded" {
				req.MessageId = 0
				want = domain.PendingDelivery
				slots = 1
			}
			if outcome == "blocked" {
				req.BlockReason = "permanent_failure"
				want = domain.DeliveryBlocked
			}
			before, e := journal.Current(ctx, pool)
			if e != nil {
				t.Fatal(e)
			}
			first, e := client.SaveDeliveryResult(ctx, req)
			if e != nil || first.State != string(want) || first.SubscriptionVersion != a.Record.Version+1 {
				t.Fatal(first, e)
			}
			if outcome == "blocked" && (first.BlockReason != "permanent_failure" || first.BlockedBindingVersion != 1) {
				t.Fatal(first)
			}
			// Concurrent repetitions acknowledge one durable observation and one transition.
			errs := make(chan error, 10)
			for i := 0; i < 10; i++ {
				go func() {
					v, e := client.SaveDeliveryResult(ctx, req)
					if e == nil && !proto.Equal(v, first) {
						e = fmt.Errorf("changed replay: %v", v)
					}
					errs <- e
				}()
			}
			for i := 0; i < 10; i++ {
				if e := <-errs; e != nil {
					t.Fatal(e)
				}
			}
			assertSaved(t, pool, r, want, 1, slots)
			if after, e := journal.Current(ctx, pool); e != nil || after != before+1 {
				t.Fatal(before, after, e)
			}
			if outcome == "succeeded" {
				var message int64
				if e = pool.QueryRow(ctx, `SELECT delivered_message_id FROM app.subscriptions WHERE id=$1`, r.ID).Scan(&message); e != nil || message != 123 {
					t.Fatal(message, e)
				}
				conflict := proto.Clone(req).(*pb.SaveDeliveryResultRequest)
				conflict.MessageId = 456
				if _, e = client.SaveDeliveryResult(ctx, conflict); status.Code(e) != codes.AlreadyExists {
					t.Fatal(e)
				}
			}
			for _, field := range []string{"attempt", "workflow", "owner", "subscription"} {
				bad := proto.Clone(req).(*pb.SaveDeliveryResultRequest)
				wantCode := codes.FailedPrecondition
				switch field {
				case "attempt":
					bad.AttemptId = attemptID
				case "workflow":
					bad.WorkflowId = workflowID
				case "owner":
					bad.OwnerId = ownerID
					wantCode = codes.NotFound
				case "subscription":
					bad.SubscriptionId = subID
					wantCode = codes.NotFound
				}
				if _, e = client.SaveDeliveryResult(ctx, bad); status.Code(e) != wantCode {
					t.Fatal(field, e)
				}
			}
			assertSaved(t, pool, r, want, 1, slots)
		})
	}
}
func TestPostgresLateSuccessAfterCancellation(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	r, _, req := admitted(t, pool)
	sessions := session.New(pool, nil)
	cred, e := sessions.Issue(ctx, r.OwnerID)
	if e != nil {
		t.Fatal(e)
	}
	hreq := httptest.NewRequest(http.MethodDelete, "/v1/subscriptions/"+r.ID, nil)
	hreq.Header.Set("Authorization", "Bearer "+cred.AccessToken)
	w := httptest.NewRecorder()
	auth.New(sessions, sub.NewCancelHandler(pool)).ServeHTTP(w, hreq)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	before, e := journal.Current(ctx, pool)
	if e != nil {
		t.Fatal(e)
	}
	client := resultrpc.NewClient(connect(t, resultrpc.NewServer(resultrpc.NewPostgres(pool)), notificationsURI))
	for i := 0; i < 2; i++ {
		v, e := client.SaveDeliveryResult(ctx, req)
		if e != nil || v.State != "cancelled" {
			t.Fatal(v, e)
		}
	}
	assertSaved(t, pool, r, domain.Cancelled, 1, 0)
	var message int64
	if e = pool.QueryRow(ctx, `SELECT message_id FROM app.delivery_results WHERE attempt_id=$1`, req.AttemptId).Scan(&message); e != nil || message != 123 {
		t.Fatal(message, e)
	}
	if after, e := journal.Current(ctx, pool); e != nil || after != before {
		t.Fatal(before, after, e)
	}
}
func TestPostgresTimeoutAfterCommit(t *testing.T) {
	pool := postgrestest.Open(t)
	r, _, req := admitted(t, pool)
	committed := make(chan struct{})
	finished := make(chan struct{})
	var calls atomic.Int32
	real := resultrpc.NewPostgres(pool)
	server := resultrpc.NewServer(storeFunc(func(ctx context.Context, c sub.DeliveryResultCommand) (sub.Record, error) {
		v, e := real.SaveDeliveryResult(ctx, c)
		if calls.Add(1) == 1 {
			defer close(finished)
			if e == nil {
				close(committed)
				<-ctx.Done()
			}
		}
		return v, e
	}))
	client := resultrpc.NewClient(connect(t, server, notificationsURI))
	before, e := journal.Current(context.Background(), pool)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { _, e := client.SaveDeliveryResult(ctx, req); result <- e }()
	select {
	case <-committed:
	case <-ctx.Done():
		t.Fatal("did not commit before deadline")
	}
	if e := <-result; status.Code(e) != codes.DeadlineExceeded {
		t.Fatal(e)
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("server did not finish")
	}
	assertSaved(t, pool, r, domain.Delivered, 1, 0)
	// Only the API result is retried; no new admission or external send is involved.
	v, e := client.SaveDeliveryResult(context.Background(), req)
	if e != nil || v.State != "delivered" || calls.Load() != 2 {
		t.Fatal(v, e, calls.Load())
	}
	assertSaved(t, pool, r, domain.Delivered, 1, 0)
	if after, e := journal.Current(context.Background(), pool); e != nil || after != before+1 {
		t.Fatal(before, after, e)
	}
}
