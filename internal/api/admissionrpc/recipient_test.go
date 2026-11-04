package admissionrpc_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/admissionrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/auth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/session"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/latest"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/storage"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestPostgresRecipientProtocol(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	for _, mode := range []string{"first", "rebind_before", "rebind_during", "chat_only_change", "missing", "cancel", "false", "unavailable", "commit_timeout"} {
		t.Run(mode, func(t *testing.T) {
			r := fixture(t, pool)
			var calls, sends atomic.Int64
			httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { sends.Add(1); w.WriteHeader(200) }))
			defer httpServer.Close()
			var change, unavailable atomic.Bool
			conn := marketConnection(t, reader(func(context.Context, string) (priceobserved.Observed, error) {
				calls.Add(1)
				if change.CompareAndSwap(true, false) {
					if _, e := pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=789,telegram_version=2 WHERE id=$1`, r.OwnerID); e != nil {
						return priceobserved.Observed{}, e
					}
				}
				if unavailable.Load() {
					return priceobserved.Observed{}, storage.ErrNotFound
				}
				if mode == "false" {
					return observed("99"), nil
				}
				return observed("101"), nil
			}))
			var db sub.Beginner = pool
			var lost *lostReplyDB
			if mode == "commit_timeout" {
				lost = &lostReplyDB{Pool: pool}
				db = lost
			}
			client := admissionrpc.NewClient(connect(t, newServer(admissionrpc.NewPostgres(db), latest.NewClient(conn), func() time.Time { return now }), notificationsURI))
			prepare := func() *pb.PrepareRecipientResponse {
				t.Helper()
				v, e := client.PrepareRecipient(ctx, r.OwnerID, r.ID, r.Workflow.TriggerID)
				if e != nil {
					t.Fatal(e)
				}
				return v
			}
			prepared := prepare()
			if prepared.Outcome != "recipient_ready" || prepared.Recipient == nil || prepared.Recipient.BindingVersion != 1 {
				t.Fatal(prepared)
			}
			assertCount(t, pool, r, 0)
			assertState(t, pool, r, domain.PendingDelivery)
			if calls.Load() != 0 || sends.Load() != 0 {
				t.Fatal("preparation performed external I/O")
			}
			if _, e := client.PrepareRecipient(ctx, ownerID, r.ID, r.Workflow.TriggerID); status.Code(e) != codes.NotFound {
				t.Fatal(e)
			}
			if _, e := client.PrepareRecipient(ctx, r.OwnerID, r.ID, workflowID); status.Code(e) != codes.FailedPrecondition {
				t.Fatal(e)
			}
			switch mode {
			case "rebind_before":
				if _, e := pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=789,telegram_version=2 WHERE id=$1`, r.OwnerID); e != nil {
					t.Fatal(e)
				}
			case "chat_only_change":
				if _, e := pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=789 WHERE id=$1`, r.OwnerID); e != nil {
					t.Fatal(e)
				}
			case "rebind_during":
				change.Store(true)
			case "missing":
				if _, e := pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=NULL,telegram_version=2 WHERE id=$1`, r.OwnerID); e != nil {
					t.Fatal(e)
				}
				if p := prepare(); p.Outcome != "missing_channel" || p.Recipient != nil {
					t.Fatal(p)
				}
				assertCount(t, pool, r, 0)
			case "cancel":
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
					t.Fatal(w.Code)
				}
				if p := prepare(); p.Outcome != "terminal" || p.Recipient != nil {
					t.Fatal(p)
				}
			case "unavailable":
				unavailable.Store(true)
			}
			callctx := ctx
			if mode == "commit_timeout" {
				var cancel context.CancelFunc
				callctx, cancel = context.WithTimeout(ctx, 300*time.Millisecond)
				defer cancel()
			}
			admitted, e := client.AdmitDelivery(callctx, r.OwnerID, r.ID, r.Workflow.TriggerID, "limited-chat", prepared.Recipient)
			switch mode {
			case "rebind_before", "rebind_during", "chat_only_change":
				if status.Code(e) != codes.Aborted || admitted != nil {
					t.Fatal(admitted, e)
				}
				assertCount(t, pool, r, 0)
				assertState(t, pool, r, domain.PendingDelivery)
				next := prepare()
				if next.Recipient == nil || next.Recipient.ChatId != 789 {
					t.Fatal(next)
				}
				admitted, e = client.AdmitDelivery(ctx, r.OwnerID, r.ID, r.Workflow.TriggerID, "new-limiter-cycle", next.Recipient)
			case "unavailable":
				if status.Code(e) != codes.Unavailable {
					t.Fatal(admitted, e)
				}
				assertCount(t, pool, r, 0)
				assertState(t, pool, r, domain.PendingDelivery)
				unavailable.Store(false)
				admitted, e = client.AdmitDelivery(ctx, r.OwnerID, r.ID, r.Workflow.TriggerID, "limited-chat", prepared.Recipient)
			case "commit_timeout":
				if status.Code(e) != codes.DeadlineExceeded || !lost.lost.Load() {
					t.Fatal(admitted, e)
				}
				assertCount(t, pool, r, 1)
				unavailable.Store(true)
				if _, e := pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=789,telegram_version=2 WHERE id=$1`, r.OwnerID); e != nil {
					t.Fatal(e)
				}
				admitted, e = client.AdmitDelivery(ctx, r.OwnerID, r.ID, r.Workflow.TriggerID, "limited-chat", prepared.Recipient)
			}
			if e != nil {
				t.Fatal(e)
			}
			switch mode {
			case "cancel", "missing", "false":
				want := map[string]string{"cancel": "terminal", "missing": "blocked", "false": "wait_condition"}[mode]
				if admitted.Outcome != want || admitted.Attempt != nil {
					t.Fatal(admitted)
				}
				if mode == "missing" {
					if p := prepare(); p.Outcome != "blocked" || p.Recipient != nil {
						t.Fatal(p)
					}
				}
			default:
				if admitted.Attempt == nil {
					t.Fatal(admitted)
				}
				want := prepared.Recipient.ChatId
				if mode == "rebind_before" || mode == "rebind_during" || mode == "chat_only_change" {
					want = 789
				}
				if admitted.Attempt.ChatId != want {
					t.Fatal(admitted)
				}
				if _, e := pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=790,telegram_version=3 WHERE id=$1`, r.OwnerID); e != nil {
					t.Fatal(e)
				}
				requestID := "limited-chat"
				if mode == "rebind_before" || mode == "rebind_during" || mode == "chat_only_change" {
					requestID = "new-limiter-cycle"
				}
				before := calls.Load()
				replay, e := client.AdmitDelivery(ctx, r.OwnerID, r.ID, r.Workflow.TriggerID, requestID, prepared.Recipient)
				if e != nil || !proto.Equal(replay, admitted) || calls.Load() != before {
					t.Fatal(replay, e)
				}
			}
			assertCount(t, pool, r, 1)
			if sends.Load() != 0 {
				t.Fatal("protocol invoked Telegram")
			}
		})
	}
}

func TestRecipientMTLSAndValidation(t *testing.T) {
	for _, identity := range []string{notificationsURI, matcherURI, apiURI, ""} {
		t.Run(identity, func(t *testing.T) {
			server := newServer(&store{value: record()}, nil, nil)
			client := admissionrpc.NewClient(connect(t, server, identity))
			r, e := client.PrepareRecipient(context.Background(), ownerID, subID, workflowID)
			want := codes.PermissionDenied
			if identity == notificationsURI {
				want = codes.OK
			}
			if identity == "" {
				want = codes.Unavailable
			}
			if status.Code(e) != want {
				t.Fatal(r, e)
			}
		})
	}
	client := admissionrpc.NewClient(connect(t, newServer(&store{value: record()}, nil, nil), notificationsURI))
	if _, e := client.AdmitDelivery(context.Background(), ownerID, subID, workflowID, "nil-cas", nil); status.Code(e) != codes.InvalidArgument {
		t.Fatal(e)
	}
	server := newServer(&store{value: record()}, nil, nil)
	if _, e := server.PrepareRecipient(context.Background(), nil); status.Code(e) != codes.InvalidArgument {
		t.Fatal(e)
	}
	invalid := request()
	invalid.ExpectedRecipient = &pb.Recipient{BindingVersion: -1}
	if _, e := server.AdmitDelivery(context.Background(), invalid); status.Code(e) != codes.InvalidArgument {
		t.Fatal(e)
	}
}

func TestPostgresRecipientBlocksAndDeadline(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	for _, reason := range []string{"channel_unavailable", "permanent_failure", "attempts_exhausted"} {
		t.Run(reason, func(t *testing.T) {
			r := fixture(t, pool)
			if _, e := pool.Exec(ctx, `UPDATE app.subscriptions SET state='delivery_blocked',block_reason=$2,blocked_binding_version=1 WHERE id=$1`, r.ID, reason); e != nil {
				t.Fatal(e)
			}
			client := admissionrpc.NewClient(connect(t, newServer(admissionrpc.NewPostgres(pool), nil, nil), notificationsURI))
			p, e := client.PrepareRecipient(ctx, r.OwnerID, r.ID, r.Workflow.TriggerID)
			if e != nil || p.Outcome != "blocked" || p.BlockReason != reason || p.Recipient != nil {
				t.Fatal(p, e)
			}
			if _, e := pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=789,telegram_version=2 WHERE id=$1`, r.OwnerID); e != nil {
				t.Fatal(e)
			}
			p, e = client.PrepareRecipient(ctx, r.OwnerID, r.ID, r.Workflow.TriggerID)
			want := "blocked"
			if reason == "channel_unavailable" {
				want = "recipient_ready"
			}
			if e != nil || p.Outcome != want {
				t.Fatal(p, e)
			}
			assertCount(t, pool, r, 0)
			assertState(t, pool, r, domain.DeliveryBlocked)
		})
	}
	r := fixture(t, pool)
	client := admissionrpc.NewClient(connect(t, newServer(admissionrpc.NewPostgres(pool), nil, nil), notificationsURI))
	tx, e := pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	if _, e := sub.LockQuota(ctx, tx, r.OwnerID); e != nil {
		t.Fatal(e)
	}
	short, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	if _, e := client.PrepareRecipient(short, r.OwnerID, r.ID, r.Workflow.TriggerID); status.Code(e) != codes.DeadlineExceeded {
		t.Fatal(e)
	}
	if e := tx.Rollback(ctx); e != nil {
		t.Fatal(e)
	}
	p, e := client.PrepareRecipient(ctx, r.OwnerID, r.ID, r.Workflow.TriggerID)
	if e != nil || p.Recipient == nil {
		t.Fatal(p, e)
	}
	assertCount(t, pool, r, 0)
}

// A missing channel can be durably parked by admission, but a concurrent new
// binding must never turn that price-free check into an unthrottled permit.
func TestPostgresMissingRecipientGuard(t *testing.T) {
	pool := postgrestest.Open(t)
	ctx := context.Background()
	r := fixture(t, pool)
	if _, e := pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=NULL,telegram_version=2 WHERE id=$1`, r.OwnerID); e != nil {
		t.Fatal(e)
	}
	client := admissionrpc.NewClient(connect(t, newServer(admissionrpc.NewPostgres(pool), nil, nil), notificationsURI))
	p, e := client.PrepareRecipient(ctx, r.OwnerID, r.ID, r.Workflow.TriggerID)
	if e != nil || p.Outcome != "missing_channel" || p.BlockReason != "channel_unavailable" || p.BlockedBindingVersion != 2 {
		t.Fatal(p, e)
	}
	absent := &pb.Recipient{BindingVersion: p.BlockedBindingVersion}
	blocked, e := client.AdmitDelivery(ctx, r.OwnerID, r.ID, r.Workflow.TriggerID, "park-missing", absent)
	if e != nil || blocked.Outcome != "blocked" || blocked.Attempt != nil {
		t.Fatal(blocked, e)
	}
	if _, e := pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=789,telegram_version=3 WHERE id=$1`, r.OwnerID); e != nil {
		t.Fatal(e)
	}
	if v, e := client.AdmitDelivery(ctx, r.OwnerID, r.ID, r.Workflow.TriggerID, "missing-raced", absent); status.Code(e) != codes.Aborted || v != nil {
		t.Fatal(v, e)
	}
	assertCount(t, pool, r, 1)
	assertState(t, pool, r, domain.DeliveryBlocked)
}
