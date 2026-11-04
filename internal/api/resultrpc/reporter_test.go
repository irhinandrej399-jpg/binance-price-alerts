package resultrpc_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/auth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/resultrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/session"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/jobs"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/report"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/retry"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/telegram"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
)

func TestReporterAcrossOwnerDatabasesAndLostCommitResponse(t *testing.T) {
	for _, outcome := range []string{"succeeded", "uncertain", "retryable", "blocked", "exhausted", "cancelled"} {
		t.Run(outcome, func(t *testing.T) {
			ctx := context.Background()
			apiPool := postgrestest.Open(t)
			local := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
			r, _, req := admitted(t, apiPool)
			if _, e := local.Exec(ctx, `INSERT INTO app.delivery_jobs(trigger_id,subscription_id,user_id,event_id,body) VALUES($1,$2,$3,'reporter-fixture',$4)`, req.WorkflowId, req.SubscriptionId, req.OwnerId, []byte("original")); e != nil {
				t.Fatal(e)
			}
			store, e := jobs.New(local, time.Minute)
			if e != nil {
				t.Fatal(e)
			}
			lease, e := store.Claim(ctx, req.WorkflowId)
			if e != nil {
				t.Fatal(e)
			}
			if e = store.RegisterAttempt(ctx, lease, req.AttemptId); e != nil {
				t.Fatal(e)
			}
			want, slots, audits := domain.PendingDelivery, 1, 1
			switch outcome {
			case "succeeded", "cancelled":
				e = store.SaveSuccess(ctx, lease, req.AttemptId, 123)
				want = domain.Delivered
				slots = 0
			case "uncertain":
				e = store.SaveUncertain(ctx, lease, req.AttemptId, time.Now().Add(time.Hour))
			case "blocked":
				e = store.SaveBlocked(ctx, lease, req.AttemptId, domain.ChannelUnavailable, 1)
				want = domain.DeliveryBlocked
			default:
				maxFailures := uint32(3)
				if outcome == "exhausted" {
					maxFailures = 1
					want = domain.DeliveryBlocked
					audits = 2
				}
				policy, err := retry.New(retry.Config{BaseDelay: time.Second, MaxDelay: time.Second, ConditionInterval: time.Second, MaxFailures: maxFailures})
				if err != nil {
					t.Fatal(err)
				}
				_, e = store.SaveRetryable(ctx, lease, req.AttemptId, &telegram.Error{Kind: telegram.Server}, policy, 0, 1)
			}
			if e != nil {
				t.Fatal(e)
			}
			if outcome == "cancelled" {
				sessions := session.New(apiPool, nil)
				cred, err := sessions.Issue(ctx, r.OwnerID)
				if err != nil {
					t.Fatal(err)
				}
				hreq := httptest.NewRequest(http.MethodDelete, "/v1/subscriptions/"+r.ID, nil)
				hreq.Header.Set("Authorization", "Bearer "+cred.AccessToken)
				w := httptest.NewRecorder()
				auth.New(sessions, sub.NewCancelHandler(apiPool)).ServeHTTP(w, hreq)
				if w.Code != 200 {
					t.Fatal(w.Code, w.Body.String())
				}
				want = domain.Cancelled
			}
			real := resultrpc.NewPostgres(apiPool)
			var calls atomic.Int32
			server := resultrpc.NewServer(storeFunc(func(ctx context.Context, c sub.DeliveryResultCommand) (sub.Record, error) {
				v, e := real.SaveDeliveryResult(ctx, c)
				// Real API commit succeeds, but its first reply is lost on the way back.
				if calls.Add(1) == 1 && e == nil {
					return v, context.DeadlineExceeded
				}
				return v, e
			}))
			client := resultrpc.NewClient(connect(t, server, notificationsURI))
			reporter, e := report.New(local, client)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = reporter.Reconcile(ctx, req.WorkflowId, 10); e == nil {
				t.Fatal("lost reply acknowledged")
			}
			// New process and pool recover only SQL obligations, not an external send.
			restarted := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
			reporter, e = report.New(restarted, client)
			if e != nil {
				t.Fatal(e)
			}
			if n, e := reporter.Reconcile(ctx, req.WorkflowId, 10); e != nil || n != 1 {
				t.Fatal(n, e)
			}
			if n, e := reporter.Reconcile(ctx, req.WorkflowId, 10); e != nil || n != 0 {
				t.Fatal(n, e)
			}
			assertSaved(t, apiPool, r, want, audits, slots)
			if outcome == "uncertain" {
				if e = store.SaveSuccess(ctx, lease, req.AttemptId, 123); e != nil {
					t.Fatal(e)
				}
				if n, e := reporter.Reconcile(ctx, req.WorkflowId, 10); e != nil || n != 1 {
					t.Fatal(n, e)
				}
				assertSaved(t, apiPool, r, domain.Delivered, 2, 0)
			}
		})
	}
}
