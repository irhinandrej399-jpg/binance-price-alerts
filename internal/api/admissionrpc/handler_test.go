package admissionrpc_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/admissionrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/resultrpc"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/alerttriggered"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	market "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/botlimit"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/chatlimit"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/handler"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/jobs"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/message"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/report"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/retry"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/telegram"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/grpctest"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
)

type resultStore func(context.Context, sub.DeliveryResultCommand) (sub.Record, error)

func (f resultStore) SaveDeliveryResult(ctx context.Context, c sub.DeliveryResultCommand) (sub.Record, error) {
	return f(ctx, c)
}
func resultConnection(t *testing.T, s *resultrpc.Server) *grpc.ClientConn {
	t.Helper()
	ca := newAuthority(t)
	cert := ca.issue(t, nil, true, false)
	client := ca.issue(t, []string{notificationsURI}, false, false)
	opts, e := resultrpc.ServerOptions(cert.cert, cert.key, ca.pem, notificationsURI)
	if e != nil {
		t.Fatal(e)
	}
	return grpctest.StartWithClientOptions(t, func(g *grpc.Server) { pb.RegisterDeliveryResultsServer(g, s) }, []grpc.DialOption{grpc.WithTransportCredentials(clientCreds(t, client, ca.pem)), grpc.WithAuthority("market.test")}, opts...)
}

type handlerFixture struct {
	cfg        handler.Config
	api, local *pgxpool.Pool
	record     sub.Record
	value      atomic.Int64
	mode       atomic.Int64
	calls      atomic.Int64
	lost       atomic.Bool
}

func workerFixture(t *testing.T) *handlerFixture {
	t.Helper()
	f := &handlerFixture{api: postgrestest.Open(t), local: postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")}
	f.record = fixture(t, f.api)
	f.value.Store(101)
	r := f.record
	q := observed("999")
	b, e := alerttriggered.Encode(event.Envelope{EventID: "handler-" + r.Workflow.TriggerID, Type: alerttriggered.Type, Version: 1, OccurredAt: now, CorrelationID: r.ID, OrderingKey: r.ID}, alerttriggered.Triggered{SchemaVersion: 1, Intent: alerttriggered.DeliveryRequested, TriggerID: r.Workflow.TriggerID, WorkflowVersion: uint64(r.Workflow.Version), UserID: r.OwnerID, SubscriptionID: r.ID, SubscriptionVersion: uint64(r.Version), Pair: r.Pair, Threshold: &r.Threshold, Direction: r.Direction, CandidateQuote: &q})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = f.local.Exec(context.Background(), `INSERT INTO app.delivery_jobs(trigger_id,subscription_id,user_id,event_id,body) VALUES($1,$2,$3,$4,$5)`, r.Workflow.TriggerID, r.ID, r.OwnerID, "handler-"+r.ID, b); e != nil {
		t.Fatal(e)
	}
	store, e := jobs.New(f.local, time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	a, e := admissionrpc.NewServer(admissionrpc.NewPostgres(f.api), quotes(func(context.Context, string) (*market.LatestResponse, error) {
		return quote(fmt.Sprint(f.value.Load())), nil
	}), time.Minute, time.Now)
	if e != nil {
		t.Fatal(e)
	}
	results := resultrpc.NewPostgres(f.api)
	reporter, e := report.New(f.local, resultrpc.NewClient(resultConnection(t, resultrpc.NewServer(resultStore(func(ctx context.Context, c sub.DeliveryResultCommand) (sub.Record, error) {
		r, e := results.SaveDeliveryResult(ctx, c)
		if e == nil && f.lost.Swap(false) {
			return r, context.DeadlineExceeded
		}
		return r, e
	})))))
	if e != nil {
		t.Fatal(e)
	}
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		f.calls.Add(1)
		var payload struct {
			Chat int64  `json:"chat_id"`
			Text string `json:"text"`
		}
		if e := json.NewDecoder(req.Body).Decode(&payload); e != nil {
			t.Error(e)
		}
		req.Body.Close()
		if e := checkActualPrice(payload.Text, fmt.Sprint(f.value.Load())); e != nil {
			t.Error(e)
		}
		var chat int64
		if e := f.api.QueryRow(req.Context(), `SELECT telegram_chat_id FROM app.users WHERE id=$1`, r.OwnerID).Scan(&chat); e != nil || payload.Chat != chat {
			t.Errorf("recipient mismatch: %d %d %v", payload.Chat, chat, e)
		}
		switch f.mode.Load() {
		case 1:
			c, _, e := w.(http.Hijacker).Hijack()
			if e != nil {
				t.Error(e)
				return
			}
			c.Close()
		case 2:
			w.WriteHeader(429)
			io.WriteString(w, `{"ok":false,"error_code":429,"parameters":{"retry_after":1}}`)
		case 3:
			w.WriteHeader(403)
			io.WriteString(w, `{"ok":false,"error_code":403}`)
		case 4:
			w.WriteHeader(400)
			io.WriteString(w, `{"ok":false,"error_code":400,"description":"other"}`)
		default:
			io.WriteString(w, `{"ok":true,"result":{"message_id":88}}`)
		}
	}))
	t.Cleanup(httpServer.Close)
	sender, e := telegram.New(telegram.Config{Token: "fixture", Timeout: time.Second, BaseURL: httpServer.URL})
	if e != nil {
		t.Fatal(e)
	}
	bot, e := botlimit.New(f.local, fmt.Sprint(fixtureSeq.Add(1)), 1000000)
	if e != nil {
		t.Fatal(e)
	}
	chat, e := chatlimit.New(f.local, fmt.Sprint(fixtureSeq.Add(1)), time.Microsecond)
	if e != nil {
		t.Fatal(e)
	}
	policy, e := retry.New(retry.Config{BaseDelay: 30 * time.Millisecond, MaxDelay: time.Second, MaxFailures: 5, ConditionInterval: 30 * time.Millisecond})
	if e != nil {
		t.Fatal(e)
	}
	f.cfg = handler.Config{Store: store, Admission: admissionrpc.NewClient(connect(t, a, notificationsURI)), Reporter: reporter, Bot: bot, Chat: chat, Sender: sender, Policy: policy, RecoveryInterval: time.Second, ReportLimit: 10}
	return f
}
func (f *handlerFixture) run(t *testing.T) error {
	t.Helper()
	h, e := handler.New(f.cfg)
	if e != nil {
		t.Fatal(e)
	}
	return h.Handle(context.Background(), f.record.Workflow.TriggerID)
}
func (f *handlerFixture) state(t *testing.T, want string, canonical domain.State) {
	t.Helper()
	var state string
	var lease *string
	if e := f.local.QueryRow(context.Background(), `SELECT state,lease_id FROM app.delivery_jobs WHERE trigger_id=$1`, f.record.Workflow.TriggerID).Scan(&state, &lease); e != nil || state != want || lease != nil {
		t.Fatalf("local %s lease=%v %v", state, lease, e)
	}
	assertState(t, f.api, f.record, canonical)
}
func (f *handlerFixture) due(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		var due bool
		e := f.local.QueryRow(ctx, `SELECT GREATEST(COALESCE(next_check_at,next_attempt_at),COALESCE(lease_expires_at,'-infinity'),COALESCE((SELECT max(u.next_attempt_at) FROM app.delivery_uncertainties u JOIN app.delivery_attempts a USING(attempt_id) WHERE a.trigger_id=$1),'-infinity'))<=clock_timestamp() FROM app.delivery_jobs WHERE trigger_id=$1`, f.record.Workflow.TriggerID).Scan(&due)
		if e != nil {
			t.Fatal(e)
		}
		if due {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("job never due")
		case <-time.After(5 * time.Millisecond):
		}
	}
}
func TestHandlerDisconnectFalseTrueAndReporterRestart(t *testing.T) {
	f := workerFixture(t)
	f.mode.Store(1)
	if e := f.run(t); e != nil {
		t.Fatal(e)
	}
	f.state(t, "uncertain", domain.PendingDelivery)
	f.due(t)
	f.value.Store(99)
	if e := f.run(t); e != nil {
		t.Fatal(e)
	}
	f.state(t, "wait_condition", domain.Active)
	if f.calls.Load() != 1 {
		t.Fatal("sent false condition")
	}
	f.due(t)
	f.value.Store(102)
	f.mode.Store(0)
	f.lost.Store(true)
	e := f.run(t)
	var pending *handler.PendingWrite
	if !errors.As(e, &pending) {
		t.Fatal(e)
	}
	f.state(t, "succeeded", domain.Delivered)
	// Restart owns a different pool; only durable reports recover the lost reply.
	restarted := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
	store, _ := jobs.New(restarted, time.Minute)
	f.cfg.Store = store
	for range 2 {
		if e = f.run(t); e != nil {
			t.Fatal(e)
		}
	}
	if f.calls.Load() != 2 {
		t.Fatal("report retry resent Telegram")
	}
	var attempts, uncertain, success int
	if e = f.local.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM app.delivery_attempts WHERE trigger_id=$1),(SELECT count(*) FROM app.delivery_uncertainties u JOIN app.delivery_attempts a USING(attempt_id) WHERE a.trigger_id=$1),(SELECT count(*) FROM app.delivery_successes s JOIN app.delivery_attempts a USING(attempt_id) WHERE a.trigger_id=$1)`, f.record.Workflow.TriggerID).Scan(&attempts, &uncertain, &success); e != nil || attempts != 2 || uncertain != 1 || success != 1 {
		t.Fatal(attempts, uncertain, success, e)
	}
}
func TestHandlerSQLTemporaryPermanentAndRecovery(t *testing.T) {
	for _, mode := range []int64{2, 3, 4} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			f := workerFixture(t)
			f.mode.Store(mode)
			if e := f.run(t); e != nil {
				t.Fatal(e)
			}
			switch mode {
			case 2:
				f.state(t, "retryable", domain.PendingDelivery)
				f.due(t)
			case 3:
				f.state(t, "blocked", domain.DeliveryBlocked)
				f.due(t)
				if e := f.run(t); e != nil {
					t.Fatal(e)
				}
				f.state(t, "blocked", domain.DeliveryBlocked)
				if f.calls.Load() != 1 {
					t.Fatal("same binding resent")
				}
				if _, e := f.api.Exec(context.Background(), `UPDATE app.users SET telegram_chat_id=telegram_chat_id+1,telegram_version=telegram_version+1 WHERE id=$1`, f.record.OwnerID); e != nil {
					t.Fatal(e)
				}
				f.due(t)
			case 4:
				f.state(t, "blocked", domain.DeliveryBlocked)
				if e := f.run(t); e != nil {
					t.Fatal(e)
				}
				if f.calls.Load() != 1 {
					t.Fatal("permanent refusal resent")
				}
				return
			}
			f.mode.Store(0)
			if e := f.run(t); e != nil {
				t.Fatal(e)
			}
			f.state(t, "succeeded", domain.Delivered)
			if f.calls.Load() != 2 {
				t.Fatal(f.calls.Load())
			}
		})
	}
}

// Model a committed registration whose success reply was lost before HTTP.
// Recovery must close the original API attempt without issuing Telegram.
type lostRegistration struct {
	handler.Store
	lost bool
}

func (s *lostRegistration) RegisterAttempt(ctx context.Context, l jobs.Lease, id string) error {
	if e := s.Store.RegisterAttempt(ctx, l, id); e != nil {
		return e
	}
	if !s.lost {
		s.lost = true
		return errors.New("registration commit reply lost")
	}
	return nil
}

// Slow admission models scheduling/RPC latency after API committed sending.
type slowAdmission struct{ handler.Admission }

func (a slowAdmission) AdmitDelivery(ctx context.Context, owner, subscription, workflow, request string, recipient ...*pb.Recipient) (*pb.AdmitDeliveryResponse, error) {
	r, err := a.Admission.AdmitDelivery(ctx, owner, subscription, workflow, request, recipient...)
	if err == nil && r.Outcome == "admitted" {
		timer := time.NewTimer(75 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return r, err
}

func TestHandlerRegisteredCrashRecovery(t *testing.T) {
	for _, delayed := range []bool{false, true} {
		t.Run(fmt.Sprintf("slow_admission=%t", delayed), func(t *testing.T) {
			testRegisteredCrashRecovery(t, delayed)
		})
	}
}

func testRegisteredCrashRecovery(t *testing.T, delayed bool) {
	f := workerFixture(t)
	// This fixture must reach committed registration before the simulated crash.
	// 50 ms also covered SQL and mTLS setup, so ordinary scheduling could expire
	// the lease before registration and exercise attempt_busy instead of recovery.
	store, e := jobs.New(f.local, time.Second)
	if e != nil {
		t.Fatal(e)
	}
	registration := &lostRegistration{Store: store}
	f.cfg.Store = registration
	if delayed {
		f.cfg.Admission = slowAdmission{f.cfg.Admission}
	}
	if e = f.run(t); e == nil {
		t.Fatal("registration error hidden")
	}
	if !registration.lost {
		t.Fatalf("crash fixture never committed registration: %v", e)
	}
	var attempts int
	if e = f.local.QueryRow(context.Background(), `SELECT count(*) FROM app.delivery_attempts WHERE trigger_id=$1`, f.record.Workflow.TriggerID).Scan(&attempts); e != nil || attempts != 1 {
		t.Fatal("expected one durable registration", attempts, e)
	}
	assertState(t, f.api, f.record, domain.Sending)
	if f.calls.Load() != 0 {
		t.Fatal("ambiguous registration sent")
	}
	f.due(t)
	f.value.Store(99)
	restarted := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
	store, e = jobs.New(restarted, time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	f.cfg.Store = store
	if e = f.run(t); e != nil {
		t.Fatal(e)
	} // save/report uncertain, no new claim
	assertState(t, f.api, f.record, domain.PendingDelivery)
	var uncertainties, pendingReports int
	if e = f.local.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM app.delivery_uncertainties u JOIN app.delivery_attempts a USING(attempt_id) WHERE a.trigger_id=$1),(SELECT count(*) FROM app.delivery_uncertain_reports r JOIN app.delivery_attempts a USING(attempt_id) WHERE a.trigger_id=$1 AND r.pending)`, f.record.Workflow.TriggerID).Scan(&uncertainties, &pendingReports); e != nil || uncertainties != 1 || pendingReports != 0 {
		t.Fatal("expected durable acknowledged uncertain outcome", uncertainties, pendingReports, e)
	}
	f.due(t)
	if e = f.run(t); e != nil {
		t.Fatal(e)
	}
	f.state(t, "wait_condition", domain.Active)
	if f.calls.Load() != 0 {
		t.Fatal("recovery sent historical permission")
	}
}

func TestHandlerSQLLimiterReleasesLease(t *testing.T) {
	for _, kind := range []string{"bot", "chat"} {
		t.Run(kind, func(t *testing.T) {
			f := workerFixture(t)
			ctx := context.Background()
			if kind == "bot" {
				lim, e := botlimit.New(f.local, fmt.Sprint(fixtureSeq.Add(1)), 1)
				if e != nil {
					t.Fatal(e)
				}
				if wait, e := lim.TryAcquire(ctx); e != nil || wait != 0 {
					t.Fatal(wait, e)
				}
				f.cfg.Bot = lim
			} else {
				lim, e := chatlimit.New(f.local, fmt.Sprint(fixtureSeq.Add(1)), time.Second)
				if e != nil {
					t.Fatal(e)
				}
				var chat int64
				if e = f.api.QueryRow(ctx, `SELECT telegram_chat_id FROM app.users WHERE id=$1`, f.record.OwnerID).Scan(&chat); e != nil {
					t.Fatal(e)
				}
				if wait, e := lim.TryAcquire(ctx, chat); e != nil || wait != 0 {
					t.Fatal(wait, e)
				}
				f.cfg.Chat = lim
			}
			if e := f.run(t); e != nil {
				t.Fatal(e)
			}
			f.state(t, "wait_limiter", domain.PendingDelivery)
			var attempts int
			if e := f.local.QueryRow(ctx, `SELECT count(*) FROM app.delivery_attempts WHERE trigger_id=$1`, f.record.Workflow.TriggerID).Scan(&attempts); e != nil || attempts != 0 || f.calls.Load() != 0 {
				t.Fatal(attempts, f.calls.Load(), e)
			}
		})
	}
}

// Shared by the HTTP fixture and its deterministic payload regression.
func checkActualPrice(text, want string) error {
	const prefix = "Фактическая цена: "
	var values []string
	for line := range strings.SplitSeq(text, "\n") {
		if value, ok := strings.CutPrefix(line, prefix); ok {
			values = append(values, value)
		}
	}
	if len(values) != 1 || values[0] != want {
		return fmt.Errorf("actual price field = %q, want %q", values, want)
	}
	return nil
}

func TestHandlerPayloadActualPrice(t *testing.T) {
	r := record()
	actual, err := price.Parse("101")
	if err != nil {
		t.Fatal(err)
	}
	text, err := message.Format(message.Input{Pair: r.Pair, Threshold: r.Threshold, Direction: r.Direction, ActualPrice: actual, TradeTime: now, CheckedAt: now.Add(999 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, "Возраст сделки: 999 с") {
		t.Fatal(text)
	}
	if err := checkActualPrice(text, "101"); err != nil {
		t.Fatal(err)
	}
	for _, wrong := range []string{"999", "1010"} {
		bad := strings.Replace(text, "Фактическая цена: 101\n", "Фактическая цена: "+wrong+"\n", 1)
		if err := checkActualPrice(bad, "101"); err == nil {
			t.Fatalf("accepted wrong price %s", wrong)
		}
	}
}
