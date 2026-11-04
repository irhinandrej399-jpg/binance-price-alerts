package handler

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/alerttriggered"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	market "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/jobs"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/retry"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/telegram"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var now = time.Now().UTC().Truncate(time.Microsecond)

func body(t *testing.T) []byte {
	t.Helper()
	p, _ := price.Parse("100")
	candidate, _ := price.Parse("999")
	q := priceobserved.Observed{SchemaVersion: 1, Pair: "BTCUSDT", Price: &candidate, SourceID: "candidate", Revision: priceobserved.Revision{StreamID: "s", Generation: 1, Sequence: 1}, TradeTime: now.Add(-time.Second), ReceivedAt: now, StreamState: priceobserved.Live}
	b, e := alerttriggered.Encode(event.Envelope{EventID: "event", Type: alerttriggered.Type, Version: 1, OccurredAt: now, CorrelationID: "test", OrderingKey: "workflow-sub"}, alerttriggered.Triggered{SchemaVersion: 1, Intent: alerttriggered.DeliveryRequested, TriggerID: "workflow", WorkflowVersion: 1, UserID: "owner", SubscriptionID: "workflow-sub", SubscriptionVersion: 1, Pair: "BTCUSDT", Threshold: &p, Direction: price.Up, CandidateQuote: &q})
	if e != nil {
		t.Fatal(e)
	}
	return b
}

type memory struct {
	Store
	lease             jobs.Lease
	claimErr          error
	parked            jobs.WaitReason
	next              time.Time
	registered, saved string
	reason            domain.BlockReason
	version           int64
	scheduled         bool
	saveErr           error
}

func (m *memory) Claim(context.Context, string) (jobs.Lease, error) { return m.lease, m.claimErr }
func (m *memory) Park(_ context.Context, _ jobs.Lease, r jobs.WaitReason, n time.Time) error {
	m.parked = r
	m.next = n
	return nil
}
func (m *memory) RegisterAttempt(_ context.Context, _ jobs.Lease, a string) error {
	m.registered = a
	return nil
}
func (m *memory) SaveSuccess(_ context.Context, _ jobs.Lease, a string, id int64) error {
	if id != 88 {
		panic("message ID")
	}
	m.saved = "success"
	return m.saveErr
}
func (m *memory) SaveUncertain(_ context.Context, _ jobs.Lease, a string, n time.Time) error {
	m.saved = "uncertain"
	m.next = n
	return m.saveErr
}
func (m *memory) SaveBlocked(_ context.Context, _ jobs.Lease, a string, r domain.BlockReason, v int64) error {
	m.saved = "blocked"
	m.reason = r
	m.version = v
	return m.saveErr
}
func (m *memory) ParkBlocked(_ context.Context, _ jobs.Lease, r domain.BlockReason, v int64) error {
	m.saved = "park_blocked"
	m.reason = r
	m.version = v
	return nil
}
func (m *memory) SaveRetryable(_ context.Context, _ jobs.Lease, a string, c error, p *retry.Policy, e uint64, v int64) (retry.Plan, error) {
	m.saved = "retryable"
	m.version = v
	return retry.Plan{}, m.saveErr
}
func (m *memory) ScheduleRecovery(context.Context, string, int64, time.Duration) error {
	m.scheduled = true
	return nil
}

type api struct {
	prepared *pb.PrepareRecipientResponse
	admitted *pb.AdmitDeliveryResponse
	err      error
	ids      []string
	expected []*pb.Recipient
	calls    int
	lost     bool
}

func (a *api) PrepareRecipient(context.Context, string, string, string) (*pb.PrepareRecipientResponse, error) {
	return a.prepared, nil
}
func (a *api) AdmitDelivery(_ context.Context, o, s, w, id string, r ...*pb.Recipient) (*pb.AdmitDeliveryResponse, error) {
	a.calls++
	a.ids = append(a.ids, id)
	a.expected = append(a.expected, r[0])
	if a.lost && a.calls == 1 {
		return nil, status.Error(codes.Unavailable, "lost")
	}
	return a.admitted, a.err
}

type bot struct {
	wait  time.Duration
	calls int
}

func (b *bot) TryAcquire(context.Context) (time.Duration, error) { b.calls++; return b.wait, nil }

type chat struct {
	wait time.Duration
	ids  []int64
}

func (c *chat) TryAcquire(_ context.Context, id int64) (time.Duration, error) {
	c.ids = append(c.ids, id)
	return c.wait, nil
}

type sender struct {
	calls int
	err   error
	text  string
	chat  int64
}

func (s *sender) Send(_ context.Context, c int64, text string) (int64, error) {
	s.calls++
	s.text = text
	s.chat = c
	return 88, s.err
}

type reporter struct {
	calls int
	err   error
}

func (r *reporter) Reconcile(context.Context, string, int) (int, error) { r.calls++; return 0, r.err }
func setup(t *testing.T) (*Handler, *memory, *api, *bot, *chat, *sender, *reporter) {
	t.Helper()
	m := &memory{lease: jobs.Lease{TriggerID: "workflow", ID: "claim", Fence: 1, ExpiresAt: now.Add(time.Minute), Body: body(t)}}
	a := &api{prepared: &pb.PrepareRecipientResponse{Outcome: "recipient_ready", Recipient: &pb.Recipient{ChatId: 5, BindingVersion: 7}}, admitted: &pb.AdmitDeliveryResponse{Outcome: "admitted", SubscriptionId: "workflow-sub", WorkflowId: "workflow", Attempt: &pb.AttemptPermission{AttemptId: "attempt", Fence: 1, ChatId: 5, BindingVersion: 7, ExpiresAt: now.Add(time.Minute).Format(time.RFC3339Nano), Quote: &market.Quote{Pair: "BTCUSDT", Price: "101", TradeTime: now.Add(-time.Second).Format(time.RFC3339Nano), ReceivedAt: now.Format(time.RFC3339Nano), SourceId: "source", Revision: &market.Revision{StreamId: "s", Generation: 1, Sequence: 2}, StreamState: "live"}}}}
	b, c, s, r := &bot{}, &chat{}, &sender{}, &reporter{}
	p, _ := retry.New(retry.Config{BaseDelay: time.Second, MaxDelay: time.Minute, ConditionInterval: 3 * time.Second, MaxFailures: 5})
	h, e := New(Config{Store: m, Admission: a, Bot: b, Chat: c, Sender: s, Reporter: r, Policy: p, RecoveryInterval: time.Second, ReportLimit: 10})
	if e != nil {
		t.Fatal(e)
	}
	h.now = func() time.Time { return now }
	return h, m, a, b, c, s, r
}
func TestOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name     string
		cause    error
		want     string
		reason   domain.BlockReason
		schedule bool
	}{{"success", nil, "success", "", false}, {"429", &telegram.Error{Kind: telegram.RateLimited, RetryAfterSeconds: 9}, "retryable", "", false}, {"server", &telegram.Error{Kind: telegram.Server}, "retryable", "", false}, {"channel", &telegram.Error{Kind: telegram.Forbidden}, "blocked", domain.ChannelUnavailable, true}, {"permanent", &telegram.Error{Kind: telegram.Rejected}, "blocked", domain.PermanentFailure, false}, {"unknown", &telegram.Error{Kind: telegram.Unknown}, "uncertain", "", false}, {"not_sent", &telegram.Error{Kind: telegram.NotSent}, "uncertain", "", false}} {
		t.Run(tc.name, func(t *testing.T) {
			h, m, a, b, c, s, r := setup(t)
			s.err = tc.cause
			if e := h.Handle(context.Background(), "workflow"); e != nil {
				t.Fatal(e)
			}
			if s.calls != 1 || m.saved != tc.want || m.reason != tc.reason || m.scheduled != tc.schedule || m.registered != "attempt" || r.calls != 2 || b.calls != 1 || len(c.ids) != 1 || c.ids[0] != 5 || a.expected[0].ChatId != 5 {
				t.Fatalf("state=%+v sender=%+v", m, s)
			}
			if !strings.Contains(s.text, "Фактическая цена: 101") || strings.Contains(s.text, "999") {
				t.Fatal(s.text)
			}
		})
	}
}
func TestNoHTTP(t *testing.T) {
	for _, name := range []string{"bot_wait", "chat_wait", "false", "busy", "terminal", "blocked", "missing", "changed", "expired", "wrong_recipient", "wrong_workflow", "report_failure", "budget"} {
		t.Run(name, func(t *testing.T) {
			h, m, a, b, c, s, r := setup(t)
			switch name {
			case "bot_wait":
				b.wait = time.Second
			case "chat_wait":
				c.wait = time.Second
			case "false":
				a.admitted.Outcome = "wait_condition"
			case "busy":
				a.admitted.Outcome = "attempt_busy"
			case "terminal":
				a.prepared.Outcome = "terminal"
			case "blocked":
				a.prepared = &pb.PrepareRecipientResponse{Outcome: "blocked", BlockReason: "channel_unavailable", BlockedBindingVersion: 7}
			case "missing":
				a.prepared = &pb.PrepareRecipientResponse{Outcome: "missing_channel", BlockedBindingVersion: 7}
				a.admitted.Outcome = "blocked"
				a.admitted.BlockReason = "channel_unavailable"
				a.admitted.BlockedBindingVersion = 7
			case "changed":
				a.err = status.Error(codes.Aborted, "changed")
			case "expired":
				a.admitted.Attempt.ExpiresAt = now.Format(time.RFC3339Nano)
			case "wrong_recipient":
				a.admitted.Attempt.ChatId = 6
			case "wrong_workflow":
				a.admitted.WorkflowId = "other"
			case "report_failure":
				r.err = errors.New("API down")
			case "budget":
				m.lease.Failures = 5
			}
			_ = h.Handle(context.Background(), "workflow")
			if s.calls != 0 || m.registered != "" {
				t.Fatalf("sent or registered: %+v %+v", s, m)
			}
			switch name {
			case "bot_wait", "chat_wait":
				if m.parked != jobs.WaitLimiter || !m.next.Equal(now.Add(time.Second)) {
					t.Fatal(m)
				}
			case "false", "busy", "changed":
				if m.parked != jobs.WaitCondition {
					t.Fatal(m)
				}
			case "blocked", "missing":
				if m.saved != "park_blocked" || !m.scheduled {
					t.Fatal(m)
				}
			}
			if name == "missing" && (a.expected[0].ChatId != 0 || b.calls != 0 || len(c.ids) != 0) {
				t.Fatal("unguarded missing channel")
			}
		})
	}
}
func TestLostAdmissionAndPendingWrite(t *testing.T) {
	h, m, a, _, _, s, r := setup(t)
	a.lost = true
	m.saveErr = errors.New("commit reply lost")
	e := h.Handle(context.Background(), "workflow")
	var pending *PendingWrite
	if !errors.As(e, &pending) {
		t.Fatal(e)
	}
	if s.calls != 1 || a.calls != 2 || a.ids[0] != a.ids[1] || a.expected[0] != a.expected[1] {
		t.Fatal("unsafe replay")
	}
	m.saveErr = nil
	if e = pending.Retry(context.Background()); e != nil {
		t.Fatal(e)
	}
	if s.calls != 1 || r.calls != 2 {
		t.Fatal("write retry sent HTTP")
	}
}
func TestUnresolvedAndTerminalReports(t *testing.T) {
	h, m, _, b, _, s, r := setup(t)
	m.claimErr = &jobs.UnresolvedAttempt{Lease: m.lease, AttemptID: "old"}
	if e := h.Handle(context.Background(), "workflow"); e != nil {
		t.Fatal(e)
	}
	if m.saved != "uncertain" || s.calls != 0 || b.calls != 0 || r.calls != 2 {
		t.Fatal(m, s, r)
	}
	m.claimErr = jobs.ErrUnavailable
	if e := h.Handle(context.Background(), "workflow"); e != nil {
		t.Fatal(e)
	}
	if r.calls != 3 {
		t.Fatal("stopped job reports not drained")
	}
}

func TestPendingUncertainAfterDeadlineAndExhaustedRecovery(t *testing.T) {
	h, m, a, b, _, s, _ := setup(t)
	s.err = &telegram.Error{Kind: telegram.Unknown}
	m.saveErr = errors.New("database unavailable")
	var pending *PendingWrite
	if e := h.Handle(context.Background(), "workflow"); !errors.As(e, &pending) {
		t.Fatal(e)
	}
	h.now = func() time.Time { return now.Add(time.Hour) }
	m.saveErr = nil
	if e := pending.Retry(context.Background()); e != nil {
		t.Fatal(e)
	}
	if !m.next.After(h.now()) || s.calls != 1 {
		t.Fatal("expired first write or HTTP replay")
	}
	// A registered historical claim can have an exhausted budget after a late
	// earlier error. Recover the original binding from its deterministic API replay.
	h.now = func() time.Time { return now }
	m.lease.Failures = 5
	m.claimErr = &jobs.UnresolvedAttempt{Lease: m.lease, AttemptID: "attempt"}
	s.calls = 0
	b.calls = 0
	if e := h.Handle(context.Background(), "workflow"); e != nil {
		t.Fatal(e)
	}
	if m.reason != domain.AttemptsExhausted || m.version != 7 || s.calls != 0 || b.calls != 0 || a.expected[len(a.expected)-1] != nil {
		t.Fatal("exhausted recovery issued HTTP or used a current binding")
	}
}

func TestClaimFailureAndInvalidPermission(t *testing.T) {
	for _, name := range []string{"claim_error", "stale_lease", "nil_quote", "invalid_price", "invalid_time", "nil_revision", "cancelled"} {
		t.Run(name, func(t *testing.T) {
			h, m, a, _, _, s, _ := setup(t)
			ctx := context.Background()
			switch name {
			case "claim_error":
				m.claimErr = errors.New("claim commit unknown")
			case "stale_lease":
				m.lease.ExpiresAt = now
			case "nil_quote":
				a.admitted.Attempt.Quote = nil
			case "invalid_price":
				a.admitted.Attempt.Quote.Price = "NaN"
			case "invalid_time":
				a.admitted.Attempt.Quote.TradeTime = "bad"
			case "nil_revision":
				a.admitted.Attempt.Quote.Revision = nil
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			_ = h.Handle(ctx, "workflow")
			if s.calls != 0 {
				t.Fatal("unsafe HTTP")
			}
		})
	}
}
