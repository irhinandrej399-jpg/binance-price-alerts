// Package handler executes one bounded delivery cycle for a selected workflow.
package handler

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/alerttriggered"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/jobs"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/message"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/retry"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/telegram"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Store interface {
	Claim(context.Context, string) (jobs.Lease, error)
	Park(context.Context, jobs.Lease, jobs.WaitReason, time.Time) error
	RegisterAttempt(context.Context, jobs.Lease, string) error
	SaveSuccess(context.Context, jobs.Lease, string, int64) error
	SaveUncertain(context.Context, jobs.Lease, string, time.Time) error
	SaveBlocked(context.Context, jobs.Lease, string, domain.BlockReason, int64) error
	SaveRetryable(context.Context, jobs.Lease, string, error, *retry.Policy, uint64, int64) (retry.Plan, error)
	ParkBlocked(context.Context, jobs.Lease, domain.BlockReason, int64) error
	ScheduleRecovery(context.Context, string, int64, time.Duration) error
}
type Admission interface {
	PrepareRecipient(context.Context, string, string, string) (*pb.PrepareRecipientResponse, error)
	AdmitDelivery(context.Context, string, string, string, string, ...*pb.Recipient) (*pb.AdmitDeliveryResponse, error)
}
type Bot interface {
	TryAcquire(context.Context) (time.Duration, error)
}
type Chat interface {
	TryAcquire(context.Context, int64) (time.Duration, error)
}
type Sender interface {
	Send(context.Context, int64, string) (int64, error)
}
type Reporter interface {
	Reconcile(context.Context, string, int) (int, error)
}
type Config struct {
	Store            Store
	Admission        Admission
	Bot              Bot
	Chat             Chat
	Sender           Sender
	Reporter         Reporter
	Policy           *retry.Policy
	RecoveryInterval time.Duration
	ReportLimit      int
}
type Handler struct {
	cfg Config
	now func() time.Time
}

func New(c Config) (*Handler, error) {
	if c.Store == nil || c.Admission == nil || c.Bot == nil || c.Chat == nil || c.Sender == nil || c.Reporter == nil || c.Policy == nil || c.ReportLimit < 1 || c.ReportLimit > 1000 || c.RecoveryInterval < time.Second || c.RecoveryInterval > time.Hour || c.RecoveryInterval%time.Microsecond != 0 {
		return nil, errors.New("invalid delivery handler configuration")
	}
	return &Handler{cfg: c, now: time.Now}, nil
}

// PendingWrite retains a known outcome when its SQL commit is ambiguous. Retry
// repeats only the same storage operation, recovery schedule and reporter; it
// NEVER sends HTTP. Caller must retry with bounded backoff before discarding it.
// After process loss the registered attempt is recovered conservatively as
// uncertain, since no local transaction can include an external HTTP response.
// Like a job invocation, Retry must not be called concurrently.
type PendingWrite struct {
	cause error
	write func(context.Context) error
}

func (e *PendingWrite) Error() string { return "delivery result persistence pending" }
func (e *PendingWrite) Unwrap() error { return e.cause }
func (e *PendingWrite) Retry(ctx context.Context) error {
	if err := e.write(ctx); err != nil {
		return &PendingWrite{cause: err, write: e.write}
	}
	return nil
}
func (h *Handler) persist(ctx context.Context, write func(context.Context) error) error {
	if e := write(ctx); e != nil {
		return &PendingWrite{cause: e, write: write}
	}
	return nil
}
func (h *Handler) report(ctx context.Context, id string) error {
	_, e := h.cfg.Reporter.Reconcile(ctx, id, h.cfg.ReportLimit)
	return e
}

// Handle drains a bounded report batch even for stopped jobs, then tries one
// claim. There is no sleep, loop over jobs, renewal, or retry of Telegram here.
// Errors require scheduler backoff; ErrUnavailable is an ordinary idle result.
func (h *Handler) Handle(ctx context.Context, id string) error {
	n, e := h.cfg.Reporter.Reconcile(ctx, id, h.cfg.ReportLimit)
	if e != nil {
		return e
	}
	if n > 0 {
		return nil
	} // drain all older outcomes before considering another send
	l, e := h.cfg.Store.Claim(ctx, id)
	var unresolved *jobs.UnresolvedAttempt
	if errors.As(e, &unresolved) {
		return h.recover(ctx, unresolved)
	}
	if errors.Is(e, jobs.ErrUnavailable) {
		return nil
	}
	if e != nil {
		return e
	}
	_, work, e := alerttriggered.Decode(l.Body)
	if e != nil || work.TriggerID != id || l.TriggerID != id {
		return errors.New("invalid claimed workflow")
	}
	if !h.now().Before(l.ExpiresAt) {
		return jobs.ErrStale
	}
	prepared, e := h.cfg.Admission.PrepareRecipient(ctx, work.UserID, work.SubscriptionID, id)
	if e != nil {
		return errors.Join(e, h.wait(ctx, l, jobs.WaitCondition, 0))
	}
	if prepared == nil {
		return errors.New("missing recipient response")
	}
	switch prepared.Outcome {
	case "terminal":
		return nil
	case "blocked":
		return h.block(ctx, l, domain.BlockReason(prepared.BlockReason), prepared.BlockedBindingVersion)
	case "recipient_ready", "missing_channel":
	default:
		return errors.New("invalid recipient outcome")
	}
	if h.cfg.Policy.WaitCondition(h.now(), l.Failures).Exhausted {
		return errors.New("delivery failure budget exhausted; sending stopped")
	}
	expected := prepared.Recipient
	if prepared.Outcome == "missing_channel" {
		expected = &pb.Recipient{BindingVersion: prepared.BlockedBindingVersion}
	} else {
		if expected == nil || expected.ChatId <= 0 || expected.BindingVersion < 0 {
			return errors.New("invalid prepared recipient")
		}
		wait, e := h.cfg.Bot.TryAcquire(ctx)
		if e != nil {
			return errors.Join(e, h.wait(ctx, l, jobs.WaitLimiter, 0))
		}
		if wait < 0 {
			return errors.New("invalid limiter wait")
		}
		if wait > 0 {
			return h.wait(ctx, l, jobs.WaitLimiter, wait)
		}
		wait, e = h.cfg.Chat.TryAcquire(ctx, expected.ChatId)
		if e != nil {
			return errors.Join(e, h.wait(ctx, l, jobs.WaitLimiter, 0))
		}
		if wait < 0 {
			return errors.New("invalid limiter wait")
		}
		if wait > 0 {
			return h.wait(ctx, l, jobs.WaitLimiter, wait)
		}
	}
	if !h.now().Before(l.ExpiresAt) {
		return jobs.ErrStale
	}
	// A fresh claim has one deterministic request ID. Timeout replay uses the
	// identical ID and CAS; a different claim always obtains a different ID.
	response, e := h.cfg.Admission.AdmitDelivery(ctx, work.UserID, work.SubscriptionID, id, requestID(l), expected)
	if status.Code(e) == codes.Unavailable || status.Code(e) == codes.DeadlineExceeded {
		if ctx.Err() == nil && h.now().Before(l.ExpiresAt) {
			response, e = h.cfg.Admission.AdmitDelivery(ctx, work.UserID, work.SubscriptionID, id, requestID(l), expected)
		}
	}
	if e != nil {
		if status.Code(e) == codes.Aborted {
			return h.wait(ctx, l, jobs.WaitCondition, 0)
		}
		return errors.Join(e, h.wait(ctx, l, jobs.WaitCondition, 0))
	}
	if response == nil || response.WorkflowId != id || response.SubscriptionId != work.SubscriptionID {
		return errors.New("invalid admission identity")
	}
	switch response.Outcome {
	case "terminal":
		return nil
	case "blocked":
		return h.block(ctx, l, domain.BlockReason(response.BlockReason), response.BlockedBindingVersion)
	case "wait_condition", "attempt_busy":
		return h.wait(ctx, l, jobs.WaitCondition, 0)
	case "admitted":
	default:
		return errors.New("invalid admission outcome")
	}
	if prepared.Outcome != "recipient_ready" {
		return errors.New("missing channel guard returned permission")
	}
	text, deadline, e := h.permission(work, l, expected, response.Attempt)
	if e != nil {
		return errors.Join(e, h.wait(ctx, l, jobs.WaitCondition, 0))
	}
	a := response.Attempt
	// Register before HTTP. An ambiguous registration prohibits Send; Claim
	// will recover it as uncertain after expiry if the insert actually committed.
	if e = h.cfg.Store.RegisterAttempt(ctx, l, a.AttemptId); e != nil {
		return e
	}
	sendCtx, cancel := context.WithDeadline(ctx, deadline)
	var mid int64
	if !h.now().Before(deadline) || sendCtx.Err() != nil {
		e = &telegram.Error{Kind: telegram.NotSent}
	} else {
		mid, e = h.cfg.Sender.Send(sendCtx, a.ChatId, text)
	}
	cancel()
	return h.finish(ctx, l, a, e, mid, id)
}

func requestID(l jobs.Lease) string {
	sum := sha256.Sum256([]byte("notifications/handler/v1/" + l.ID))
	return fmt.Sprintf("%x-%x-%x-%x-%x", sum[:4], sum[4:6], sum[6:8], sum[8:10], sum[10:16])
}
func (h *Handler) wait(ctx context.Context, l jobs.Lease, reason jobs.WaitReason, delay time.Duration) error {
	next := h.cfg.Policy.WaitCondition(h.now(), l.Failures).NextAttemptAt
	if delay > 0 {
		next = h.now().Add(delay)
	}
	if next.IsZero() {
		return errors.New("delivery failure budget exhausted; sending stopped")
	}
	return h.cfg.Store.Park(ctx, l, reason, next)
}
func (h *Handler) schedule(ctx context.Context, l jobs.Lease) error {
	e := h.cfg.Store.ScheduleRecovery(ctx, l.TriggerID, l.Fence, h.cfg.RecoveryInterval)
	// A newer fence/terminal success needs no schedule from this old invocation.
	// Discovery in the scheduler also repairs unscheduled historical blocks.
	if errors.Is(e, jobs.ErrStale) || errors.Is(e, jobs.ErrUnavailable) {
		return nil
	}
	return e
}
func (h *Handler) block(ctx context.Context, l jobs.Lease, r domain.BlockReason, v int64) error {
	return h.persist(ctx, func(ctx context.Context) error {
		if e := h.cfg.Store.ParkBlocked(ctx, l, r, v); e != nil {
			return e
		}
		if r == domain.ChannelUnavailable {
			return h.schedule(ctx, l)
		}
		return nil
	})
}
func (h *Handler) finish(ctx context.Context, l jobs.Lease, a *pb.AttemptPermission, cause error, mid int64, id string) error {
	entropy := rand.Uint64()
	next := h.cfg.Policy.WaitCondition(h.now(), l.Failures).NextAttemptAt
	return h.persist(ctx, func(ctx context.Context) error {
		var e error
		if cause == nil && mid > 0 {
			e = h.cfg.Store.SaveSuccess(ctx, l, a.AttemptId, mid)
		} else if reason, ok := jobs.PermanentReason(cause); ok {
			e = h.cfg.Store.SaveBlocked(ctx, l, a.AttemptId, reason, a.BindingVersion)
			if e == nil && reason == domain.ChannelUnavailable {
				e = h.schedule(ctx, l)
			}
		} else {
			var te *telegram.Error
			if errors.As(cause, &te) && te != nil && (te.Kind == telegram.RateLimited || te.Kind == telegram.Server) {
				_, e = h.cfg.Store.SaveRetryable(ctx, l, a.AttemptId, cause, h.cfg.Policy, entropy, a.BindingVersion)
			} else {
				check := next
				if !check.After(h.now()) {
					check = h.cfg.Policy.WaitCondition(h.now(), l.Failures).NextAttemptAt
				}
				e = h.cfg.Store.SaveUncertain(ctx, l, a.AttemptId, check)
			}
		}
		if e != nil {
			return e
		}
		return h.report(ctx, id)
	})
}
func (h *Handler) recover(ctx context.Context, u *jobs.UnresolvedAttempt) error {
	plan := h.cfg.Policy.WaitCondition(h.now(), u.Lease.Failures)
	var version int64
	if plan.Exhausted {
		// Registration followed a committed admission under this deterministic ID.
		// Recover its immutable permission ONLY for evidence, never for HTTP. Replay
		// precedes CAS in API; a missing/mismatched admission stops recovery safely.
		_, w, e := alerttriggered.Decode(u.Lease.Body)
		if e != nil {
			return e
		}
		r, e := h.cfg.Admission.AdmitDelivery(ctx, w.UserID, w.SubscriptionID, u.Lease.TriggerID, requestID(u.Lease), nil)
		if e != nil {
			return e
		}
		if r == nil || r.WorkflowId != u.Lease.TriggerID || r.SubscriptionId != w.SubscriptionID || r.Attempt == nil || r.Attempt.AttemptId != u.AttemptID {
			return errors.New("cannot recover original permission for exhausted attempt")
		}
		version = r.Attempt.BindingVersion
		plan.NextAttemptAt = h.now().Add(h.cfg.RecoveryInterval)
	}
	return h.persist(ctx, func(ctx context.Context) error {
		next := plan.NextAttemptAt
		// A failed first SQL write may be retried after its proposed deadline. A
		// committed outcome keeps its original deadline through Store idempotency.
		if !next.After(h.now()) {
			next = h.now().Add(h.cfg.RecoveryInterval)
		}
		if e := h.cfg.Store.SaveUncertain(ctx, u.Lease, u.AttemptID, next); e != nil {
			return e
		}
		if plan.Exhausted {
			if e := h.cfg.Store.SaveBlocked(ctx, u.Lease, u.AttemptID, domain.AttemptsExhausted, version); e != nil {
				return e
			}
		}
		return h.report(ctx, u.Lease.TriggerID)
	})
}
func (h *Handler) permission(w alerttriggered.Triggered, l jobs.Lease, r *pb.Recipient, a *pb.AttemptPermission) (string, time.Time, error) {
	invalid := errors.New("invalid or expired delivery permission")
	if a == nil || a.AttemptId == "" || a.Fence <= 0 || a.ChatId != r.ChatId || a.BindingVersion != r.BindingVersion || a.Quote == nil {
		return "", time.Time{}, invalid
	}
	expires, e := time.Parse(time.RFC3339Nano, a.ExpiresAt)
	if e != nil {
		return "", time.Time{}, invalid
	}
	if l.ExpiresAt.Before(expires) {
		expires = l.ExpiresAt
	}
	if !h.now().Before(expires) {
		return "", time.Time{}, invalid
	}
	q := a.Quote
	p, e := price.Parse(q.Price)
	if e != nil {
		return "", time.Time{}, invalid
	}
	trade, e := time.Parse(time.RFC3339Nano, q.TradeTime)
	if e != nil {
		return "", time.Time{}, invalid
	}
	received, e := time.Parse(time.RFC3339Nano, q.ReceivedAt)
	if e != nil || q.Revision == nil {
		return "", time.Time{}, invalid
	}
	observed := priceobserved.Observed{SchemaVersion: 1, Pair: q.Pair, Price: &p, SourceID: q.SourceId, Revision: priceobserved.Revision{StreamID: q.Revision.StreamId, Generation: q.Revision.Generation, Sequence: q.Revision.Sequence}, TradeTime: trade, ReceivedAt: received, StreamState: priceobserved.StreamState(q.StreamState), GapDetected: q.GapDetected}
	if observed.Validate() != nil || q.Pair != w.Pair {
		return "", time.Time{}, invalid
	}
	text, e := message.Format(message.Input{Pair: w.Pair, Threshold: *w.Threshold, Direction: w.Direction, ActualPrice: p, TradeTime: trade, CheckedAt: h.now(), StreamUnconfirmed: q.StreamState == "unconfirmed", GapDetected: q.GapDetected})
	return text, expires, e
}
