// Package subscription models the one-shot lifecycle from DEC-004/005.
// It performs no I/O and owns neither quote retrieval nor durable delivery work.
package subscription

import "errors"

var (
	ErrTransition = errors.New("subscription: transition not allowed")
	ErrEvent      = errors.New("subscription: invalid event")
)

type State string

const (
	Active          State = "active"
	PendingDelivery State = "pending_delivery"
	Sending         State = "sending"
	DeliveryBlocked State = "delivery_blocked"
	Delivered       State = "delivered"
	Cancelled       State = "cancelled"
)

type Kind string

const (
	Candidate Kind = "candidate"
	Admit     Kind = "admit"
	Retry     Kind = "retry"
	Block     Kind = "block"
	Success   Kind = "success"
	Cancel    Kind = "cancel"
	Resume    Kind = "resume"
)

// Condition is the result of checking the latest valid quote against the
// original fixed threshold. Unavailable defers work without changing state.
type Condition string

const (
	Unavailable Condition = ""
	False       Condition = "false"
	True        Condition = "true"
)

type BlockReason string

const (
	// ChannelUnavailable includes missing binding, blocked bot and invalid chat.
	ChannelUnavailable BlockReason = "channel_unavailable"
	AttemptsExhausted  BlockReason = "attempts_exhausted"
	PermanentFailure   BlockReason = "permanent_failure"
)

// Event describes already validated domain input. Condition is used only by
// Candidate, Admit and Resume; Reason by Block; BindingVersion by Block/Resume.
// Resume requires an existing binding with a version newer than the failed one.
// Classify channel-related failures as ChannelUnavailable even when retries
// have been exhausted; other exhaustion cannot be cleared by a new binding.
type Event struct {
	Kind           Kind
	Condition      Condition
	Reason         BlockReason
	BindingVersion uint64
}

// Lifecycle is a value; the zero value is active and occupies one slot.
// Threshold, creation snapshot and workflow ID belong to the enclosing
// subscription and never enter this state machine, including during Resume.
// Copies and Apply do not mutate the original value.
type Lifecycle struct {
	state          State
	reason         BlockReason
	bindingVersion uint64
}

func (l Lifecycle) State() State {
	if l.state == "" {
		return Active
	}
	return l.state
}

func (l Lifecycle) OccupiesSlot() bool {
	return l.State() != Delivered && l.State() != Cancelled
}

func (l Lifecycle) Blocked() (BlockReason, uint64, bool) {
	return l.reason, l.bindingVersion, l.State() == DeliveryBlocked
}

// Transition describes effects for the caller's atomic persistence operation.
// ReleaseSlot is true only on the first transition to a terminal state.
// AuditSuccess asks the caller to record a confirmed result, including after
// cancellation; audit records must be deduplicated by workflow/attempt ID.
// Replaying the same Success never releases another slot.
type Transition struct {
	Next         Lifecycle
	ReleaseSlot  bool
	AuditSuccess bool
}

// Apply models allowed transitions, not the full admission/result protocol.
// The caller must validate workflow/attempt identity, binding and subscription
// versions, leases/fencing and command deduplication before applying an event.
// In particular Success must refer to a known admitted attempt; delayed failure
// must not replace a newer attempt or a recovered binding. Admit from sending
// is rejected; a replayed command must restore its previously saved response.
//
// Retry also represents an expired/ambiguous attempt eligible for a fresh
// check under the accepted duplicate-risk policy. A verified late success is
// allowed from any state, but never revives cancelled. Cancellation cannot
// revoke an already admitted external HTTP request. No transition generates
// a new workflow; the caller keeps the existing job when returning to active.
func (l Lifecycle) Apply(e Event) (Transition, error) {
	unchanged := Transition{Next: l}
	if !valid(e) {
		return unchanged, ErrEvent
	}
	state := l.State()
	next := l
	switch e.Kind {
	case Cancel:
		if l.OccupiesSlot() {
			next = Lifecycle{state: Cancelled}
		}
	case Success:
		if state != Cancelled {
			next = Lifecycle{state: Delivered}
		}
	case Candidate:
		switch state {
		case Active, PendingDelivery:
			next = checked(l, e.Condition, PendingDelivery)
		case Sending, DeliveryBlocked:
			// A duplicate candidate cannot reset admission or a blocked workflow.
		default:
			return unchanged, ErrTransition
		}
	case Admit:
		if state != Active && state != PendingDelivery {
			return unchanged, ErrTransition
		}
		next = checked(l, e.Condition, Sending)
	case Retry:
		if state != Sending && state != PendingDelivery {
			return unchanged, ErrTransition
		}
		next = Lifecycle{state: PendingDelivery}
	case Block:
		if !l.OccupiesSlot() {
			return unchanged, ErrTransition
		}
		if state == DeliveryBlocked && (e.Reason != l.reason || e.BindingVersion != l.bindingVersion) {
			return unchanged, ErrTransition
		}
		next = Lifecycle{state: DeliveryBlocked, reason: e.Reason, bindingVersion: e.BindingVersion}
	case Resume:
		if state != DeliveryBlocked || l.reason != ChannelUnavailable || e.BindingVersion <= l.bindingVersion {
			return unchanged, ErrTransition
		}
		next = checked(l, e.Condition, PendingDelivery)
	}
	return Transition{
		Next:         next,
		ReleaseSlot:  l.OccupiesSlot() && !next.OccupiesSlot(),
		AuditSuccess: e.Kind == Success,
	}, nil
}

func checked(l Lifecycle, condition Condition, satisfied State) Lifecycle {
	switch condition {
	case True:
		return Lifecycle{state: satisfied}
	case False:
		return Lifecycle{state: Active}
	default:
		return l
	}
}

func valid(e Event) bool {
	switch e.Kind {
	case Candidate, Admit, Resume:
		return e.Condition == Unavailable || e.Condition == True || e.Condition == False
	case Block:
		return e.Reason == ChannelUnavailable || e.Reason == AttemptsExhausted || e.Reason == PermanentFailure
	case Cancel, Success, Retry:
		return true
	default:
		return false
	}
}
