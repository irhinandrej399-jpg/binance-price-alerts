package subscription_test

import (
	"errors"
	"testing"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
)

func TestTransitions(t *testing.T) {
	// Each fixture is reached through public operations, not an invalid snapshot.
	states := map[subscription.State]subscription.Lifecycle{
		subscription.Active: {},
	}
	pending := apply(t, subscription.Lifecycle{}, subscription.Event{Kind: subscription.Candidate, Condition: subscription.True}).Next
	sending := apply(t, pending, subscription.Event{Kind: subscription.Admit, Condition: subscription.True}).Next
	blocked := apply(t, sending, subscription.Event{Kind: subscription.Block, Reason: subscription.ChannelUnavailable, BindingVersion: 4}).Next
	delivered := apply(t, sending, subscription.Event{Kind: subscription.Success}).Next
	cancelled := apply(t, sending, subscription.Event{Kind: subscription.Cancel}).Next
	states[subscription.PendingDelivery] = pending
	states[subscription.Sending] = sending
	states[subscription.DeliveryBlocked] = blocked
	states[subscription.Delivered] = delivered
	states[subscription.Cancelled] = cancelled

	// Omitted pairs are forbidden. Candidate duplicates cannot roll back sending
	// or blocked; command deduplication for Admit belongs to the API adapter.
	allowed := map[subscription.Kind]map[subscription.State]subscription.State{
		subscription.Candidate: {subscription.Active: subscription.PendingDelivery, subscription.PendingDelivery: subscription.PendingDelivery, subscription.Sending: subscription.Sending, subscription.DeliveryBlocked: subscription.DeliveryBlocked},
		subscription.Admit:     {subscription.Active: subscription.Sending, subscription.PendingDelivery: subscription.Sending},
		subscription.Retry:     {subscription.Sending: subscription.PendingDelivery, subscription.PendingDelivery: subscription.PendingDelivery},
		subscription.Block:     {subscription.Active: subscription.DeliveryBlocked, subscription.PendingDelivery: subscription.DeliveryBlocked, subscription.Sending: subscription.DeliveryBlocked, subscription.DeliveryBlocked: subscription.DeliveryBlocked},
		subscription.Success:   {subscription.Active: subscription.Delivered, subscription.PendingDelivery: subscription.Delivered, subscription.Sending: subscription.Delivered, subscription.DeliveryBlocked: subscription.Delivered, subscription.Delivered: subscription.Delivered, subscription.Cancelled: subscription.Cancelled},
		subscription.Cancel:    {subscription.Active: subscription.Cancelled, subscription.PendingDelivery: subscription.Cancelled, subscription.Sending: subscription.Cancelled, subscription.DeliveryBlocked: subscription.Cancelled, subscription.Delivered: subscription.Delivered, subscription.Cancelled: subscription.Cancelled},
		subscription.Resume:    {subscription.DeliveryBlocked: subscription.PendingDelivery},
	}
	for kind, transitions := range allowed {
		for state, before := range states {
			t.Run(string(state)+"/"+string(kind), func(t *testing.T) {
				version := uint64(4)
				if kind == subscription.Resume {
					version = 5
				}
				event := subscription.Event{Kind: kind, Condition: subscription.True, Reason: subscription.ChannelUnavailable, BindingVersion: version}
				result, err := before.Apply(event)
				want, ok := transitions[state]
				if !ok {
					if !errors.Is(err, subscription.ErrTransition) {
						t.Fatalf("err = %v, want ErrTransition", err)
					}
					if result.Next != before || result.ReleaseSlot || result.AuditSuccess {
						t.Fatalf("rejected event changed lifecycle: %+v", result)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if result.Next.State() != want {
					t.Fatalf("state = %s, want %s", result.Next.State(), want)
				}
				terminalBefore := state == subscription.Delivered || state == subscription.Cancelled
				terminalAfter := want == subscription.Delivered || want == subscription.Cancelled
				if result.ReleaseSlot != (!terminalBefore && terminalAfter) {
					t.Fatalf("ReleaseSlot = %v", result.ReleaseSlot)
				}
				if result.Next.OccupiesSlot() == terminalAfter {
					t.Fatalf("slot occupancy for %s", want)
				}
				if result.AuditSuccess != (kind == subscription.Success) {
					t.Fatalf("AuditSuccess = %v", result.AuditSuccess)
				}
				if before.State() != state {
					t.Fatal("Apply mutated original")
				}
			})
		}
	}
}

func TestWaitConditionKeepsSlot(t *testing.T) {
	for _, kind := range []subscription.Kind{subscription.Candidate, subscription.Admit} {
		pending := apply(t, subscription.Lifecycle{}, subscription.Event{Kind: subscription.Candidate, Condition: subscription.True}).Next
		result := apply(t, pending, subscription.Event{Kind: kind, Condition: subscription.False})
		if result.Next.State() != subscription.Active || !result.Next.OccupiesSlot() || result.ReleaseSlot {
			t.Fatalf("lost condition: %+v", result)
		}
		// Same lifecycle can wait and try again without allocating another slot.
		pending = apply(t, result.Next, subscription.Event{Kind: subscription.Candidate, Condition: subscription.True}).Next
		if !pending.OccupiesSlot() {
			t.Fatal("queue released slot")
		}
	}
}

func TestUnavailablePriceDefers(t *testing.T) {
	active := subscription.Lifecycle{}
	pending := apply(t, active, subscription.Event{Kind: subscription.Candidate, Condition: subscription.True}).Next
	blocked := apply(t, pending, subscription.Event{Kind: subscription.Block, Reason: subscription.ChannelUnavailable, BindingVersion: 4}).Next
	for _, before := range []subscription.Lifecycle{active, pending, blocked} {
		kinds := []subscription.Kind{subscription.Candidate, subscription.Admit}
		if before.State() == subscription.DeliveryBlocked {
			kinds = []subscription.Kind{subscription.Resume}
		}
		for _, kind := range kinds {
			result := apply(t, before, subscription.Event{Kind: kind, Condition: subscription.Unavailable, BindingVersion: 5})
			if result.Next != before || result.ReleaseSlot || result.AuditSuccess {
				t.Fatalf("unavailable quote changed lifecycle: %+v", result)
			}
		}
	}
}

func TestResumeChannel(t *testing.T) {
	for _, reason := range []subscription.BlockReason{subscription.ChannelUnavailable, subscription.AttemptsExhausted, subscription.PermanentFailure} {
		blocked := apply(t, subscription.Lifecycle{}, subscription.Event{Kind: subscription.Block, Reason: reason, BindingVersion: 4}).Next
		for _, version := range []uint64{0, 3, 4, 5} {
			for _, condition := range []subscription.Condition{subscription.False, subscription.True} {
				result, err := blocked.Apply(subscription.Event{Kind: subscription.Resume, BindingVersion: version, Condition: condition})
				permitted := reason == subscription.ChannelUnavailable && version > 4
				if !permitted {
					if !errors.Is(err, subscription.ErrTransition) || result.Next != blocked || result.ReleaseSlot {
						t.Fatalf("unexpected recovery: %+v, %v", result, err)
					}
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
				want := subscription.Active
				if condition == subscription.True {
					want = subscription.PendingDelivery
				}
				if result.Next.State() != want || !result.Next.OccupiesSlot() || result.ReleaseSlot {
					t.Fatalf("recovery: %+v", result)
				}
				if _, _, ok := result.Next.Blocked(); ok {
					t.Fatal("block reason retained after recovery")
				}
			}
		}
		gotReason, gotVersion, ok := blocked.Blocked()
		if !ok || gotReason != reason || gotVersion != 4 {
			t.Fatalf("block metadata: %s/%d/%v", gotReason, gotVersion, ok)
		}
	}
}

func TestCancellationAndLateSuccess(t *testing.T) {
	active := subscription.Lifecycle{}
	pending := apply(t, active, subscription.Event{Kind: subscription.Candidate, Condition: subscription.True}).Next
	sending := apply(t, pending, subscription.Event{Kind: subscription.Admit, Condition: subscription.True}).Next
	cancelled := apply(t, sending, subscription.Event{Kind: subscription.Cancel})
	if !cancelled.ReleaseSlot {
		t.Fatal("cancel did not release slot")
	}
	for range 2 {
		cancelled = apply(t, cancelled.Next, subscription.Event{Kind: subscription.Cancel})
		if cancelled.ReleaseSlot {
			t.Fatal("cancel released slot twice")
		}
		late := apply(t, cancelled.Next, subscription.Event{Kind: subscription.Success})
		if late.Next.State() != subscription.Cancelled || late.ReleaseSlot || !late.AuditSuccess {
			t.Fatalf("late success: %+v", late)
		}
	}
	if _, err := cancelled.Next.Apply(subscription.Event{Kind: subscription.Admit, Condition: subscription.True}); !errors.Is(err, subscription.ErrTransition) {
		t.Fatalf("admission after cancel: %v", err)
	}
	// Late success also finishes a workflow waiting after an ambiguous outcome.
	waiting := apply(t, sending, subscription.Event{Kind: subscription.Retry}).Next
	waiting = apply(t, waiting, subscription.Event{Kind: subscription.Admit, Condition: subscription.False}).Next
	success := apply(t, waiting, subscription.Event{Kind: subscription.Success})
	if success.Next.State() != subscription.Delivered || !success.ReleaseSlot {
		t.Fatalf("late success while waiting: %+v", success)
	}
	repeat := apply(t, success.Next, subscription.Event{Kind: subscription.Success})
	if repeat.ReleaseSlot || repeat.Next != success.Next {
		t.Fatal("duplicate success changed terminal lifecycle")
	}
}

func TestStaleBlockDoesNotReplaceReason(t *testing.T) {
	blocked := apply(t, subscription.Lifecycle{}, subscription.Event{Kind: subscription.Block, Reason: subscription.ChannelUnavailable, BindingVersion: 5}).Next
	for _, event := range []subscription.Event{
		{Kind: subscription.Block, Reason: subscription.ChannelUnavailable, BindingVersion: 4},
		{Kind: subscription.Block, Reason: subscription.PermanentFailure, BindingVersion: 5},
	} {
		result, err := blocked.Apply(event)
		if !errors.Is(err, subscription.ErrTransition) || result.Next != blocked {
			t.Fatalf("replaced block: %+v, %v", result, err)
		}
	}
}

func TestInvalidEvent(t *testing.T) {
	for _, event := range []subscription.Event{
		{Kind: "unknown"},
		{Kind: subscription.Candidate, Condition: "unknown"},
		{Kind: subscription.Admit, Condition: "unknown"},
		{Kind: subscription.Block, Reason: "unknown"},
	} {
		before := subscription.Lifecycle{}
		result, err := before.Apply(event)
		if !errors.Is(err, subscription.ErrEvent) || result.Next != before || result.ReleaseSlot {
			t.Fatalf("invalid event: %+v, %v", result, err)
		}
	}
}

func apply(t *testing.T, before subscription.Lifecycle, event subscription.Event) subscription.Transition {
	t.Helper()
	result, err := before.Apply(event)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

// Compose the lifecycle with the existing price domain exactly as an API
// aggregate would: recovery checks the latest price against stored terms.
func TestRecoveryKeepsPercentageTermsAndWorkflow(t *testing.T) {
	base, err := price.Parse("100")
	if err != nil {
		t.Fatal(err)
	}
	terms, err := price.NewPercentageThreshold(base, "+5")
	if err != nil {
		t.Fatal(err)
	}
	aggregate := struct {
		Lifecycle subscription.Lifecycle
		Terms     price.PercentageThreshold
		Workflow  string
	}{Terms: terms, Workflow: "original-workflow"}
	aggregate.Lifecycle = apply(t, aggregate.Lifecycle, subscription.Event{Kind: subscription.Candidate, Condition: subscription.True}).Next
	for i, quote := range []string{"120", "104", "106"} {
		aggregate.Lifecycle = apply(t, aggregate.Lifecycle, subscription.Event{Kind: subscription.Block, Reason: subscription.ChannelUnavailable, BindingVersion: uint64(4 + i)}).Next
		current, err := price.Parse(quote)
		if err != nil {
			t.Fatal(err)
		}
		condition := subscription.False
		if price.Satisfied(current, aggregate.Terms.Threshold, aggregate.Terms.Direction) {
			condition = subscription.True
		}
		aggregate.Lifecycle = apply(t, aggregate.Lifecycle, subscription.Event{Kind: subscription.Resume, BindingVersion: uint64(5 + i), Condition: condition}).Next
		want := subscription.PendingDelivery
		if quote == "104" {
			want = subscription.Active
		}
		if aggregate.Lifecycle.State() != want {
			t.Fatalf("quote %s: state = %s, want %s", quote, aggregate.Lifecycle.State(), want)
		}
		if aggregate.Workflow != "original-workflow" || aggregate.Terms.Threshold.String() != "105" || aggregate.Terms.Base.String() != "100" || aggregate.Terms.Percent != "+5" || aggregate.Terms.Direction != price.Up {
			t.Fatalf("recovery changed original terms/workflow: %+v", aggregate)
		}
	}
}
