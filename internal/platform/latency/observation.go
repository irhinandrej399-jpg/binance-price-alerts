// Package latency records opt-in raw measurement boundaries, never business state.
package latency

import (
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
)

// Stamp survives JSON encoding. Scale identifies the kernel boot and clock kind;
// Nanos is a kernel clock reading, not a serialized Go time.Time or Unix time.
type Stamp struct {
	Scale string `json:"scale"`
	Nanos int64  `json:"nanos"`
}

func Between(start, end Stamp) (time.Duration, error) {
	if start.Scale == "" || end.Scale != start.Scale || start.Nanos < 0 || end.Nanos < start.Nanos {
		return 0, errors.New("latency: missing, incomparable or backward clock")
	}
	return time.Duration(end.Nanos - start.Nanos), nil
}

const (
	PriceReceived     = "price_received"
	PriceAccepted     = "price_accepted"
	WorkflowCommitted = "workflow_committed"
)

// Observation v1 is raw data; identities must never become metric labels.
// PriceAccepted links the assigned durable revision to the Receive attempt.
// WorkflowCommitted is emitted only by the transaction that creates the workflow.
type Observation struct {
	Version             int                    `json:"version"`
	Kind                string                 `json:"kind"`
	At                  Stamp                  `json:"at"`
	AttemptID           string                 `json:"attempt_id,omitempty"`
	Pair                string                 `json:"pair"`
	SourceID            string                 `json:"source_id"`
	EventID             string                 `json:"event_id,omitempty"`
	Generation          uint64                 `json:"generation,omitempty"`
	Revision            priceobserved.Revision `json:"revision"`
	OwnerID             string                 `json:"owner_id,omitempty"`
	SubscriptionID      string                 `json:"subscription_id,omitempty"`
	SubscriptionVersion int64                  `json:"subscription_version,omitempty"`
	TriggerID           string                 `json:"trigger_id,omitempty"`
}

type Clock func() (Stamp, error)
type Sink func(Observation) error

// Recorder is concurrency-safe; the supplied clock and sink must be concurrency-
// safe and bounded. Calls are synchronous and their overhead is part of latency.
// Sink/clock failures are counted independently of business results. The runner
// must check Failures on every process as well as expected raw identities.
type Recorder struct {
	clock    Clock
	sink     Sink
	mu       sync.Mutex
	failures uint64
	last     error
}

func New(clock Clock, sink Sink) (*Recorder, error) {
	if clock == nil || sink == nil {
		return nil, errors.New("latency: clock and sink required")
	}
	return &Recorder{clock: clock, sink: sink}, nil
}
func (r *Recorder) Failures() (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failures, r.last
}
func (r *Recorder) fail(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failures++
	r.last = err
}
func (r *Recorder) emit(o Observation) {
	if r == nil {
		return
	}
	s, e := r.clock()
	if e == nil && (s.Scale == "" || s.Nanos < 0) {
		e = errors.New("latency: invalid clock stamp")
	}
	if e != nil {
		r.fail(e)
		return
	}
	o.Version = 1
	o.At = s
	if e = r.sink(o); e != nil {
		r.fail(fmt.Errorf("latency: sink: %w", e))
	}
}

// Receipt is a per-call correlation handle, including failed and duplicate calls.
// It is not proof of durable acceptance; require a matching PriceAccepted record.
type Receipt struct{ observation Observation }

func (r *Recorder) Receive(pair, source, event string, gen uint64) Receipt {
	if r == nil {
		return Receipt{}
	}
	o := Observation{Kind: PriceReceived, AttemptID: rand.Text(), Pair: pair, SourceID: source, EventID: event, Generation: gen}
	r.emit(o)
	return Receipt{o}
}
func (r *Recorder) Accepted(receipt Receipt, revision priceobserved.Revision) {
	if r == nil {
		return
	}
	o := receipt.observation
	o.Kind = PriceAccepted
	o.Revision = revision
	r.emit(o)
}
func (r *Recorder) Committed(owner, subscription, trigger, event string, version int64, q priceobserved.Observed) {
	r.emit(Observation{Kind: WorkflowCommitted, OwnerID: owner, SubscriptionID: subscription, TriggerID: trigger, EventID: event, SubscriptionVersion: version, Pair: q.Pair, SourceID: q.SourceID, Revision: q.Revision})
}
