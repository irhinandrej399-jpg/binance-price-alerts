package latency_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/latency"
)

func controlInput() latency.Input {
	rev := priceobserved.Revision{StreamID: "trade", Generation: 1, Sequence: 1}
	later := rev
	later.Sequence = 2
	stamp := func(ms int64) latency.Stamp {
		return latency.Stamp{Scale: "control/raw", Nanos: ms * int64(time.Millisecond)}
	}
	return latency.Input{
		Expected:  []latency.Expected{{OwnerID: "owner-secret", SubscriptionID: "sub-secret", Pair: "BTCUSDT", FirstAttemptID: "first", FirstEventID: "price-1", FirstSourceID: "1", Generation: 1}},
		Processes: []latency.ProcessStatus{{Name: "market-data", Complete: true}, {Name: "api", Complete: true}},
		Workflows: []latency.Workflow{{OwnerID: "owner-secret", SubscriptionID: "sub-secret", Pair: "BTCUSDT", SourceID: "2", Revision: later, TriggerID: "trigger-secret", EventID: "work-event", SubscriptionVersion: 2}},
		Observations: []latency.Observation{
			{Version: 1, Kind: latency.PriceReceived, At: stamp(10), AttemptID: "first", Pair: "BTCUSDT", SourceID: "1", EventID: "price-1", Generation: 1},
			{Version: 1, Kind: latency.PriceAccepted, At: stamp(50), AttemptID: "first", Pair: "BTCUSDT", SourceID: "1", EventID: "price-1", Generation: 1, Revision: rev},
			{Version: 1, Kind: latency.PriceReceived, At: stamp(110), AttemptID: "later", Pair: "BTCUSDT", SourceID: "2", EventID: "price-2", Generation: 1},
			{Version: 1, Kind: latency.PriceAccepted, At: stamp(120), AttemptID: "later", Pair: "BTCUSDT", SourceID: "2", EventID: "price-2", Generation: 1, Revision: later},
			{Version: 1, Kind: latency.WorkflowCommitted, At: stamp(210), OwnerID: "owner-secret", SubscriptionID: "sub-secret", TriggerID: "trigger-secret", EventID: "work-event", SubscriptionVersion: 2, Pair: "BTCUSDT", SourceID: "2", Revision: later},
		},
	}
}

func TestReportFirstPriceBucketsAndExport(t *testing.T) {
	in := controlInput()
	// Serialize the independently collected process observations before joining.
	b, _ := json.Marshal(in)
	if e := json.Unmarshal(b, &in); e != nil {
		t.Fatal(e)
	}
	r, e := latency.Analyze(in)
	if e != nil {
		t.Fatal(e)
	}
	if !r.Complete || !r.WithinOneSecond || r.Missing != 0 || len(r.Samples) != 1 || r.Samples[0].Nanos != int64(200*time.Millisecond) {
		t.Fatalf("%+v", r)
	}
	if r.P99Nanos == nil || *r.P99Nanos != int64(200*time.Millisecond) {
		t.Fatal(r)
	}
	text := r.Prometheus()
	for _, want := range []string{`price_to_workflow_seconds_bucket{le="0.1"} 0`, `price_to_workflow_seconds_bucket{le="0.25"} 1`, `price_to_workflow_seconds_bucket{le="1"} 1`, `price_to_workflow_seconds_count 1`, `price_to_workflow_complete 1`} {
		if !strings.Contains(text, want) {
			t.Fatal(want, text)
		}
	}
	for _, secret := range []string{"owner-secret", "sub-secret", "trigger-secret", "BTCUSDT", "chat_id", "user_id", "subscription_id"} {
		if strings.Contains(text, secret) {
			t.Fatal("identity leaked", text)
		}
	}
}

func TestReportMissingAndInvalidBoundaries(t *testing.T) {
	cases := map[string]func(*latency.Input){
		"missing receive":            func(i *latency.Input) { i.Observations = i.Observations[1:] },
		"missing acceptance":         func(i *latency.Input) { i.Observations = append(i.Observations[:1], i.Observations[2:]...) },
		"missing t1 after recovery":  func(i *latency.Input) { i.Observations = i.Observations[:4] },
		"duplicate receive":          func(i *latency.Input) { i.Observations = append(i.Observations, i.Observations[0]) },
		"duplicate acceptance":       func(i *latency.Input) { i.Observations = append(i.Observations, i.Observations[1]) },
		"duplicate t1 retry":         func(i *latency.Input) { i.Observations = append(i.Observations, i.Observations[4]) },
		"wrong first source":         func(i *latency.Input) { i.Observations[0].SourceID = "other" },
		"wrong generation":           func(i *latency.Input) { i.Observations[1].Revision.Generation = 2 },
		"foreign clock":              func(i *latency.Input) { i.Observations[4].At.Scale = "foreign" },
		"backward clock":             func(i *latency.Input) { i.Observations[4].At.Nanos = 0 },
		"accept after t1":            func(i *latency.Input) { i.Observations[1].At.Nanos = int64(time.Second) },
		"wrong trigger":              func(i *latency.Input) { i.Observations[4].TriggerID = "other" },
		"wrong outbox":               func(i *latency.Input) { i.Observations[4].EventID = "other" },
		"old revision":               func(i *latency.Input) { i.Observations[4].Revision.Sequence = 0 },
		"missing canonical workflow": func(i *latency.Input) { i.Workflows = nil },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			in := controlInput()
			mutate(&in)
			r, e := latency.Analyze(in)
			if e != nil {
				t.Fatal(e)
			}
			if r.Complete || r.WithinOneSecond || r.Missing != 1 || r.P99Nanos != nil || len(r.Samples) != 0 {
				t.Fatalf("%+v", r)
			}
		})
	}
}

func TestReportCompletenessAndPercentiles(t *testing.T) {
	in := controlInput()
	in.Expected = append(in.Expected, latency.Expected{OwnerID: "owner-secret", SubscriptionID: "missing", Pair: "BTCUSDT", FirstAttemptID: "first", FirstEventID: "price-1", FirstSourceID: "1", Generation: 1})
	r, e := latency.Analyze(in)
	if e != nil {
		t.Fatal(e)
	}
	if r.Complete || r.Missing != 1 || len(r.Samples) != 1 || r.P99Nanos != nil || !strings.Contains(r.Prometheus(), "price_to_workflow_missing 1") {
		t.Fatal(r)
	}
	for _, mutate := range []func(*latency.Input){
		func(i *latency.Input) { i.Processes = nil },
		func(i *latency.Input) { i.Processes[0].Failures = 1 },
		func(i *latency.Input) { i.Processes[0].Complete = false },
		func(i *latency.Input) {
			o := i.Observations[4]
			o.SubscriptionID = "unexpected"
			i.Observations = append(i.Observations, o)
		},
		func(i *latency.Input) {
			w := i.Workflows[0]
			w.SubscriptionID = "unexpected"
			w.TriggerID = "another-trigger"
			w.EventID = "another-event"
			i.Workflows = append(i.Workflows, w)
		},
		func(i *latency.Input) { i.Observations[2].Version = 99 },
	} {
		v := controlInput()
		mutate(&v)
		r, e := latency.Analyze(v)
		if e != nil {
			t.Fatal(e)
		}
		if r.Complete || r.P99Nanos != nil || len(r.Issues) == 0 {
			t.Fatal(r)
		}
	}
	// An actually slow complete sample must fail the target, not completeness.
	in = controlInput()
	in.Observations[4].At.Nanos = int64(2010 * time.Millisecond)
	r, e = latency.Analyze(in)
	if e != nil || !r.Complete || r.WithinOneSecond || r.P99Nanos == nil || *r.P99Nanos != int64(2*time.Second) {
		t.Fatal(r, e)
	}
}

func TestReportRejectsAmbiguousOracle(t *testing.T) {
	for _, mutate := range []func(*latency.Input){
		func(i *latency.Input) { i.Expected = nil },
		func(i *latency.Input) { i.Expected = append(i.Expected, i.Expected[0]) },
		func(i *latency.Input) { i.Expected[0].FirstAttemptID = "" },
		func(i *latency.Input) { i.Workflows = append(i.Workflows, i.Workflows[0]) },
	} {
		in := controlInput()
		mutate(&in)
		if _, e := latency.Analyze(in); e == nil {
			t.Fatal("accepted ambiguous oracle")
		}
	}
}

func TestReportNearestRankAndTargetBoundary(t *testing.T) {
	in := controlInput()
	base := in.Expected[0]
	w := in.Workflows[0]
	commit := in.Observations[4]
	in.Expected = nil
	in.Workflows = nil
	in.Observations = in.Observations[:4]
	for n := 0; n < 100; n++ {
		id := fmt.Sprint(n)
		e := base
		e.SubscriptionID = id
		in.Expected = append(in.Expected, e)
		canonical := w
		canonical.SubscriptionID = id
		canonical.TriggerID = "trigger" + id
		canonical.EventID = "work" + id
		in.Workflows = append(in.Workflows, canonical)
		o := commit
		o.SubscriptionID = id
		o.TriggerID = canonical.TriggerID
		o.EventID = canonical.EventID
		o.At.Nanos = int64(1010 * time.Millisecond)
		if n == 99 {
			o.At.Nanos = int64(5010 * time.Millisecond)
		}
		in.Observations = append(in.Observations, o)
	}
	r, e := latency.Analyze(in)
	if e != nil || !r.Complete || !r.WithinOneSecond || *r.P50Nanos != int64(time.Second) || *r.P95Nanos != int64(time.Second) || *r.P99Nanos != int64(time.Second) {
		t.Fatal(r, e)
	}
	if !strings.Contains(r.Prometheus(), `price_to_workflow_seconds_bucket{le="1"} 99`) {
		t.Fatal(r.Prometheus())
	}
	in.Observations[len(in.Observations)-2].At.Nanos = int64(5010 * time.Millisecond)
	r, e = latency.Analyze(in)
	if e != nil || !r.Complete || r.WithinOneSecond || *r.P99Nanos != int64(5*time.Second) {
		t.Fatal(r, e)
	}
}
