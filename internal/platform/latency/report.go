package latency

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
)

// Expected is the control oracle, frozen independently of the measured outcome.
// One input covers one run and one continuously satisfied interval per subscription.
type Expected struct {
	OwnerID        string `json:"owner_id"`
	SubscriptionID string `json:"subscription_id"`
	Pair           string `json:"pair"`
	FirstAttemptID string `json:"first_attempt_id"`
	FirstEventID   string `json:"first_event_id"`
	FirstSourceID  string `json:"first_source_id"`
	Generation     uint64 `json:"generation"`
}

// Workflow comes from canonical API workflow/outbox results, not from raw t1.
// SubscriptionVersion is the version at workflow creation, not a later state.
type Workflow struct {
	OwnerID             string                 `json:"owner_id"`
	SubscriptionID      string                 `json:"subscription_id"`
	Pair                string                 `json:"pair"`
	SourceID            string                 `json:"source_id"`
	Revision            priceobserved.Revision `json:"revision"`
	TriggerID           string                 `json:"trigger_id"`
	EventID             string                 `json:"event_id"`
	SubscriptionVersion int64                  `json:"subscription_version"`
}

// Complete attests collection of Failures and successful sink flush/close from
// this expected process. A crashed or uncollected process must remain incomplete.
type ProcessStatus struct {
	Name     string `json:"name"`
	Complete bool   `json:"complete"`
	Failures uint64 `json:"failures"`
}

type Input struct {
	Expected     []Expected      `json:"expected"`
	Workflows    []Workflow      `json:"workflows"`
	Processes    []ProcessStatus `json:"processes"`
	Observations []Observation   `json:"observations"`
}

type Sample struct {
	OwnerID        string `json:"owner_id"`
	SubscriptionID string `json:"subscription_id"`
	Nanos          int64  `json:"nanos"`
}

type Missing struct {
	OwnerID        string `json:"owner_id"`
	SubscriptionID string `json:"subscription_id"`
	Reason         string `json:"reason"`
}

type Bucket struct {
	UpperSeconds float64 `json:"upper_seconds"`
	Count        int     `json:"count"` // cumulative, inclusive
}

type Report struct {
	Expected        int       `json:"expected"`
	Missing         int       `json:"missing"`
	Complete        bool      `json:"complete"`
	WithinOneSecond bool      `json:"within_one_second"`
	Samples         []Sample  `json:"samples"`
	MissingDetails  []Missing `json:"missing_details"`
	Issues          []string  `json:"issues"`
	Buckets         []Bucket  `json:"buckets"`
	SumSeconds      float64   `json:"sum_seconds"`
	P50Nanos        *int64    `json:"p50_nanos"`
	P95Nanos        *int64    `json:"p95_nanos"`
	P99Nanos        *int64    `json:"p99_nanos"`
}

type identity struct{ owner, subscription string }

func key(owner, subscription string) identity { return identity{owner, subscription} }
func validRevision(r priceobserved.Revision) bool {
	return r.StreamID != "" && r.Generation > 0 && r.Sequence > 0
}
func atLeast(a, b priceobserved.Revision) bool {
	return validRevision(a) && a.StreamID == b.StreamID && (a.Generation > b.Generation || a.Generation == b.Generation && a.Sequence >= b.Sequence)
}

// Analyze joins raw observations without inferring missing timestamps. Malformed
// or ambiguous oracle data is an error; missing/invalid evidence is a failed
// report with diagnostics. Quantiles are withheld unless the entire run is complete.
func Analyze(in Input) (Report, error) {
	r := Report{Expected: len(in.Expected), Samples: []Sample{}, MissingDetails: []Missing{}, Issues: []string{}}
	if len(in.Expected) == 0 {
		return r, errors.New("latency: nonempty expected oracle required")
	}
	expected := map[identity]Expected{}
	for _, e := range in.Expected {
		k := key(e.OwnerID, e.SubscriptionID)
		if e.OwnerID == "" || e.SubscriptionID == "" || e.Pair == "" || e.FirstAttemptID == "" || e.FirstEventID == "" || e.FirstSourceID == "" || e.Generation == 0 {
			return r, errors.New("latency: incomplete expected oracle")
		}
		if _, ok := expected[k]; ok {
			return r, errors.New("latency: duplicate expected subscription")
		}
		expected[k] = e
	}
	workflows := map[identity]Workflow{}
	triggers, events := map[string]bool{}, map[string]bool{}
	for _, w := range in.Workflows {
		k := key(w.OwnerID, w.SubscriptionID)
		if _, ok := workflows[k]; ok {
			return r, errors.New("latency: duplicate canonical workflow")
		}
		if w.OwnerID == "" || w.SubscriptionID == "" || w.Pair == "" || w.SourceID == "" || w.TriggerID == "" || w.EventID == "" || w.SubscriptionVersion < 1 || !validRevision(w.Revision) {
			return r, errors.New("latency: incomplete canonical workflow")
		}
		if triggers[w.TriggerID] || events[w.EventID] {
			return r, errors.New("latency: reused canonical trigger/event")
		}
		triggers[w.TriggerID] = true
		events[w.EventID] = true
		workflows[k] = w
		if _, ok := expected[k]; !ok {
			r.Issues = append(r.Issues, "unexpected canonical workflow")
		}
	}
	if len(in.Processes) == 0 {
		r.Issues = append(r.Issues, "missing process capture status")
	}
	processes := map[string]bool{}
	for _, p := range in.Processes {
		if p.Name == "" || processes[p.Name] || !p.Complete || p.Failures != 0 {
			r.Issues = append(r.Issues, "incomplete or failed process capture")
		}
		processes[p.Name] = true
	}
	received, accepted := map[string][]Observation{}, map[string][]Observation{}
	committed := map[identity][]Observation{}
	scale := ""
	for _, o := range in.Observations {
		if o.Version != 1 {
			r.Issues = append(r.Issues, "unsupported raw version")
		}
		if _, err := Between(o.At, o.At); err != nil {
			r.Issues = append(r.Issues, "invalid raw stamp")
		}
		if scale == "" {
			scale = o.At.Scale
		}
		if o.At.Scale != scale {
			r.Issues = append(r.Issues, "mixed raw clock scales")
		}
		switch o.Kind {
		case PriceReceived:
			received[o.AttemptID] = append(received[o.AttemptID], o)
		case PriceAccepted:
			accepted[o.AttemptID] = append(accepted[o.AttemptID], o)
		case WorkflowCommitted:
			k := key(o.OwnerID, o.SubscriptionID)
			committed[k] = append(committed[k], o)
			if _, ok := expected[k]; !ok {
				r.Issues = append(r.Issues, "unexpected raw workflow")
			}
		default:
			r.Issues = append(r.Issues, "unknown raw kind")
		}
	}
	for _, e := range in.Expected {
		k := key(e.OwnerID, e.SubscriptionID)
		duration, reason := join(e, workflows[k], received[e.FirstAttemptID], accepted[e.FirstAttemptID], committed[k])
		if reason != "" {
			r.MissingDetails = append(r.MissingDetails, Missing{e.OwnerID, e.SubscriptionID, reason})
			continue
		}
		r.Samples = append(r.Samples, Sample{e.OwnerID, e.SubscriptionID, int64(duration)})
	}
	r.Missing = len(r.MissingDetails)
	// Fixed buckets include the exact one-second target; identities are never labels.
	for _, upper := range []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10} {
		r.Buckets = append(r.Buckets, Bucket{UpperSeconds: upper})
	}
	values := make([]int64, 0, len(r.Samples))
	for _, s := range r.Samples {
		seconds := float64(s.Nanos) / float64(time.Second)
		r.SumSeconds += seconds
		values = append(values, s.Nanos)
		for j := range r.Buckets {
			if seconds <= r.Buckets[j].UpperSeconds {
				r.Buckets[j].Count++
			}
		}
	}
	r.Complete = r.Missing == 0 && len(r.Issues) == 0
	if r.Complete {
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		quantile := func(percent int) *int64 { // exact nearest-rank; no histogram interpolation
			rank := len(values)/100*percent + (len(values)%100*percent+99)/100
			v := values[rank-1]
			return &v
		}
		r.P50Nanos = quantile(50)
		r.P95Nanos = quantile(95)
		r.P99Nanos = quantile(99)
		r.WithinOneSecond = *r.P99Nanos <= int64(time.Second)
	}
	return r, nil
}

func join(e Expected, w Workflow, receipts, acceptances, commits []Observation) (time.Duration, string) {
	if len(receipts) != 1 {
		return 0, "missing or ambiguous first receipt"
	}
	if len(acceptances) != 1 {
		return 0, "missing or ambiguous first acceptance"
	}
	if len(commits) != 1 {
		return 0, "missing or ambiguous workflow commit"
	}
	a, b, c := receipts[0], acceptances[0], commits[0]
	matches := func(o Observation) bool {
		return o.Version == 1 && o.AttemptID == e.FirstAttemptID && o.Pair == e.Pair && o.SourceID == e.FirstSourceID && o.EventID == e.FirstEventID && o.Generation == e.Generation
	}
	if !matches(a) || !matches(b) || !validRevision(b.Revision) || b.Revision.Generation != e.Generation {
		return 0, "first price identity mismatch"
	}
	if w.OwnerID != e.OwnerID || w.SubscriptionID != e.SubscriptionID || w.Pair != e.Pair {
		return 0, "missing canonical workflow or pair mismatch"
	}
	if c.Revision == b.Revision && c.SourceID != b.SourceID {
		return 0, "same revision has different source identity"
	}
	if c.Version != 1 || c.Pair != w.Pair || c.SourceID != w.SourceID || c.TriggerID != w.TriggerID || c.EventID != w.EventID || c.SubscriptionVersion != w.SubscriptionVersion || c.Revision != w.Revision || !atLeast(c.Revision, b.Revision) {
		return 0, "canonical workflow mismatch or older revision"
	}
	if _, err := Between(a.At, b.At); err != nil {
		return 0, "invalid receipt/acceptance clock"
	}
	if _, err := Between(b.At, c.At); err != nil {
		return 0, "invalid acceptance/commit clock"
	}
	d, err := Between(a.At, c.At)
	if err != nil {
		return 0, "invalid receipt/commit clock"
	}
	return d, ""
}

// Prometheus exports only fixed metric names and numeric le labels. A partial
// histogram remains visible, but complete=0 and absent quantiles forbid an SLA pass.
func (r Report) Prometheus() string {
	var b strings.Builder
	b.WriteString("# HELP price_to_workflow_seconds Time from first control price receipt to durable workflow/outbox commit.\n# TYPE price_to_workflow_seconds histogram\n")
	for _, v := range r.Buckets {
		fmt.Fprintf(&b, "price_to_workflow_seconds_bucket{le=%q} %d\n", strconv.FormatFloat(v.UpperSeconds, 'g', -1, 64), v.Count)
	}
	fmt.Fprintf(&b, "price_to_workflow_seconds_bucket{le=\"+Inf\"} %d\nprice_to_workflow_seconds_sum %s\nprice_to_workflow_seconds_count %d\n", len(r.Samples), strconv.FormatFloat(r.SumSeconds, 'g', -1, 64), len(r.Samples))
	gauges := []struct {
		name  string
		value int
	}{{"expected", r.Expected}, {"missing", r.Missing}, {"issues", len(r.Issues)}, {"complete", boolInt(r.Complete)}, {"within_one_second", boolInt(r.WithinOneSecond)}}
	for _, g := range gauges {
		fmt.Fprintf(&b, "# TYPE price_to_workflow_%s gauge\nprice_to_workflow_%s %d\n", g.name, g.name, g.value)
	}
	for _, q := range []struct {
		name  string
		value *int64
	}{{"p50", r.P50Nanos}, {"p95", r.P95Nanos}, {"p99", r.P99Nanos}} {
		if q.value != nil {
			fmt.Fprintf(&b, "# TYPE price_to_workflow_%s_seconds gauge\nprice_to_workflow_%s_seconds %s\n", q.name, q.name, strconv.FormatFloat(float64(*q.value)/float64(time.Second), 'g', -1, 64))
		}
	}
	return b.String()
}
func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
