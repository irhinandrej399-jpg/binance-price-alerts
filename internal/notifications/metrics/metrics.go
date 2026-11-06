// Package metrics exposes durable notification queue state and admission RPC time.
package metrics

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/handler"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/jobs"
)

type Source interface {
	QueueMetrics(context.Context) (jobs.QueueSnapshot, error)
}

var bounds = [...]float64{.001, .005, .01, .05, .1, .5, 1, 5}

type timing struct {
	count   uint64
	sum     float64
	buckets [len(bounds)]uint64
}
type Metrics struct {
	source Source
	mu     sync.Mutex
	times  [2]timing
	now    func() time.Time
}

// New requires a non-nil Source. Use a bounded HTTP server context/timeout in
// runtime: collection performs SQL and can fail; it never serves cached values.
func New(source Source) *Metrics { return &Metrics{source: source, now: time.Now} }

// Admission wraps the client's final current-price admission call. Each RPC,
// including replay/error, is timed separately using Go's monotonic clock.
// PrepareRecipient is not a price check. No IDs, outcomes or error text are labels.
func (m *Metrics) Admission(next handler.Admission) handler.Admission {
	return &admissionMetrics{m, next}
}

type admissionMetrics struct {
	m    *Metrics
	next handler.Admission
}

func (a *admissionMetrics) PrepareRecipient(ctx context.Context, u, s, t string) (*pb.PrepareRecipientResponse, error) {
	return a.next.PrepareRecipient(ctx, u, s, t)
}
func (a *admissionMetrics) AdmitDelivery(ctx context.Context, u, s, t, r string, recipient ...*pb.Recipient) (*pb.AdmitDeliveryResponse, error) {
	start := a.m.now()
	response, e := a.next.AdmitDelivery(ctx, u, s, t, r, recipient...)
	elapsed := a.m.now().Sub(start).Seconds()
	result := 0
	if e != nil {
		result = 1
	}
	a.m.mu.Lock()
	v := &a.m.times[result]
	v.count++
	v.sum += elapsed
	for i, b := range bounds {
		if elapsed <= b {
			v.buckets[i]++
		}
	}
	a.m.mu.Unlock()
	return response, e
}

// DrainRate counts newly API-acknowledged successful workflows per second.
// Queue shrinkage caused by cancellation or arrivals is not delivery throughput.
// Use snapshots of the SAME database. A decreasing count indicates retention or
// reset and cannot be interpreted as a valid rate.
func DrainRate(a, b jobs.QueueSnapshot) (float64, error) {
	dt := b.At.Sub(a.At).Seconds()
	if dt <= 0 || b.Completed < a.Completed {
		return 0, errors.New("invalid drain interval or evidence reset")
	}
	return float64(b.Completed-a.Completed) / dt, nil
}

func (m *Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	snapshot, e := m.source.QueueMetrics(r.Context())
	if e != nil {
		http.Error(w, "delivery metrics unavailable", http.StatusServiceUnavailable)
		return
	}
	m.mu.Lock()
	times := m.times
	m.mu.Unlock()
	var b bytes.Buffer
	gauge := func(name, help string) { fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s gauge\n", name, help, name) }
	gauge("delivery_backlog", "Unfinished workflows in this database, including waits and blocks.")
	fmt.Fprintf(&b, "delivery_backlog %d\n", snapshot.Backlog)
	gauge("delivery_oldest_seconds", "Creation age of oldest unfinished workflow, zero for empty queue.")
	fmt.Fprintf(&b, "delivery_oldest_seconds %g\n", snapshot.OldestSeconds)
	gauge("delivery_jobs", "Workflows by effective state; success overrides stale local state.")
	for _, state := range []string{"pending", "leased", "retryable", "uncertain", "wait_condition", "wait_limiter", "blocked", "succeeded", "cancelled", "stopped", "delivered", "other"} {
		fmt.Fprintf(&b, "delivery_jobs{state=%q} %d\n", state, snapshot.States[state].Count)
	}
	gauge("delivery_state_oldest_seconds", "Creation age of oldest workflow in each effective state.")
	for _, state := range []string{"pending", "leased", "retryable", "uncertain", "wait_condition", "wait_limiter", "blocked", "succeeded", "cancelled", "stopped", "delivered", "other"} {
		fmt.Fprintf(&b, "delivery_state_oldest_seconds{state=%q} %g\n", state, snapshot.States[state].OldestSeconds)
	}
	gauge("delivery_pending_reports", "Unacknowledged API reports including reports of stopped workflows.")
	for _, kind := range []string{"success", "uncertain", "blocked", "retryable"} {
		fmt.Fprintf(&b, "delivery_pending_reports{kind=%q} %d\n", kind, snapshot.PendingReports[kind])
	}
	gauge("delivery_outcomes", "Durable attempt evidence; categories overlap and survive acknowledgement.")
	for _, kind := range []string{"success", "uncertain", "channel_unavailable", "permanent_failure", "attempts_exhausted", "rate_limited", "server"} {
		fmt.Fprintf(&b, "delivery_outcomes{kind=%q} %d\n", kind, snapshot.Outcomes[kind])
	}
	gauge("delivery_blocks", "Current workflow blocks by reason, excluding successful workflows.")
	for _, reason := range []string{"channel_unavailable", "permanent_failure", "attempts_exhausted"} {
		fmt.Fprintf(&b, "delivery_blocks{reason=%q} %d\n", reason, snapshot.Blocks[reason])
	}
	gauge("delivery_possible_duplicates", "Durable uncertain attempts, not proven external duplicates.")
	fmt.Fprintf(&b, "delivery_possible_duplicates %d\n", snapshot.PossibleDuplicates)
	gauge("delivery_completed", "Distinct workflows with API-acknowledged success; use deriv over a window for drain rate.")
	fmt.Fprintf(&b, "delivery_completed %d\n", snapshot.Completed)
	fmt.Fprintln(&b, "# HELP delivery_admission_rpc_seconds Current-price admission RPC duration including transport and API work; replays are calls too.\n# TYPE delivery_admission_rpc_seconds histogram")
	for i, result := range []string{"response", "error"} {
		v := times[i]
		for j, bound := range bounds {
			fmt.Fprintf(&b, "delivery_admission_rpc_seconds_bucket{result=%q,le=%q} %d\n", result, strconv.FormatFloat(bound, 'g', -1, 64), v.buckets[j])
		}
		fmt.Fprintf(&b, "delivery_admission_rpc_seconds_bucket{result=%q,le=\"+Inf\"} %d\ndelivery_admission_rpc_seconds_sum{result=%q} %g\ndelivery_admission_rpc_seconds_count{result=%q} %d\n", result, v.count, result, v.sum, result, v.count)
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write(b.Bytes())
}
