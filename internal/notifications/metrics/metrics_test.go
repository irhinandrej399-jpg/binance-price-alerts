package metrics

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/jobs"
)

type source struct {
	m   jobs.QueueSnapshot
	err error
}

func (s source) QueueMetrics(context.Context) (jobs.QueueSnapshot, error) { return s.m, s.err }

type admission struct{ err error }

func (a admission) PrepareRecipient(context.Context, string, string, string) (*pb.PrepareRecipientResponse, error) {
	return nil, a.err
}
func (a admission) AdmitDelivery(context.Context, string, string, string, string, ...*pb.Recipient) (*pb.AdmitDeliveryResponse, error) {
	return &pb.AdmitDeliveryResponse{Outcome: "private-id"}, a.err
}

func TestExportBoundedLabelsFailureAndTiming(t *testing.T) {
	m := jobs.QueueSnapshot{Backlog: 3, Completed: 2, OldestSeconds: 12, States: map[string]jobs.QueueState{"private-id": {Count: 999}, "wait_condition": {Count: 2, OldestSeconds: 12}}, Outcomes: map[string]int64{"rate_limited": 4, "private-error": 99}, PendingReports: map[string]int64{"success": 1}}
	h := New(source{m: m})
	a := h.Admission(admission{})
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			a.AdmitDelivery(context.Background(), "user", "sub", "trigger", "request")
			a.PrepareRecipient(context.Background(), "user", "sub", "trigger")
		})
	}
	wg.Wait()
	a = h.Admission(admission{err: errors.New("secret")})
	a.AdmitDelivery(context.Background(), "u", "s", "t", "r")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	body := w.Body.String()
	for _, want := range []string{"delivery_backlog 3", "delivery_oldest_seconds 12", `delivery_jobs{state="wait_condition"} 2`, `delivery_outcomes{kind="rate_limited"} 4`, `delivery_admission_rpc_seconds_count{result="response"} 10`, `delivery_admission_rpc_seconds_count{result="error"} 1`, `delivery_admission_rpc_seconds_bucket{result="response",le="+Inf"} 10`} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %s in %s", want, body)
		}
	}
	for _, secret := range []string{"private", "secret", "trigger", "request"} {
		if strings.Contains(body, secret) {
			t.Fatal(body)
		}
	}
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	bad := New(source{m: m, err: errors.New("secret")})
	w = httptest.NewRecorder()
	bad.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if w.Code != 503 || strings.Contains(w.Body.String(), "secret") || strings.Contains(w.Body.String(), "delivery_backlog") {
		t.Fatal(w)
	}
}
func TestDrainRate(t *testing.T) {
	a := jobs.QueueSnapshot{At: time.Unix(100, 0), Completed: 2}
	b := jobs.QueueSnapshot{At: time.Unix(104, 0), Completed: 4}
	r, e := DrainRate(a, b)
	if e != nil || r != 0.5 {
		t.Fatal(r, e)
	}
	for _, b := range []jobs.QueueSnapshot{{At: a.At, Completed: 5}, {At: a.At.Add(time.Second), Completed: 1}} {
		if _, e = DrainRate(a, b); e == nil {
			t.Fatal("invalid interval/reset accepted")
		}
	}
}

func TestAdmissionHistogramExactBuckets(t *testing.T) {
	h := New(source{})
	base := time.Unix(100, 0)
	stamps := []time.Time{base, base.Add(5 * time.Millisecond), base, base.Add(6 * time.Second)}
	h.now = func() time.Time { v := stamps[0]; stamps = stamps[1:]; return v }
	a := h.Admission(admission{})
	for range 2 {
		if _, e := a.AdmitDelivery(context.Background(), "u", "s", "t", "r"); e != nil {
			t.Fatal(e)
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	for _, want := range []string{
		`delivery_admission_rpc_seconds_bucket{result="response",le="0.001"} 0`,
		`delivery_admission_rpc_seconds_bucket{result="response",le="0.005"} 1`,
		`delivery_admission_rpc_seconds_bucket{result="response",le="5"} 1`,
		`delivery_admission_rpc_seconds_bucket{result="response",le="+Inf"} 2`,
		`delivery_admission_rpc_seconds_sum{result="response"} 6.005`,
		`delivery_jobs{state="pending"} 0`,
		`delivery_oldest_seconds 0`,
	} {
		if !strings.Contains(w.Body.String(), want) {
			t.Fatalf("missing %s in %s", want, w.Body.String())
		}
	}
}
