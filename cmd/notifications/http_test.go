package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/health"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/jobs"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/metrics"
)

type queueSource func(context.Context) (jobs.QueueSnapshot, error)

func (f queueSource) QueueMetrics(ctx context.Context) (jobs.QueueSnapshot, error) { return f(ctx) }

func TestMetricsCollectionFailure(t *testing.T) {
	for _, mode := range []string{"error", "cancelled", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			probe := health.New(nil, nil, time.Second)
			source := queueSource(func(ctx context.Context) (jobs.QueueSnapshot, error) {
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > 2*time.Second {
					t.Error("SQL collection lacks bounded context")
					return jobs.QueueSnapshot{}, errors.New("missing deadline")
				}
				if mode == "error" {
					return jobs.QueueSnapshot{Backlog: 999}, errors.New("postgres://private:secret@db SQL")
				}
				<-ctx.Done()
				return jobs.QueueSnapshot{Backlog: 999}, ctx.Err()
			})
			h := notificationsHTTP(probe, metrics.New(source))
			req := httptest.NewRequest("GET", "/metrics", nil)
			if mode == "cancelled" {
				ctx, cancel := context.WithCancel(req.Context())
				cancel()
				req = req.WithContext(ctx)
			}
			w := httptest.NewRecorder()
			started := time.Now()
			h.ServeHTTP(w, req)
			if w.Code != 503 || w.Body.String() != "delivery metrics unavailable\n" {
				t.Fatal(w)
			}
			if time.Since(started) > 4*time.Second {
				t.Fatal("collection exceeded budget")
			}
			for _, tc := range []struct {
				method, path string
				code         int
			}{
				{"GET", "/healthz", 200}, {"GET", "/readyz", 503}, {"POST", "/metrics", 405},
			} {
				w := httptest.NewRecorder()
				h.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
				if w.Code != tc.code {
					t.Fatalf("%s %s: %d", tc.method, tc.path, w.Code)
				}
			}
		})
	}
}

func TestSupervisorCancelsActiveCollection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, exited := make(chan struct{}), make(chan struct{})
	source := queueSource(func(ctx context.Context) (jobs.QueueSnapshot, error) {
		close(entered)
		<-ctx.Done()
		defer close(exited)
		return jobs.QueueSnapshot{}, ctx.Err()
	})
	probe := health.New(func(context.Context) error { return nil }, func() bool { return true }, time.Second)
	done := make(chan error, 1)
	go func() {
		done <- supervise(ctx, listener, probe, notificationsHTTP(probe, metrics.New(source)), func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
	}()
	response := make(chan error, 1)
	go func() {
		resp, err := (&http.Client{Timeout: 3 * time.Second}).Get("http://" + listener.Addr().String() + "/metrics")
		if err == nil {
			defer resp.Body.Close()
			body, e := io.ReadAll(resp.Body)
			err = e
			if resp.StatusCode != 503 || string(body) != "delivery metrics unavailable\n" {
				err = errors.New("unsafe shutdown collection response")
			}
		}
		response <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("collection did not enter")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-exited:
		default:
			t.Fatal("SQL collection not joined")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not join")
	}
	if err := <-response; err != nil {
		t.Fatal(err)
	}
}
