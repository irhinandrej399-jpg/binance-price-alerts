package stream

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/catalog"
)

type planCommand struct {
	Method string   `json:"method"`
	Params []string `json:"params"`
	ID     int64    `json:"id"`
}
type planServer struct {
	mu       sync.Mutex
	next     int
	active   map[string]int
	log      []planCommand
	reject   bool
	drop     bool
	shutdown bool
}

func newPlanServer(t *testing.T, reject, drop, shutdown bool) (*planServer, string) {
	t.Helper()
	state := &planServer{active: map[string]int{}, reject: reject, drop: drop, shutdown: shutdown}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		state.mu.Lock()
		state.next++
		owner := state.next
		state.mu.Unlock()
		defer func() {
			state.mu.Lock()
			defer state.mu.Unlock()
			for name, n := range state.active {
				if n == owner {
					delete(state.active, name)
				}
			}
		}()
		c.SetReadDeadline(time.Now().Add(5 * time.Second))
		for {
			var cmd planCommand
			if err := c.ReadJSON(&cmd); err != nil {
				return
			}
			state.mu.Lock()
			state.log = append(state.log, cmd)
			if state.reject {
				state.reject = false
				state.mu.Unlock()
				c.WriteJSON(map[string]any{"id": cmd.ID, "code": 2, "msg": "reject once"})
				continue
			}
			for _, name := range cmd.Params {
				if cmd.Method == "SUBSCRIBE" {
					if previous, ok := state.active[name]; ok && previous != owner {
						t.Errorf("duplicate stream %s on %d and %d", name, previous, owner)
					}
					state.active[name] = owner
				} else if cmd.Method == "UNSUBSCRIBE" {
					if state.active[name] == owner {
						delete(state.active, name)
					}
				} else {
					t.Errorf("method %s", cmd.Method)
				}
			}
			drop := state.drop
			state.drop = false
			shutdown := state.shutdown
			state.shutdown = false
			if drop { // Model a lost ACK after the exchange applied the request.
				state.mu.Unlock()
				return
			}
			err = c.WriteJSON(map[string]any{"result": nil, "id": cmd.ID})
			if shutdown && err == nil {
				err = c.WriteJSON(map[string]any{"e": "serverShutdown"})
			}
			state.mu.Unlock()
			if err != nil {
				return
			}
		}
	}))
	t.Cleanup(s.Close)
	return state, "ws" + strings.TrimPrefix(s.URL, "http") + "/ws"
}
func (s *planServer) view() (int, []planCommand, map[string]int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := map[string]int{}
	for k, v := range s.active {
		a[k] = v
	}
	return s.next, append([]planCommand(nil), s.log...), a
}
func snapshot(names ...string) catalog.Snapshot {
	s := catalog.Snapshot{}
	for _, n := range names {
		s.Pairs = append(s.Pairs, catalog.Pair{Symbol: n})
	}
	return s
}
func reconcilerForTest(t *testing.T, endpoint string, limit int, handle func(context.Context, []byte) error) *Reconciler {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	r, err := NewReconciler(ctx, ReconcileConfig{Endpoint: endpoint, Limit: limit, Transport: DefaultConfig(), Retry: RetryConfig{time.Millisecond, 2 * time.Millisecond}, AckTimeout: time.Second}, handle, func(int, error, time.Duration) {})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	return r
}
func TestReconcileCatalogDifferenceAndNoop(t *testing.T) {
	s, url := newPlanServer(t, false, false, false)
	r := reconcilerForTest(t, url, 2, func(context.Context, []byte) error { return nil })
	apply := func(names ...string) {
		t.Helper()
		if err := r.Reconcile(context.Background(), snapshot(names...)); err != nil {
			t.Fatal(err)
		}
	}
	apply("BTCUSDT", "ETHUSDT", "BTCUSDT")
	before, commands, _ := s.view()
	apply("ETHUSDT", "BTCUSDT")
	after, again, _ := s.view()
	if before != after || !reflect.DeepEqual(commands, again) {
		t.Fatal("identical catalog changed connections")
	}
	apply("BTCUSDT", "SOLUSDT")
	_, commands, active := s.view()
	if len(commands) != 3 || commands[1].Method != "UNSUBSCRIBE" || !reflect.DeepEqual(commands[1].Params, []string{"ethusdt@trade"}) || commands[2].Method != "SUBSCRIBE" || !reflect.DeepEqual(commands[2].Params, []string{"solusdt@trade"}) {
		t.Fatalf("commands: %+v", commands)
	}
	if len(active) != 2 || active["btcusdt@trade"] == 0 || active["solusdt@trade"] == 0 {
		t.Fatal(active)
	}
	apply()
	_, _, active = s.view()
	if len(active) != 0 {
		t.Fatal(active)
	}
}
func TestReconcileMovesAcrossGroupsWithoutDuplicates(t *testing.T) {
	s, url := newPlanServer(t, false, false, false)
	r := reconcilerForTest(t, url, 1, func(context.Context, []byte) error { return nil })
	for _, names := range [][]string{{"ETHUSDT", "SOLUSDT"}, {"BTCUSDT", "ETHUSDT", "SOLUSDT"}, {"SOLUSDT"}, {}} {
		if err := r.Reconcile(context.Background(), snapshot(names...)); err != nil {
			t.Fatal(err)
		}
		_, _, active := s.view()
		if len(active) != len(names) {
			t.Fatalf("active %v want %v", active, names)
		}
	}
}
func TestReconcileRetriesCurrentPlan(t *testing.T) {
	for _, mode := range []string{"reject", "lost-ack", "shutdown"} {
		t.Run(mode, func(t *testing.T) {
			s, url := newPlanServer(t, mode == "reject", mode == "lost-ack", mode == "shutdown")
			r := reconcilerForTest(t, url, 2, func(context.Context, []byte) error { return nil })
			if err := r.Reconcile(context.Background(), snapshot("BTCUSDT")); err != nil {
				t.Fatal(err)
			}
			if err := r.Reconcile(context.Background(), snapshot("ETHUSDT")); err != nil {
				t.Fatal(err)
			}
			_, _, active := s.view()
			if len(active) != 1 || active["ethusdt@trade"] == 0 {
				t.Fatal(active)
			}
			r.Close()
			n, _, _ := s.view()
			if n < 2 {
				t.Fatalf("no retry: %d", n)
			}
		})
	}
}
func TestReconcileValidationAndCancellation(t *testing.T) {
	s, url := newPlanServer(t, false, false, false)
	r := reconcilerForTest(t, url, 2, func(context.Context, []byte) error { return nil })
	if err := r.Reconcile(context.Background(), snapshot("")); err == nil {
		t.Fatal("accepted invalid catalog")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Reconcile(ctx, snapshot("BTCUSDT")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	n, _, _ := s.view()
	if n != 0 {
		t.Fatal("invalid input dialed")
	}
	r.Close()
	if err := r.Reconcile(context.Background(), snapshot("BTCUSDT")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestReconcileLostRemovalACKRestoresUpdatedPlan(t *testing.T) {
	s, url := newPlanServer(t, false, false, false)
	r := reconcilerForTest(t, url, 2, func(context.Context, []byte) error { return nil })
	if err := r.Reconcile(context.Background(), snapshot("BTCUSDT", "ETHUSDT")); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.drop = true
	s.mu.Unlock()
	if err := r.Reconcile(context.Background(), snapshot("ETHUSDT", "SOLUSDT")); err != nil {
		t.Fatal(err)
	}
	n, log, active := s.view()
	if n != 2 || len(log) != 4 || log[1].Method != "UNSUBSCRIBE" || !reflect.DeepEqual(log[2].Params, []string{"ethusdt@trade"}) || log[2].Method != "SUBSCRIBE" || !reflect.DeepEqual(log[3].Params, []string{"solusdt@trade"}) {
		t.Fatalf("connections %d commands %+v", n, log)
	}
	if len(active) != 2 || active["btcusdt@trade"] != 0 || active["ethusdt@trade"] == 0 || active["solusdt@trade"] == 0 {
		t.Fatal(active)
	}
}

func TestReconcileBadACKRetriesAndDoesNotReachHandler(t *testing.T) {
	for _, mode := range []string{"timeout", "wrong-id", "missing-result", "error-no-id"} {
		t.Run(mode, func(t *testing.T) {
			var mu sync.Mutex
			attempts := 0
			delivered := make(chan string, 4)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				c, err := (&websocket.Upgrader{}).Upgrade(w, req, nil)
				if err != nil {
					return
				}
				defer c.Close()
				mu.Lock()
				attempts++
				n := attempts
				mu.Unlock()
				var cmd planCommand
				if c.ReadJSON(&cmd) != nil {
					return
				}
				if n == 1 {
					switch mode {
					case "timeout":
					case "wrong-id":
						c.WriteJSON(map[string]any{"result": nil, "id": cmd.ID + 1})
					case "missing-result":
						c.WriteJSON(map[string]any{"id": cmd.ID})
					case "error-no-id":
						c.WriteJSON(map[string]any{"code": 2, "msg": "invalid"})
					}
				} else {
					c.WriteJSON(map[string]any{"result": nil, "id": cmd.ID})
					c.WriteMessage(websocket.TextMessage, []byte(`{"e":"trade","t":42}`))
				}
				c.ReadMessage()
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			r, err := NewReconciler(ctx, ReconcileConfig{Endpoint: "ws" + strings.TrimPrefix(server.URL, "http") + "/ws", Limit: 2, Transport: DefaultConfig(), Retry: RetryConfig{time.Millisecond, time.Millisecond}, AckTimeout: 30 * time.Millisecond}, func(_ context.Context, b []byte) error { delivered <- string(b); return nil }, func(int, error, time.Duration) {})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			if err := r.Reconcile(ctx, snapshot("BTCUSDT")); err != nil {
				t.Fatal(err)
			}
			if got := receive(t, delivered); got != `{"e":"trade","t":42}` {
				t.Fatal(got)
			}
			mu.Lock()
			n := attempts
			mu.Unlock()
			if n != 2 {
				t.Fatalf("attempts %d", n)
			}
		})
	}
}

func TestReconcileTerminalHandlerAndClosePending(t *testing.T) {
	for _, mode := range []string{"handler", "close", "resume"} {
		t.Run(mode, func(t *testing.T) {
			command := make(chan struct{}, 1)
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				c, err := (&websocket.Upgrader{}).Upgrade(w, req, nil)
				if err != nil {
					return
				}
				defer c.Close()
				var cmd planCommand
				if c.ReadJSON(&cmd) != nil {
					return
				}
				command <- struct{}{}
				if mode == "handler" {
					c.WriteMessage(websocket.TextMessage, []byte(`{"e":"trade"}`))
				} else if mode == "resume" {
					<-release
					c.WriteJSON(map[string]any{"id": cmd.ID, "result": nil})
				}
				c.ReadMessage()
			}))
			defer server.Close()
			failure := errors.New("durable accept failed")
			r := reconcilerForTest(t, "ws"+strings.TrimPrefix(server.URL, "http")+"/ws", 2, func(context.Context, []byte) error { return failure })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- r.Reconcile(ctx, snapshot("BTCUSDT")) }()
			receive(t, command)
			switch mode {
			case "handler":
				if err := receive(t, done); !errors.Is(err, failure) {
					t.Fatal(err)
				}
			case "close":
				r.Close()
				if err := receive(t, done); !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case "resume":
				cancel()
				if err := receive(t, done); !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				close(release)
				if err := r.Reconcile(context.Background(), snapshot("BTCUSDT")); err != nil {
					t.Fatal(err)
				}
			}
			r.Close()
		})
	}
}

func TestReconcileRejectsInvalidConfigBeforeDial(t *testing.T) {
	valid := ReconcileConfig{Endpoint: "ws://localhost/ws?timeUnit=MICROSECOND", Limit: 1024, Transport: DefaultConfig(), Retry: DefaultRetryConfig(), AckTimeout: time.Second}
	handle := func(context.Context, []byte) error { return nil }
	observer := func(int, error, time.Duration) {}
	for _, mutate := range []func(*ReconcileConfig){
		func(c *ReconcileConfig) { c.Endpoint = "ws://localhost/ws/btcusdt@trade" },
		func(c *ReconcileConfig) { c.Endpoint = "ws://localhost/ws?streams=btcusdt@trade" },
		func(c *ReconcileConfig) { c.Endpoint = "ws://localhost/ws?timeUnit=bad" },
		func(c *ReconcileConfig) { c.Endpoint = "http://localhost/ws" },
		func(c *ReconcileConfig) { c.Limit = 0 },
		func(c *ReconcileConfig) { c.Transport.Messages = 6 },
		func(c *ReconcileConfig) { c.Retry.MaxBackoff = 0 },
		func(c *ReconcileConfig) { c.AckTimeout = 0 },
	} {
		c := valid
		mutate(&c)
		if r, err := NewReconciler(context.Background(), c, handle, observer); err == nil {
			r.Close()
			t.Fatalf("accepted %+v", c)
		}
	}
	if r, err := NewReconciler(context.Background(), valid, nil, observer); err == nil {
		r.Close()
		t.Fatal("nil handler")
	}
	if r, err := NewReconciler(context.Background(), valid, handle, nil); err == nil {
		r.Close()
		t.Fatal("nil observer")
	}
	r, err := NewReconciler(context.Background(), valid, handle, observer)
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
}

func TestReconcileRestoresLatestPlanWithoutAnotherCall(t *testing.T) {
	firstSocket := make(chan *websocket.Conn, 1)
	replay := make(chan planCommand, 1)
	var mu sync.Mutex
	connections := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		c, err := (&websocket.Upgrader{}).Upgrade(w, req, nil)
		if err != nil {
			return
		}
		defer c.Close()
		mu.Lock()
		connections++
		n := connections
		mu.Unlock()
		if n == 1 {
			firstSocket <- c
		}
		for {
			var cmd planCommand
			if c.ReadJSON(&cmd) != nil {
				return
			}
			if err := c.WriteJSON(map[string]any{"result": nil, "id": cmd.ID}); err != nil {
				return
			}
			if n > 1 {
				replay <- cmd
			}
		}
	}))
	defer server.Close()
	r := reconcilerForTest(t, "ws"+strings.TrimPrefix(server.URL, "http")+"/ws", 2, func(context.Context, []byte) error { return nil })
	if err := r.Reconcile(context.Background(), snapshot("BTCUSDT")); err != nil {
		t.Fatal(err)
	}
	socket := receive(t, firstSocket)
	if err := r.Reconcile(context.Background(), snapshot("ETHUSDT")); err != nil {
		t.Fatal(err)
	}
	socket.Close()
	restored := receive(t, replay)
	if restored.Method != "SUBSCRIBE" || !reflect.DeepEqual(restored.Params, []string{"ethusdt@trade"}) {
		t.Fatalf("restored stale plan: %+v", restored)
	}
	r.Close()
	mu.Lock()
	n := connections
	mu.Unlock()
	if n != 2 {
		t.Fatalf("connections %d", n)
	}
}

func preparedForTest(t *testing.T, endpoint string, prepare PrepareGroupAttempt) *Reconciler {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	r, err := NewPreparedReconciler(ctx, ReconcileConfig{Endpoint: endpoint, Limit: 2, Transport: DefaultConfig(), Retry: RetryConfig{time.Millisecond, 2 * time.Millisecond}, AckTimeout: time.Second}, prepare)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	return r
}

func TestPreparedReconcileLifecycle(t *testing.T) {
	for _, mode := range []string{"disconnect", "rotation"} {
		t.Run(mode, func(t *testing.T) {
			s, endpoint := newPlanServer(t, false, mode == "disconnect", mode == "rotation")
			var mu sync.Mutex
			generations := 0
			observations := make(chan int, 8)
			prepared := make(chan int, 8)
			r := preparedForTest(t, endpoint, func(ctx context.Context, group int, streams []string) (AttemptCallbacks, error) {
				n, _, _ := s.view()
				mu.Lock()
				defer mu.Unlock()
				generations++
				token := generations
				prepared <- token
				if n != token-1 {
					t.Errorf("prepare after dial: sockets=%d token=%d", n, token)
				}
				if group != 0 || !reflect.DeepEqual(streams, []string{"btcusdt@trade"}) {
					t.Errorf("plan %d %v", group, streams)
				}
				streams[0] = "caller mutation"
				return AttemptCallbacks{Handle: func(context.Context, []byte) error { return nil }, Disconnected: func(ctx context.Context, err error, d time.Duration) error { observations <- token; return nil }}, nil
			})
			if err := r.Reconcile(context.Background(), snapshot("BTCUSDT")); err != nil {
				t.Fatal(err)
			}
			if token := receive(t, observations); token != 1 {
				t.Fatal(token)
			}
			// Observe preparation of the reconnect independently of ACK scheduling.
			if n := receive(t, prepared); n != 1 {
				t.Fatal(n)
			}
			if n := receive(t, prepared); n != 2 {
				t.Fatal(n)
			}
			if err := r.Reconcile(context.Background(), snapshot("BTCUSDT")); err != nil {
				t.Fatal(err)
			}
			r.Close()
			mu.Lock()
			defer mu.Unlock()
			if generations < 2 {
				t.Fatal("no new generation")
			}
		})
	}
}

func TestPreparedReconcilePreparationFailure(t *testing.T) {
	s, endpoint := newPlanServer(t, false, false, false)
	failure := errors.New("generation write failed")
	r := preparedForTest(t, endpoint, func(context.Context, int, []string) (AttemptCallbacks, error) { return AttemptCallbacks{}, failure })
	if err := r.Reconcile(context.Background(), snapshot("BTCUSDT")); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if err := r.Wait(); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if n, _, _ := s.view(); n != 0 {
		t.Fatal("dial without token")
	}
}

func TestPreparedReconcilePlanChangeJoinsOldCallbacks(t *testing.T) {
	s, endpoint := newPlanServer(t, false, false, false)
	var mu sync.Mutex
	var plans [][]string
	stopped := 0
	r := preparedForTest(t, endpoint, func(ctx context.Context, group int, streams []string) (AttemptCallbacks, error) {
		mu.Lock()
		defer mu.Unlock()
		if len(plans) > stopped {
			t.Error("previous callbacks not joined")
		}
		plans = append(plans, append([]string(nil), streams...))
		return AttemptCallbacks{Handle: func(context.Context, []byte) error { return nil }, Disconnected: func(ctx context.Context, err error, d time.Duration) error {
			mu.Lock()
			stopped++
			mu.Unlock()
			return nil
		}}, nil
	})
	for _, names := range [][]string{{"BTCUSDT"}, {"BTCUSDT"}, {"BTCUSDT", "ETHUSDT"}, {"ETHUSDT"}, {}} {
		if err := r.Reconcile(context.Background(), snapshot(names...)); err != nil {
			t.Fatal(err)
		}
	}
	r.Close()
	mu.Lock()
	defer mu.Unlock()
	want := [][]string{{"btcusdt@trade"}, {"btcusdt@trade", "ethusdt@trade"}, {"ethusdt@trade"}}
	if !reflect.DeepEqual(plans, want) || stopped != 3 {
		t.Fatalf("plans %v stopped %d", plans, stopped)
	}
	_, _, active := s.view()
	if len(active) != 0 {
		t.Fatal(active)
	}
}

func TestPreparedReconcileObserverFailureAfterReturn(t *testing.T) {
	socket := make(chan *websocket.Conn, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		c, err := (&websocket.Upgrader{}).Upgrade(w, req, nil)
		if err != nil {
			return
		}
		defer c.Close()
		var cmd planCommand
		if c.ReadJSON(&cmd) != nil {
			return
		}
		c.WriteJSON(map[string]any{"id": cmd.ID, "result": nil})
		socket <- c
		c.ReadMessage()
	}))
	defer server.Close()
	entered := make(chan struct{})
	release := make(chan struct{})
	failure := errors.New("gap write failed")
	r := preparedForTest(t, "ws"+strings.TrimPrefix(server.URL, "http")+"/ws", func(context.Context, int, []string) (AttemptCallbacks, error) {
		return AttemptCallbacks{Handle: func(context.Context, []byte) error { return nil }, Disconnected: func(ctx context.Context, err error, d time.Duration) error {
			close(entered)
			select {
			case <-release:
				return failure
			case <-ctx.Done():
				return ctx.Err()
			}
		}}, nil
	})
	if err := r.Reconcile(context.Background(), snapshot("BTCUSDT")); err != nil {
		t.Fatal(err)
	}
	receive(t, socket).Close()
	receive(t, entered)
	close(release)
	if err := r.Wait(); !errors.Is(err, failure) {
		t.Fatal(err)
	}
}

func TestPreparedReconcileCloseCancelsPreparation(t *testing.T) {
	_, endpoint := newPlanServer(t, false, false, false)
	entered := make(chan struct{})
	exited := make(chan struct{})
	r := preparedForTest(t, endpoint, func(ctx context.Context, _ int, _ []string) (AttemptCallbacks, error) {
		close(entered)
		<-ctx.Done()
		close(exited)
		return AttemptCallbacks{}, ctx.Err()
	})
	done := make(chan error, 1)
	go func() { done <- r.Reconcile(context.Background(), snapshot("BTCUSDT")) }()
	receive(t, entered)
	r.Close()
	receive(t, exited)
	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestPreparedReconcileDataBoundToAttempt(t *testing.T) {
	sockets := make(chan *websocket.Conn, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		c, err := (&websocket.Upgrader{}).Upgrade(w, req, nil)
		if err != nil {
			return
		}
		defer c.Close()
		var cmd planCommand
		if c.ReadJSON(&cmd) != nil {
			return
		}
		c.WriteJSON(map[string]any{"id": cmd.ID, "result": nil})
		sockets <- c
		c.WriteMessage(websocket.TextMessage, []byte(`{"e":"trade"}`))
		c.ReadMessage()
	}))
	defer server.Close()
	endpoint := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws"
	var token int
	data := make(chan int, 2)
	retired := make(chan int, 2)
	failure := errors.New("accept failed on second generation")
	r := preparedForTest(t, endpoint, func(context.Context, int, []string) (AttemptCallbacks, error) {
		token++
		bound := token
		return AttemptCallbacks{Handle: func(context.Context, []byte) error {
			data <- bound
			if bound == 2 {
				return failure
			}
			return nil
		}, Disconnected: func(context.Context, error, time.Duration) error { retired <- bound; return nil }}, nil
	})
	if err := r.Reconcile(context.Background(), snapshot("BTCUSDT")); err != nil {
		t.Fatal(err)
	}
	if n := receive(t, data); n != 1 {
		t.Fatal(n)
	}
	receive(t, sockets).Close()
	if n := receive(t, retired); n != 1 {
		t.Fatal(n)
	}
	if n := receive(t, data); n != 2 {
		t.Fatal(n)
	}
	if err := r.Wait(); !errors.Is(err, failure) {
		t.Fatal(err)
	}
}

func TestPreparedReconcileCloseJoinsObserver(t *testing.T) {
	_, endpoint := newPlanServer(t, false, true, false)
	entered := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	r := preparedForTest(t, endpoint, func(context.Context, int, []string) (AttemptCallbacks, error) {
		return AttemptCallbacks{Handle: func(context.Context, []byte) error { return nil }, Disconnected: func(ctx context.Context, _ error, _ time.Duration) error {
			close(entered)
			<-ctx.Done()
			close(canceled)
			<-release
			return ctx.Err()
		}}, nil
	})
	done := make(chan error, 1)
	go func() { done <- r.Reconcile(context.Background(), snapshot("BTCUSDT")) }()
	receive(t, entered)
	closed := make(chan struct{})
	go func() { r.Close(); close(closed) }()
	receive(t, canceled)
	select {
	case <-closed:
		t.Fatal("Close returned before observer")
	default:
	}
	close(release)
	receive(t, closed)
	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestPreparedReconcileMovesGroups(t *testing.T) {
	s, endpoint := newPlanServer(t, false, false, false)
	r := preparedForTest(t, endpoint, func(context.Context, int, []string) (AttemptCallbacks, error) {
		return AttemptCallbacks{Handle: func(context.Context, []byte) error { return nil }, Disconnected: func(context.Context, error, time.Duration) error { return nil }}, nil
	})
	for _, names := range [][]string{{"ETHUSDT", "SOLUSDT"}, {"BTCUSDT", "ETHUSDT", "SOLUSDT"}, {"SOLUSDT"}, {}} {
		if err := r.Reconcile(context.Background(), snapshot(names...)); err != nil {
			t.Fatal(err)
		}
		_, _, active := s.view()
		if len(active) != len(names) {
			t.Fatalf("active %v want %v", active, names)
		}
	}
}

func TestPreparedReconcileRetirementFailurePreventsNewOwner(t *testing.T) {
	s, endpoint := newPlanServer(t, false, false, false)
	failure := fmt.Errorf("durable retirement: %w", context.Canceled)
	r := preparedForTest(t, endpoint, func(context.Context, int, []string) (AttemptCallbacks, error) {
		return AttemptCallbacks{Handle: func(context.Context, []byte) error { return nil }, Disconnected: func(context.Context, error, time.Duration) error { return failure }}, nil
	})
	if err := r.Reconcile(context.Background(), snapshot("BTCUSDT")); err != nil {
		t.Fatal(err)
	}
	if err := r.Reconcile(context.Background(), snapshot("ETHUSDT")); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if err := r.Wait(); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if n, _, _ := s.view(); n != 1 {
		t.Fatalf("new owner dialed despite retirement failure: %d", n)
	}
}

func TestPreparedReconcileFailedHandshakeThenPreparationFailure(t *testing.T) {
	handshakes := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handshakes <- struct{}{}
		http.Error(w, "offline", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	failure := errors.New("second generation failed")
	var attempts int
	r := preparedForTest(t, "ws"+strings.TrimPrefix(server.URL, "http")+"/ws", func(context.Context, int, []string) (AttemptCallbacks, error) {
		attempts++
		if attempts == 2 {
			return AttemptCallbacks{}, failure
		}
		return AttemptCallbacks{Handle: func(context.Context, []byte) error { return nil }, Disconnected: func(context.Context, error, time.Duration) error { return nil }}, nil
	})
	if err := r.Reconcile(context.Background(), snapshot("BTCUSDT")); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if err := r.Wait(); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	receive(t, handshakes)
	select {
	case <-handshakes:
		t.Fatal("failed preparation dialed")
	default:
	}
}

func TestPreparedReconcileInvalidCallbacksAndCanceledOwner(t *testing.T) {
	for _, mode := range []string{"nil-handler", "nil-observer", "canceled", "nil-prepare"} {
		t.Run(mode, func(t *testing.T) {
			s, endpoint := newPlanServer(t, false, false, false)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			prepare := PrepareGroupAttempt(func(context.Context, int, []string) (AttemptCallbacks, error) {
				calls++
				callbacks := AttemptCallbacks{Handle: func(context.Context, []byte) error { return nil }, Disconnected: func(context.Context, error, time.Duration) error { return nil }}
				if mode == "nil-handler" {
					callbacks.Handle = nil
				}
				if mode == "nil-observer" {
					callbacks.Disconnected = nil
				}
				return callbacks, nil
			})
			if mode == "canceled" {
				cancel()
			}
			if mode == "nil-prepare" {
				prepare = nil
			}
			r, err := NewPreparedReconciler(ctx, ReconcileConfig{Endpoint: endpoint, Limit: 2, Transport: DefaultConfig(), Retry: DefaultRetryConfig(), AckTimeout: time.Second}, prepare)
			if mode == "nil-prepare" {
				if err == nil {
					r.Close()
					t.Fatal("nil prepare accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			if err := r.Reconcile(context.Background(), snapshot("BTCUSDT")); err == nil {
				t.Fatal("invalid lifecycle accepted")
			}
			if err := r.Wait(); err == nil {
				t.Fatal("missing terminal error")
			}
			if n, _, _ := s.view(); n != 0 {
				t.Fatal("invalid lifecycle dialed")
			}
			if mode == "canceled" && calls != 0 {
				t.Fatal("prepared canceled owner")
			}
		})
	}
}
