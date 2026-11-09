// Command notifications wires durable delivery components. Migrations run separately.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/admissionrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/resultrpc"
	progresspb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/notifications/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/botlimit"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/chatlimit"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/handler"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/health"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/intake"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/jobs"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/metrics"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/progressrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/report"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/retry"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/telegram"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/worker"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/postgres"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
)

func run(parent context.Context, c config) error {
	read := func(path string) ([]byte, error) {
		b, e := os.ReadFile(path)
		if e != nil {
			return nil, errors.New("notifications: credentials unavailable")
		}
		return b, nil
	}
	cert, e := read(c.cert)
	if e != nil {
		return e
	}
	key, e := read(c.key)
	if e != nil {
		return e
	}
	ca, e := read(c.ca)
	if e != nil {
		return e
	}
	pair, e := tls.X509KeyPair(cert, key)
	if e != nil {
		return errors.New("notifications: invalid credentials")
	}
	leaf, e := x509.ParseCertificate(pair.Certificate[0])
	if e != nil || len(leaf.URIs) != 1 || leaf.URIs[0].String() != c.identity {
		return errors.New("notifications: invalid service identity")
	}
	creds, e := serviceauth.ClientCredentials(cert, key, ca)
	if e != nil {
		return errors.New("notifications: invalid credentials")
	}
	api, e := grpc.NewClient(c.api, grpc.WithTransportCredentials(creds), grpc.WithAuthority(c.serverName))
	if e != nil {
		return errors.New("notifications: RPC startup failed")
	}
	defer api.Close()
	startup, stop := context.WithTimeout(parent, 15*time.Second)
	pool, e := postgres.Open(startup, c.dsn)
	stop()
	if e != nil {
		return errors.New("notifications: database startup failed")
	}
	defer pool.Close()
	intakeStore := intake.NewStore(pool)
	startup, stop = context.WithTimeout(parent, 15*time.Second)
	e = validateIntake(startup, c, intakeStore)
	stop()
	if e != nil {
		return errors.New("notifications: intake identity startup failed")
	}
	store, e := jobs.New(pool, c.leaseTTL)
	if e != nil {
		return errConfig
	}
	bot, e := botlimit.New(pool, c.botID, c.botRate)
	if e != nil {
		return errConfig
	}
	chat, e := chatlimit.New(pool, c.botID, c.chatInterval)
	if e != nil {
		return errConfig
	}
	sender, e := telegram.New(c.telegram)
	if e != nil {
		return errConfig
	}
	reporter, e := report.New(pool, resultrpc.NewClient(api))
	if e != nil {
		return errConfig
	}
	policy, e := retry.New(c.retry)
	if e != nil {
		return errConfig
	}
	observability := metrics.New(store)
	h, e := handler.New(handler.Config{Store: store, Admission: observability.Admission(admissionrpc.NewClient(api)), Bot: bot, Chat: chat, Sender: sender, Reporter: reporter, Policy: policy, RecoveryInterval: c.recoveryInterval, ReportLimit: c.reportLimit})
	if e != nil {
		return errConfig
	}
	w, e := worker.New(worker.Config{Source: store, Handler: h, Concurrency: c.concurrency, Interval: c.interval, Timeout: c.timeout, WriteTimeout: c.writeTimeout, RecoveryInterval: c.recoveryInterval})
	if e != nil {
		return errConfig
	}
	consumer, e := intake.New(intake.Config{Brokers: c.brokers, Topic: c.topic, Group: c.group, Binding: c.binding}, intakeStore)
	if e != nil {
		return errConfig
	}
	defer consumer.Close()
	listener, e := net.Listen("tcp", c.addr)
	if e != nil {
		return errors.New("notifications: HTTP bind failed")
	}
	defer listener.Close()
	rpcCert, e := read(c.rpcCert)
	if e != nil {
		return e
	}
	rpcKey, e := read(c.rpcKey)
	if e != nil {
		return e
	}
	opts, e := progressrpc.ServerOptions(rpcCert, rpcKey, ca, c.cleanerURI)
	if e != nil {
		return errors.New("notifications: invalid exporter credentials")
	}
	rpcListener, e := net.Listen("tcp", c.rpcAddr)
	if e != nil {
		return errors.New("notifications: RPC bind failed")
	}
	defer rpcListener.Close()
	rpcServer := grpc.NewServer(opts...)
	progresspb.RegisterNotificationsProgressServer(rpcServer, progressrpc.NewServer(intakeStore))
	defer rpcServer.Stop()
	serveRPC := func(ctx context.Context) error {
		ctx, stopRPC := context.WithCancel(ctx)
		defer stopRPC()
		joined := make(chan struct{})
		go func() { defer close(joined); <-ctx.Done(); rpcServer.Stop() }()
		err := rpcServer.Serve(rpcListener)
		// A terminal Serve failure must also release the cancellation waiter.
		stopRPC()
		<-joined
		if errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		return err
	}
	probe := health.New(pool.Ping, w.Running, time.Second)
	return supervise(parent, listener, probe, notificationsHTTP(probe, observability), consumer.Run, w.Run, serveRPC)
}

// Validate every persisted partition before exposing HTTP or starting workers.
// Provisioning is separate: a restart/member never creates SQL provenance.
func validateIntake(ctx context.Context, c config, store *intake.Store) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(c.brokers...))
	if err != nil {
		return err
	}
	defer client.Close()
	parts, err := c.binding.Source.Verify(ctx, client)
	if err != nil {
		return err
	}
	if len(parts) != 16 {
		return intake.ErrConfig
	}
	seen := map[int32]bool{}
	for _, part := range parts {
		p := part.Partition
		if p < 0 || p >= 16 || seen[p] {
			return intake.ErrConfig
		}
		seen[p] = true
		if _, err := store.ReadProgress(ctx, c.binding, p); err != nil {
			return err
		}
	}
	return nil
}

// notificationsHTTP shares the internal health listener. The SQL collection
// deadline is independent of socket write timeouts and inherits request/shutdown
// cancellation. The exporter buffers the complete snapshot and sanitizes errors.
func notificationsHTTP(probe *health.Handler, observability *metrics.Metrics) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /healthz", probe)
	mux.Handle("GET /readyz", probe)
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		observability.ServeHTTP(w, r.WithContext(ctx))
	})
	return mux
}

// supervise owns component cancellation and joins. Resources remain owned by
// run until all callbacks and active HTTP requests have returned.
func supervise(parent context.Context, listener net.Listener, probe *health.Handler, httpHandler http.Handler, components ...func(context.Context) error) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	server := &http.Server{Handler: httpHandler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
	defer server.Close()
	// Each component withdraws readiness before reporting completion. All results
	// are joined before the deferred Kafka, SQL and RPC resources are closed.
	results := make(chan error, len(components)+1)
	launch := func(f func(context.Context) error) {
		go func() { e := f(ctx); probe.SetInitialized(false); results <- e }()
	}
	probe.SetInitialized(true)
	for _, component := range components {
		launch(component)
	}
	launch(func(context.Context) error { return server.Serve(listener) })
	remaining := len(components) + 1
	componentFailed := false
	record := func(err error) {
		var pending interface {
			error
			Retry(context.Context) error
		}
		if errors.As(err, &pending) || (err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, http.ErrServerClosed)) {
			componentFailed = true
		}
	}
	select {
	case <-parent.Done():
	case err := <-results:
		remaining--
		record(err)
	}
	probe.SetInitialized(false)
	cancel()
	httpCtx, stopHTTP := context.WithTimeout(context.Background(), 2*time.Second)
	httpErr := server.Shutdown(httpCtx)
	stopHTTP()
	if httpErr != nil {
		_ = server.Close()
	}
	for i := 0; i < remaining; i++ {
		record(<-results)
	}
	if httpErr != nil {
		return errors.New("notifications: HTTP cleanup incomplete")
	}
	if parent.Err() != nil && !componentFailed {
		return nil
	}
	return errors.New("notifications: required component stopped")
}

// Bound the entire signal drain, including final writes and deferred Close calls.
func execute(c config) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	finished := make(chan struct{})
	defer close(finished)
	go func() {
		if shutdownExpired(ctx, finished, 20*time.Second) {
			fmt.Fprintln(os.Stderr, "notifications: shutdown deadline exceeded; durable work requires recovery")
			os.Exit(1)
		}
	}()
	return run(ctx, c)
}

// The drain budget starts at cancellation, not process startup.
func shutdownExpired(ctx context.Context, finished <-chan struct{}, budget time.Duration) bool {
	select {
	case <-ctx.Done():
	case <-finished:
		return false
	}
	timer := time.NewTimer(budget)
	defer timer.Stop()
	select {
	case <-finished:
		return false
	case <-timer.C:
		return true
	}
}

func main() {
	c, e := loadConfig(os.Getenv)
	if e == nil {
		e = execute(c)
	}
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
