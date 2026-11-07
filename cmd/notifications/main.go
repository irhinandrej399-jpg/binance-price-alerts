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
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/admissionrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/resultrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/botlimit"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/chatlimit"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/handler"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/health"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/intake"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/jobs"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/report"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/retry"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/telegram"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/worker"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/postgres"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
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
	h, e := handler.New(handler.Config{Store: store, Admission: admissionrpc.NewClient(api), Bot: bot, Chat: chat, Sender: sender, Reporter: reporter, Policy: policy, RecoveryInterval: c.recoveryInterval, ReportLimit: c.reportLimit})
	if e != nil {
		return errConfig
	}
	w, e := worker.New(worker.Config{Source: store, Handler: h, Concurrency: c.concurrency, Interval: c.interval, Timeout: c.timeout, WriteTimeout: c.writeTimeout, RecoveryInterval: c.recoveryInterval})
	if e != nil {
		return errConfig
	}
	consumer, e := intake.New(intake.Config{Brokers: c.brokers, Topic: c.topic, Group: c.group}, intake.NewStore(pool))
	if e != nil {
		return errConfig
	}
	defer consumer.Close()
	listener, e := net.Listen("tcp", c.addr)
	if e != nil {
		return errors.New("notifications: HTTP bind failed")
	}
	defer listener.Close()
	probe := health.New(pool.Ping, w.Running, time.Second)
	return supervise(parent, listener, probe, consumer.Run, w.Run)
}

// supervise owns component cancellation and joins. Resources remain owned by
// run until all callbacks and active HTTP requests have returned.
func supervise(parent context.Context, listener net.Listener, probe *health.Handler, components ...func(context.Context) error) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	server := &http.Server{Handler: probe, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
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
	select {
	case <-parent.Done():
	case <-results:
		remaining--
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
		<-results
	}
	if httpErr != nil {
		return errors.New("notifications: HTTP cleanup incomplete")
	}
	if parent.Err() != nil {
		return parent.Err()
	}
	return errors.New("notifications: required component stopped")
}
func main() {
	c, e := loadConfig(os.Getenv)
	if e == nil {
		e = run(context.Background(), c)
	}
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
