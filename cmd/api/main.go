// Command api runs the minimal local API (OPS-046).
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/auth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/authlimit"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/bodylimit"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/deliveryrelay"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/health"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/login"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/registration"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/relay"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/session"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/telegramcode"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/telegramlink"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/telegramwebhook"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/catalogrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/latest"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafka"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

type config struct {
	addr, dsn, changes, deliveries string
	subscriptions, delivery        kafka.Config
	rpc                            rpcConfig
	webhookSecret                  string
}

var kafkaName = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

func loadConfig(get func(string) string) (config, error) {
	c := config{addr: get("HTTP_ADDR"), dsn: get("DATABASE_URL"), changes: get("SUBSCRIPTION_CHANGES_TOPIC"), deliveries: get("DELIVERY_REQUESTS_TOPIC")}
	bad := errors.New("api: invalid configuration")
	c.webhookSecret = get("TELEGRAM_WEBHOOK_SECRET")
	if _, err := telegramwebhook.Handler(c.webhookSecret, http.NotFoundHandler()); err != nil {
		return config{}, bad
	}

	if !addressOK(c.addr) || c.dsn == "" {
		return config{}, bad
	}
	db, err := pgxpool.ParseConfig(c.dsn)
	if err != nil || db.ConnConfig.User != "api_runtime" || db.ConnConfig.Database != "api" {
		return config{}, bad
	}
	brokers := strings.Split(get("KAFKA_BROKERS"), ",")
	for _, b := range brokers {
		if !addressOK(b) {
			return config{}, bad
		}
	}
	c.subscriptions = kafka.Config{Brokers: brokers, TransactionalID: get("SUBSCRIPTION_RELAY_TRANSACTIONAL_ID")}
	c.delivery = kafka.Config{Brokers: brokers, TransactionalID: get("DELIVERY_RELAY_TRANSACTIONAL_ID")}
	for _, s := range []string{c.changes, c.deliveries, c.subscriptions.TransactionalID, c.delivery.TransactionalID} {
		if !kafkaName.MatchString(s) || len(s) > 249 || s == "." || s == ".." {
			return config{}, bad
		}
	}
	if c.changes == c.deliveries || c.subscriptions.TransactionalID == c.delivery.TransactionalID {
		return config{}, bad
	}
	c.rpc, err = loadRPCConfig(get)
	if err != nil {
		return config{}, err
	}
	return c, nil
}

func addressOK(s string) bool {
	host, port, err := net.SplitHostPort(s)
	n, e := strconv.Atoi(port)
	return err == nil && e == nil && host != "" && n > 0 && n <= 65535
}

func expectedStop(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, http.ErrServerClosed)
}

// supervise cancels and joins every sibling when any required component exits.
// Relay Run already classifies retryable versus terminal errors.
func supervise(parent context.Context, runs ...func(context.Context) error) error {
	return superviseWithStop(parent, func() {}, runs...)
}

func superviseWithStop(parent context.Context, stopping func(), runs ...func(context.Context) error) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	results := make(chan error, len(runs))
	for _, run := range runs {
		go func() { results <- run(ctx) }()
	}
	var first error
	for range runs {
		err := <-results
		if err == nil {
			err = errors.New("api: required component stopped")
		}
		if first == nil || (expectedStop(first) && !expectedStop(err)) {
			first = err
		}
		stopping()
		cancel()
	}
	if parent.Err() != nil && expectedStop(first) {
		return parent.Err()
	}

	return first
}

func run(ctx context.Context, c config) error {
	// The same final bound covers a signal and any terminal sibling failure,
	// including handler joining and pool cleanup. Completion disarms the timer.
	finished := make(chan struct{})
	defer close(finished)
	var once sync.Once
	stopping := func() {
		once.Do(func() {
			go func() {
				timer := time.NewTimer(10 * time.Second)
				defer timer.Stop()
				select {
				case <-finished:
				case <-timer.C:
					fmt.Fprintln(os.Stderr, "api: shutdown deadline exceeded")
					os.Exit(1)
				}
			}()
		})
	}
	stopWatch := context.AfterFunc(ctx, stopping)
	defer stopWatch()

	listener, err := net.Listen("tcp", c.addr)
	if err != nil {
		return errors.New("api: HTTP bind failed")
	}
	defer listener.Close()
	rpc, err := newAPIRPC(c.rpc)
	if err != nil {
		return err
	}
	defer rpc.close()
	startup, done := context.WithTimeout(ctx, 15*time.Second)
	defer done()
	pool, err := postgres.Open(startup, c.dsn)
	if err != nil {
		return errors.New("api: database startup failed")
	}
	defer pool.Close()
	// This defer runs before pool.Close, including startup failures.
	defer rpc.server.Stop()
	if err = checkRPCSchema(startup, pool); err != nil {
		return err
	}
	if err = rpc.register(pool); err != nil {
		return err
	}
	// Check the HTTP storage contract as well as relay tables before readiness.
	rows, err := pool.Query(startup, `SELECT s.token_hash,u.id,u.telegram_chat_id,u.telegram_version,x.id,l.used,c.code_hash,c.expires_at FROM app.sessions s JOIN app.users u ON u.id=s.user_id JOIN app.subscriptions x ON x.owner_id=u.id CROSS JOIN app.auth_limits l CROSS JOIN app.telegram_link_codes c LIMIT 0`)
	if err != nil {
		return errors.New("api: API schema unavailable; run migrations separately")
	}
	rows.Close()
	changes, err := relay.New(pool, c.subscriptions, c.changes)
	if err != nil {
		return errors.New("api: subscription relay configuration failed")
	}
	deliveries, err := deliveryrelay.New(pool, c.delivery, c.deliveries)
	if err != nil {
		return errors.New("api: delivery relay configuration failed")
	}
	// Initial steps verify both SQL queues and any pending publication before
	// readiness. Failure leaves durable work intact and closes startup resources.
	if _, err = changes.Step(startup); err != nil {
		return errors.New("api: subscription relay startup failed")
	}
	if _, err = deliveries.Step(startup); err != nil {
		return errors.New("api: delivery relay startup failed")
	}
	done()
	webhook, err := telegramwebhook.Handler(c.webhookSecret, telegramlink.Handler(telegramlink.New(pool, nil)))
	if err != nil {
		return errors.New("api: invalid configuration")
	}
	h := health.New(pool.Ping, time.Second)
	mux := http.NewServeMux()
	limits := authlimit.New(pool, nil)
	mux.Handle("POST /v1/accounts", limits.Handler(registration.New(account.New(pool))))
	mux.Handle("POST /v1/sessions", limits.Handler(login.New(account.New(pool), session.New(pool, nil))))
	mux.Handle("POST /v1/telegram/link-codes", telegramcode.Handler(session.New(pool, nil), telegramcode.New(pool, nil)))
	mux.Handle("POST /v1/telegram/webhook", webhook)
	mux.Handle("/healthz", h)
	mux.Handle("/readyz", h)
	sessions := session.New(pool, nil)
	mux.Handle("POST /v1/subscriptions", auth.New(sessions, subscription.NewHandler(pool, catalogrpc.NewClient(rpc.market), latest.NewClient(rpc.market), time.Now)))
	mux.Handle("GET /v1/subscriptions", auth.New(sessions, subscription.NewListHandler(pool)))
	mux.Handle("/v1/subscriptions/", auth.New(sessions, subscription.NewCancelHandler(pool)))
	server := &http.Server{Handler: bodylimit.New(bodylimit.PublicLimit, mux), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	defer server.Close()
	h.SetInitialized(true)
	defer h.SetInitialized(false)
	err = superviseWithStop(ctx, stopping,
		rpc.serve,
		func(c context.Context) error { return changes.Run(c, nil) },
		func(c context.Context) error { return deliveries.Run(c, nil) },
		func(c context.Context) error {
			return serveHTTP(c, server, listener, func() { h.SetInitialized(false) }, 5*time.Second)
		})
	if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return nil
	}

	if err != nil {
		return errors.New("api: required component stopped")
	}
	return nil
}
func main() {
	c, err := loadConfig(os.Getenv)
	if err == nil {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
		defer stop()
		err = run(ctx, c)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// serveHTTP stops admission immediately, lets accepted handlers finish, then
// cancels their SQL contexts and closes connections at the drain deadline.
// It joins handlers before run can close the shared pool.
func serveHTTP(ctx context.Context, server *http.Server, listener net.Listener, withdraw func(), budget time.Duration) error {
	requests, cancel := context.WithCancel(context.Background())
	defer cancel()
	server.BaseContext = func(net.Listener) context.Context { return requests }
	var active sync.WaitGroup
	var gate sync.Mutex
	retired := false
	handler := server.Handler
	server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gate.Lock()
		if retired {
			gate.Unlock()
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		active.Add(1)
		gate.Unlock()
		defer active.Done()
		handler.ServeHTTP(w, r)
	})
	drained := make(chan error, 1)
	serving := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			withdraw()
			drain, done := context.WithTimeout(context.Background(), budget)
			defer done()
			err := server.Shutdown(drain)
			if err != nil {
				cancel()
				_ = server.Close()
			}
			drained <- err
		case <-serving:
			drained <- nil
		}
	}()
	err := server.Serve(listener)
	close(serving)
	drainErr := <-drained
	if ctx.Err() == nil {
		withdraw()
		cancel()
		_ = server.Close()
	}
	// Shutdown/Close have stopped dispatch before Wait. Context-aware handlers
	// release their transactions; main imposes a final bound on all cleanup.
	gate.Lock()
	retired = true
	gate.Unlock()
	active.Wait()
	if drainErr != nil {
		return drainErr
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}
