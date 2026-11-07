// Command market-data runs one fixed raw trade stream in the local profile.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/health"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/publish"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/relay"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/storage"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/stream"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/trade"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafka"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

type config struct {
	addr, dsn, endpoint, pair, topic string
	producer                         kafka.Config
}

var kafkaName = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)
var pairName = regexp.MustCompile(`^[A-Z0-9]+USDT$`)

func loadConfig(get func(string) string) (config, error) {
	c := config{addr: get("HTTP_ADDR"), dsn: get("MARKET_DATA_DATABASE_URL"), endpoint: get("RAW_TRADE_URL"), pair: get("RAW_TRADE_PAIR"), topic: get("PRICES_TOPIC")}
	bad := errors.New("market-data: invalid configuration")
	addressOK := func(s string) bool {
		host, port, err := net.SplitHostPort(s)
		n, e := strconv.Atoi(port)
		return err == nil && e == nil && host != "" && n > 0 && n <= 65535
	}
	if !addressOK(c.addr) || c.dsn == "" || !pairName.MatchString(c.pair) {
		return config{}, bad
	}
	db, err := pgxpool.ParseConfig(c.dsn)
	if err != nil || db.ConnConfig.User != "market_data_runtime" || db.ConnConfig.Database != "market_data" {
		return config{}, bad
	}
	u, err := url.Parse(c.endpoint)
	if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Path != "/ws/"+strings.ToLower(c.pair)+"@trade" || u.RawPath != "" {
		return config{}, bad
	}
	brokers := strings.Split(get("KAFKA_BROKERS"), ",")
	for _, b := range brokers {
		if !addressOK(b) {
			return config{}, bad
		}
	}
	c.producer = kafka.Config{Brokers: brokers, TransactionalID: get("MARKET_DATA_RELAY_TRANSACTIONAL_ID")}
	for _, s := range []string{c.topic, c.producer.TransactionalID} {
		if !kafkaName.MatchString(s) || len(s) > 249 || s == "." || s == ".." {
			return config{}, bad
		}
	}
	return c, nil
}

// prepare binds all callbacks to the durable generation before the transport dials.
// The fixed raw endpoint provides no independent live confirmation.
func prepare(repo *storage.Repository, pair string, state *atomic.Value, initialized func(bool), generation *atomic.Uint64) stream.PrepareAttempt {
	return func(ctx context.Context) (stream.AttemptCallbacks, error) {
		op, cancel := context.WithTimeout(ctx, 5*time.Second)
		gen, err := repo.BeginGeneration(op, pair)
		cancel()
		if err != nil {
			return stream.AttemptCallbacks{}, err
		}
		generation.Store(gen)
		state.Store(health.Unconfirmed)
		initialized(true)
		return stream.AttemptCallbacks{
			Handle: func(ctx context.Context, raw []byte) error {
				now := time.Now().UTC()
				q, err := trade.Decode(raw, trade.Metadata{Unit: priceobserved.Milliseconds, ReceivedAt: now, Generation: gen, Sequence: 1, StreamState: priceobserved.Unconfirmed})
				if err != nil {
					return err
				}
				if q.Pair != pair {
					return errors.New("market-data: unexpected pair")
				}
				var id [16]byte
				if _, err = rand.Read(id[:]); err != nil {
					return err
				}
				eventID := hex.EncodeToString(id[:])
				meta := event.Envelope{EventID: eventID, Type: priceobserved.Type, Version: event.Version1, OccurredAt: now, CorrelationID: eventID, OrderingKey: pair}
				op, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				_, err = repo.Accept(op, gen, q, meta)
				return err
			},
			Disconnected: func(ctx context.Context, _ error, _ time.Duration) error {
				op, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				if err := repo.SetStatus(op, pair, gen, priceobserved.Unconfirmed, true); err != nil {
					return err
				}
				state.Store(health.Disconnected)
				return nil
			},
		}, nil
	}
}

func run(parent context.Context, c config) error {
	listener, err := net.Listen("tcp", c.addr)
	if err != nil {
		return errors.New("market-data: HTTP bind failed")
	}
	defer listener.Close()
	startup, done := context.WithTimeout(parent, 15*time.Second)
	defer done()
	pool, err := postgres.Open(startup, c.dsn)
	if err != nil {
		return errors.New("market-data: database startup failed")
	}
	defer pool.Close()
	rows, err := pool.Query(startup, `SELECT s.generation,s.sequence,s.latest,s.state,s.gap_detected,o.data,o.claim_token,o.lease_until,o.published FROM app.market_streams s CROSS JOIN app.price_outbox o LIMIT 0`)
	if err != nil {
		return errors.New("market-data: schema unavailable; run migrations separately")
	}
	rows.Close()
	// Bounded broker startup, without claiming durable work. Run establishes its
	// own epoch before its first Claim; all owners use this same stable shard ID.
	producer, err := publish.New(startup, c.producer, c.topic)
	if err != nil {
		return errors.New("market-data: Kafka startup failed")
	}
	producer.Close()
	done()
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	var generation atomic.Uint64
	var state atomic.Value
	state.Store(health.Starting)
	h := health.New(pool.Ping, func(context.Context) (health.FeedState, error) { return state.Load().(health.FeedState), nil }, time.Second)
	defer h.SetInitialized(false)
	server := &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
	defer server.Close()
	results := make(chan error, 3)
	repo := storage.New(pool)
	go func() { results <- relay.Run(ctx, repo, c.producer, c.topic, nil) }()
	go func() {
		results <- stream.ReconnectPrepared(ctx, c.endpoint, stream.DefaultConfig(), stream.DefaultRetryConfig(), nil, prepare(repo, c.pair, &state, h.SetInitialized, &generation))
	}()
	go func() { results <- server.Serve(listener) }()
	remaining := 3
	componentFailed := false
	record := func(err error) {
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, http.ErrServerClosed) {
			componentFailed = true
		}
	}
	select {
	case <-parent.Done():
	case err := <-results:
		remaining--
		record(err)
	}
	h.SetInitialized(false)
	cancel()
	httpStop, stopHTTP := context.WithTimeout(context.Background(), 2*time.Second)
	httpErr := server.Shutdown(httpStop)
	stopHTTP()
	if httpErr != nil {
		_ = server.Close()
	}
	// Cancellation stops admission, Accept, publication and retry. The durable
	// outbox is the accepted buffer; no drain-to-empty claim is made.
	for i := 0; i < remaining; i++ {
		record(<-results)
	}
	state.Store(health.Disconnected)
	// The reconnect loop skips its observer on owner cancellation. After joining
	// it, persist the gap with the last generation owned by this process. A newer
	// owner fences this write. Cleanup uses an independent, bounded SQL context.
	cleanup, finish := context.WithTimeout(context.Background(), 5*time.Second)
	defer finish()
	if gen := generation.Load(); gen != 0 {
		err = repo.SetStatus(cleanup, c.pair, gen, priceobserved.Unconfirmed, true)
		if err != nil && !errors.Is(err, storage.ErrStale) {
			return errors.New("market-data: stopped; durable gap write failed")
		}
	}
	var pending int64
	if err = pool.QueryRow(cleanup, `SELECT count(*) FROM app.price_outbox WHERE NOT published`).Scan(&pending); err != nil {
		return errors.New("market-data: stopped; publication status unavailable")
	}
	fmt.Fprintf(os.Stderr, "market-data: stopped; feed disconnected; pending publications=%d (unconfirmed outcomes retained)\n", pending)
	if httpErr != nil {
		return errors.New("market-data: HTTP shutdown incomplete")
	}
	if parent.Err() != nil && !componentFailed {
		return nil
	}

	// Component errors may contain credentials/remote response bodies.
	return errors.New("market-data: required component stopped")
}

// execute is shared with process fault tests; production main uses this exact
// signal and deadline owner. Deadline failure is explicit, never graceful success.
func execute(c config) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	finished := make(chan struct{})
	defer close(finished)
	go func() {
		select {
		case <-ctx.Done():
		case <-finished:
			return
		}
		timer := time.NewTimer(20 * time.Second)
		defer timer.Stop()
		select {
		case <-finished:
		case <-timer.C:
			fmt.Fprintln(os.Stderr, "market-data: shutdown deadline exceeded; publication/gap status may be unconfirmed")
			os.Exit(1)
		}
	}()
	return run(ctx, c)
}
func main() {
	c, err := loadConfig(os.Getenv)
	if err == nil {
		err = execute(c)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
