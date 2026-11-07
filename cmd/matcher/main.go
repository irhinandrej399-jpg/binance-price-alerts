// Command matcher runs the minimal local replica. Migrations run separately.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/deliveryrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/replayrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/snapshotrpc"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	progresspb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/matcher/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/latest"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/changefeed"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/health"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/live"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricefeed"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/progressrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/replica"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/snapshotload"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/sourcebinding"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/postgres"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type registration struct {
	*changefeed.Registration
	source *changefeed.Source
}

func (r *registration) Close() { r.source.Close() }
func changes(c config) func(context.Context, *changefeed.Position) (live.Registration, error) {
	return func(ctx context.Context, pos *changefeed.Position) (live.Registration, error) {
		s, e := changefeed.New(ctx, changefeed.Config{Brokers: c.brokers, Topic: c.changes, Source: c.changesSource})
		if e != nil {
			return nil, e
		}
		var r *changefeed.Registration
		if pos == nil {
			r, e = s.Register(ctx)
		} else {
			r, e = s.RegisterAt(ctx, *pos)
		}
		if e != nil {
			s.Close()
			return nil, e
		}
		return &registration{r, s}, nil
	}
}
func snapshotRequestID(id string) string {
	h := sha256.Sum256([]byte(id))
	return "matcher-" + hex.EncodeToString(h[:])
}

func run(parent context.Context, c config) error {
	// Credential, transport and SQL errors never expose supplied values.
	read := func(path string) ([]byte, error) {
		b, e := os.ReadFile(path)
		if e != nil {
			return nil, errors.New("matcher: credentials unavailable")
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
	creds, e := serviceauth.ClientCredentials(cert, key, ca)
	if e != nil {
		return errors.New("matcher: invalid service credentials")
	}
	dial := func(addr string) (*grpc.ClientConn, error) {
		return grpc.NewClient(addr, grpc.WithTransportCredentials(creds), grpc.WithAuthority(c.serverName))
	}
	api, e := dial(c.api)
	if e != nil {
		return errors.New("matcher: API client startup failed")
	}
	defer api.Close()
	quotes, e := dial(c.latest)
	if e != nil {
		return errors.New("matcher: Latest client startup failed")
	}
	defer quotes.Close()
	listener, e := net.Listen("tcp", c.addr)
	if e != nil {
		return errors.New("matcher: HTTP bind failed")
	}
	defer listener.Close()
	startup, done := context.WithTimeout(parent, 15*time.Second)
	pool, e := postgres.Open(startup, c.dsn)
	done()
	if e != nil {
		return errors.New("matcher: database startup failed")
	}
	defer pool.Close()
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
		return errors.New("matcher: invalid exporter credentials")
	}
	rpcListener, e := net.Listen("tcp", c.rpcAddr)
	if e != nil {
		return errors.New("matcher: RPC bind failed")
	}
	defer rpcListener.Close()
	rpcServer := grpc.NewServer(opts...)
	progresspb.RegisterMatcherProgressServer(rpcServer, progressrpc.NewServer(progressrpc.NewPostgres(pool)))
	defer rpcServer.Stop()
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	r, e := replica.New(replica.Config{Pool: pool, ID: c.id, Environment: c.environment, Group: c.group, ChangesSource: c.changesSource, Prices: pricefeed.Config{Brokers: c.brokers, Topic: c.prices, Source: c.pricesSource}, Bootstrap: c.bootstrap, Replay: replayrpc.NewClient(api), Delivery: deliveryrpc.NewClient(api), Latest: latest.NewClient(quotes), Live: live.Config{ReplayTTL: c.replayTTL, HeadInterval: c.headInterval, PageSize: c.replayPage}, Changes: changes(c), Snapshot: func(ctx context.Context, id string) (string, error) {
		g, e := snapshotload.New(pool, snapshotrpc.NewClient(api)).Load(ctx, &pb.BuildSnapshotRequest{RequestId: snapshotRequestID(id), TtlMillis: c.snapshotTTL.Milliseconds(), PageSize: c.snapshotPage})
		return g.SnapshotID, e
	}})
	if e != nil {
		return errors.New("matcher: replica startup failed")
	}
	h := health.New(pool.Ping, r.Status, time.Second)
	h.SetInitialized(true)
	defer h.SetInitialized(false)
	server := &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
	defer server.Close()
	results := make(chan error, 3)
	go func() { results <- r.Run(ctx) }()
	go func() { results <- server.Serve(listener) }()
	go func() { results <- rpcServer.Serve(rpcListener) }()
	remaining := 3
	componentFailed := false
	var sourceFailure error
	record := func(err error) {
		for _, kind := range []error{sourcebinding.ErrUnbound, kafkasource.ErrMismatch, kafkasource.ErrIdentity, kafkasource.ErrMetadata} {
			if errors.Is(err, kind) {
				// Cancellation of metadata during a requested shutdown is normal.
				if kind == kafkasource.ErrMetadata && parent.Err() != nil && errors.Is(err, context.Canceled) {
					continue
				}
				componentFailed = true
				sourceFailure = errors.Join(sourceFailure, kind)
			}
		}
		if errors.Is(err, replica.ErrCleanup) {
			componentFailed = true
			return
		}
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, grpc.ErrServerStopped) && status.Code(err) != codes.Canceled {
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
	rpcServer.Stop()
	httpStop, stopHTTP := context.WithTimeout(context.Background(), 2*time.Second)
	httpErr := server.Shutdown(httpStop)
	stopHTTP()
	if httpErr != nil {
		_ = server.Close()
	}
	// Join the replica/readers before closing the pool and RPC clients.
	for i := 0; i < remaining; i++ {
		record(<-results)
	}
	if httpErr != nil {
		return errors.New("matcher: HTTP shutdown incomplete")
	}
	if parent.Err() != nil && !componentFailed {
		return nil
	}
	return errors.Join(errors.New("matcher: required component stopped"), sourceFailure)
}

// Cancellation stops admission immediately; accepted SQL work is the drain
// buffer. A hard bound also covers stuck dependency joins/pool.Close. Exceeding
// it is an explicit failure with durable outcomes left for the successor.
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
			fmt.Fprintln(os.Stderr, "matcher: shutdown deadline exceeded; durable work requires recovery")
			os.Exit(1)
		}
	}()
	return run(ctx, c)
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
