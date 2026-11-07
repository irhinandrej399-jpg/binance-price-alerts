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
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/deliveryrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/replayrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/snapshotrpc"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/latest"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/changefeed"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/health"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/live"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricefeed"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/replica"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/snapshotload"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/postgres"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
	"google.golang.org/grpc"
)

type registration struct {
	*changefeed.Registration
	source *changefeed.Source
}

func (r *registration) Close() { r.source.Close() }
func changes(c config) func(context.Context, *changefeed.Position) (live.Registration, error) {
	return func(ctx context.Context, pos *changefeed.Position) (live.Registration, error) {
		s, e := changefeed.New(ctx, changefeed.Config{Brokers: c.brokers, Topic: c.changes})
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
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	r, e := replica.New(replica.Config{Pool: pool, ID: c.id, Environment: c.environment, Group: c.group, Prices: pricefeed.Config{Brokers: c.brokers, Topic: c.prices}, Bootstrap: c.bootstrap, Replay: replayrpc.NewClient(api), Delivery: deliveryrpc.NewClient(api), Latest: latest.NewClient(quotes), Live: live.Config{ReplayTTL: c.replayTTL, HeadInterval: c.headInterval, PageSize: c.replayPage}, Changes: changes(c), Snapshot: func(ctx context.Context, id string) (string, error) {
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
	results := make(chan error, 2)
	go func() { results <- r.Run(ctx) }()
	go func() { results <- server.Serve(listener) }()
	remaining := 2
	select {
	case <-parent.Done():
	case <-results:
		remaining--
	}
	h.SetInitialized(false)
	cancel()
	_ = server.Close()
	// Join the replica/readers before closing the pool and RPC clients.
	for i := 0; i < remaining; i++ {
		<-results
	}
	if parent.Err() != nil {
		return parent.Err()
	}
	return errors.New("matcher: required component stopped")
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
