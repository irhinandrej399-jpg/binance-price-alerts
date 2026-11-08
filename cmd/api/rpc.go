package main

import (
	"context"
	"errors"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/deliveryrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/replayrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/snapshotrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/latest"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
)

type rpcConfig struct {
	addr, cert, key, ca, matcherURI                         string
	marketAddr, marketName, marketCert, marketKey, marketCA string
}

func loadRPCConfig(get func(string) string) (rpcConfig, error) {
	c := rpcConfig{addr: get("RPC_ADDR"), cert: get("RPC_CERT_FILE"), key: get("RPC_KEY_FILE"), ca: get("RPC_CA_FILE"), matcherURI: get("MATCHER_SERVICE_URI"), marketAddr: get("MARKET_RPC_ADDR"), marketName: get("MARKET_RPC_SERVER_NAME"), marketCert: get("MARKET_RPC_CERT_FILE"), marketKey: get("MARKET_RPC_KEY_FILE"), marketCA: get("MARKET_RPC_CA_FILE")}
	u, e := url.Parse(c.matcherURI)
	validURI := e == nil && u.Scheme != "" && u.Host != "" && u.User == nil && u.RawQuery == "" && u.Fragment == "" && u.String() == c.matcherURI && len(c.matcherURI) <= 200
	if !addressOK(c.addr) || !addressOK(c.marketAddr) || !validURI || c.marketName == "" || strings.ContainsAny(c.marketName, " /\\:\t\r\n") {
		return rpcConfig{}, errors.New("api: invalid RPC configuration")
	}
	for _, v := range []string{c.cert, c.key, c.ca, c.marketCert, c.marketKey, c.marketCA} {
		if v == "" {
			return rpcConfig{}, errors.New("api: invalid RPC configuration")
		}
	}
	return c, nil
}

type matcherRPC struct {
	listener net.Listener
	server   *grpc.Server
	market   *grpc.ClientConn
}

func newMatcherRPC(c rpcConfig) (*matcherRPC, error) {
	read := func(path string) ([]byte, error) {
		b, e := os.ReadFile(path)
		if e != nil {
			return nil, errors.New("api: RPC credentials unavailable")
		}
		return b, nil
	}
	cert, e := read(c.cert)
	if e != nil {
		return nil, e
	}
	key, e := read(c.key)
	if e != nil {
		return nil, e
	}
	ca, e := read(c.ca)
	if e != nil {
		return nil, e
	}
	opts, e := serviceauth.ServerOptions(cert, key, ca, map[string][]string{c.matcherURI: {
		pb.SubscriptionSnapshots_BuildSnapshot_FullMethodName, pb.SubscriptionSnapshots_ReadSnapshotPage_FullMethodName,
		pb.SubscriptionReplay_BeginReplay_FullMethodName, pb.SubscriptionReplay_ReadReplayPage_FullMethodName, pb.SubscriptionReplay_AcknowledgeReplay_FullMethodName, pb.SubscriptionReplay_AbandonReplay_FullMethodName,
		pb.Delivery_RequestDelivery_FullMethodName,
	}})
	if e != nil {
		return nil, errors.New("api: invalid RPC credentials or policy")
	}
	cert, e = read(c.marketCert)
	if e != nil {
		return nil, e
	}
	key, e = read(c.marketKey)
	if e != nil {
		return nil, e
	}
	ca, e = read(c.marketCA)
	if e != nil {
		return nil, e
	}
	creds, e := serviceauth.ClientCredentials(cert, key, ca)
	if e != nil {
		return nil, errors.New("api: invalid market RPC credentials")
	}
	conn, e := grpc.NewClient(c.marketAddr, grpc.WithTransportCredentials(creds), grpc.WithAuthority(c.marketName))
	if e != nil {
		return nil, errors.New("api: invalid market RPC target")
	}
	listener, e := net.Listen("tcp", c.addr)
	if e != nil {
		conn.Close()
		return nil, errors.New("api: RPC bind failed")
	}
	return &matcherRPC{listener: listener, server: grpc.NewServer(append(opts, grpc.WaitForHandlers(true))...), market: conn}, nil
}
func (r *matcherRPC) close() { r.server.Stop(); r.listener.Close(); r.market.Close() }
func (r *matcherRPC) register(pool *pgxpool.Pool) {
	snapshots := subscription.NewSnapshots(pool)
	pb.RegisterSubscriptionSnapshotsServer(r.server, snapshotrpc.NewServer(snapshots))
	pb.RegisterSubscriptionReplayServer(r.server, replayrpc.NewServer(snapshots, pool))
	pb.RegisterDeliveryServer(r.server, deliveryrpc.NewServer(deliveryrpc.NewPostgres(pool), latest.NewClient(r.market), time.Now))
}
func (r *matcherRPC) serve(ctx context.Context) error {
	joined := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(joined)
		select {
		case <-ctx.Done():
			r.server.Stop()
		case <-exited:
		}
	}()
	err := r.server.Serve(r.listener)
	close(exited)
	// Stop cancels calls and WaitForHandlers joins them, also on Serve failure.
	r.server.Stop()
	<-joined
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// Check the RPC storage contract before any listener serves or relay claims work.
func checkRPCSchema(ctx context.Context, pool *pgxpool.Pool) error {
	rows, e := pool.Query(ctx, `SELECT s.id,s.owner,s.boundary,s.expires_at,p.id,p.owner,p.progress,p.issued,p.phase,p.expires_at,r.owner,r.request_id,r.ttl_ns,r.snapshot_id,x.snapshot_id,x.id,x.workflow_id,x.workflow_version,x.block_reason,x.blocked_binding_version,j.position,j.transaction_end,j.body,b.transaction_end,o.subscription_id,o.trigger_id,o.event_id,o.body FROM app.subscription_snapshots s CROSS JOIN app.subscription_replay_pins p CROSS JOIN app.subscription_snapshot_requests r CROSS JOIN app.subscription_snapshot_rows x CROSS JOIN app.api_journal j CROSS JOIN app.api_journal_batches b CROSS JOIN app.delivery_outbox o LIMIT 0`)
	if e != nil {
		return errors.New("api: RPC schema unavailable; run migrations separately")
	}
	rows.Close()
	if rows.Err() != nil {
		return errors.New("api: RPC schema unavailable; run migrations separately")
	}
	return nil
}
