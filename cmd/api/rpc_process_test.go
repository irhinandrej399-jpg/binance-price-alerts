package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	store "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	triggered "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/alerttriggered"
	changed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/subscriptionchanged"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	marketpb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/apiprocess"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func freeRPCAddr(t testing.TB) string {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	return l.Addr().String()
}
func configureRPC(t testing.TB, env map[string]string) (authority, string, *processQuotes) {
	t.Helper()
	ca := newAuthority(t)
	server := ca.issue(t, nil, true, false)
	api := ca.issue(t, []string{"spiffe://local/api"}, false, false)
	for name, data := range map[string][]byte{"RPC_CERT_FILE": server.cert, "RPC_KEY_FILE": server.key, "RPC_CA_FILE": ca.pem, "MARKET_RPC_CERT_FILE": api.cert, "MARKET_RPC_KEY_FILE": api.key, "MARKET_RPC_CA_FILE": ca.pem} {
		path := filepath.Join(t.TempDir(), name)
		if e := os.WriteFile(path, data, 0600); e != nil {
			t.Fatal(e)
		}
		env[name] = path
	}
	addr := freeRPCAddr(t)
	env["RPC_ADDR"] = addr
	env["MATCHER_SERVICE_URI"] = "spiffe://local/matcher"
	// A local authenticated Latest endpoint; production Latest adapter is already
	// covered by OPS-069. Only this test fixture supplies quote bytes.
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	opts, e := serviceauth.ServerOptions(server.cert, server.key, ca.pem, map[string][]string{"spiffe://local/api": {marketpb.Quotes_Latest_FullMethodName}})
	if e != nil {
		t.Fatal(e)
	}
	market := grpc.NewServer(opts...)
	quotes := &processQuotes{entered: make(chan struct{}, 10), canceled: make(chan struct{}, 10)}
	marketpb.RegisterQuotesServer(market, quotes)
	go market.Serve(listener)
	t.Cleanup(func() { market.Stop(); listener.Close() })
	env["MARKET_RPC_ADDR"] = listener.Addr().String()
	env["MARKET_RPC_SERVER_NAME"] = "api.test"
	return ca, addr, quotes
}

type processQuotes struct {
	marketpb.UnimplementedQuotesServer
	mode              atomic.Int32
	entered, canceled chan struct{}
}

func (q *processQuotes) Latest(ctx context.Context, r *marketpb.LatestRequest) (*marketpb.LatestResponse, error) {
	switch q.mode.Load() {
	case 1:
		return nil, status.Error(codes.Unavailable, "test quote failure")
	case 2:
		q.entered <- struct{}{}
		<-ctx.Done()
		q.canceled <- struct{}{}
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	now := time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano)
	return &marketpb.LatestResponse{Freshness: marketpb.Freshness_FRESHNESS_FRESH, CheckedAt: time.Now().UTC().Format(time.RFC3339Nano), Quote: &marketpb.Quote{Pair: r.Pair, Price: "101.000000000000000001", SourceId: "ops-070", Revision: &marketpb.Revision{StreamId: "test", Generation: 1, Sequence: 1}, TradeTime: now, ReceivedAt: now, StreamState: "unconfirmed"}}, nil
}
func rpcConn(t testing.TB, ca authority, addr, uri string) *grpc.ClientConn {
	t.Helper()
	pair := ca.issue(t, []string{uri}, false, false)
	creds, e := serviceauth.ClientCredentials(pair.cert, pair.key, ca.pem)
	if e != nil {
		t.Fatal(e)
	}
	conn, e := grpc.NewClient(addr, grpc.WithTransportCredentials(creds), grpc.WithAuthority("api.test"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}
func testMatcherRPCProcess(t *testing.T, ctx context.Context, cfg apiprocess.Config, pool *pgxpool.Pool, ca authority, addr string) {
	t.Helper()
	t.Log("matcher RPC runtime, restart and cancellation")
	matcher := rpcConn(t, ca, addr, cfg.Env["MATCHER_SERVICE_URI"])
	stranger := rpcConn(t, ca, addr, "spiffe://local/notifications")
	snapshots := pb.NewSubscriptionSnapshotsClient(matcher)
	replay := pb.NewSubscriptionReplayClient(matcher)
	delivery := pb.NewDeliveryClient(matcher)
	f, e := seedDrainFixture(ctx, pool)
	if e != nil {
		t.Fatal("RPC seed")
	}
	req := &pb.RequestDeliveryRequest{OwnerId: f.OwnerID, SubscriptionId: f.Active.ID, ExpectedVersion: f.Active.Version}
	var snap *pb.SnapshotPageResponse
	var result *pb.RequestDeliveryResponse
	var outbox []byte
	for restart := 0; restart < 2; restart++ {
		e = withDrainedAPI(ctx, cfg, pool, func(p *apiprocess.Process) error {
			call, cancel := context.WithTimeout(ctx, 8*time.Second)
			defer cancel()
			page, err := snapshots.BuildSnapshot(call, &pb.BuildSnapshotRequest{RequestId: "ops-070", TtlMillis: 60000, PageSize: 1000}, grpc.WaitForReady(true))
			if err != nil {
				return fmt.Errorf("snapshot runtime: %w", err)
			}
			if restart == 0 {
				snap = page
			} else if page.SnapshotId != snap.SnapshotId || page.BoundaryC != snap.BoundaryC || page.ExpiresAt != snap.ExpiresAt {
				return errors.New("restart changed snapshot")
			}
			page, err = snapshots.ReadSnapshotPage(call, &pb.ReadSnapshotPageRequest{SnapshotId: snap.SnapshotId, PageSize: 1000})
			if err != nil || len(page.Subscriptions) < 2 {
				return errors.New("snapshot page unavailable")
			}
			var owner string
			if err = pool.QueryRow(call, `SELECT owner FROM app.subscription_replay_pins WHERE id=$1`, snap.SnapshotId).Scan(&owner); err != nil || owner != cfg.Env["MATCHER_SERVICE_URI"] {
				return errors.New("pin owner was not certificate identity")
			}
			// Each registered method must reject another signed service, even with a
			// valid public token and forged consumer metadata. Unknown RPC is closed.
			denied := metadata.NewOutgoingContext(call, metadata.Pairs("authorization", "Bearer "+f.Token, "consumer-id", owner))
			methods := []string{pb.SubscriptionSnapshots_BuildSnapshot_FullMethodName, pb.SubscriptionSnapshots_ReadSnapshotPage_FullMethodName, pb.SubscriptionReplay_BeginReplay_FullMethodName, pb.SubscriptionReplay_ReadReplayPage_FullMethodName, pb.SubscriptionReplay_AcknowledgeReplay_FullMethodName, pb.SubscriptionReplay_AbandonReplay_FullMethodName, pb.Delivery_RequestDelivery_FullMethodName, "/api.v1.Unknown/Call"}
			for _, method := range methods {
				if err = stranger.Invoke(denied, method, &pb.BuildSnapshotRequest{}, &pb.SnapshotPageResponse{}); status.Code(err) != codes.PermissionDenied {
					return fmt.Errorf("authorization %s: %v", method, err)
				}
			}
			if err = matcher.Invoke(call, "/api.v1.Unknown/Call", &pb.BuildSnapshotRequest{}, &pb.SnapshotPageResponse{}); status.Code(err) != codes.PermissionDenied {
				return errors.New("unknown matcher method allowed")
			}
			// Bearer alone cannot negotiate mTLS.
			plain, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			probe, stop := context.WithTimeout(denied, 300*time.Millisecond)
			err = plain.Invoke(probe, methods[0], &pb.BuildSnapshotRequest{}, &pb.SnapshotPageResponse{})
			stop()
			plain.Close()
			if err == nil {
				return errors.New("Bearer accepted without mTLS")
			}
			state, err := replay.BeginReplay(call, &pb.BeginReplayRequest{SnapshotId: snap.SnapshotId, TtlMillis: 60000})
			if err != nil {
				return err
			}
			got, err := delivery.RequestDelivery(call, req)
			if err != nil {
				return err
			}
			if got.Workflow == nil || got.State != "pending_delivery" {
				return errors.New("delivery workflow missing")
			}
			var body []byte
			if err = pool.QueryRow(call, `SELECT body FROM app.delivery_outbox WHERE subscription_id=$1`, req.SubscriptionId).Scan(&body); err != nil {
				return errors.New("outbox missing")
			}

			_, event, err := triggered.Decode(body)
			if err != nil || event.CandidateQuote == nil || event.CandidateQuote.Price.String() != "101.000000000000000001" || event.CandidateQuote.SourceID != "ops-070" || event.TriggerID != got.Workflow.TriggerId {
				return errors.New("Latest diagnostic/outbox changed")
			}
			if err = consumeCopies(call, cfg, f.Active.ID, body, 1); err != nil {
				return err
			}
			if restart == 0 {
				result = got
				outbox = body
			} else if got.Workflow.TriggerId != result.Workflow.TriggerId || string(body) != string(outbox) {
				return errors.New("restart changed canonical delivery")
			}
			record, err := store.New(pool).ByID(call, f.OwnerID, f.Active.ID)
			if err != nil || record.Workflow == nil || record.Workflow.TriggerID != got.Workflow.TriggerId {
				return errors.New("canonical workflow differs")
			}
			pageReplay, err := replay.ReadReplayPage(call, &pb.ReadReplayPageRequest{ResourceId: state.ResourceId, After: state.Progress, MaxTransactions: 1000})
			if err != nil {
				return err
			}
			if restart == 0 {
				found := false
				for _, body := range pageReplay.Events {
					_, event, e := changed.Decode(body)
					if e != nil {
						return e
					}
					if event.SubscriptionID == f.Active.ID && event.State == "pending_delivery" {
						found = true
					}
				}
				if !found {
					return errors.New("delivery absent from replay")
				}
			}
			if _, err = replay.AcknowledgeReplay(call, &pb.AcknowledgeReplayRequest{ResourceId: state.ResourceId, Boundary: pageReplay.Next}); err != nil {
				return err
			}
			// A second resource exercises Abandon without losing the restart resource.
			other, err := snapshots.BuildSnapshot(call, &pb.BuildSnapshotRequest{RequestId: fmt.Sprintf("abandon-%d", restart), TtlMillis: 60000, PageSize: 1})
			if err != nil {
				return err
			}
			if _, err = replay.AbandonReplay(call, &pb.AbandonReplayRequest{ResourceId: other.SnapshotId}); err != nil {
				return err
			}
			// Hold real SQL inside a new BuildSnapshot until the signal cancels the
			// handler. Its session locks/connection must be released before pool.Close.
			lock, err := pool.Acquire(call)
			if err != nil {
				return err
			}
			defer lock.Release()
			if _, err = lock.Exec(call, `SELECT pg_advisory_lock($1)`, store.RetentionLock); err != nil {
				return err
			}
			defer lock.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, store.RetentionLock)
			active := make(chan error, 1)
			go func() {
				_, err := snapshots.BuildSnapshot(call, &pb.BuildSnapshotRequest{RequestId: fmt.Sprintf("held-%d", restart), TtlMillis: 60000, PageSize: 1})
				active <- err
			}()
			if err = eventually(call, func() bool {
				var yes bool
				e := pool.QueryRow(call, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%pg_advisory_lock_shared%')`).Scan(&yes)
				return e == nil && yes
			}); err != nil {
				return errors.New("RPC not waiting on SQL")
			}
			if err = syscall.Kill(p.PID(), syscall.SIGTERM); err != nil {
				return err
			}
			select {
			case <-p.Done():
			case <-call.Done():
				return errors.New("RPC shutdown exceeded bound")
			}
			if code, ok := p.ExitCode(); !ok || code != 0 {
				return errors.New("RPC stop not graceful")
			}
			select {
			case err = <-active:
				if err == nil {
					return errors.New("held RPC succeeded")
				}
			case <-call.Done():
				return errors.New("RPC not canceled")
			}
			var waiting bool
			if err = pool.QueryRow(call, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%pg_advisory_lock_shared%')`).Scan(&waiting); err != nil || waiting {
				return errors.New("RPC SQL leaked")
			}
			return nil
		})
		if e != nil {
			t.Fatal(e)
		}
	}
	// Occupied listener and malformed credentials fail before HTTP/relay serve.
	occupied, e := net.Listen("tcp", addr)
	if e != nil {
		t.Fatal("RPC listener leaked")
	}
	e = apiprocess.With(ctx, cfg, func(*apiprocess.Process) error { return errors.New("unexpected startup") })
	occupied.Close()
	if !errors.Is(e, apiprocess.ErrEarlyExit) {
		t.Fatalf("RPC bind failure: %v", e)
	}
	old := cfg.Env["RPC_CERT_FILE"]
	for _, invalid := range []string{"absent", "malformed"} {
		path := filepath.Join(t.TempDir(), invalid)
		if invalid == "malformed" {
			if e = os.WriteFile(path, []byte("invalid"), 0600); e != nil {
				t.Fatal(e)
			}
		}
		cfg.Env["RPC_CERT_FILE"] = path
		e = apiprocess.With(ctx, cfg, func(*apiprocess.Process) error { return errors.New("unexpected startup") })
		if !errors.Is(e, apiprocess.ErrEarlyExit) {
			t.Fatalf("TLS startup failure: %v", e)
		}
	}
	cfg.Env["RPC_CERT_FILE"] = old
	// Lock the canonical API schema before relays can run.
	tx, e := pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(context.Background())
	if _, e = tx.Exec(ctx, `LOCK TABLE app.subscriptions IN ACCESS EXCLUSIVE MODE`); e != nil {
		t.Fatal(e)
	}
	e = apiprocess.With(ctx, cfg, func(*apiprocess.Process) error { return errors.New("unexpected schema startup") })
	if !errors.Is(e, apiprocess.ErrEarlyExit) {
		t.Fatalf("schema startup: %v", e)
	}
	if e = tx.Rollback(ctx); e != nil {
		t.Fatal(e)
	}
	l, e := net.Listen("tcp", addr)
	if e != nil {
		t.Fatal("startup leaked RPC")
	}
	l.Close()
}
