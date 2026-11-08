package main

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/notifications/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/intake"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/progressrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/grpctest"
	np "github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/notificationsprocess"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Runs within the existing disposable process scenario, before workers start.
// The exporter uses an independent read-only SQL pool, never a fake reader.
func testSQLProgress(t *testing.T, ctx context.Context, f *np.Fixture, pool *pgxpool.Pool, record *kgo.Record, d np.Dataset) {
	t.Helper()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	cfg := pool.Config()
	cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	ro, err := pgxpool.NewWithConfig(ctx, cfg)
	must(err)
	t.Cleanup(ro.Close)
	cert, err := os.ReadFile(f.RPCServerCertFile)
	must(err)
	key, err := os.ReadFile(f.RPCServerKeyFile)
	must(err)
	ca, err := os.ReadFile(f.CAFile)
	must(err)
	opts, err := progressrpc.ServerOptions(cert, key, ca, np.CleanerURI)
	must(err)
	var server *grpc.Server
	transport := grpctest.Start(t, func(g *grpc.Server) {
		server = g
		pb.RegisterNotificationsProgressServer(g, progressrpc.NewServer(progressrpc.NewPostgres(ro)))
	}, opts...)
	rpc := progressrpc.NewClient(f.ProgressConn(t, strings.TrimPrefix(transport.Target(), "passthrough:///")))
	b := f.Binding
	request := func() *pb.ReadProgressRequest {
		return &pb.ReadProgressRequest{ConsumerId: b.ConsumerID, Generation: b.Generation, ClusterId: b.Source.ClusterID, TopicId: append([]byte(nil), b.Source.TopicID[:]...), Topic: b.Source.Topic, Partition: record.Partition}
	}
	check := func(q *pb.ReadProgressRequest, n int64, code codes.Code) {
		t.Helper()
		out, e := rpc.ReadProgress(ctx, q)
		if status.Code(e) != code {
			t.Fatal(out, e)
		}
		if code == codes.OK {
			if out.NextOffset == nil || *out.NextOffset != n || out.Generation != q.Generation || out.ConsumerId != q.ConsumerId || out.ClusterId != q.ClusterId || !bytes.Equal(out.TopicId, q.TopicId) || out.Topic != q.Topic || out.Partition != q.Partition {
				t.Fatal(out)
			}
		} else if out != nil {
			t.Fatal("error returned proof", out)
		}
	}
	snapshot := func() string {
		t.Helper()
		var v string
		must(pool.QueryRow(ctx, `SELECT jsonb_build_object('source',(SELECT jsonb_agg(to_jsonb(s)) FROM app.delivery_intake_source s),'checkpoints',(SELECT jsonb_agg(to_jsonb(c) ORDER BY partition) FROM app.delivery_checkpoints c),'jobs',(SELECT jsonb_agg(to_jsonb(j)) FROM app.delivery_jobs j))::text`).Scan(&v))
		return v
	}
	// Legacy SQL cannot inherit the request identity; no exporter write repairs it.
	_, err = pool.Exec(ctx, `SELECT app.test_legacy_delivery_source()`)
	must(err)
	legacy := snapshot()
	check(request(), 0, codes.FailedPrecondition)
	if snapshot() != legacy {
		t.Fatal("export relabeled legacy")
	}
	_, err = pool.Exec(ctx, `SELECT app.test_restore_delivery_source()`)
	must(err)
	before := snapshot()
	check(request(), 0, codes.OK) // explicit bootstrap zero is valid, synthetic zero is not
	for _, edit := range []func(*pb.ReadProgressRequest){func(q *pb.ReadProgressRequest) { q.ConsumerId = "other" }, func(q *pb.ReadProgressRequest) { q.Generation = "new" }, func(q *pb.ReadProgressRequest) { q.ClusterId = "new" }, func(q *pb.ReadProgressRequest) { q.TopicId[0] ^= 1 }, func(q *pb.ReadProgressRequest) { q.Topic = "other" }, func(q *pb.ReadProgressRequest) { q.Partition = 99 }} {
		q := request()
		edit(q)
		check(q, 0, codes.FailedPrecondition)
	}
	if snapshot() != before {
		t.Fatal("export mutated intake")
	}
	// Accept's job+checkpoint remain invisible in an uncommitted transaction.
	tx, err := pool.Begin(ctx)
	must(err)
	defer tx.Rollback(context.Background())
	_, err = tx.Exec(ctx, `SELECT app.delivery_accept_bound($1,$2,$3,$4,$5,$6,0,$7,$8,$9,$10,$11,$12)`, b.ConsumerID, b.Generation, b.Source.ClusterID, b.Source.TopicID[:], b.Source.Topic, record.Partition, record.Offset, d.Record.Workflow.TriggerID, d.Record.ID, d.Record.OwnerID, "proof-event", d.Body)
	must(err)
	check(request(), 0, codes.OK)
	must(tx.Rollback(ctx))
	check(request(), 0, codes.OK)
	if snapshot() != before {
		t.Fatal("rollback changed durable job/prefix")
	}
	must(intake.NewStore(pool).AcceptBound(ctx, b, record.Partition, 0, record.Offset, d.Key, d.Body))
	// No Kafka offset commit has occurred: SQL alone proves durable acceptance.
	n, err := f.BrokerOffset(ctx, f.Prefix+"-notifications", record.Partition)
	if err == nil && n > record.Offset {
		t.Fatal("broker unexpectedly committed")
	}
	check(request(), record.Offset+1, codes.OK)
	committed := snapshot()
	check(request(), record.Offset+1, codes.OK)
	if snapshot() != committed {
		t.Fatal("export changed pending job")
	}
	lock, err := pool.Begin(ctx)
	must(err)
	defer lock.Rollback(context.Background())
	_, err = lock.Exec(ctx, `LOCK TABLE app.delivery_checkpoints IN ACCESS EXCLUSIVE MODE`)
	must(err)
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	out, err := rpc.ReadProgress(short, request())
	cancel()
	if out != nil || status.Code(err) != codes.DeadlineExceeded {
		t.Fatal(out, err)
	}
	// Stop must cancel a real blocked SQL reader and join it before returning.
	done := make(chan error, 1)
	go func() { _, e := rpc.ReadProgress(ctx, request()); done <- e }()
	waiting, stopWait := context.WithTimeout(ctx, 2*time.Second)
	defer stopWait()
	for {
		var active bool
		must(pool.QueryRow(waiting, `SELECT EXISTS(SELECT FROM pg_stat_activity WHERE datname=current_database() AND state='active' AND wait_event_type='Lock' AND query LIKE 'SELECT s.consumer_id%')`).Scan(&active))
		if active {
			break
		}
		select {
		case <-waiting.Done():
			t.Fatal("read did not reach SQL lock")
		case <-time.After(5 * time.Millisecond):
		}
	}
	server.Stop()
	select {
	case e := <-done:
		if e == nil {
			t.Fatal("stopped exporter confirmed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SQL/RPC not joined")
	}
	must(lock.Rollback(ctx))
	broken, err := pgxpool.NewWithConfig(ctx, pool.Config())
	must(err)
	broken.Close()
	out, err = progressrpc.NewServer(progressrpc.NewPostgres(broken)).ReadProgress(ctx, request())
	if out != nil || status.Code(err) != codes.Unavailable {
		t.Fatal(out, err)
	}
	if snapshot() != committed {
		t.Fatal("fault export changed intake")
	}
}
