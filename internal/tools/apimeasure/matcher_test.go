package apimeasure

// OPS-025 infrastructure phase driver. No functional suite or new runtime.
import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/journal"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/replayrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/snapshotrpc"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	observed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	changed "github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/subscriptionchanged"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/fencing"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricefeed"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricework"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/projection"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/snapshotload"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
)

type sourceRecord struct {
	Event     string `json:"event_id"`
	Body      string `json:"body_hex"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Key       string `json:"key"`
}
type matcherSource struct {
	Source     string                    `json:"source_id"`
	Journal    []sourceRecord            `json:"journal"`
	Identities []identity                `json:"identities"`
	Records    map[string][]sourceRecord `json:"read_committed"`
	Outbox     []struct {
		Body string `json:"body_hex"`
	} `json:"outbox"`
}
type matcherDriver struct {
	t          *testing.T
	pool       *pgxpool.Pool
	source     matcherSource
	sourceHash string
	snapshots  *snapshotrpc.Client
	replay     *replayrpc.Client
	request    *pb.BuildSnapshotRequest
	snapshot   string
	grant      fencing.Grant
	assign     fencing.Request
	store      *projection.Store
	work       *pricework.Store
	input      pricework.Input
	records    []*kgo.Record
	phases     []map[string]any
	topic      string
}

func matcherSetup(t *testing.T) *matcherDriver {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	d := &matcherDriver{t: t, topic: os.Getenv("KAFKA_PREFIX") + "-subscription-changes"}
	b, e := os.ReadFile(os.Getenv("OPS025_SOURCE"))
	check(t, e)
	sum := sha256.Sum256(b)
	d.sourceHash = hex.EncodeToString(sum[:])
	check(t, json.Unmarshal(b, &d.source))
	if d.source.Source != sourceID || len(d.source.Journal) != 19 || len(d.source.Records["subscription-changes"]) != 20 {
		t.Fatal("wrong source")
	}
	d.pool, e = postgres.Open(ctx, os.Getenv("MATCHER_DATABASE_URL"))
	check(t, e)
	t.Cleanup(d.pool.Close)
	var role string
	check(t, d.pool.QueryRow(ctx, "SELECT current_user").Scan(&role))
	if role != "matcher_runtime" {
		t.Fatal(role)
	}
	api, e := postgres.Open(ctx, os.Getenv("DATABASE_URL"))
	check(t, e)
	t.Cleanup(api.Close)
	pins := sub.NewSnapshots(api)
	ca := newAuthority(t)
	cert := ca.issue(t, nil, true, false)
	grants := map[string][]string{matcherURI: {pb.SubscriptionSnapshots_BuildSnapshot_FullMethodName, pb.SubscriptionSnapshots_ReadSnapshotPage_FullMethodName, pb.SubscriptionReplay_BeginReplay_FullMethodName, pb.SubscriptionReplay_ReadReplayPage_FullMethodName, pb.SubscriptionReplay_AcknowledgeReplay_FullMethodName, pb.SubscriptionReplay_AbandonReplay_FullMethodName}}
	conn := connect(t, ca, cert, matcherURI, grants, func(s *grpc.Server) {
		pb.RegisterSubscriptionSnapshotsServer(s, snapshotrpc.NewServer(pins))
		pb.RegisterSubscriptionReplayServer(s, replayrpc.NewServer(pins, api))
	})
	d.snapshots = snapshotrpc.NewClient(conn)
	d.replay = replayrpc.NewClient(conn)
	d.request = &pb.BuildSnapshotRequest{RequestId: "OPS025-empty-origin", TtlMillis: 3600000, PageSize: 1000}
	snap, e := d.snapshots.BuildSnapshot(ctx, d.request)
	check(t, e)
	d.snapshot = snap.SnapshotId
	if snap.BoundaryC != 0 || len(snap.Subscriptions) != 0 {
		t.Fatal("origin must precede source")
	}
	// API-owner fixture import, outside all measured windows. No business rows are
	// reconstructed. Replay is a frozen journal source, not a serving API dataset.
	for _, r := range d.source.Journal {
		raw, e := hex.DecodeString(r.Body)
		check(t, e)
		meta, c, e := changed.Decode(raw)
		check(t, e)
		tx, e := api.Begin(ctx)
		check(t, e)
		_, e = journal.Append(ctx, tx, []journal.Entry{{Metadata: meta, Change: c}})
		if e != nil {
			_ = tx.Rollback(ctx)
			check(t, e)
		}
		check(t, tx.Commit(ctx))
	}
	entries, e := journal.ReadAfter(ctx, api, 0, 1000)
	check(t, e)
	for i, v := range entries {
		raw, e := changed.Encode(v.Metadata, v.Change)
		check(t, e)
		if hex.EncodeToString(raw) != d.source.Journal[i].Body {
			t.Fatal("source bytes changed")
		}
	}
	// Re-publish the complete recorded read_committed prefix, including its real
	// physical duplicate. Current offsets are recorded separately from source offsets.
	producer, e := kgo.NewClient(kgo.SeedBrokers(os.Getenv("KAFKA_BROKERS")), kgo.TransactionalID(os.Getenv("KAFKA_PREFIX")+"-ops025"), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	check(t, e)
	defer producer.Close()
	for _, r := range d.source.Records["subscription-changes"] {
		raw, e := hex.DecodeString(r.Body)
		check(t, e)
		record := &kgo.Record{Topic: d.topic, Partition: 0, Key: []byte(r.Key), Value: raw}
		check(t, producer.BeginTransaction())
		check(t, producer.ProduceSync(ctx, record).FirstErr())
		check(t, producer.EndTransaction(ctx, kgo.TryCommit))
		d.records = append(d.records, record)
	}
	consumer, e := kgo.NewClient(kgo.SeedBrokers(os.Getenv("KAFKA_BROKERS")), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{d.topic: {0: kgo.NewOffset().AtStart()}}), kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	check(t, e)
	defer consumer.Close()
	n := 0
	for n < len(d.records) {
		f := consumer.PollFetches(ctx)
		check(t, f.Err())
		for _, r := range f.Records() {
			want := d.records[n]
			if r.Offset != want.Offset || !bytes.Equal(r.Value, want.Value) {
				t.Fatal("source prefix mismatch")
			}
			n++
		}
	}
	d.assign = fencing.Request{Identity: fencing.Identity{Environment: "OPS025", Group: "fixture", Topic: os.Getenv("KAFKA_PREFIX") + "-prices", Partition: pricefeed.PairPartition("BTCUSDT")}, Scope: "OPS025-price-scope", ProjectionID: "OPS025-projection", RequestID: "assign-one"}
	// Original candidate quote and canonical results are immutable source inputs.
	var env struct {
		Payload struct {
			Quote observed.Observed `json:"candidate_quote"`
		} `json:"payload"`
	}
	raw, e := hex.DecodeString(d.source.Outbox[0].Body)
	check(t, e)
	check(t, json.Unmarshal(raw, &env))
	d.input = pricework.Input{Key: "OPS025-current", ProjectionID: d.assign.ProjectionID, C: 5, Quote: env.Payload.Quote}
	for _, id := range d.source.Identities[:4] {
		d.input.Candidates = append(d.input.Candidates, pricework.Candidate{OwnerID: id.Owner, SubscriptionID: id.Subscription, Version: 1})
	}
	return d
}
func (d *matcherDriver) operation(name string) any {
	t := d.t
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ack := func(context.Context, int64) error { return nil }
	handle := func(i int) {
		r := d.records[i]
		check(t, d.store.Handle(ctx, projection.Batch{Topic: d.topic, Partition: 0, NextOffset: r.Offset + 1, Events: [][]byte{r.Value}}, ack))
	}
	operations := 1
	switch name {
	case "observe_first", "observe_duplicate":
		images := []projection.Image{}
		cp, e := d.store.Walk(ctx, func(image projection.Image) error { images = append(images, image); return nil })
		check(t, e)
		return map[string]any{"checkpoint": cp, "images": images}
	case "empty":
		return map[string]any{"operations": 0}
	case "snapshot_load", "snapshot_repeat":
		_, e := snapshotload.New(d.pool, d.snapshots).Load(ctx, d.request)
		check(t, e)
	case "assign", "assign_repeat":
		g, e := fencing.New(d.pool).Assign(ctx, d.assign)
		check(t, e)
		d.grant = g
		d.store = projection.NewFenced(d.pool, d.assign.ProjectionID, []fencing.Grant{g})
		d.work = pricework.NewFenced(d.pool, g)
	case "initialize", "initialize_repeat":
		check(t, d.store.Initialize(ctx, d.snapshot))
	case "replay_first":
		_, e := d.replay.BeginReplay(ctx, &pb.BeginReplayRequest{SnapshotId: d.snapshot, TtlMillis: 3600000})
		check(t, e)
		p, e := d.replay.ReadReplayPage(ctx, &pb.ReadReplayPageRequest{ResourceId: d.snapshot, MaxTransactions: 1})
		check(t, e)
		if len(p.Events) != 1 || !bytes.Equal(p.Events[0], d.records[0].Value) {
			t.Fatal("replay source mismatch")
		}
		check(t, d.store.ApplyReplay(ctx, p.Events))
		cp, e := d.store.Checkpoint(ctx)
		check(t, e)
		_, e = d.replay.AcknowledgeReplay(ctx, &pb.AcknowledgeReplayRequest{ResourceId: d.snapshot, Boundary: cp.C})
		check(t, e)
	case "replay_repeat":
		check(t, d.store.ApplyReplay(ctx, [][]byte{d.records[0].Value}))
	case "live_sql_duplicate":
		handle(0)
	case "live_first":
		handle(1)
	case "live_physical_duplicate":
		handle(2)
	case "live_creates":
		operations = 3
		for i := 3; i < 6; i++ {
			handle(i)
		}
	case "work_accept", "work_accept_repeat":
		check(t, d.work.Accept(ctx, d.input))
	case "work_results", "work_results_repeat":
		operations = 4
		for _, id := range d.source.Identities[:4] {
			check(t, d.work.Record(ctx, d.input.Key, id.Subscription, pricework.Result{Outcome: "workflow", TriggerID: id.Workflow, WorkflowVersion: 1}))
		}
	case "work_complete", "work_complete_repeat":
		check(t, d.work.Complete(ctx, d.input.Key, nil, nil, nil))
	case "live_tail":
		operations = len(d.records) - 6
		for i := 6; i < len(d.records); i++ {
			handle(i)
		}
	case "live_old_version":
		handle(0)
	case "takeover":
		r := d.assign
		r.RequestID = "assign-two"
		r.ExpectedToken = 1
		g, e := fencing.New(d.pool).Assign(ctx, r)
		check(t, e)
		d.grant = g
	case "stale_write":
		e := d.store.ApplyReplay(ctx, [][]byte{d.records[0].Value})
		if !errors.Is(e, fencing.ErrAuthority) {
			t.Fatal("stale writer", e)
		}
	case "revoke":
		check(t, fencing.New(d.pool).Revoke(ctx, d.grant))
	default:
		t.Fatal("unknown phase", name)
	}
	// Intermediate diagnostics have separate observe_* windows. Final diagnostics
	// execute in verification after both collectors finish their last snapshot.
	return map[string]any{"operations": operations}
}
func (d *matcherDriver) verify() any {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	t := d.t
	store := projection.New(d.pool, d.assign.ProjectionID)
	cp, e := store.Checkpoint(ctx)
	check(t, e)
	if cp.C != 19 || cp.NextOffset != d.records[len(d.records)-1].Offset+1 {
		t.Fatal("checkpoint", cp)
	}
	images := []any{}
	for _, id := range d.source.Identities {
		image, e := store.ReadImage(ctx, id.Subscription)
		check(t, e)
		images = append(images, image)
	}
	w, e := pricework.New(d.pool, d.assign.Scope).Get(ctx, d.input.Key)
	check(t, e)
	if w.State != "completed" || w.Completed != 4 {
		t.Fatal(w)
	}
	current, active, e := fencing.New(d.pool).Current(ctx, d.assign.Identity)
	check(t, e)
	if active || current.Token != 2 {
		t.Fatal("revoke not durable", current, active)
	}
	generation, e := snapshotload.New(d.pool, d.snapshots).Info(ctx, d.snapshot)
	check(t, e)
	if !generation.Complete || generation.C != 0 || generation.Count != 0 {
		t.Fatal("snapshot incomplete", generation)
	}
	records := []any{}
	for i, r := range d.records {
		records = append(records, map[string]any{"source": d.source.Records["subscription-changes"][i], "topic": r.Topic, "partition": r.Partition, "offset": r.Offset, "body_hex": hex.EncodeToString(r.Value)})
	}
	return map[string]any{"format_version": 1, "source_id": "OPS-025-matcher-v1", "source_sha256": d.sourceHash, "sql_role": "matcher_runtime", "snapshot_id": d.snapshot, "checkpoint": cp, "images": images, "work": w, "grant": current, "grant_active": active, "generation": generation, "records": records, "phases": d.phases}
}
func TestMatcherMeasurementDriver(t *testing.T) {
	if os.Getenv("OPS025_MEASURE") != "1" {
		t.Skip("opt-in infrastructure driver")
	}
	scanner := bufio.NewScanner(os.Stdin)
	enc := json.NewEncoder(os.Stdout)
	var d *matcherDriver
	for scanner.Scan() {
		var req struct {
			Phase string `json:"phase_id"`
		}
		check(t, json.Unmarshal(scanner.Bytes(), &req))
		var evidence any
		switch req.Phase {
		case "setup":
			d = matcherSetup(t)
		case "verification":
			evidence = d.verify()
		default:
			evidence = d.operation(req.Phase)
			d.phases = append(d.phases, map[string]any{"phase_id": req.Phase, "result": evidence})
		}
		check(t, enc.Encode(map[string]any{"phase_id": req.Phase, "status": "ok", "evidence": evidence}))
	}
	check(t, scanner.Err())
}
