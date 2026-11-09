package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/fencing"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricefeed"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricework"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/projection"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/apiprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/matcherprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Runs after the recovery/shutdown suite: only disposable prices is recreated.
func TestMatcherSourceProcess(t *testing.T) {
	if os.Getenv("MATCHER_PROCESS_INTEGRATION") != "1" {
		t.Skip("run scripts/check_matcher_process.py --package ./cmd/matcher")
	}
	if os.Getenv("POSTGRES_TEST") != "1" || os.Getenv("KAFKA_TEST_BROKER") == "" || os.Getenv("KAFKA_TEST_PREFIX") == "" {
		t.Fatal("source process opt-in requires PostgreSQL/Kafka")
	}
	binary := filepath.Join(t.TempDir(), "matcher")
	buildCtx, stopBuild := context.WithTimeout(context.Background(), 20*time.Second)
	defer stopBuild()
	if output, err := exec.CommandContext(buildCtx, "go", "build", "-race", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %s", output)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 65*time.Second)
	defer cancel()
	must := func(e error) {
		t.Helper()
		if e != nil {
			t.Fatal(e)
		}
	}
	api := postgrestest.Open(t)
	sql := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	f := matcherprocess.New(t, api, os.Getenv("KAFKA_TEST_BROKER"), os.Getenv("KAFKA_TEST_PREFIX"))
	d, e := f.Seed(ctx)
	must(e)
	sort.Strings(d.IDs)
	must(f.PublishChanges(ctx))
	starts, e := f.Bootstrap(ctx)
	must(e)
	b, e := json.Marshal(starts)
	must(e)
	env := validEnv()
	delete(env, "HTTP_ADDR")
	for k, v := range f.ChildEnv(os.Getenv("MATCHER_DATABASE_URL")) {
		env[k] = v
	}
	env["MATCHER_ENVIRONMENT"] = f.Prefix + "-source"
	env["MATCHER_GROUP"] = f.Prefix + "-source"
	env["PRICES_BOOTSTRAP"] = string(b)
	env["HEAD_INTERVAL"] = "50ms"
	cfg := apiprocess.Config{DiagnosticLines: startupDiagnosticLines(), Executable: binary, Env: env, StartupProbePath: "/readyz", StartupTimeout: 10 * time.Second, StopTimeout: 22 * time.Second}
	wait := func(label string, ok func() bool) {
		t.Helper()
		for !ok() {
			select {
			case <-ctx.Done():
				t.Fatal(label, ctx.Err())
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	failed := func(c apiprocess.Config, category string) {
		t.Helper()
		if e := apiprocess.With(ctx, c, func(*apiprocess.Process) error { return errors.New("unexpected ready") }); !errors.Is(e, apiprocess.ErrEarlyExit) || !strings.Contains(e.Error(), category) {
			t.Fatal("expected terminal refusal", e)
		}
	}
	identity := fencing.Identity{Environment: env["MATCHER_ENVIRONMENT"], Group: env["MATCHER_GROUP"], Topic: f.PricesSource.Topic, Partition: 15}
	f.SetPrice(d.Pair, "99", 10)
	// First establish a stable projection. Partition 15 has no work or cursor.
	must(apiprocess.With(ctx, cfg, func(p *apiprocess.Process) error { return p.Stop() }))
	old, _, e := fencing.New(sql).Current(ctx, identity)
	must(e)
	grant, e := fencing.New(sql).Assign(ctx, fencing.Request{Identity: identity, Scope: old.Scope, ProjectionID: old.ProjectionID, ExpectedToken: old.Token, RequestID: "legacy-fixture"})
	must(e)
	cp, e := projection.New(sql, grant.ProjectionID).Checkpoint(ctx)
	must(e)
	legacy := pricework.NewFenced(sql, grant)
	must(legacy.Accept(ctx, pricework.Input{Key: "legacy-completed-latest", ProjectionID: grant.ProjectionID, C: cp.C, Quote: f.SetPrice("LEGACYUSDT", "99", 11)}))
	must(legacy.Complete(ctx, "legacy-completed-latest", nil, nil, nil))
	must(fencing.New(sql).Revoke(ctx, grant))
	failed(cfg, "matcher source: legacy state has no proven provenance") // Completed Latest without a cursor must still reject bootstrap.
	var legacyCount int
	must(sql.QueryRow(ctx, `SELECT count(*) FROM app.matcher_price_work WHERE scope=$1 AND work_key='legacy-completed-latest' AND state='completed'`, grant.Scope).Scan(&legacyCount))
	if legacyCount != 1 {
		t.Fatal("legacy history changed")
	}

	// A separate logical consumer has no legacy history and reaches Pending.
	env["MATCHER_ENVIRONMENT"] += "-bound"
	env["MATCHER_GROUP"] += "-bound"
	// Establish a proven transport cursor before interrupting the next evaluation.
	previous, e := f.PublishPrice(ctx, f.SetPrice(d.Pair, "99", 11))
	must(e)
	must(apiprocess.With(ctx, cfg, func(p *apiprocess.Process) error {
		cursorIdentity := fencing.Identity{Environment: env["MATCHER_ENVIRONMENT"], Group: env["MATCHER_GROUP"], Topic: previous.Topic, Partition: previous.Partition}
		wait("bound cursor", func() bool {
			g, _, err := fencing.New(sql).Current(ctx, cursorIdentity)
			if err != nil {
				return false
			}
			cp, err := pricework.NewSourceBound(sql, g, f.PricesSource).ReadCheckpoint(ctx, previous.Topic, previous.Partition)
			return err == nil && cp.Source != nil && *cp.Source == f.PricesSource && cp.NextOffset == previous.Offset+1
		})
		return p.Stop()
	}))
	pause := f.PauseDelivery(d.IDs[1])
	defer pause.Release()
	q := f.SetPrice(d.Pair, "101", 12)
	record, e := f.PublishPrice(ctx, q)
	must(e)
	identity.Environment, identity.Group, identity.Partition = env["MATCHER_ENVIRONMENT"], env["MATCHER_GROUP"], pricefeed.PairPartition(d.Pair)
	cfg.StartupProbePath = "/healthz"
	client, e := kgo.NewClient(kgo.SeedBrokers(f.Broker), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	must(e)
	defer client.Close()
	var bound fencing.Grant
	var before string
	must(apiprocess.With(ctx, cfg, func(p *apiprocess.Process) error {
		select {
		case <-pause.Committed:
		case <-ctx.Done():
			return ctx.Err()
		}
		bound, _, e = fencing.New(sql).Current(ctx, identity)
		must(e)
		must(sql.QueryRow(ctx, `SELECT jsonb_build_object(
 'work',(SELECT jsonb_agg(to_jsonb(w) ORDER BY work_key) FROM app.matcher_price_work w WHERE scope=$1),
 'candidates',(SELECT jsonb_agg(to_jsonb(c) ORDER BY work_key,subscription_id) FROM app.matcher_price_candidates c WHERE scope=$1),
 'checkpoints',(SELECT jsonb_agg(to_jsonb(p) ORDER BY topic,partition) FROM app.matcher_price_checkpoints p WHERE scope=$1),
 'source',(SELECT to_jsonb(s) FROM app.matcher_price_sources s WHERE scope=$1),
 'scopes',(SELECT jsonb_agg(scope ORDER BY scope) FROM app.matcher_assignments WHERE assignment_key::jsonb->>'Environment'=$2 AND assignment_key::jsonb->>'Group'=$3)
 )::text`, bound.Scope, identity.Environment, identity.Group).Scan(&before))
		// Metadata failure on an idle/blocked stream must cancel RPC and join readers.
		del := kmsg.NewPtrDeleteTopicsRequest()
		del.TopicNames = []string{f.PricesSource.Topic}
		del.Topics = []kmsg.DeleteTopicsRequestTopic{{Topic: &f.PricesSource.Topic}}
		deleted, e := del.RequestWith(ctx, client)
		must(e)
		if len(deleted.Topics) != 1 || deleted.Topics[0].ErrorCode != 0 {
			t.Fatal("delete disposable topic")
		}
		create := kmsg.NewPtrCreateTopicsRequest()
		topic := kmsg.NewCreateTopicsRequestTopic()
		topic.Topic = f.PricesSource.Topic
		topic.NumPartitions = 16
		topic.ReplicationFactor = 1
		create.Topics = []kmsg.CreateTopicsRequestTopic{topic}
		// Deletion is asynchronous. Retry only broker's topic-already-exists response.
		wait("recreate", func() bool {
			res, e := create.RequestWith(ctx, client)
			must(e)
			return len(res.Topics) == 1 && res.Topics[0].ErrorCode == 0
		})
		// Populate the same offset range in the replacement topic.
		records := make([]*kgo.Record, record.Offset+1)
		for i := range records {
			records[i] = &kgo.Record{Topic: record.Topic, Partition: record.Partition, Key: record.Key, Value: record.Value}
		}
		must(client.ProduceSync(ctx, records...).FirstErr())

		select {
		case <-p.Done():
		case <-ctx.Done():
			return ctx.Err()
		}
		if code, ok := p.ExitCode(); !ok || code != 1 {
			t.Fatal("source loss must exit 1", code)
		}
		return nil
	}))
	pause.Release()
	cfg.StartupProbePath = "/readyz"
	failed(cfg, kafkasource.ErrMismatch.Error()) // old configured ID rejects the replacement metadata.
	replacement, _, e := kafkasource.Metadata(ctx, client, f.PricesSource.Topic)
	must(e)
	if replacement == f.PricesSource {
		t.Fatal("topic ID unchanged")
	}
	env["PRICES_TOPIC_ID"] = hex.EncodeToString(replacement.TopicID[:])
	failed(cfg, kafkasource.ErrMismatch.Error()) // valid new metadata still cannot rename durable SQL history.
	var after string
	must(sql.QueryRow(ctx, `SELECT jsonb_build_object(
 'work',(SELECT jsonb_agg(to_jsonb(w) ORDER BY work_key) FROM app.matcher_price_work w WHERE scope=$1),
 'candidates',(SELECT jsonb_agg(to_jsonb(c) ORDER BY work_key,subscription_id) FROM app.matcher_price_candidates c WHERE scope=$1),
 'checkpoints',(SELECT jsonb_agg(to_jsonb(p) ORDER BY topic,partition) FROM app.matcher_price_checkpoints p WHERE scope=$1),
 'source',(SELECT to_jsonb(s) FROM app.matcher_price_sources s WHERE scope=$1),
 'scopes',(SELECT jsonb_agg(scope ORDER BY scope) FROM app.matcher_assignments WHERE assignment_key::jsonb->>'Environment'=$2 AND assignment_key::jsonb->>'Group'=$3)
 )::text`, bound.Scope, identity.Environment, identity.Group).Scan(&after))
	if before != after {
		t.Fatal("Pending/history changed after source refusal")
	}
	var active int
	must(sql.QueryRow(ctx, `SELECT count(*) FROM app.matcher_assignments WHERE assignment_key::jsonb->>'Environment'=$1 AND assignment_key::jsonb->>'Group'=$2 AND active`, identity.Environment, identity.Group).Scan(&active))
	if active != 0 {
		t.Fatal("source refusal left grants active")
	}
}
