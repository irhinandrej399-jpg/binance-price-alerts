package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	progresspb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/notifications/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/intake"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/progressrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/apiprocess"
	np "github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/notificationsprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestNotificationsProcess(t *testing.T) {
	if os.Getenv("NOTIFICATIONS_PROCESS_INTEGRATION") != "1" {
		t.Skip("run python3 scripts/check_notifications_process.py --package ./cmd/notifications")
	}
	if os.Getenv("POSTGRES_TEST") != "1" || os.Getenv("KAFKA_TEST_BROKER") == "" || os.Getenv("KAFKA_TEST_PREFIX") == "" {
		t.Fatal("notifications opt-in requires PostgreSQL/Kafka configuration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 270*time.Second)
	defer cancel()
	must := func(e error) {
		t.Helper()
		if e != nil {
			t.Fatal(e)
		}
	}
	binary := filepath.Join(t.TempDir(), "notifications")
	build := exec.CommandContext(ctx, "go", "build", "-race", "-o", binary, ".")
	if out, e := build.CombinedOutput(); e != nil {
		t.Fatalf("build: %s", out)
	}
	api := postgrestest.Open(t)
	reader := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
	f := np.New(t, api, os.Getenv("KAFKA_TEST_BROKER"), os.Getenv("KAFKA_TEST_PREFIX"))
	d, e := f.Seed(ctx)
	must(e)
	trigger := d.Record.Workflow.TriggerID
	partitions := map[int32]kgo.Offset{}
	for p := int32(0); p < 16; p++ {
		partitions[p] = kgo.NewOffset().AtStart()
	}
	observer, e := kgo.NewClient(kgo.SeedBrokers(f.Broker), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{f.Prefix + "-delivery-requests": partitions}), kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	must(e)
	defer observer.Close()
	must(f.Publish(ctx))
	fetched := observer.PollRecords(ctx, 1)
	if len(fetched.Errors()) != 0 || len(fetched.Records()) != 1 {
		t.Fatal("canonical Kafka record missing")
	}
	record := fetched.Records()[0]
	if !bytes.Equal(record.Key, d.Key) || !bytes.Equal(record.Value, d.Body) {
		t.Fatal("canonical Kafka bytes changed")
	}
	env := validEnv()
	delete(env, "HTTP_ADDR")
	for k, v := range f.ChildEnv(os.Getenv("NOTIFICATIONS_DATABASE_URL")) {
		env[k] = v
	}
	env["NOTIFICATIONS_GROUP"] = f.Prefix + "-notifications"
	env["RPC_IDENTITY"] = np.NotificationsURI
	cfg := apiprocess.Config{Executable: binary, Env: env, StartupTimeout: 12 * time.Second, StopTimeout: time.Second}
	wait := func(label string, ok func() bool) {
		t.Helper()
		t.Log("waiting:", label)
		tick := time.NewTicker(15 * time.Millisecond)
		defer tick.Stop()
		for !ok() {
			select {
			case <-ctx.Done():
				t.Fatal("timeout: " + label)
			case <-tick.C:
			}
		}
	}
	// No implicit provisioning: startup without SQL provenance cannot become ready.
	reject := func(c apiprocess.Config) {
		t.Helper()
		if err := apiprocess.With(ctx, c, func(*apiprocess.Process) error { return errors.New("unexpected readiness") }); !errors.Is(err, apiprocess.ErrEarlyExit) {
			t.Fatal("identity startup failure not observed", err)
		}
	}
	// Discover only as an explicit operator action on this disposable empty SQL.
	client, err := kgo.NewClient(kgo.SeedBrokers(f.Broker))
	must(err)
	source, _, err := kafkasource.Metadata(ctx, client, record.Topic)
	client.Close()
	must(err)
	env["KAFKA_CLUSTER_ID"] = source.ClusterID
	env["DELIVERY_REQUESTS_TOPIC_ID"] = hex.EncodeToString(source.TopicID[:])
	env["NOTIFICATIONS_CONSUMER_ID"] = f.Prefix + "-notifications"
	env["NOTIFICATIONS_GENERATION"] = "not-provisioned"
	reject(cfg)
	var count int
	must(reader.QueryRow(ctx, `SELECT count(*) FROM app.delivery_intake_source`).Scan(&count))
	if count != 0 {
		t.Fatal("startup created binding")
	}
	must(f.Provision(ctx, reader))
	testSQLProgress(t, ctx, f, reader, record, d)
	// SQL is now ahead of the absent broker commit. A fresh canonical duplicate
	// lets the resumed consumer acknowledge its next read without a new job.
	replayProducer, err := kgo.NewClient(kgo.SeedBrokers(f.Broker), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	must(err)
	replay := &kgo.Record{Topic: record.Topic, Partition: record.Partition, Key: d.Key, Value: d.Body}
	err = replayProducer.ProduceSync(ctx, replay).FirstErr()
	replayProducer.Close()
	must(err)
	record = replay
	progress := progressrpc.NewClient(f.ProgressConn(t, f.RPCAddr))
	proof := func(part int32, next int64) {
		t.Helper()
		b := f.Binding
		out, err := progress.ReadProgress(ctx, &progresspb.ReadProgressRequest{ConsumerId: b.ConsumerID, Generation: b.Generation, ClusterId: b.Source.ClusterID, TopicId: b.Source.TopicID[:], Topic: b.Source.Topic, Partition: part})
		must(err)
		if out.NextOffset == nil || *out.NextOffset != next || out.Generation != b.Generation || out.ConsumerId != b.ConsumerID {
			t.Fatal("runtime progress mismatch", out)
		}
	}
	for k, v := range f.ChildEnv(os.Getenv("NOTIFICATIONS_DATABASE_URL")) {
		env[k] = v
	}
	// Independent snapshots include exact IDs, bytes, outcomes and reports.
	evidence := func() string {
		t.Helper()
		tables := []string{"delivery_jobs", "delivery_checkpoints", "delivery_claims", "delivery_attempts", "delivery_successes", "delivery_uncertainties", "delivery_blocks", "delivery_retryables", "delivery_success_reports", "delivery_uncertain_reports", "delivery_blocked_reports", "delivery_retryable_reports"}
		var result strings.Builder
		for _, table := range tables {
			var value string
			must(reader.QueryRow(ctx, fmt.Sprintf(`SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb)::text FROM app.%s t`, table)).Scan(&value))
			result.WriteString(value)
		}
		return result.String()
	}
	assertBinding := func() {
		t.Helper()
		for p := int32(0); p < 16; p++ {
			progress, err := f.Progress(ctx, reader, p)
			must(err)
			if progress.Binding != f.Binding || progress.Partition != p {
				t.Fatal("binding changed")
			}
		}
	}
	assertBinding()
	observe := func() np.Observation { t.Helper(); o, e := np.Observe(ctx, reader, trigger); must(e); return o }
	status := func(p *apiprocess.Process, path string) int {
		t.Helper()
		req, e := http.NewRequestWithContext(ctx, "GET", p.URL+path, nil)
		must(e)
		resp, e := (&http.Client{Timeout: 2 * time.Second}).Do(req)
		must(e)
		defer resp.Body.Close()
		return resp.StatusCode
	}
	noHTTP := func() {
		t.Helper()
		select {
		case <-f.Telegram.Requests:
			t.Fatal("unexpected HTTP")
		default:
		}
	}
	call := func() *np.Send {
		t.Helper()
		select {
		case s := <-f.Telegram.Requests:
			return s
		case <-ctx.Done():
			t.Fatal("missing HTTP")
			return nil
		}
	}
	kill := func(p *apiprocess.Process) {
		t.Helper()
		must(syscall.Kill(-p.PID(), syscall.SIGKILL))
		select {
		case <-p.Done():
		case <-ctx.Done():
			t.Fatal("kill not reaped")
		}
	}
	stop := func(p *apiprocess.Process, sig syscall.Signal, expected int) {
		t.Helper()
		must(syscall.Kill(-p.PID(), sig))
		timer := time.NewTimer(15 * time.Second)
		defer timer.Stop()
		select {
		case <-p.Done():
			code, ok := p.ExitCode()
			if !ok || code != expected {
				t.Fatalf("signal exit=%d want=%d", code, expected)
			}
		case <-timer.C:
			t.Fatal("signal drain exceeded test budget")
		}
	}
	producer, e := kgo.NewClient(kgo.SeedBrokers(f.Broker))
	must(e)
	defer producer.Close()
	// A recovered SQL job can complete before Kafka has joined. Observe a
	// canonical duplicate through intake before stopping that child, proving
	// restart deduplication and avoiding a succession of abandoned JoinGroups.
	confirmIntake := func() {
		t.Helper()
		before := observe()
		duplicate := &kgo.Record{Topic: record.Topic, Key: d.Key, Value: d.Body}
		must(producer.ProduceSync(ctx, duplicate).FirstErr())
		wait("restart duplicate SQL prefix", func() bool {
			p, e := f.Progress(ctx, reader, duplicate.Partition)
			return e == nil && p.Binding == f.Binding && p.Next >= duplicate.Offset+1
		})
		wait("restart duplicate broker prefix", func() bool {
			n, e := f.BrokerOffset(ctx, env["NOTIFICATIONS_GROUP"], duplicate.Partition)
			return e == nil && n >= duplicate.Offset+1
		})
		after := observe()
		if after.State != before.State || after.Attempts != before.Attempts || after.Claims != before.Claims || !slices.Equal(after.AttemptIDs, before.AttemptIDs) || after.Successes != before.Successes || !bytes.Equal(after.Body, before.Body) {
			t.Fatal("duplicate intake changed completed job")
		}
		noHTTP()
		assertBinding()
	}

	// Controlled durable jobs cannot be scheduled; exercise runtime-role SQL
	// gauges without introducing another delivery or changing the fixture protocol.
	controlProducer, err := kgo.NewClient(kgo.SeedBrokers(f.Broker), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	must(err)
	defer controlProducer.Close()
	for _, state := range []string{"pending", "blocked"} {
		control, err := f.Seed(ctx)
		must(err)
		r := &kgo.Record{Topic: record.Topic, Partition: (record.Partition + 1) % 16, Key: control.Key, Value: control.Body}
		must(controlProducer.ProduceSync(ctx, r).FirstErr())
		prefix, err := f.Progress(ctx, reader, r.Partition)
		must(err)
		must(intake.NewStore(reader).AcceptBound(ctx, f.Binding, r.Partition, prefix.Next, r.Offset, r.Key, r.Value))
		_, err = reader.Exec(ctx, `UPDATE app.delivery_jobs SET state=$2,next_attempt_at='infinity',
 blocked_reason=CASE WHEN $2='blocked' THEN 'permanent_failure' ELSE NULL END,
 blocked_binding_version=CASE WHEN $2='blocked' THEN 1 ELSE NULL END WHERE trigger_id=$1`, control.Record.Workflow.TriggerID, state)
		must(err)
	}
	scrape := func(p *apiprocess.Process, code int, wants ...string) string {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, "GET", p.URL+"/metrics", nil)
		must(err)
		resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
		must(err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		must(err)
		if resp.StatusCode != code {
			t.Fatalf("metrics HTTP=%d want=%d: %s", resp.StatusCode, code, body)
		}
		for _, want := range wants {
			if !slices.Contains(strings.Split(string(body), "\n"), want) {
				t.Fatalf("missing metric %q in %s", want, body)
			}
		}
		return string(body)
	}
	var crashEvidence np.Observation
	f.SetPrice("99", 1)
	// False Latest gates HTTP despite a canonical historical candidate.
	must(apiprocess.With(ctx, cfg, func(p *apiprocess.Process) error {
		wait("false Latest", func() bool { o, e := np.Observe(ctx, reader, trigger); return e == nil && o.State == "wait_condition" })
		o := observe()
		if !bytes.Equal(o.Body, d.Body) || o.Attempts != 0 || o.LeaseID != nil {
			t.Fatal("durable job mismatch")
		}
		body := scrape(p, 200, "delivery_backlog 3", `delivery_jobs{state="pending"} 1`,
			`delivery_jobs{state="blocked"} 1`, `delivery_blocks{reason="permanent_failure"} 1`,
			`delivery_pending_reports{kind="success"} 0`)
		var admissions float64
		for _, line := range strings.Split(body, "\n") {
			if v, ok := strings.CutPrefix(line, `delivery_admission_rpc_seconds_count{result="response"} `); ok {
				admissions, e = strconv.ParseFloat(v, 64)
				must(e)
			}
		}
		if admissions < 1 {
			t.Fatal("runtime admission histogram empty")
		}
		// Force real SQL collection to wait beyond its own deadline, then prove
		// neither a cached snapshot nor SQL/credentials escape in the response.
		tx, err := reader.Begin(ctx)
		must(err)
		defer tx.Rollback(context.Background())
		_, err = tx.Exec(ctx, "LOCK TABLE app.delivery_jobs IN ACCESS EXCLUSIVE MODE")
		must(err)
		started := time.Now()
		body = scrape(p, 503)
		if time.Since(started) > 4*time.Second || body != "delivery metrics unavailable\n" {
			t.Fatal("unbounded or unsafe failed collection", body)
		}
		must(tx.Rollback(ctx))
		scrape(p, 200, "delivery_backlog 3")
		noHTTP()
		wait("SQL prefix", func() bool {
			p, e := f.Progress(ctx, reader, record.Partition)
			return e == nil && p.Binding == f.Binding && p.Next == record.Offset+1
		})
		wait("broker prefix", func() bool {
			n, e := f.BrokerOffset(ctx, env["NOTIFICATIONS_GROUP"], record.Partition)
			return e == nil && n == record.Offset+1
		})
		proof(record.Partition, record.Offset+1)
		crashEvidence = observe()
		t.Logf("restart smoke pending: trigger=%s state=%s attempts=%d next=%d", trigger, crashEvidence.State, crashEvidence.Attempts, record.Offset+1)
		kill(p)
		return nil
	}))
	// Pending job survives SIGKILL with stable logical binding, bytes and IDs.
	must(apiprocess.With(ctx, cfg, func(p *apiprocess.Process) error {
		o := observe()
		if o.Attempts != 0 || !slices.Equal(o.AttemptIDs, crashEvidence.AttemptIDs) || !bytes.Equal(o.Body, crashEvidence.Body) {
			t.Fatal("pending recovery changed evidence")
		}
		assertBinding()
		proof(record.Partition, record.Offset+1)
		// Let this new member complete assignment before killing it later at the
		// report boundary; an abandoned JoinGroup otherwise waits for session expiry.
		resumed := &kgo.Record{Topic: record.Topic, Key: d.Key, Value: d.Body}
		must(producer.ProduceSync(ctx, resumed).FirstErr())
		wait("pending job intake resumed", func() bool {
			progress, err := f.Progress(ctx, reader, resumed.Partition)
			return err == nil && progress.Next >= resumed.Offset+1
		})
		// Both SQL limiters have admitted this chat before a fresh API permit.
		before := f.PauseRPC(pb.Admission_AdmitDelivery_FullMethodName, true)
		defer before.Release()
		f.SetPrice("101", 2)
		select {
		case <-before.Entered:
		case <-ctx.Done():
			return errors.New("admission boundary missing")
		}
		var n int
		must(reader.QueryRow(ctx, `SELECT count(*) FROM app.bot_limits WHERE bot_id='123'`).Scan(&n))
		if n != 1 {
			t.Fatal("bot limiter absent")
		}
		must(reader.QueryRow(ctx, `SELECT count(*) FROM app.chat_limits WHERE bot_id='123'`).Scan(&n))
		if n != 1 {
			t.Fatal("chat limiter absent")
		}
		noHTTP()
		before.Release()
		first := call()
		if !strings.Contains(first.Text, "Фактическая цена: 101") {
			t.Fatal("stale quote used")
		}
		first.RateLimit()
		wait("429 durable retry", func() bool { return observe().Retryables == 1 })
		if status(p, "/healthz") != 200 || status(p, "/readyz") != 200 {
			t.Fatal("429 broke health")
		}
		o = observe()
		if o.Attempts != 1 || !o.NextAttemptAt.Valid || time.Until(o.NextAttemptAt.Time) < 500*time.Millisecond {
			t.Fatal("retry_after not persisted")
		}
		second := call()
		second.Disconnect()
		wait("unknown durable outcome", func() bool { return observe().Uncertainties == 1 })
		// Wait for the prior report to be acked. Hold the next HTTP request
		// before installing the success gate, so it cannot catch an older report.
		wait("uncertain report ack", func() bool { return observe().PendingReports == 0 })
		third := call()
		successGate := f.PauseRPC(pb.DeliveryResults_SaveDeliveryResult_FullMethodName, true)
		defer successGate.Release()
		third.Reply(88)
		select {
		case <-successGate.Entered:
		case <-ctx.Done():
			return errors.New("success report missing")
		}
		o = observe()
		if o.Successes != 1 || o.PendingReports != 1 || o.Attempts != 3 {
			t.Fatalf("success evidence mismatch: attempts=%d success=%d reports=%d", o.Attempts, o.Successes, o.PendingReports)
		}
		canonical, e := f.Result(ctx, d)
		must(e)
		if canonical.State != domain.Delivered || canonical.Workflow.TriggerID != trigger {
			t.Fatal("canonical API success missing")
		}
		scrape(p, 200, `delivery_pending_reports{kind="success"} 1`, "delivery_backlog 2")
		crashEvidence = o
		t.Logf("restart smoke success: trigger=%s attempts=%v successes=%d pending_reports=%d message_id=88; API=delivered", trigger, o.AttemptIDs, o.Successes, o.PendingReports)
		kill(p)
		return nil
	}))
	// Same group, identity and DB: recover pending report without a new HTTP/claim.
	must(apiprocess.With(ctx, cfg, func(p *apiprocess.Process) error {
		wait("report recovery", func() bool { return observe().PendingReports == 0 })
		o := observe()
		if !slices.Equal(o.AttemptIDs, crashEvidence.AttemptIDs) || o.Claims != crashEvidence.Claims || o.Attempts != 3 || o.Successes != 1 || o.Retryables != 1 || o.Uncertainties != 1 || !bytes.Equal(o.Body, d.Body) {
			t.Fatal("recovery changed durable evidence")
		}
		noHTTP()
		var n int
		t.Logf("restart smoke report recovered: trigger=%s attempts=%v pending_reports=%d; no new HTTP", trigger, o.AttemptIDs, o.PendingReports)
		must(reader.QueryRow(ctx, `SELECT count(*) FROM app.delivery_jobs WHERE trigger_id=$1`, trigger).Scan(&n))
		if n != 1 {
			t.Fatal("job duplicated")
		}
		must(api.QueryRow(ctx, `SELECT count(*) FROM app.delivery_outbox WHERE trigger_id=$1`, trigger).Scan(&n))
		if n != 1 {
			t.Fatal("outbox duplicated")
		}
		confirmIntake()
		memberConfig := cfg
		memberEnv := make(map[string]string, len(env))
		for k, v := range env {
			memberEnv[k] = v
		}
		memberListener, err := net.Listen("tcp", "127.0.0.1:0")
		must(err)
		memberEnv["RPC_ADDR"] = memberListener.Addr().String()
		must(memberListener.Close())
		memberConfig.Env = memberEnv
		must(apiprocess.With(ctx, memberConfig, func(member *apiprocess.Process) error {
			wait("two group members", func() bool {
				req := kmsg.NewPtrDescribeGroupsRequest()
				req.Groups = []string{env["NOTIFICATIONS_GROUP"]}
				res, err := req.RequestWith(ctx, producer)
				return err == nil && len(res.Groups) == 1 && res.Groups[0].ErrorCode == 0 && res.Groups[0].State == "Stable" && len(res.Groups[0].Members) == 2
			})
			assertBinding()
			confirmIntake()
			stop(member, syscall.SIGINT, 0)
			return nil
		}))
		stop(p, syscall.SIGINT, 0)
		return nil
	}))
	// Real signals at deterministic delivery boundaries, using the same durable
	// intake group/checkpoint. A confirmed HTTP response is proved by Result RPC.
	for _, phase := range []string{"before", "during", "after", "write_failure"} {
		t.Log("signal phase:", phase)
		d, e = f.Seed(ctx)
		must(e)
		trigger = d.Record.Workflow.TriggerID
		f.SetPrice("101", 3)
		var gate *np.Pause
		if phase == "before" {
			gate = f.PauseRPC(pb.Admission_PrepareRecipient_FullMethodName, true)
		}
		must(f.Publish(ctx))
		must(apiprocess.With(ctx, cfg, func(p *apiprocess.Process) error {
			if phase == "before" {
				select {
				case <-gate.Entered:
				case <-ctx.Done():
					return ctx.Err()
				}
				noHTTP()
				stop(p, syscall.SIGTERM, 0)
				gate.Release()
				o := observe()
				if o.Attempts != 0 || o.Successes != 0 || o.LeaseID == nil {
					t.Fatal("send or lost lease before HTTP boundary")
				}
				return nil
			}
			send := call()
			o := observe()
			if o.Attempts != 1 || o.LeaseID == nil {
				t.Fatal("missing registered lease")
			}
			if phase == "during" {
				stop(p, syscall.SIGTERM, 0)
				select {
				case <-send.Done():
				case <-ctx.Done():
					return errors.New("HTTP not canceled")
				}
				o = observe()
				if o.Successes != 0 || o.Uncertainties != 1 || o.LeaseID != nil {
					t.Fatal("canceled send not durably uncertain")
				}
			} else if phase == "after" {
				gate = f.PauseRPC(pb.DeliveryResults_SaveDeliveryResult_FullMethodName, true)
				send.Reply(91)
				select {
				case <-gate.Entered:
				case <-ctx.Done():
					return ctx.Err()
				}
				stop(p, syscall.SIGTERM, 0)
				gate.Release()
				o = observe()
				if o.Successes != 1 || o.PendingReports != 0 {
					t.Fatal("confirmed success lost on shutdown")
				}
			} else {
				// Block only this job's final SQL write beyond WRITE_TIMEOUT. The
				// server has replied, but no durable success may be claimed by shutdown.
				tx, e := reader.Begin(ctx)
				must(e)
				defer tx.Rollback(context.Background())
				_, e = tx.Exec(ctx, "SELECT 1 FROM app.delivery_jobs WHERE trigger_id=$1 FOR UPDATE", trigger)
				must(e)
				send.Reply(92)
				wait("success write blocked", func() bool {
					var n int
					must(reader.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE usename='notifications_runtime' AND wait_event_type='Lock'`).Scan(&n))
					return n > 0
				})
				stop(p, syscall.SIGTERM, 1)
				must(tx.Rollback(ctx))
				o = observe()
				if o.Successes != 0 || o.Attempts != 1 {
					t.Fatal("failed final write reported success")
				}
			}
			noHTTP()
			return nil
		}))
		f.SetPrice("101", 4)
		must(apiprocess.With(ctx, cfg, func(p *apiprocess.Process) error {
			if phase != "after" {
				next := call()
				next.Reply(93)
			}
			wait("signal recovery success", func() bool { return observe().Successes == 1 && observe().PendingReports == 0 })
			o := observe()
			if !bytes.Equal(o.Body, d.Body) {
				t.Fatal("signal recovery changed bytes")
			}
			if phase == "after" && o.Attempts != 1 {
				t.Fatal("confirmed send repeated")
			}
			if phase == "write_failure" && o.Uncertainties != 1 {
				t.Fatal("lost outcome not recovered uncertain")
			}
			canonical, e := f.Result(ctx, d)
			must(e)
			if canonical.State != domain.Delivered {
				t.Fatal("signal recovery not delivered")
			}
			noHTTP()
			confirmIntake()
			stop(p, syscall.SIGINT, 0)
			return nil
		}))
	}
	// Fail startup before any readiness for missing credentials, foreign SQL owner,
	// identity mismatch, bad Telegram config and unavailable SQL.
	for _, tc := range []struct{ k, v string }{{"RPC_SERVER_CERT_FILE", "/unavailable/exporter"}, {"RPC_SERVER_KEY_FILE", f.CertFile}, {"NOTIFICATIONS_CONSUMER_ID", "foreign"}, {"NOTIFICATIONS_GENERATION", "foreign"}, {"KAFKA_CLUSTER_ID", "foreign"}, {"DELIVERY_REQUESTS_TOPIC_ID", "0102030405060708090a0b0c0d0e0f10"}, {"RPC_CERT_FILE", "/unavailable/secret"}, {"RPC_CA_FILE", f.KeyFile}, {"NOTIFICATIONS_DATABASE_URL", "postgres://notifications_runtime:secret@127.0.0.1:1/notifications?sslmode=disable&connect_timeout=1"}, {"RPC_IDENTITY", "spiffe://process.test/wrong"}, {"NOTIFICATIONS_DATABASE_URL", "postgres://api_runtime:secret@localhost/api"}, {"TELEGRAM_BASE_URL", "https://secret@host"}} {
		bad := map[string]string{}
		for k, v := range env {
			bad[k] = v
		}
		bad[tc.k] = tc.v
		badcfg := cfg
		badcfg.Env = bad
		if e := apiprocess.With(ctx, badcfg, func(*apiprocess.Process) error { return errors.New("unexpected readiness") }); !errors.Is(e, apiprocess.ErrEarlyExit) {
			t.Fatal("startup failure not observed", tc.k, e)
		}
	}
	// A running idle member detects delete/recreate. Replacement bytes overlap
	// offsets but must never become another job, send, SQL cursor or Kafka ack.
	beforeReplacement := ""
	must(apiprocess.With(ctx, cfg, func(p *apiprocess.Process) error {
		// Recreate an assigned member. HTTP readiness alone does not prove it has
		// joined; killing an in-flight JoinGroup can retain a rebalance obligation.
		confirmIntake()
		beforeReplacement = evidence()
		del := kmsg.NewPtrDeleteTopicsRequest()
		del.TopicNames = []string{record.Topic}
		del.Topics = []kmsg.DeleteTopicsRequestTopic{{Topic: &record.Topic}}
		dr, err := del.RequestWith(ctx, producer)
		must(err)
		if len(dr.Topics) != 1 || dr.Topics[0].ErrorCode != 0 {
			return errors.New("delete failed")
		}
		create := kmsg.NewPtrCreateTopicsRequest()
		spec := kmsg.NewCreateTopicsRequestTopic()
		spec.Topic, spec.NumPartitions, spec.ReplicationFactor = record.Topic, 16, 1
		create.Topics = []kmsg.CreateTopicsRequestTopic{spec}
		cr, err := create.RequestWith(ctx, producer)
		must(err)
		if len(cr.Topics) != 1 || cr.Topics[0].ErrorCode != 0 {
			return errors.New("recreate failed")
		}
		fresh, err := kgo.NewClient(kgo.SeedBrokers(f.Broker), kgo.RecordPartitioner(kgo.ManualPartitioner()))
		must(err)
		defer fresh.Close()
		replacement := &kgo.Record{Topic: record.Topic, Partition: record.Partition, Key: d.Key, Value: d.Body}
		must(fresh.ProduceSync(ctx, replacement).FirstErr())
		if replacement.Offset != 0 {
			return errors.New("replacement offsets do not overlap")
		}
		select {
		case <-p.Done():
			code, ok := p.ExitCode()
			if !ok || code != 1 {
				return errors.New("source loss did not exit 1")
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}))
	noHTTP()
	if evidence() != beforeReplacement {
		t.Fatal("recreate changed durable evidence")
	}
	assertBinding()
	baseline, err := f.BrokerOffset(ctx, env["NOTIFICATIONS_GROUP"], record.Partition)
	must(err)
	reject(cfg) // old source
	replacementClient, err := kgo.NewClient(kgo.SeedBrokers(f.Broker))
	must(err)
	replacementSource, _, err := kafkasource.Metadata(ctx, replacementClient, record.Topic)
	replacementClient.Close()
	must(err)
	if replacementSource == f.Binding.Source {
		t.Fatal("topic incarnation unchanged")
	}
	env["DELIVERY_REQUESTS_TOPIC_ID"] = hex.EncodeToString(replacementSource.TopicID[:])
	reject(cfg) // today's metadata cannot relabel old SQL
	if evidence() != beforeReplacement {
		t.Fatal("startup relabeled SQL")
	}
	n, err := f.BrokerOffset(ctx, env["NOTIFICATIONS_GROUP"], record.Partition)
	must(err)
	if n != baseline {
		t.Fatal("replacement acknowledged")
	}
	noHTTP()
	// Legacy provenance loss preserves all data and fails before worker/HTTP.
	legacyBefore := evidence()
	_, err = reader.Exec(ctx, `SELECT app.test_legacy_delivery_source()`)
	must(err)
	reject(cfg)
	if _, err := intake.NewStore(reader).ReadProgress(ctx, f.Binding, record.Partition); !errors.Is(err, intake.ErrUnbound) {
		t.Fatal("legacy accepted", err)
	}
	if err := intake.NewStore(reader).Bootstrap(ctx, f.Binding, []int32{0}); !errors.Is(err, intake.ErrUnbound) {
		t.Fatal("legacy bootstrap accepted", err)
	}
	if evidence() != legacyBefore {
		t.Fatal("legacy refusal changed evidence")
	}
	noHTTP()

	_, err = reader.Exec(ctx, `SELECT app.test_restore_delivery_source()`)
	must(err)
	assertBinding()
	// Model a destroyed SQL intake only through this trusted disposable fixture.
	// A fresh bootstrap uses a NEW externally expected generation.
	oldBinding := f.Binding
	_, err = reader.Exec(ctx, `SELECT app.test_reset_delivery_source()`)
	must(err)
	must(f.Provision(ctx, reader))
	if f.Binding.Generation == oldBinding.Generation {
		t.Fatal("generation reused")
	}
	if _, err := intake.NewStore(reader).ReadProgress(ctx, oldBinding, record.Partition); !errors.Is(err, kafkasource.ErrMismatch) {
		t.Fatal("old expectation accepted", err)
	}
	reject(cfg) // old generation remains in child env
	for k, v := range f.ChildEnv(os.Getenv("NOTIFICATIONS_DATABASE_URL")) {
		env[k] = v
	}
	assertBinding()
	f.SetPrice("99", 5)
	must(apiprocess.With(ctx, cfg, func(p *apiprocess.Process) error {
		wait("fresh SQL generation prefix", func() bool {
			progress, err := f.Progress(ctx, reader, record.Partition)
			return err == nil && progress.Next > 0
		})
		noHTTP()
		stop(p, syscall.SIGINT, 0)
		return nil
	}))
	// A malformed committed record is terminal: never ack/reset past it.
	badrecord := &kgo.Record{Topic: record.Topic, Key: d.Key, Value: []byte(`invalid`)}
	freshProducer, err := kgo.NewClient(kgo.SeedBrokers(f.Broker))
	must(err)
	defer freshProducer.Close()
	must(freshProducer.ProduceSync(ctx, badrecord).FirstErr())
	terminal := cfg
	terminal.StartupProbePath = "/healthz"
	e = apiprocess.With(ctx, terminal, func(p *apiprocess.Process) error {
		select {
		case <-p.Done():
			code, ok := p.ExitCode()
			if !ok || code != 1 {
				t.Fatal("terminal exit missing", code)
			}
		case <-ctx.Done():
			return errors.New("terminal intake did not stop owner")
		}
		return nil
	})
	if e != nil && !errors.Is(e, apiprocess.ErrEarlyExit) {
		must(e)
	}
	offset, e := np.Checkpoint(ctx, reader, badrecord.Topic, badrecord.Partition)
	must(e)
	if offset > badrecord.Offset {
		t.Fatal("SQL skipped invalid record")
	}
	brokerOffset, e := f.BrokerOffset(ctx, env["NOTIFICATIONS_GROUP"], badrecord.Partition)
	must(e)
	if brokerOffset > badrecord.Offset {
		t.Fatal("broker ack skipped invalid record")
	}
}
