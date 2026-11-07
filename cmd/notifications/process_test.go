package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/apiprocess"
	np "github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/notificationsprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestNotificationsProcess(t *testing.T) {
	if os.Getenv("NOTIFICATIONS_PROCESS_INTEGRATION") != "1" {
		t.Skip("run python3 scripts/check_notifications_process.py --package ./cmd/notifications")
	}
	if os.Getenv("POSTGRES_TEST") != "1" || os.Getenv("KAFKA_TEST_BROKER") == "" || os.Getenv("KAFKA_TEST_PREFIX") == "" {
		t.Fatal("notifications opt-in requires PostgreSQL/Kafka configuration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
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
			n, e := np.Checkpoint(ctx, reader, duplicate.Topic, duplicate.Partition)
			return e == nil && n >= duplicate.Offset+1
		})
		wait("restart duplicate broker prefix", func() bool {
			n, e := f.BrokerOffset(ctx, env["NOTIFICATIONS_GROUP"], duplicate.Partition)
			return e == nil && n >= duplicate.Offset+1
		})
		after := observe()
		if after.State != before.State || after.Attempts != before.Attempts || after.Claims != before.Claims || after.Successes != before.Successes || !bytes.Equal(after.Body, before.Body) {
			t.Fatal("duplicate intake changed completed job")
		}
		noHTTP()
	}

	f.SetPrice("99", 1)
	// False Latest gates HTTP despite a canonical historical candidate.
	must(apiprocess.With(ctx, cfg, func(p *apiprocess.Process) error {
		wait("false Latest", func() bool { o, e := np.Observe(ctx, reader, trigger); return e == nil && o.State == "wait_condition" })
		o := observe()
		if !bytes.Equal(o.Body, d.Body) || o.Attempts != 0 || o.LeaseID != nil {
			t.Fatal("durable job mismatch")
		}
		noHTTP()
		wait("SQL prefix", func() bool {
			n, e := np.Checkpoint(ctx, reader, record.Topic, record.Partition)
			return e == nil && n == record.Offset+1
		})
		wait("broker prefix", func() bool {
			n, e := f.BrokerOffset(ctx, env["NOTIFICATIONS_GROUP"], record.Partition)
			return e == nil && n == record.Offset+1
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
		kill(p)
		return nil
	}))
	// Same group, identity and DB: recover pending report without a new HTTP/claim.
	must(apiprocess.With(ctx, cfg, func(p *apiprocess.Process) error {
		wait("report recovery", func() bool { return observe().PendingReports == 0 })
		o := observe()
		if o.Attempts != 3 || o.Successes != 1 || o.Retryables != 1 || o.Uncertainties != 1 || !bytes.Equal(o.Body, d.Body) {
			t.Fatal("recovery changed durable evidence")
		}
		noHTTP()
		var n int
		must(reader.QueryRow(ctx, `SELECT count(*) FROM app.delivery_jobs WHERE trigger_id=$1`, trigger).Scan(&n))
		if n != 1 {
			t.Fatal("job duplicated")
		}
		must(api.QueryRow(ctx, `SELECT count(*) FROM app.delivery_outbox WHERE trigger_id=$1`, trigger).Scan(&n))
		if n != 1 {
			t.Fatal("outbox duplicated")
		}
		confirmIntake()
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
	for _, tc := range []struct{ k, v string }{{"RPC_CERT_FILE", "/unavailable/secret"}, {"RPC_CA_FILE", f.KeyFile}, {"NOTIFICATIONS_DATABASE_URL", "postgres://notifications_runtime:secret@127.0.0.1:1/notifications?sslmode=disable&connect_timeout=1"}, {"RPC_IDENTITY", "spiffe://process.test/wrong"}, {"NOTIFICATIONS_DATABASE_URL", "postgres://api_runtime:secret@localhost/api"}, {"TELEGRAM_BASE_URL", "https://secret@host"}} {
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
	// A malformed committed record is terminal: never ack/reset past it.
	badrecord := &kgo.Record{Topic: record.Topic, Key: d.Key, Value: []byte(`invalid`)}
	must(producer.ProduceSync(ctx, badrecord).FirstErr())
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
