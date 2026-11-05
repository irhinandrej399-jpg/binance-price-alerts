package apimeasure

// OPS-026 infrastructure windows over production adapters, not a runtime.
import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/admissionrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/resultrpc"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	market "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/latest"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/botlimit"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/chatlimit"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/handler"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/intake"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/jobs"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/report"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/retry"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/telegram"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/worker"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
)

type notificationsDriver struct {
	t               *testing.T
	f               *notificationFixture
	api, local      *pgxpool.Pool
	boundary, topic string
	records         []*kgo.Record
	intake          *intake.Store
	jobs            *jobs.Store
	reporter        *report.Reporter
	results         *resultrpc.Client
	handler         *handler.Handler
	quote           *quoteFixture
	policy          *retry.Policy
	leases          [4]jobs.Lease
	next            [4]time.Time
	pending         [4]*handler.PendingWrite
	calls           atomic.Int64
	mode            atomic.Int64
	phases          []map[string]any
}

func notificationsSetup(t *testing.T) *notificationsDriver {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	d := &notificationsDriver{t: t, f: fixtureSource(t), boundary: os.Getenv("OPS026_BOUNDARY"), topic: os.Getenv("KAFKA_PREFIX") + "-delivery-requests"}
	d.api = fixturePool(t, "DATABASE_URL", "api_runtime")
	d.local = fixturePool(t, "NOTIFICATIONS_DATABASE_URL", "notifications_runtime")
	check(t, d.f.importAPI(ctx, d.api, d.boundary))
	d.intake = intake.NewStore(d.local)
	var e error
	d.jobs, e = jobs.New(d.local, time.Minute)
	check(t, e)
	d.policy, e = retry.New(retry.Config{BaseDelay: time.Hour, MaxDelay: time.Hour, ConditionInterval: time.Hour, MaxFailures: 3})
	check(t, e)
	ca := newAuthority(t)
	cert := ca.issue(t, nil, true, false)
	d.quote = &quoteFixture{value: "101", tradeTime: time.Now().UTC().Truncate(time.Microsecond), sequence: 1}
	marketConn := connect(t, ca, cert, apiURI, map[string][]string{apiURI: {market.Quotes_Latest_FullMethodName}}, func(s *grpc.Server) { market.RegisterQuotesServer(s, latest.NewServer(d.quote, time.Now)) })
	server, e := admissionrpc.NewServer(admissionrpc.NewPostgres(d.api), latest.NewClient(marketConn), time.Minute, time.Now)
	check(t, e)
	lost := &fixtureLostReply{store: resultrpc.NewPostgres(d.api), lost: map[string]bool{}}
	conn := connect(t, ca, cert, notificationsURI, map[string][]string{notificationsURI: {pb.Admission_PrepareRecipient_FullMethodName, pb.Admission_AdmitDelivery_FullMethodName, pb.DeliveryResults_SaveDeliveryResult_FullMethodName}}, func(s *grpc.Server) {
		pb.RegisterAdmissionServer(s, server)
		pb.RegisterDeliveryResultsServer(s, resultrpc.NewServer(lost))
	})
	d.results = resultrpc.NewClient(conn)
	d.reporter, e = report.New(d.local, d.results)
	check(t, e)
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		d.calls.Add(1)
		var payload struct {
			Chat int64  `json:"chat_id"`
			Text string `json:"text"`
		}
		if e := json.NewDecoder(r.Body).Decode(&payload); e != nil || r.Method != "POST" || payload.Chat != 24001 || !strings.Contains(payload.Text, "Фактическая цена: 101") {
			t.Error("invalid local HTTP payload", e)
		}
		switch d.mode.Load() {
		case 1:
			c, _, e := w.(http.Hijacker).Hijack()
			if e != nil {
				t.Error(e)
				return
			}
			_ = c.Close()
		case 2:
			w.WriteHeader(429)
			_, _ = io.WriteString(w, `{"ok":false,"error_code":429,"parameters":{"retry_after":3600}}`)
		case 3:
			w.WriteHeader(403)
			_, _ = io.WriteString(w, `{"ok":false,"error_code":403}`)
		default:
			_, _ = io.WriteString(w, `{"ok":true,"result":{"message_id":24001}}`)
		}
	}))
	t.Cleanup(httpServer.Close)
	sender, e := telegram.New(telegram.Config{Token: "fixture", Timeout: time.Second, BaseURL: httpServer.URL})
	check(t, e)
	bot, e := botlimit.New(d.local, "26001", 25)
	check(t, e)
	chat, e := chatlimit.New(d.local, "26001", time.Second)
	check(t, e)
	d.handler, e = handler.New(handler.Config{Store: d.jobs, Admission: admissionrpc.NewClient(conn), Bot: bot, Chat: chat, Sender: sender, Reporter: d.reporter, Policy: d.policy, RecoveryInterval: time.Second, ReportLimit: 10})
	check(t, e)
	// Re-publish exact recorded bytes. New coordinates never replace source offsets.
	producer, e := kgo.NewClient(kgo.SeedBrokers(os.Getenv("KAFKA_BROKERS")), kgo.TransactionalID(os.Getenv("KAFKA_PREFIX")+"-ops026"), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	check(t, e)
	defer producer.Close()
	for _, r := range d.f.Records {
		raw, e := hex.DecodeString(r.Body)
		check(t, e)
		record := &kgo.Record{Topic: d.topic, Partition: 0, Key: []byte(r.Key), Value: raw}
		check(t, producer.BeginTransaction())
		check(t, producer.ProduceSync(ctx, record).FirstErr())
		check(t, producer.EndTransaction(ctx, kgo.TryCommit))
		d.records = append(d.records, record)
	}
	reader, e := kgo.NewClient(kgo.SeedBrokers(os.Getenv("KAFKA_BROKERS")), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{d.topic: {0: kgo.NewOffset().AtStart()}}), kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	check(t, e)
	defer reader.Close()
	n := 0
	for n < len(d.records) {
		fetch := reader.PollFetches(ctx)
		check(t, fetch.Err())
		for _, r := range fetch.Records() {
			if n >= len(d.records) || r.Offset != d.records[n].Offset || !bytes.Equal(r.Value, d.records[n].Value) || !bytes.Equal(r.Key, d.records[n].Key) {
				t.Fatal("read_committed prefix mismatch")
			}
			n++
		}
	}
	return d
}

func (d *notificationsDriver) operation(name string) any {
	t := d.t
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result := map[string]any{"operations": 1}
	accept := func(index int) {
		r := d.records[index]
		next, e := d.intake.Checkpoint(ctx, d.topic, 0)
		check(t, e)
		check(t, d.intake.Accept(ctx, d.topic, 0, next, r.Offset, r.Key, r.Value))
	}
	switch name {
	case "empty":
		result["operations"] = 0
	case "checkpoint":
		next, e := d.intake.Checkpoint(ctx, d.topic, 0)
		check(t, e)
		if next != 0 {
			t.Fatal(next)
		}
	case "intake_first":
		accept(0)
	case "intake_repeat":
		r := d.records[0]
		check(t, d.intake.Accept(ctx, d.topic, 0, 0, r.Offset, r.Key, r.Value))
	case "intake_second":
		accept(1)
	case "intake_physical_duplicate":
		accept(2)
	case "intake_tail":
		for i := 3; i < len(d.records); i++ {
			accept(i)
		}
		result["operations"] = 2
	case "observe_intake":
		var count int
		var next int64
		check(t, d.local.QueryRow(ctx, "SELECT count(*) FROM app.delivery_jobs").Scan(&count))
		check(t, d.local.QueryRow(ctx, "SELECT next_offset FROM app.delivery_checkpoints WHERE topic=$1 AND partition=0", d.topic).Scan(&next))
		result["jobs"] = count
		result["next_offset"] = next
	case "worker_sweep":
		short, stop := context.WithTimeout(ctx, 100*time.Millisecond)
		defer stop()
		w, e := worker.New(worker.Config{Source: d.jobs, Handler: d.handler, Concurrency: 4, Interval: time.Second, Timeout: time.Second, WriteTimeout: time.Second, RecoveryInterval: time.Hour})
		check(t, e)
		if e = w.Run(short); !errors.Is(e, context.DeadlineExceeded) {
			t.Fatal(e)
		}
	case "discover":
		work, e := d.jobs.Discover(ctx, "", 1000)
		check(t, e)
		result["work"] = work
	case "recovery_schedule", "recovery_repeat":
		check(t, d.jobs.ScheduleRecovery(ctx, d.f.Items[3].ID.Workflow, 1, time.Second))
	case "recovery_claim":
		l, e := d.jobs.Claim(ctx, d.f.Items[3].ID.Workflow)
		check(t, e)
		if l.Fence != 2 {
			t.Fatal("recovery fence", l)
		}
		d.leases[3] = l
		result["lease"] = l
	case "recovery_park":
		check(t, d.jobs.ParkBlocked(ctx, d.leases[3], domain.ChannelUnavailable, 1))
	default:
		parts := strings.Split(name, "_")
		index, e := strconv.Atoi(parts[len(parts)-1])
		check(t, e)
		if index < 0 || index > 3 {
			t.Fatal(name)
		}
		id := d.f.Items[index].ID
		lease := d.leases[index]
		prefix := strings.Join(parts[:len(parts)-1], "_")
		switch prefix {
		case "claim_register":
			l, e := d.jobs.Claim(ctx, id.Workflow)
			check(t, e)
			d.leases[index] = l
			check(t, d.jobs.RegisterAttempt(ctx, l, id.Attempt))
			result["lease"] = l
		case "outcome", "outcome_repeat":
			if d.next[index].IsZero() {
				d.next[index] = time.Now().Add(time.Hour)
			}
			switch index {
			case 0:
				e = d.jobs.SaveSuccess(ctx, lease, id.Attempt, 24001)
			case 1:
				var plan retry.Plan
				plan, e = d.jobs.SaveRetryable(ctx, lease, id.Attempt, &telegram.Error{Kind: telegram.Server}, d.policy, 0, 1)
				result["plan"] = plan
			case 2:
				e = d.jobs.SaveUncertain(ctx, lease, id.Attempt, d.next[index])
			case 3:
				e = d.jobs.SaveBlocked(ctx, lease, id.Attempt, domain.ChannelUnavailable, 1)
			}
			check(t, e)
		case "report_loss":
			n, e := d.reporter.Reconcile(ctx, id.Workflow, 10)
			if e == nil || n != 0 {
				t.Fatal("expected lost post-commit reply", n, e)
			}
			result["acked"] = n
			result["lost_reply"] = true
		case "report_replay", "report_empty":
			// Fresh pool/reporter after reply loss, with notifications access only.
			local := fixturePool(t, "NOTIFICATIONS_DATABASE_URL", "notifications_runtime")
			// Reporter client stays the same; its gRPC connection has no SQL access.
			reporter, e := report.New(local, d.results)
			check(t, e)
			n, e := reporter.Reconcile(ctx, id.Workflow, 10)
			check(t, e)
			result["acked"] = n
			local.Close()
		case "handle":
			d.mode.Store(int64([]int{0, 2, 1, 3}[index]))
			d.quote.tradeTime = time.Now().UTC().Truncate(time.Microsecond)
			e := d.handler.Handle(ctx, id.Workflow)
			var pending *handler.PendingWrite
			if !errors.As(e, &pending) {
				t.Fatal("expected durable result pending after lost API reply", e)
			}
			d.pending[index] = pending
			result["pending_write"] = true
			result["http_calls"] = d.calls.Load()
		case "handle_retry":
			if d.pending[index] == nil {
				t.Fatal("missing pending write")
			}
			check(t, d.pending[index].Retry(ctx))
			result["http_calls"] = d.calls.Load()
		default:
			t.Fatal("unknown phase", name)
		}
	}
	return result
}

func (d *notificationsDriver) verify() any {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	t := d.t
	records := []any{}
	for i, r := range d.records {
		records = append(records, map[string]any{"source": d.f.Records[i], "topic": r.Topic, "partition": r.Partition, "offset": r.Offset, "body_hex": hex.EncodeToString(r.Value), "key": string(r.Key)})
	}
	items := []any{}
	for _, item := range d.f.Items {
		id := item.ID
		row := map[string]any{"identity": id, "event_id": item.Event, "body_sha256": fmt.Sprintf("%x", sha256.Sum256(item.Body)), "source_response_sha256": fmt.Sprintf("%x", sha256.Sum256(item.Response)), "source_permit": item.Admission.Attempt}
		var state, body string
		var fence int64
		check(t, d.local.QueryRow(ctx, "SELECT state,encode(body,'hex'),lease_fence FROM app.delivery_jobs WHERE trigger_id=$1", id.Workflow).Scan(&state, &body, &fence))
		row["job_state"] = state
		row["body_hex"] = body
		row["fence"] = fence
		var attempt, request, expires string
		var response []byte
		check(t, d.api.QueryRow(ctx, "SELECT attempt_id,request_id,expires_at::text,response FROM app.delivery_admissions WHERE subscription_id=$1 AND attempt_id IS NOT NULL", id.Subscription).Scan(&attempt, &request, &expires, &response))
		row["current_attempt_id"] = attempt
		row["current_request_id"] = request
		row["expires_at"] = expires
		row["response_hex"] = hex.EncodeToString(response)
		check(t, d.api.QueryRow(ctx, "SELECT state FROM app.subscriptions WHERE id=$1", id.Subscription).Scan(&state))
		row["api_state"] = state
		items = append(items, row)
	}
	counts := map[string]int{}
	for _, table := range []string{"delivery_jobs", "delivery_claims", "delivery_attempts", "delivery_successes", "delivery_uncertainties", "delivery_retryables", "delivery_blocks", "delivery_block_parks"} {
		var n int
		check(t, d.local.QueryRow(ctx, "SELECT count(*) FROM app."+table).Scan(&n))
		counts[table] = n
	}
	for _, kind := range []string{"success", "uncertain", "retryable", "blocked"} {
		var n int
		check(t, d.local.QueryRow(ctx, "SELECT count(*) FROM app.delivery_"+kind+"_reports WHERE pending").Scan(&n))
		counts[kind+"_pending"] = n
	}
	var audits, journal, slots int
	check(t, d.api.QueryRow(ctx, "SELECT count(*) FROM app.delivery_results").Scan(&audits))
	check(t, d.api.QueryRow(ctx, "SELECT count(*) FROM app.api_journal").Scan(&journal))
	check(t, d.api.QueryRow(ctx, "SELECT active_subscriptions FROM app.users").Scan(&slots))
	var next int64
	check(t, d.local.QueryRow(ctx, "SELECT next_offset FROM app.delivery_checkpoints WHERE topic=$1 AND partition=0", d.topic).Scan(&next))
	return map[string]any{"format_version": 1, "source_id": "OPS-026-notifications-v1", "source_sha256": notificationSourceHash, "boundary": d.boundary, "sql_role": "notifications_runtime", "records": records, "items": items, "counts": counts, "api_results": audits, "api_journal": journal, "api_slots": slots, "checkpoint": next, "http_calls": d.calls.Load(), "phases": d.phases}
}
func TestNotificationsMeasurementDriver(t *testing.T) {
	if os.Getenv("OPS026_MEASURE") != "1" {
		t.Skip("opt-in infrastructure driver")
	}
	scanner := bufio.NewScanner(os.Stdin)
	enc := json.NewEncoder(os.Stdout)
	var d *notificationsDriver
	for scanner.Scan() {
		var req struct {
			Phase string `json:"phase_id"`
		}
		check(t, json.Unmarshal(scanner.Bytes(), &req))
		var evidence any
		switch req.Phase {
		case "setup":
			d = notificationsSetup(t)
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
