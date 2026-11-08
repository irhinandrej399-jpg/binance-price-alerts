package notificationsprocess

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/admissionrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/resultrpc"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/botlimit"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/chatlimit"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/handler"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/intake"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/jobs"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/report"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/retry"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/telegram"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/postgres"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/apiprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A connectivity probe only: no product worker, intake or signal handling.
func TestChild(t *testing.T) {
	if os.Getenv("NOTIFICATIONS_PROCESS_CHILD") != "1" {
		return
	}
	fail := func() { os.Exit(25) }
	for _, k := range []string{"DATABASE_URL", "API_DATABASE_URL", "MATCHER_DATABASE_URL", "MARKET_DATA_DATABASE_URL", "PARENT_SECRET"} {
		if os.Getenv(k) != "" {
			fail()
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	pool, e := postgres.Open(ctx, os.Getenv("NOTIFICATIONS_DATABASE_URL"))
	if e != nil {
		fail()
	}
	defer pool.Close()
	var db, role string
	var foreign bool
	e = pool.QueryRow(ctx, `SELECT current_database(),current_user,has_database_privilege(current_user,'api','CONNECT') OR has_database_privilege(current_user,'matcher','CONNECT') OR has_database_privilege(current_user,'market_data','CONNECT')`).Scan(&db, &role, &foreign)
	if e != nil || db != "notifications" || role != "notifications_runtime" || foreign {
		fail()
	}
	read := func(k string) []byte {
		b, err := os.ReadFile(os.Getenv(k))
		if err != nil {
			fail()
		}
		return b
	}
	creds, e := serviceauth.ClientCredentials(read("RPC_CERT_FILE"), read("RPC_KEY_FILE"), read("RPC_CA_FILE"))
	if e != nil {
		fail()
	}
	conn, e := grpc.NewClient(os.Getenv("API_RPC_ADDR"), grpc.WithTransportCredentials(creds), grpc.WithAuthority(os.Getenv("RPC_SERVER_NAME")))
	if e != nil {
		fail()
	}
	defer conn.Close()
	r, e := admissionrpc.NewClient(conn).PrepareRecipient(ctx, os.Getenv("PROBE_OWNER"), os.Getenv("PROBE_SUBSCRIPTION"), os.Getenv("PROBE_WORKFLOW"))
	if e != nil || r.Recipient == nil || r.Recipient.ChatId <= 0 {
		fail()
	}
	_, e = pb.NewDeliveryClient(conn).RequestDelivery(ctx, &pb.RequestDeliveryRequest{})
	if status.Code(e) != codes.PermissionDenied {
		fail()
	}
	// Allowed Result method reaches the production adapter's input validation.
	_, e = resultrpc.NewClient(conn).SaveDeliveryResult(ctx, &pb.SaveDeliveryResultRequest{})
	if status.Code(e) != codes.InvalidArgument {
		fail()
	}
	c, e := kgo.NewClient(kgo.SeedBrokers(os.Getenv("KAFKA_BROKERS")))
	if e != nil {
		fail()
	}
	defer c.Close()
	if c.Ping(ctx) != nil {
		fail()
	}
	meta, e := kmsg.NewPtrMetadataRequest().RequestWith(ctx, c)
	if e != nil {
		fail()
	}
	found := false
	for _, topic := range meta.Topics {
		if topic.Topic != nil && *topic.Topic == os.Getenv("DELIVERY_REQUESTS_TOPIC") && len(topic.Partitions) == 16 {
			found = true
		}
	}
	if !found {
		fail()
	}
	sender, e := telegram.New(telegram.Config{Token: os.Getenv("TELEGRAM_BOT_TOKEN"), BaseURL: os.Getenv("TELEGRAM_BASE_URL"), Timeout: 2 * time.Second})
	if e != nil {
		fail()
	}
	id, e := sender.Send(ctx, r.Recipient.ChatId, "<b>connectivity probe</b>")
	if e != nil || id != 71 {
		fail()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	_ = http.ListenAndServe(os.Getenv("HTTP_ADDR"), mux)
	os.Exit(26)
}

func TestDisposableFixture(t *testing.T) {
	if os.Getenv("NOTIFICATIONS_PROCESS_INTEGRATION") != "1" {
		t.Skip("run python3 scripts/check_notifications_process.py")
	}
	if os.Getenv("POSTGRES_TEST") != "1" || os.Getenv("KAFKA_TEST_BROKER") == "" || os.Getenv("KAFKA_TEST_PREFIX") == "" {
		t.Fatal("notifications opt-in requires PostgreSQL/Kafka configuration")
	}
	api := postgrestest.Open(t)
	pool := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
	reader := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 65*time.Second)
	defer cancel()
	must := func(e error) {
		t.Helper()
		if e != nil {
			t.Fatal(e)
		}
	}
	f := New(t, api, os.Getenv("KAFKA_TEST_BROKER"), os.Getenv("KAFKA_TEST_PREFIX"))
	must(f.Provision(ctx, pool))
	d, e := f.Seed(ctx)
	must(e)
	trigger := d.Record.Workflow.TriggerID
	// Independent read_committed observer captures immutable producer bytes.
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
		t.Fatal("relay bytes differ from API outbox")
	}
	executable, e := os.Executable()
	must(e)
	t.Setenv("PARENT_SECRET", "must-not-leak")
	env := f.ChildEnv(os.Getenv("NOTIFICATIONS_DATABASE_URL"))
	env["NOTIFICATIONS_PROCESS_CHILD"] = "1"
	env["PROBE_OWNER"], env["PROBE_SUBSCRIPTION"], env["PROBE_WORKFLOW"] = d.Record.OwnerID, d.Record.ID, trigger
	cfg := apiprocess.Config{Executable: executable, Args: []string{"-test.run=^TestChild$"}, Env: env, StartupTimeout: 10 * time.Second, StopTimeout: time.Second}
	// The probe's HTTP request also has an explicit parent-controlled response.
	probeDone := make(chan error, 1)
	go func() { probeDone <- apiprocess.With(ctx, cfg, func(*apiprocess.Process) error { return nil }) }()
	select {
	case call := <-f.Telegram.Requests:
		if call.Text != "<b>connectivity probe</b>" {
			t.Fatal("unexpected probe")
		}
		call.Reply(71)
	case e := <-probeDone:
		t.Fatal("child exited before HTTP", e)
	case <-ctx.Done():
		t.Fatal("probe HTTP missing")
	}
	select {
	case e := <-probeDone:
		must(e)
	case <-ctx.Done():
		t.Fatal("probe owner did not finish")
	}
	bad := f.ChildEnv("invalid-dsn")
	bad["NOTIFICATIONS_PROCESS_CHILD"] = "1"
	cfg.Env = bad
	if e = apiprocess.With(ctx, cfg, func(*apiprocess.Process) error { return errors.New("unexpected startup") }); !errors.Is(e, apiprocess.ErrEarlyExit) {
		t.Fatal("startup failure not observed", e)
	}
	group := f.Prefix + "-notifications"
	consumer, e := intake.New(intake.Config{Binding: f.Binding, Brokers: []string{f.Broker}, Topic: record.Topic, Group: group}, intake.NewStore(pool))
	must(e)
	for {
		must(consumer.Step(ctx))
		progress, err := f.Progress(ctx, reader, record.Partition)
		must(err)
		if progress.Next >= record.Offset+1 {
			break
		}
	}
	consumer.Close()
	o, e := Observe(ctx, reader, trigger)
	must(e)
	if o.State != "pending" || !bytes.Equal(o.Body, d.Body) || o.Claims != 0 {
		t.Fatal("durable intake mismatch")
	}
	checkpoint, e := Checkpoint(ctx, reader, record.Topic, record.Partition)
	must(e)
	offset, e := f.BrokerOffset(ctx, group, record.Partition)
	must(e)
	if checkpoint != record.Offset+1 || offset != checkpoint {
		t.Fatal("SQL/broker prefix mismatch")
	}
	store, e := jobs.New(pool, time.Minute)
	must(e)
	reporter, e := report.New(pool, resultrpc.NewClient(f.Conn))
	must(e)
	bot, e := botlimit.New(pool, "1", 100)
	must(e)
	chat, e := chatlimit.New(pool, "1", time.Microsecond)
	must(e)
	sender, e := telegram.New(telegram.Config{Token: TestToken, BaseURL: f.Telegram.URL, Timeout: 5 * time.Second})
	must(e)
	policy, e := retry.New(retry.Config{BaseDelay: 30 * time.Millisecond, MaxDelay: time.Second, MaxFailures: 5, ConditionInterval: 30 * time.Millisecond})
	must(e)
	h, e := handler.New(handler.Config{Store: store, Admission: admissionrpc.NewClient(f.Conn), Bot: bot, Chat: chat, Sender: sender, Reporter: reporter, Policy: policy, RecoveryInterval: time.Second, ReportLimit: 10})
	must(e)
	// Event price=100 met the threshold, current Latest=99 must prohibit Send.
	f.SetPrice("99", 1)
	must(h.Handle(ctx, trigger))
	o, e = Observe(ctx, reader, trigger)
	must(e)
	if o.State != "wait_condition" || o.Attempts != 0 || o.LeaseID != nil {
		t.Fatal("historical event authorized Send")
	}
	select {
	case <-f.Telegram.Requests:
		t.Fatal("false condition sent HTTP")
	default:
	}
	canonical, e := f.Result(ctx, d)
	must(e)
	if canonical.State != domain.Active || canonical.Workflow.TriggerID != trigger {
		t.Fatal("wait lost canonical workflow")
	}
	// Wait for the real SQL deadline; no direct mutation of lease/attempt state.
	if !o.NextAttemptAt.Valid || o.NextAttemptAt.Time.IsZero() {
		t.Fatal("scheduled condition check missing")
	}
	next := o.NextAttemptAt.Time
	timer := time.NewTimer(time.Until(next) + time.Millisecond)
	select {
	case <-timer.C:
	case <-ctx.Done():
		timer.Stop()
		t.Fatal("condition deadline")
	}
	f.SetPrice("101", 2)
	before := f.PauseRPC(pb.Admission_AdmitDelivery_FullMethodName, true)
	defer before.Release()
	after := f.PauseRPC(pb.DeliveryResults_SaveDeliveryResult_FullMethodName, true)
	defer after.Release()
	done := make(chan error, 1)
	go func() { done <- h.Handle(ctx, trigger) }()
	select {
	case <-before.Entered:
	case <-ctx.Done():
		t.Fatal("admission boundary missing")
	}
	o, e = Observe(ctx, reader, trigger)
	must(e)
	if o.LeaseID == nil || o.Fence != 2 || o.Attempts != 0 {
		t.Fatal("pre-Send observer mismatch")
	}
	select {
	case <-f.Telegram.Requests:
		t.Fatal("HTTP before permission response")
	default:
	}
	before.Release()
	var call *Send
	select {
	case call = <-f.Telegram.Requests:
	case <-ctx.Done():
		t.Fatal("Send missing")
	}
	if !strings.Contains(call.Text, "Фактическая цена: 101") {
		t.Fatal("Send did not use current permit quote")
	}
	o, e = Observe(ctx, reader, trigger)
	must(e)
	if o.Attempts != 1 || o.Successes != 0 || o.LeaseID == nil {
		t.Fatal("in-flight observer mismatch")
	}
	call.Reply(88)
	select {
	case <-after.Entered:
	case <-ctx.Done():
		t.Fatal("result boundary missing")
	}
	o, e = Observe(ctx, reader, trigger)
	must(e)
	if o.State != "succeeded" || o.Successes != 1 || o.PendingReports != 1 || o.LeaseID != nil {
		t.Fatal("pending report observation mismatch")
	}
	canonical, e = f.Result(ctx, d)
	must(e)
	if canonical.State != domain.Delivered || canonical.Workflow.TriggerID != trigger {
		t.Fatal("API result missing")
	}
	// Lose the result response after API commit. The durable obligation survives;
	// a fresh Reporter/pool repeats the result, never sendMessage.
	// Gate's RPC context has its own 3s budget.
	select {
	case e = <-done:
		if e == nil {
			t.Fatal("held result unexpectedly succeeded")
		}
	case <-ctx.Done():
		t.Fatal("result timeout missing")
	}
	after.Release()
	restarted, e := report.New(reader, resultrpc.NewClient(f.Conn))
	must(e)
	n, e := restarted.Reconcile(ctx, trigger, 10)
	must(e)
	if n != 1 {
		t.Fatal("pending report not recovered")
	}
	o, e = Observe(ctx, reader, trigger)
	must(e)
	if o.PendingReports != 0 || o.Successes != 1 || o.Attempts != 1 {
		t.Fatal("report replay changed evidence")
	}
	must(h.Handle(ctx, trigger))
	select {
	case <-f.Telegram.Requests:
		t.Fatal("confirmed outcome resent HTTP")
	default:
	}
	var count int
	must(api.QueryRow(ctx, `SELECT count(*) FROM app.delivery_outbox WHERE trigger_id=$1`, trigger).Scan(&count))
	if count != 1 {
		t.Fatal("canonical outbox duplicated")
	}
}
