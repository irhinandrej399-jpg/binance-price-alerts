package apimeasure

// Infrastructure-only OPS-028 fixture. Canonical API imports are confined to
// setup and use api_runtime; notifications receives only its own SQL pool.
import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/admissionrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/resultrpc"
	sub "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/alerttriggered"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/api/v1"
	market "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/latest"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/intake"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/jobs"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/report"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/retry"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/telegram"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const notificationSourceHash = "60187b1c96bab46ddc9009a6853e43c0780099659470c60fdfd67a97fcd85106"

type fixtureAdmission struct {
	Subscription string  `json:"subscription_id"`
	Request      string  `json:"request_id"`
	Attempt      *string `json:"attempt_id"`
	Fence        *int64  `json:"fence"`
	Response     string  `json:"response"`
}
type notificationSource struct {
	matcherSource
	Admissions []fixtureAdmission `json:"admissions"`
}
type notificationItem struct {
	ID        identity
	Event     string
	Body      []byte
	Response  []byte
	Admission sub.AdmissionResult
	Trigger   alerttriggered.Triggered
}
type notificationFixture struct {
	Items   []notificationItem
	Records []sourceRecord
}

func loadNotificationFixture(raw, matcher []byte) (*notificationFixture, error) {
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != notificationSourceHash {
		return nil, errors.New("source SHA256 mismatch")
	}
	var source notificationSource
	if err := json.Unmarshal(raw, &source); err != nil {
		return nil, err
	}
	var link struct {
		Source string `json:"source_sha256"`
	}
	if err := json.Unmarshal(matcher, &link); err != nil {
		return nil, err
	}
	if link.Source != notificationSourceHash {
		return nil, errors.New("matcher source SHA256 mismatch")
	}
	return validateNotificationSource(source, matcher)
}
func validateNotificationSource(source notificationSource, matcher []byte) (*notificationFixture, error) {
	bad := func() (*notificationFixture, error) { return nil, errors.New("source identity/boundary mismatch") }
	if source.Source != sourceID || len(source.Identities) != 5 || len(source.Outbox) != 4 || len(source.Admissions) != 5 || len(source.Records["delivery-requests"]) != 5 {
		return bad()
	}
	var link struct {
		Work struct {
			Entries []struct {
				Candidate struct{ OwnerID, SubscriptionID string }
				Result    struct {
					Outcome, TriggerID string
					WorkflowVersion    int64
				}
			}
		}
	}
	if err := json.Unmarshal(matcher, &link); err != nil {
		return nil, err
	}
	if len(link.Work.Entries) != 4 {
		return bad()
	}
	f := &notificationFixture{Records: source.Records["delivery-requests"]}
	seen := map[string]bool{}
	for _, id := range source.Identities[:4] {
		if seen[id.Subscription] || id.Owner != source.Identities[0].Owner || id.Attempt == "" {
			return bad()
		}
		seen[id.Subscription] = true
		item := notificationItem{ID: id}
		for _, out := range source.Outbox {
			body, err := hex.DecodeString(out.Body)
			if err != nil {
				return nil, err
			}
			meta, tr, err := alerttriggered.Decode(body)
			if err != nil {
				return nil, err
			}
			if tr.SubscriptionID != id.Subscription {
				continue
			}
			if item.Body != nil || tr.UserID != id.Owner || tr.TriggerID != id.Workflow || tr.WorkflowVersion != 1 || tr.SubscriptionVersion != 2 {
				return bad()
			}
			item.Event = meta.EventID
			item.Body = body
			item.Trigger = tr
		}
		if item.Body == nil {
			return bad()
		}
		matches := 0
		for _, a := range source.Admissions {
			if a.Attempt == nil || a.Subscription != id.Subscription {
				continue
			}
			if a.Request != id.Request || *a.Attempt != id.Attempt || a.Fence == nil || *a.Fence != 1 {
				return bad()
			}
			if len(a.Response) < 2 || a.Response[:2] != `\x` {
				return bad()
			}
			response, err := hex.DecodeString(a.Response[2:])
			if err != nil {
				return bad()
			}
			if err = json.Unmarshal(response, &item.Admission); err != nil {
				return nil, err
			}
			item.Response = response
			r, p := item.Admission.Record, item.Admission.Attempt
			if item.Admission.Outcome != sub.Admitted || p == nil || p.ID != id.Attempt || p.Fence != *a.Fence || p.ChatID != 24001 || p.BindingVersion != 1 || !p.ExpiresAt.Before(time.Now()) || r.ID != id.Subscription || r.OwnerID != id.Owner || r.Workflow == nil || r.Workflow.TriggerID != id.Workflow || r.Workflow.Version != 1 || r.State != domain.Sending || r.Version < 3 || r.Blocked != nil || r.Pair != item.Trigger.Pair || r.Threshold.String() != item.Trigger.Threshold.String() || r.Direction != item.Trigger.Direction || r.Activation != sub.LatestPrice || p.Quote.Validate() != nil {
				return bad()
			}
			matches++
		}
		if item.Body == nil || matches != 1 {
			return bad()
		}
		linked := 0
		for _, e := range link.Work.Entries {
			if e.Candidate.SubscriptionID == id.Subscription && e.Candidate.OwnerID == id.Owner && e.Result.TriggerID == id.Workflow && e.Result.Outcome == "workflow" && e.Result.WorkflowVersion == 1 {
				linked++
			}
		}
		if linked != 1 {
			return bad()
		}
		f.Items = append(f.Items, item)
	}
	counts := map[string]int{}
	offsets := map[int32]int64{}
	for _, r := range f.Records {
		body, err := hex.DecodeString(r.Body)
		if err != nil {
			return nil, err
		}
		matched := false
		for _, i := range f.Items {
			if r.Event == i.Event && r.Key == i.ID.Subscription && bytes.Equal(body, i.Body) {
				matched = true
				counts[i.Event]++
			}
		}
		if !matched || r.Partition < 0 || r.Partition >= 16 || r.Offset < offsets[r.Partition] {
			return bad()
		}
		offsets[r.Partition] = r.Offset + 1
	}
	doubles := 0
	for _, i := range f.Items {
		if counts[i.Event] == 2 {
			doubles++
		} else if counts[i.Event] != 1 {
			return bad()
		}
	}
	if doubles != 1 {
		return bad()
	}
	return f, nil
}

// Atomic setup import. No history is fabricated: each record is its own
// admission-response cut (reports), or source workflow v2 (handler). C starts
// at zero in this new dataset; source final journal is deliberately not imported.
func (f *notificationFixture) importAPI(ctx context.Context, p *pgxpool.Pool, boundary string) error {
	if boundary != "reports" && boundary != "handler" {
		return errors.New("unknown fixture boundary")
	}
	tx, e := p.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	var role string
	var n int
	if e = tx.QueryRow(ctx, "SELECT current_user").Scan(&role); e != nil {
		return e
	}
	if role != "api_runtime" {
		return errors.New("wrong fixture SQL role")
	}
	if e = tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM app.users)+(SELECT count(*) FROM app.subscriptions)+(SELECT count(*) FROM app.delivery_admissions)+(SELECT count(*) FROM app.api_journal)+(SELECT count(*) FROM app.delivery_outbox)`).Scan(&n); e != nil {
		return e
	}
	if n != 0 {
		return errors.New("fixture needs clean API dataset")
	}
	pmt := f.Items[0].Admission.Attempt
	_, e = tx.Exec(ctx, `INSERT INTO app.users(id,email,password_hash,telegram_chat_id,telegram_version,active_subscriptions) VALUES($1,'ops028@example.test','infrastructure-fixture',$2,$3,4)`, f.Items[0].ID.Owner, pmt.ChatID, pmt.BindingVersion)
	if e != nil {
		return e
	}
	for _, i := range f.Items {
		r := i.Admission.Record
		if boundary == "handler" {
			r.State = domain.PendingDelivery
			r.Version = int64(i.Trigger.SubscriptionVersion)
		}
		if e = sub.New(tx).Insert(ctx, r); e != nil {
			return e
		}
		_, e = tx.Exec(ctx, `UPDATE app.subscriptions SET quota_reserved=true WHERE id=$1`, r.ID)
		if e != nil {
			return e
		}
		_, e = tx.Exec(ctx, `INSERT INTO app.delivery_outbox(subscription_id,trigger_id,event_id,body) VALUES($1,$2,$3,$4)`, r.ID, i.ID.Workflow, i.Event, i.Body)
		if e != nil {
			return e
		}
		if boundary == "reports" {
			a := i.Admission.Attempt
			_, e = tx.Exec(ctx, `UPDATE app.subscriptions SET admission_fence=$2,current_attempt_id=$3 WHERE id=$1`, r.ID, a.Fence, a.ID)
			if e != nil {
				return e
			}
			_, e = tx.Exec(ctx, `INSERT INTO app.delivery_admissions(subscription_id,request_id,workflow_id,attempt_id,fence,expires_at,chat_id,binding_version,response) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, r.ID, i.ID.Request, i.ID.Workflow, a.ID, a.Fence, a.ExpiresAt, a.ChatID, a.BindingVersion, i.Response)
			if e != nil {
				return e
			}
		}
	}
	return tx.Commit(ctx)
}

func fixturePool(t *testing.T, key, role string) *pgxpool.Pool {
	t.Helper()
	p, e := postgres.Open(context.Background(), os.Getenv(key))
	check(t, e)
	t.Cleanup(p.Close)
	var got string
	check(t, p.QueryRow(context.Background(), "SELECT current_user").Scan(&got))
	if got != role {
		t.Fatal("wrong SQL role", got)
	}
	return p
}
func fixtureSource(t *testing.T) *notificationFixture {
	t.Helper()
	raw, e := os.ReadFile(os.Getenv("OPS028_SOURCE"))
	check(t, e)
	matcher, e := os.ReadFile(os.Getenv("OPS028_MATCHER"))
	check(t, e)
	f, e := loadNotificationFixture(raw, matcher)
	check(t, e)
	return f
}

// Same post-commit reply-loss seam as resultrpc/reporter_test.go.
type fixtureLostReply struct {
	mu    sync.Mutex
	store *resultrpc.Postgres
	lost  map[string]bool
}

func (s *fixtureLostReply) SaveDeliveryResult(ctx context.Context, c sub.DeliveryResultCommand) (sub.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, e := s.store.SaveDeliveryResult(ctx, c)
	if e == nil && !s.lost[c.AttemptID] {
		s.lost[c.AttemptID] = true
		return v, context.DeadlineExceeded
	}
	return v, e
}
func fixtureClients(t *testing.T, p *pgxpool.Pool) (*admissionrpc.Client, *resultrpc.Client) {
	ca := newAuthority(t)
	cert := ca.issue(t, nil, true, false)
	quote := &quoteFixture{value: "101", tradeTime: time.Now().UTC().Truncate(time.Microsecond), sequence: 1}
	marketConn := connect(t, ca, cert, apiURI, map[string][]string{apiURI: {market.Quotes_Latest_FullMethodName}}, func(s *grpc.Server) { market.RegisterQuotesServer(s, latest.NewServer(quote, time.Now)) })
	server, e := admissionrpc.NewServer(admissionrpc.NewPostgres(p), latest.NewClient(marketConn), time.Minute, time.Now)
	check(t, e)
	lost := &fixtureLostReply{store: resultrpc.NewPostgres(p), lost: map[string]bool{}}
	conn := connect(t, ca, cert, notificationsURI, map[string][]string{notificationsURI: {pb.Admission_PrepareRecipient_FullMethodName, pb.Admission_AdmitDelivery_FullMethodName, pb.DeliveryResults_SaveDeliveryResult_FullMethodName}}, func(s *grpc.Server) {
		pb.RegisterAdmissionServer(s, server)
		pb.RegisterDeliveryResultsServer(s, resultrpc.NewServer(lost))
	})
	return admissionrpc.NewClient(conn), resultrpc.NewClient(conn)
}

func TestNotificationFixtureSource(t *testing.T) {
	raw, e := os.ReadFile("../../../docs/measurements/OPS-024/run-a2/identities.json")
	check(t, e)
	matcher, e := os.ReadFile("../../../docs/measurements/OPS-025/run-a3/identities.json")
	check(t, e)
	f, e := loadNotificationFixture(raw, matcher)
	check(t, e)
	if len(f.Items) != 4 || len(f.Records) != 5 {
		t.Fatal("wrong input")
	}
	if _, e = loadNotificationFixture(append(raw, ' '), matcher); e == nil {
		t.Fatal("foreign hash accepted")
	}
	for _, field := range []string{"owner", "trigger", "event", "response", "matcher"} {
		t.Run(field, func(t *testing.T) {
			var s notificationSource
			check(t, json.Unmarshal(raw, &s))
			m := append([]byte(nil), matcher...)
			switch field {
			case "owner":
				s.Identities[0].Owner = "foreign"
			case "trigger":
				s.Identities[0].Workflow = "foreign"
			case "event":
				s.Records["delivery-requests"][0].Event = "foreign"
			case "response":
				s.Admissions[1].Response = `\x7b7d`
			case "matcher":
				m = bytes.ReplaceAll(m, []byte(f.Items[0].ID.Workflow), []byte("foreign"))
			}
			if _, e := validateNotificationSource(s, m); e == nil {
				t.Fatal("mismatch accepted")
			}
		})
	}
}

func TestNotificationFixture(t *testing.T) {
	if os.Getenv("OPS028_FIXTURE") != "1" {
		t.Skip("opt-in disposable fixture")
	}
	f := fixtureSource(t)
	api := fixturePool(t, "DATABASE_URL", "api_runtime")
	local := fixturePool(t, "NOTIFICATIONS_DATABASE_URL", "notifications_runtime")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	boundary := os.Getenv("OPS028_BOUNDARY")
	// Force a conflict after the owner and first subscription have been written.
	broken := *f
	broken.Items = append([]notificationItem(nil), f.Items...)
	broken.Items[1].Admission.Record.ID = broken.Items[0].ID.Subscription
	if e := broken.importAPI(ctx, api, boundary); e == nil {
		t.Fatal("broken restoration accepted")
	}
	var remaining int
	check(t, api.QueryRow(ctx, "SELECT count(*) FROM app.users").Scan(&remaining))
	if remaining != 0 {
		t.Fatal("partial restoration survived rollback")
	}
	if e := f.importAPI(ctx, api, "unknown"); e == nil {
		t.Fatal("unknown boundary accepted")
	}
	check(t, f.importAPI(ctx, api, boundary))
	// Re-import must fail without partial additions; the same transaction also
	// rolls back SQL errors (tested separately with a conflicting second row).
	if e := f.importAPI(ctx, api, boundary); e == nil {
		t.Fatal("dirty import accepted")
	}
	admission, results := fixtureClients(t, api)
	for _, i := range f.Items {
		var body []byte
		check(t, api.QueryRow(ctx, "SELECT body FROM app.delivery_outbox WHERE subscription_id=$1", i.ID.Subscription).Scan(&body))
		if !bytes.Equal(body, i.Body) {
			t.Fatal("outbox bytes changed")
		}
		if boundary == "reports" {
			var response []byte
			var fence, binding, chat int64
			var expires time.Time
			check(t, api.QueryRow(ctx, "SELECT response,fence,binding_version,chat_id,expires_at FROM app.delivery_admissions WHERE attempt_id=$1", i.ID.Attempt).Scan(&response, &fence, &binding, &chat, &expires))
			a := i.Admission.Attempt
			if !bytes.Equal(response, i.Response) || fence != a.Fence || binding != a.BindingVersion || chat != a.ChatID || !expires.Equal(a.ExpiresAt) {
				t.Fatal("permit restore mismatch")
			}
		}
	}
	first := f.Items[0].ID
	for _, wrong := range []struct {
		owner, workflow string
		code            codes.Code
	}{
		{"00000000-0000-4000-8000-000000000001", first.Workflow, codes.NotFound},
		{first.Owner, "00000000-0000-4000-8000-000000000001", codes.FailedPrecondition},
	} {
		_, err := results.SaveDeliveryResult(ctx, &pb.SaveDeliveryResultRequest{OwnerId: wrong.owner, SubscriptionId: first.Subscription, WorkflowId: wrong.workflow, AttemptId: first.Attempt, Outcome: "uncertain"})
		if status.Code(err) != wrong.code {
			t.Fatal("identity check bypassed", err)
		}
	}

	store := intake.NewStore(local)
	for _, r := range f.Records {
		next, e := store.Checkpoint(ctx, "OPS028-source-delivery", r.Partition)
		check(t, e)
		body, e := hex.DecodeString(r.Body)
		check(t, e)
		check(t, store.Accept(ctx, "OPS028-source-delivery", r.Partition, next, r.Offset, []byte(r.Key), body))
	}
	var count int
	check(t, local.QueryRow(ctx, "SELECT count(*) FROM app.delivery_jobs").Scan(&count))
	if count != 4 {
		t.Fatal(count)
	}
	evidence := []map[string]any{}
	jobStore, e := jobs.New(local, time.Minute)
	check(t, e)
	outcomes := []string{"succeeded", "retryable", "uncertain", "blocked"}
	for index, i := range f.Items {
		id := i.ID
		row := map[string]any{"identity": id, "event_id": i.Event, "body_sha256": fmt.Sprintf("%x", sha256.Sum256(i.Body)), "source_response_sha256": fmt.Sprintf("%x", sha256.Sum256(i.Response)), "boundary_version": i.Admission.Record.Version, "source_permit": i.Admission.Attempt}
		if boundary == "handler" {
			prepared, e := admission.PrepareRecipient(ctx, id.Owner, id.Subscription, id.Workflow)
			check(t, e)
			if prepared.Recipient == nil || prepared.Recipient.ChatId != 24001 || prepared.Recipient.BindingVersion != 1 {
				t.Fatal(prepared)
			}
			fresh, e := admission.AdmitDelivery(ctx, id.Owner, id.Subscription, id.Workflow, "OPS028-new-"+id.Workflow, prepared.Recipient)
			check(t, e)
			if fresh.Outcome != "admitted" || fresh.Attempt == nil || fresh.Attempt.AttemptId == id.Attempt || fresh.Attempt.Fence != 1 || !fixtureTime(t, fresh.Attempt.ExpiresAt).After(time.Now()) || fixtureTime(t, fresh.Attempt.Quote.TradeTime).Before(time.Now().Add(-time.Minute)) {
				t.Fatal(fresh)
			}
			row["boundary_version"] = i.Trigger.SubscriptionVersion
			row["fresh_admission"] = fresh
		} else {
			old, e := admission.AdmitDelivery(ctx, id.Owner, id.Subscription, id.Workflow, id.Request)
			check(t, e)
			if old.Attempt == nil || old.Attempt.AttemptId != id.Attempt || old.Attempt.Fence != i.Admission.Attempt.Fence || !fixtureTime(t, old.Attempt.ExpiresAt).Equal(i.Admission.Attempt.ExpiresAt) || !fixtureTime(t, old.Attempt.ExpiresAt).Before(time.Now()) {
				t.Fatal("historical permit renewed", old)
			}
			lease, e := jobStore.Claim(ctx, id.Workflow)
			check(t, e)
			check(t, jobStore.RegisterAttempt(ctx, lease, id.Attempt))
			switch outcomes[index] {
			case "succeeded":
				e = jobStore.SaveSuccess(ctx, lease, id.Attempt, 24001)
			case "uncertain":
				e = jobStore.SaveUncertain(ctx, lease, id.Attempt, time.Now().Add(time.Hour))
			case "blocked":
				e = jobStore.SaveBlocked(ctx, lease, id.Attempt, domain.PermanentFailure, 1)
			case "retryable":
				policy, err := retry.New(retry.Config{BaseDelay: time.Second, MaxDelay: time.Second, ConditionInterval: time.Second, MaxFailures: 3})
				check(t, err)
				_, e = jobStore.SaveRetryable(ctx, lease, id.Attempt, &telegram.Error{Kind: telegram.Server}, policy, 0, 1)
			}
			check(t, e)
			reporter, e := report.New(local, results)
			check(t, e)
			if n, e := reporter.Reconcile(ctx, id.Workflow, 10); e == nil || n != 0 {
				t.Fatal("lost reply acknowledged", n, e)
			}
			// Recreate reporter using a fresh notifications-only pool after lost reply.
			restarted := fixturePool(t, "NOTIFICATIONS_DATABASE_URL", "notifications_runtime")
			reporter, e = report.New(restarted, results)
			check(t, e)
			n, e := reporter.Reconcile(ctx, id.Workflow, 10)
			check(t, e)
			if n != 1 {
				t.Fatal(n)
			}
			n, e = reporter.Reconcile(ctx, id.Workflow, 10)
			check(t, e)
			if n != 0 {
				t.Fatal(n)
			}
			var state string
			var version, binding int64
			check(t, api.QueryRow(ctx, `SELECT s.state,s.version,r.binding_version FROM app.subscriptions s JOIN app.delivery_results r ON r.subscription_id=s.id WHERE s.id=$1`, id.Subscription).Scan(&state, &version, &binding))
			want := []string{"delivered", "pending_delivery", "pending_delivery", "delivery_blocked"}[index]
			if state != want || version != i.Admission.Record.Version+1 || binding != 1 {
				t.Fatal(state, version, binding)
			}
			row["outcome"] = outcomes[index]
			row["state"] = state
			row["version"] = version
			row["lost_reply_recovered"] = true
		}
		evidence = append(evidence, row)
	}
	var audits, journalRows int
	check(t, api.QueryRow(ctx, "SELECT count(*) FROM app.delivery_results").Scan(&audits))
	check(t, api.QueryRow(ctx, "SELECT count(*) FROM app.api_journal").Scan(&journalRows))
	if boundary == "reports" && (audits != 4 || journalRows != 4) {
		t.Fatal("duplicate result/journal", audits, journalRows)
	}
	if boundary == "handler" && (audits != 0 || journalRows != 4) {
		t.Fatal("unexpected handler history", audits, journalRows)
	}
	slots := 4
	if boundary == "reports" {
		slots = 3
	}
	check(t, api.QueryRow(ctx, `SELECT active_subscriptions FROM app.users`).Scan(&count))
	if count != slots {
		t.Fatal("quota", count)
	}
	// API runtime must retain immutable permission rights.
	if _, e = api.Exec(ctx, `UPDATE app.delivery_admissions SET expires_at=clock_timestamp()`); e == nil {
		t.Fatal("immutable permission UPDATE allowed")
	}
	output := map[string]any{"format_version": 1, "source_sha256": notificationSourceHash, "boundary": boundary, "items": evidence, "records": f.Records, "jobs": 4, "result_rows": audits, "journal_rows": journalRows, "quota": slots, "api_role": "api_runtime", "notifications_role": "notifications_runtime", "external_http_calls": 0, "restoration_error_rollback": true, "identity_rpc_rejections": true, "expired_permit_http_allowed": false}
	raw, e := json.MarshalIndent(output, "", "  ")
	check(t, e)
	check(t, os.WriteFile(os.Getenv("OPS028_EVIDENCE"), append(raw, '\n'), 0600))
}

func fixtureTime(t *testing.T, s string) time.Time {
	t.Helper()
	v, e := time.Parse(time.RFC3339Nano, s)
	check(t, e)
	return v
}
