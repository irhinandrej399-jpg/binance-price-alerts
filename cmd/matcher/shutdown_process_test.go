package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"reflect"
	"sort"
	"syscall"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/deliveryrpc"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/fencing"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricework"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/apiprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/matcherprocess"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Called after the crash/startup scenarios on the same disposable runner. The
// new group has independent SQL authority; prior API history stays canonical.
func testMatcherShutdown(t *testing.T, binary string) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	api := postgrestest.Open(t)
	reader := postgrestest.OpenWithDSN(t, "MATCHER_DATABASE_URL")
	f := matcherprocess.New(t, api, os.Getenv("KAFKA_TEST_BROKER"), os.Getenv("KAFKA_TEST_PREFIX"))
	d, err := f.Seed(ctx)
	must(err)
	sort.Strings(d.IDs)
	must(f.PublishChanges(ctx))
	bootstrap, err := f.Bootstrap(ctx)
	must(err)
	b, err := json.Marshal(bootstrap)
	must(err)
	env := validEnv()
	delete(env, "HTTP_ADDR")
	for k, v := range f.ChildEnv(os.Getenv("MATCHER_DATABASE_URL")) {
		env[k] = v
	}
	env["MATCHER_ENVIRONMENT"] = f.Prefix
	env["MATCHER_GROUP"] = f.Prefix + "-shutdown"
	env["PRICES_BOOTSTRAP"] = string(b)
	env["HEAD_INTERVAL"] = "50ms"
	cfg := apiprocess.Config{Executable: binary, Env: env, StartupProbePath: "/healthz", StartupTimeout: 12 * time.Second, StopTimeout: 22 * time.Second}
	pause := f.PauseDelivery(d.IDs[1])
	defer pause.Release()
	record, err := f.PublishPrice(ctx, f.SetPrice(d.Pair, "101", 10))
	must(err)
	identity := fencing.Identity{Environment: env["MATCHER_ENVIRONMENT"], Group: env["MATCHER_GROUP"], Topic: record.Topic, Partition: record.Partition}
	observer, err := kgo.NewClient(kgo.SeedBrokers(f.Broker))
	must(err)
	defer observer.Close()
	brokerOffset := func() int64 {
		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = env["MATCHER_GROUP"]
		topic := kmsg.NewOffsetFetchRequestTopic()
		topic.Topic = record.Topic
		topic.Partitions = []int32{record.Partition}
		req.Topics = []kmsg.OffsetFetchRequestTopic{topic}
		res, e := req.RequestWith(ctx, observer)
		must(e)
		if len(res.Topics) != 1 || len(res.Topics[0].Partitions) != 1 || res.Topics[0].Partitions[0].ErrorCode != 0 {
			t.Fatal("broker offset response")
		}
		return res.Topics[0].Partitions[0].Offset
	}
	stop := func(p *apiprocess.Process, signal syscall.Signal) {
		t.Helper()
		must(syscall.Kill(p.PID(), signal))
		select {
		case <-p.Done():
		case <-time.After(18 * time.Second):
			t.Fatal("signal did not join process within shutdown budget")
		}
		code, ok := p.ExitCode()
		if !ok || code != 0 {
			t.Fatalf("graceful exit code=%d reaped=%v", code, ok)
		}
		// Proves actual cleanup, even if an OS-default signal exits promptly.
		for part := int32(0); part < 16; part++ {
			id := identity
			id.Partition = part
			_, active, e := fencing.New(reader).Current(ctx, id)
			must(e)
			if active {
				t.Fatal("SQL grant left active after graceful shutdown", part)
			}
		}
		req, e := http.NewRequestWithContext(ctx, "GET", p.URL+"/readyz", nil)
		must(e)
		client := &http.Client{Timeout: time.Second}
		if resp, e := client.Do(req); e == nil {
			resp.Body.Close()
			t.Fatal("HTTP admission remained open after shutdown")
		}
	}
	var grant fencing.Grant
	var partial pricework.Work
	var key, canonical string
	must(apiprocess.With(ctx, cfg, func(p *apiprocess.Process) error {
		select {
		case response := <-pause.Committed:
			canonical = response.Workflow.TriggerId
		case <-ctx.Done():
			return errors.New("no partial delivery")
		}
		grant, _, err = fencing.New(reader).Current(ctx, identity)
		must(err)
		store := pricework.New(reader, grant.Scope)
		keys, e := store.Pending(ctx)
		must(e)
		if len(keys) != 1 {
			t.Fatal("missing pending price", keys)
		}
		key = keys[0]
		partial, e = store.Get(ctx, key)
		must(e)
		if partial.Completed < 1 || partial.Completed >= len(partial.Entries) {
			t.Fatal("not a partial candidate list")
		}
		if brokerOffset() > record.Offset {
			t.Fatal("broker acknowledged pending work")
		}
		stop(p, syscall.SIGTERM)
		after, e := store.Get(ctx, key)
		must(e)
		if !reflect.DeepEqual(after, partial) {
			t.Fatal("shutdown changed immutable pending work/outcomes")
		}
		if brokerOffset() > record.Offset {
			t.Fatal("shutdown acknowledged unfinished price")
		}
		prefix, e := store.Checkpoint(ctx, record.Topic, record.Partition)
		if e != nil && !errors.Is(e, pgx.ErrNoRows) {
			t.Fatal(e)
		}
		if e == nil && prefix > record.Offset {
			t.Fatal("SQL prefix overtook unfinished price")
		}
		// No new API request is allowed to finish the remaining candidates at stop.
		var n int
		must(api.QueryRow(ctx, `SELECT count(*) FROM app.delivery_outbox WHERE subscription_id=$1`, d.IDs[2]).Scan(&n))
		if n != 0 {
			t.Fatal("new candidate accepted after cancellation")
		}
		return nil
	}))
	pause.Release()
	// Same suitable Latest on restart: all remaining candidates must finish. A
	// fresh timestamp is explicit; no historical false-price behavior is assumed.
	f.SetPrice(d.Pair, "101", 11)
	cfg.StartupProbePath = "/readyz"
	must(apiprocess.With(ctx, cfg, func(p *apiprocess.Process) error {
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		store := pricework.New(reader, grant.Scope)
		for {
			cp, e := store.Checkpoint(ctx, record.Topic, record.Partition)
			if e == nil && cp == record.Offset+1 && brokerOffset() == cp {
				break
			}
			select {
			case <-ctx.Done():
				return errors.New("prefix not recovered")
			case <-tick.C:
			}
		}
		current, active, e := fencing.New(reader).Current(ctx, identity)
		must(e)
		if !active || current.Token <= grant.Token || current.Scope != grant.Scope || current.ProjectionID != grant.ProjectionID {
			t.Fatal("recovery identity changed")
		}
		work, e := store.Get(ctx, key)
		must(e)
		if work.State != "completed" && work.State != "superseded" {
			t.Fatal("candidates not resumed", work.State, work.Completed)
		}
		for i := 0; i < partial.Completed; i++ {
			if !reflect.DeepEqual(work.Entries[i].Result, partial.Entries[i].Result) {
				t.Fatal("durable result changed")
			}
		}
		keys, e := store.Pending(ctx)
		must(e)
		if len(keys) != 0 {
			t.Fatal("pending remained after recovery")
		}
		// Idempotent API lookup supplies the canonical oracle per subscription;
		// matcher results and the unique SQL outbox must agree with this exact set.
		expected := map[string]bool{}
		bySubscription := map[string]string{}
		for _, entry := range partial.Entries {
			id := entry.Candidate.SubscriptionID
			var n int
			var actual string
			must(api.QueryRow(ctx, `SELECT count(*),coalesce(min(trigger_id::text),'') FROM app.delivery_outbox WHERE subscription_id=$1`, id).Scan(&n, &actual))
			if n != 1 {
				t.Fatal("matcher did not create exactly one workflow", n)
			}
			response, e := deliveryrpc.NewClient(f.Conn).RequestDelivery(ctx, entry.Candidate.OwnerID, id, int64(entry.Candidate.Version))
			must(e)
			if response.Workflow == nil {
				t.Fatal("missing canonical workflow")
			}
			trigger := response.Workflow.TriggerId
			if id == d.IDs[1] && trigger != canonical {
				t.Fatal("ambiguous response changed canonical ID")
			}
			if expected[trigger] {
				t.Fatal("trigger reused across subscriptions")
			}
			expected[trigger] = true
			bySubscription[id] = trigger
			if actual != trigger {
				t.Fatal("outbox set differs from canonical oracle")
			}
		}
		// Superseding an older observation may leave its ambiguous response
		// unrecorded locally. Every retained matcher result must still agree.
		for _, entry := range work.Entries {
			if want, ok := bySubscription[entry.Candidate.SubscriptionID]; ok && entry.Result != nil && entry.Result.TriggerID != want {
				t.Fatal("retained matcher result differs from canonical workflow")
			}
		}
		stop(p, syscall.SIGINT)
		return nil
	}))
	// A blocked SQL authority cleanup must report failure, even though the
	// worker also returns cancellation. Use a real row lock, not a mock error.
	f.SetPrice(d.Pair, "101", 12)
	must(apiprocess.With(ctx, cfg, func(p *apiprocess.Process) error {
		tx, e := reader.Begin(ctx)
		must(e)
		defer tx.Rollback(context.Background())
		rows, e := tx.Query(ctx, `SELECT assignment_key FROM app.matcher_assignments WHERE scope=$1 FOR UPDATE`, grant.Scope)
		must(e)
		if !rows.Next() {
			rows.Close()
			t.Fatal("missing SQL authority to lock")
		}
		rows.Close()
		must(syscall.Kill(p.PID(), syscall.SIGTERM))
		select {
		case <-p.Done():
		case <-time.After(18 * time.Second):
			return errors.New("SQL cleanup failure did not exit within budget")
		}
		if code, ok := p.ExitCode(); !ok || code != 1 {
			t.Fatalf("cleanup failure masked: exit=%d reaped=%v", code, ok)
		}
		return nil
	}))

}
