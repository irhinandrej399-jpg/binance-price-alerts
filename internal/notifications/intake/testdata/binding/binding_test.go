package binding_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/intake"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/jobs"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const message = `{"event_id":"outbox-1","type":"alert.triggered","version":1,"occurred_at":"2026-10-27T10:00:01Z","correlation_id":"request-1","ordering_key":"sub-1","payload":{"schema_version":1,"intent":"delivery_requested","trigger_id":"workflow-1","workflow_version":9007199254740993,"user_id":"user-1","subscription_id":"sub-1","subscription_version":9007199254740994,"pair":"BTCUSDT","threshold":"999999999999999999.123456789012345677","direction":"up","candidate_quote":{"schema_version":1,"pair":"BTCUSDT","price":"999999999999999999.123456789012345678","source_id":"9007199254740993","revision":{"stream_id":"binance-spot-trade","generation":2,"sequence":9007199254740993},"trade_time":"2026-10-27T10:00:00.123456Z","received_at":"2026-10-27T10:00:01.123456Z","stream_state":"unconfirmed","gap_detected":true}}}`

func TestBinding(t *testing.T) {
	p := postgrestest.OpenWithDSN(t, "NOTIFICATIONS_DATABASE_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s := intake.NewStore(p)
	b := intake.Binding{ConsumerID: "notifications", Generation: "bootstrap-1", Source: kafkasource.Identity{ClusterID: "cluster-1", TopicID: [16]byte{1}, Topic: "delivery"}}
	reset := func() {
		t.Helper()
		if _, e := p.Exec(ctx, `SELECT app.test_reset_delivery_source()`); e != nil {
			t.Fatal(e)
		}
	}
	defer reset()
	must := func(e error) {
		t.Helper()
		if e != nil {
			t.Fatal(e)
		}
	}
	want := func(e, target error) {
		t.Helper()
		if !errors.Is(e, target) {
			t.Fatalf("want %v, got %v", target, e)
		}
	}
	count := func(table string) int {
		t.Helper()
		var n int
		must(p.QueryRow(ctx, "SELECT count(*) FROM app."+table).Scan(&n))
		return n
	}
	wire := func(id string) []byte { return bytes.ReplaceAll([]byte(message), []byte("workflow-1"), []byte(id)) }
	accept := func(binding intake.Binding, part int32, expected, offset int64, id string) error {
		return s.AcceptBound(ctx, binding, part, expected, offset, []byte("sub-1"), wire(id))
	}
	sqlCode := func(err error, code string) {
		t.Helper()
		var e *pgconn.PgError
		if !errors.As(err, &e) || e.Code != code {
			t.Fatalf("want SQLSTATE %s, got %v", code, err)
		}
	}
	bootstrapSQL := func(tx interface {
		Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	}, binding intake.Binding) error {
		_, e := tx.Exec(ctx, `SELECT app.delivery_bootstrap($1,$2,$3,$4,$5,ARRAY[0,1])`, binding.ConsumerID, binding.Generation, binding.Source.ClusterID, binding.Source.TopicID[:], binding.Source.Topic)
		return e
	}

	t.Log("migration_preserves_legacy_bytes_and_all_outcomes")
	{
		var same bool
		must(p.QueryRow(ctx, `SELECT data=app.test_delivery_snapshot() FROM app.test_delivery_legacy_snapshot`).Scan(&same))
		if !same {
			t.Fatal("migration changed legacy state")
		}
		_, e := s.ReadProgress(ctx, b, 0)
		want(e, intake.ErrUnbound)
		want(s.Bootstrap(ctx, b, []int32{0, 1}), intake.ErrUnbound)
		must(p.QueryRow(ctx, `SELECT data=app.test_delivery_snapshot() FROM app.test_delivery_legacy_snapshot`).Scan(&same))
		if !same || count("delivery_intake_source") != 0 {
			t.Fatal("legacy bootstrap changed state")
		}
		reset()
	}
	t.Log("unknown_read_has_no_writes")
	{
		_, e := s.ReadProgress(ctx, b, 0)
		want(e, intake.ErrMissing)
		want(accept(b, 0, 0, 0, "unknown"), intake.ErrMissing)
		if count("delivery_checkpoints") != 0 || count("delivery_jobs") != 0 || count("delivery_intake_source") != 0 {
			t.Fatal("synthetic state")
		}
	}
	t.Log("legacy_zero_checkpoint")
	{
		_, e := s.Checkpoint(ctx, b.Source.Topic, 0)
		must(e)
		want(s.Bootstrap(ctx, b, []int32{0, 1}), intake.ErrUnbound)
		_, e = s.ReadProgress(ctx, b, 0)
		want(e, intake.ErrUnbound)
		want(accept(b, 0, 0, 0, "legacy"), intake.ErrUnbound)
		if count("delivery_intake_source") != 0 || count("delivery_checkpoints") != 1 {
			t.Fatal("legacy rebound")
		}
		reset()
	}
	t.Log("legacy_job_without_checkpoint")
	{
		_, e := p.Exec(ctx, `INSERT INTO app.delivery_jobs(trigger_id,subscription_id,user_id,event_id,body) VALUES('job-only','s','u','e','x')`)
		must(e)
		want(s.Bootstrap(ctx, b, []int32{0, 1}), intake.ErrUnbound)
		_, e = s.ReadProgress(ctx, b, 0)
		want(e, intake.ErrUnbound)
		if count("delivery_checkpoints") != 0 || count("delivery_intake_source") != 0 || count("delivery_jobs") != 1 {
			t.Fatal("legacy job rebound")
		}
		reset()
	}
	t.Log("legacy_jobs_outcomes_untouched")
	{
		_, e := s.Checkpoint(ctx, b.Source.Topic, 0)
		must(e)
		must(s.Accept(ctx, b.Source.Topic, 0, 0, 0, []byte("sub-1"), wire("legacy")))
		j, e := jobs.New(p, time.Minute)
		must(e)
		l, e := j.Claim(ctx, "legacy")
		must(e)
		must(j.RegisterAttempt(ctx, l, "legacy-attempt"))
		must(j.SaveBlocked(ctx, l, "legacy-attempt", domain.ChannelUnavailable, 7))
		var before, after string
		snapshot := `SELECT jsonb_build_object('jobs',(SELECT jsonb_agg(to_jsonb(j)) FROM app.delivery_jobs j),'claims',(SELECT jsonb_agg(to_jsonb(c)) FROM app.delivery_claims c),'attempts',(SELECT jsonb_agg(to_jsonb(a)) FROM app.delivery_attempts a),'blocks',(SELECT jsonb_agg(to_jsonb(a)) FROM app.delivery_blocks a),'reports',(SELECT jsonb_agg(to_jsonb(a)) FROM app.delivery_blocked_reports a),'checkpoints',(SELECT jsonb_agg(to_jsonb(c)) FROM app.delivery_checkpoints c))::text`
		must(p.QueryRow(ctx, snapshot).Scan(&before))
		want(s.Bootstrap(ctx, b, []int32{0, 1}), intake.ErrUnbound)
		_, e = s.ReadProgress(ctx, b, 0)
		want(e, intake.ErrUnbound)
		must(p.QueryRow(ctx, snapshot).Scan(&after))
		if before != after {
			t.Fatal("legacy changed")
		}
		reset()
	}
	t.Log("bootstrap_rollback_and_deferred_commit")
	{
		for _, topic := range []string{"fail-bootstrap", "fail-bootstrap-commit"} {
			bad := b
			bad.Source.Topic = topic
			if e := s.Bootstrap(ctx, bad, []int32{0, 1}); e == nil {
				t.Fatal("injected failure accepted")
			}
			if count("delivery_intake_source") != 0 || count("delivery_checkpoints") != 0 {
				t.Fatal("partial bootstrap")
			}
		}
		tx, e := p.Begin(ctx)
		must(e)
		must(bootstrapSQL(tx, b))
		_, e = s.ReadProgress(ctx, b, 0)
		want(e, intake.ErrMissing)
		must(tx.Rollback(ctx))
		if count("delivery_intake_source") != 0 || count("delivery_checkpoints") != 0 {
			t.Fatal("rollback lost")
		}
	}
	t.Log("checkpoint_failure_rolls_back_job")
	{
		bad := b
		bad.Source.Topic = "fail-checkpoint"
		must(s.Bootstrap(ctx, bad, []int32{0, 1}))
		if e := accept(bad, 0, 0, 0, "failed-checkpoint"); e == nil {
			t.Fatal("failed UPDATE accepted")
		}
		v, e := s.ReadProgress(ctx, bad, 0)
		must(e)
		if v.Next != 0 || count("delivery_jobs") != 0 {
			t.Fatal("partial failed checkpoint")
		}
		reset()
	}
	t.Log("bootstrap_serializes_old_writer_and_SQL_cancellation")
	{
		tx, e := p.Begin(ctx)
		must(e)
		_, e = tx.Exec(ctx, `INSERT INTO app.delivery_checkpoints VALUES('legacy-race',0,0)`)
		must(e)
		short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		e = s.Bootstrap(short, b, []int32{0, 1})
		cancel()
		if e == nil {
			t.Fatal("bootstrap passed uncommitted legacy writer")
		}
		must(tx.Rollback(ctx))
		if count("delivery_intake_source") != 0 || count("delivery_checkpoints") != 0 {
			t.Fatal("canceled bootstrap changed state")
		}
		tx, e = p.Begin(ctx)
		must(e)
		must(bootstrapSQL(tx, b))
		result := make(chan error, 1)
		go func() {
			_, e := p.Exec(ctx, `INSERT INTO app.delivery_jobs(trigger_id,subscription_id,user_id,event_id,body) VALUES('old-race','s','u','e','x')`)
			result <- e
		}()
		select {
		case e := <-result:
			t.Fatalf("legacy writer did not wait: %v", e)
		case <-time.After(100 * time.Millisecond):
		}
		must(tx.Commit(ctx))
		sqlCode(<-result, "42501")
		if count("delivery_jobs") != 0 {
			t.Fatal("old writer bypassed binding")
		}
		reset()
	}
	t.Log("bootstrap_concurrent_idempotence")
	{
		var wg sync.WaitGroup
		errs := make(chan error, 8)
		for range 8 {
			wg.Go(func() { errs <- s.Bootstrap(ctx, b, []int32{1, 0}) })
		}
		wg.Wait()
		close(errs)
		for e := range errs {
			must(e)
		}
		if count("delivery_checkpoints") != 2 || count("delivery_intake_source") != 1 {
			t.Fatal("bootstrap count")
		}
		for _, part := range []int32{0, 1} {
			v, e := s.ReadProgress(ctx, b, part)
			must(e)
			if v.Binding != b || v.Partition != part || v.Next != 0 {
				t.Fatal(v)
			}
		}
		want(s.Bootstrap(ctx, b, []int32{0, 1, 2}), kafkasource.ErrMismatch)
	}
	t.Log("prefix_duplicate_worker_and_conflicts")
	{
		must(accept(b, 0, 0, 2, "bound")) // invisible records may leave offset gaps
		must(s.Bootstrap(ctx, b, []int32{0, 1}))
		v, e := s.ReadProgress(ctx, b, 0)
		must(e)
		if v.Next != 3 {
			t.Fatal(v)
		}
		j, e := jobs.New(p, time.Minute)
		must(e)
		l, e := j.Claim(ctx, "bound")
		must(e)
		must(j.RegisterAttempt(ctx, l, "bound-attempt"))
		must(j.SaveBlocked(ctx, l, "bound-attempt", domain.ChannelUnavailable, 7))
		must(accept(b, 0, 0, 2, "bound"))
		must(accept(b, 0, 3, 3, "bound"))
		var state string
		var body []byte
		var version int64
		must(p.QueryRow(ctx, `SELECT state,body,blocked_binding_version FROM app.delivery_jobs WHERE trigger_id='bound'`).Scan(&state, &body, &version))
		if state != "blocked" || version != 7 || !bytes.Equal(body, wire("bound")) {
			t.Fatal("worker state reset")
		}
		want(accept(b, 0, 3, 4, "stale"), intake.ErrCheckpoint)
		_, e = s.ReadProgress(ctx, b, 2)
		want(e, intake.ErrMissing)
		want(accept(b, 2, 0, 0, "missing"), intake.ErrMissing)
		changes := []intake.Binding{b, b, b, b, b}
		changes[0].ConsumerID = "other"
		changes[1].Generation = "bootstrap-2"
		changes[2].Source.ClusterID = "cluster-2"
		changes[3].Source.TopicID[1] = 2
		changes[4].Source.Topic = "other"
		for _, bad := range changes {
			want(s.Bootstrap(ctx, bad, []int32{0, 1}), kafkasource.ErrMismatch)
			_, e = s.ReadProgress(ctx, bad, 0)
			want(e, kafkasource.ErrMismatch)
			want(accept(bad, 0, 0, 0, "duplicate-mismatch"), kafkasource.ErrMismatch)
			want(accept(bad, 0, 4, 4, "new-mismatch"), kafkasource.ErrMismatch)
		}
		if count("delivery_jobs") != 1 {
			t.Fatal("failed accept inserted job")
		}
	}
	t.Log("real_SQL_old_writer_and_permissions")
	{
		for _, sql := range []string{
			`UPDATE app.delivery_checkpoints SET next_offset=99`,
			`UPDATE app.delivery_checkpoints SET topic='renamed'`,
			`INSERT INTO app.delivery_checkpoints VALUES('other',0,0)`,
			`DELETE FROM app.delivery_checkpoints`,
			`DELETE FROM app.delivery_intake_source`,
			`UPDATE app.delivery_intake_source SET generation='other'`,
			`INSERT INTO app.delivery_jobs(trigger_id,subscription_id,user_id,event_id,body) VALUES('bypass','sub','user','event','x')`,
			`UPDATE app.delivery_jobs SET body='changed'`,
		} {
			_, e := p.Exec(ctx, sql)
			sqlCode(e, "42501")
		}
		if e := s.Accept(ctx, b.Source.Topic, 0, 4, 4, []byte("sub-1"), wire("legacy-new")); e == nil {
			t.Fatal("old API accepted")
		}
		if e := s.Accept(ctx, b.Source.Topic, 0, 0, 0, []byte("sub-1"), wire("old-duplicate")); e == nil {
			t.Fatal("old duplicate accepted without identity")
		}
		if _, e := s.Checkpoint(ctx, b.Source.Topic, 0); e == nil {
			t.Fatal("old checkpoint registered")
		}
		tx, e := p.Begin(ctx)
		must(e)
		_, e = tx.Exec(ctx, `SET LOCAL app.source_bound='true'; SET LOCAL application_name='notifications_migration'`)
		must(e)
		_, e = tx.Exec(ctx, `UPDATE app.delivery_checkpoints SET next_offset=99`)
		sqlCode(e, "42501")
		must(tx.Rollback(ctx))
		var can bool
		must(p.QueryRow(ctx, `SELECT pg_has_role(current_user,'notifications_migration','MEMBER')`).Scan(&can))
		if can {
			t.Fatal("runtime can become owner")
		}
	}
	t.Log("CAS_concurrency_and_commit_failure")
	{
		var wg sync.WaitGroup
		errs := make(chan error, 2)
		for _, id := range []string{"race-1", "race-2"} {
			wg.Go(func() { errs <- accept(b, 1, 0, 1, id) })
		}
		wg.Wait()
		close(errs)
		for e := range errs {
			must(e)
		} // same prefix: one accepted job, one duplicate
		if count("delivery_jobs") != 2 {
			t.Fatal("duplicate race job")
		}
		if e := accept(b, 1, 2, 2, "fail-commit"); e == nil {
			t.Fatal("deferred commit acknowledged")
		}
		v, e := s.ReadProgress(ctx, b, 1)
		must(e)
		if v.Next != 2 || count("delivery_jobs") != 2 {
			t.Fatal("partial deferred commit")
		}
		must(accept(b, 1, 2, 2, "after-fault"))
	}
	t.Log("accept_transaction_MVCC_and_rollback")
	{
		tx, e := p.Begin(ctx)
		must(e)
		_, e = tx.Exec(ctx, `SELECT app.delivery_accept_bound($1,$2,$3,$4,$5,0,4,4,'rollback','sub-1','user-1','event',$6)`, b.ConsumerID, b.Generation, b.Source.ClusterID, b.Source.TopicID[:], b.Source.Topic, wire("rollback"))
		must(e)
		v, e := s.ReadProgress(ctx, b, 0)
		must(e)
		if v.Next != 4 {
			t.Fatal("uncommitted prefix exposed")
		}
		must(tx.Rollback(ctx))
		v, e = s.ReadProgress(ctx, b, 0)
		must(e)
		if v.Next != 4 || count("delivery_jobs") != 3 {
			t.Fatal("rollback changed durable prefix/job")
		}
	}
	t.Log("accept_SQL_cancellation_and_idempotent_recovery")
	{
		tx, e := p.Begin(ctx)
		must(e)
		_, e = tx.Exec(ctx, `SELECT FROM app.delivery_checkpoints WHERE topic=$1 AND partition=0 FOR UPDATE`, b.Source.Topic)
		must(e)
		short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		e = s.AcceptBound(short, b, 0, 4, 4, []byte("sub-1"), wire("canceled-lock"))
		cancel()
		if e == nil {
			t.Fatal("accept passed row lock")
		}
		// Cancellation returns before the server necessarily receives its
		// CancelRequest. Keep the lock held to check the pre-commit view.
		v, e := s.ReadProgress(ctx, b, 0)
		must(e)
		if v.Next != 4 || count("delivery_jobs") != 3 {
			t.Fatal("write passed held lock")
		}
		must(tx.Rollback(ctx))
		// Release can race cancellation at the server: the result is unknown,
		// never ackable. Retry the same record; both outcomes converge.
		must(accept(b, 0, 4, 4, "canceled-lock"))
		v, e = s.ReadProgress(ctx, b, 0)
		must(e)
		if v.Next != 5 || count("delivery_jobs") != 4 {
			t.Fatal("cancellation recovery lost atomicity")
		}

	}
	t.Log("read_only_and_cancel")
	{
		cfg := p.Config()
		cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
		ro, e := pgxpool.NewWithConfig(ctx, cfg)
		must(e)
		v, e := intake.NewStore(ro).ReadProgress(ctx, b, 0)
		ro.Close()
		must(e)
		if v.Next != 5 || v.Binding != b {
			t.Fatal("read-only API position")
		}
		conn, e := p.Acquire(ctx)
		must(e)
		defer conn.Release()
		_, e = conn.Exec(ctx, `BEGIN READ ONLY`)
		must(e)
		var n int64
		must(conn.QueryRow(ctx, `SELECT c.next_offset FROM app.delivery_intake_source s JOIN app.delivery_checkpoints c ON c.topic=s.topic WHERE c.partition=0`).Scan(&n))
		_, e = conn.Exec(ctx, `ROLLBACK`)
		must(e)
		if n != 5 {
			t.Fatal(n)
		}
		dead, c := context.WithCancel(ctx)
		c()
		if _, e = s.ReadProgress(dead, b, 0); e == nil {
			t.Fatal("cancel read")
		}
		if e = s.Bootstrap(dead, b, []int32{0, 1}); e == nil {
			t.Fatal("cancel bootstrap")
		}
		if e = s.AcceptBound(dead, b, 0, 4, 4, []byte("sub-1"), wire("cancel")); e == nil {
			t.Fatal("cancel accept")
		}
		if e = s.AcceptBound(ctx, b, 0, 4, 4, []byte("sub-1"), []byte(strings.Replace(message, `"version":1`, `"version":2`, 1))); !errors.Is(e, intake.ErrContract) {
			t.Fatal(e)
		}
	}
	t.Log("new_SQL_bootstrap_requires_new_expected_generation")
	{
		reset()
		next := b
		next.Generation = "bootstrap-2"
		must(s.Bootstrap(ctx, next, []int32{0, 1}))
		_, e := s.ReadProgress(ctx, b, 0)
		want(e, kafkasource.ErrMismatch)
		v, e := s.ReadProgress(ctx, next, 0)
		must(e)
		if v.Next != 0 || v.Binding != next {
			t.Fatal(v)
		}
	}

}
