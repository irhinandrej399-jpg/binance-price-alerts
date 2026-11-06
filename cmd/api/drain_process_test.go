package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"syscall"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/apiprocess"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

func eventually(ctx context.Context, check func() bool) error {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if check() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func testGracefulProcess(t *testing.T, ctx context.Context, cfg apiprocess.Config, pool *pgxpool.Pool) {
	t.Helper()
	cfg.StopTimeout = 12 * time.Second

	waitLock := func(fragment string) error {
		deadline, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		return eventually(deadline, func() bool {
			var found bool
			e := pool.QueryRow(deadline, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE '%'||$1||'%')`, fragment).Scan(&found)
			return e == nil && found
		})
	}
	signal := func(p *apiprocess.Process) error { return syscall.Kill(p.PID(), syscall.SIGTERM) }
	waitExit := func(p *apiprocess.Process) error {
		select {
		case <-p.Done():
			return nil
		case <-time.After(9 * time.Second):
			return errors.New("shutdown exceeded bound")
		}
	}
	// Block journal append after the subscription UPDATE. Releasing within the
	// drain window commits; holding past it must roll back state/quota/journal.
	for _, rollback := range []bool{false, true} {
		t.Logf("drain scenario rollback=%v", rollback)
		f, e := seedDrainFixture(ctx, pool)
		if e != nil {
			t.Fatal("seed")
		}
		var before int
		if e = pool.QueryRow(ctx, `SELECT count(*) FROM app.api_journal`).Scan(&before); e != nil {
			t.Fatal("count")
		}
		e = withDrainedAPI(ctx, cfg, pool, func(p *apiprocess.Process) error {
			tx, e := pool.Begin(ctx)
			if e != nil {
				return errors.New("begin lock")
			}
			defer tx.Rollback(ctx)
			if _, e = tx.Exec(ctx, `SELECT position FROM app.api_journal_counter WHERE singleton FOR UPDATE`); e != nil {
				return errors.New("lock journal")
			}
			response := make(chan int, 1)
			go func() {
				req, _ := http.NewRequestWithContext(ctx, "DELETE", p.URL+"/v1/subscriptions/"+f.Active.ID, nil)
				req.Header.Set("Authorization", "Bearer "+f.Token)
				resp, e := http.DefaultClient.Do(req)
				if e != nil {
					response <- 0
					return
				}
				defer resp.Body.Close()
				io.Copy(io.Discard, resp.Body)
				response <- resp.StatusCode
			}()
			if e = waitLock("api_journal_counter"); e != nil {
				return errors.New("HTTP did not reach journal lock")
			}
			if e = signal(p); e != nil {
				return e
			}
			deadline, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			if e = eventually(deadline, func() bool {
				resp, e := http.Get(p.URL + "/healthz")
				if e == nil {
					resp.Body.Close()
				}
				return e != nil
			}); e != nil {
				return errors.New("HTTP admission remains open")
			}
			select {
			case <-p.Done():
				return errors.New("exited before transaction drained")
			default:
			}
			if !rollback {
				if e = tx.Rollback(ctx); e != nil {
					return errors.New("unlock")
				}
			}
			if e = waitExit(p); e != nil {
				return e
			}
			code := <-response
			if !rollback && code != 200 {
				return fmt.Errorf("drained HTTP status %d", code)
			}
			return nil
		})
		if e != nil {
			t.Fatalf("transaction/restart stage: %v", e)
		}
		var state string
		var quota int
		var reserved bool
		var after int
		if e = pool.QueryRow(ctx, `SELECT s.state,s.quota_reserved,u.active_subscriptions FROM app.subscriptions s JOIN app.users u ON u.id=s.owner_id WHERE s.id=$1`, f.Active.ID).Scan(&state, &reserved, &quota); e != nil {
			t.Fatal("state")
		}
		if e = pool.QueryRow(ctx, `SELECT count(*) FROM app.api_journal`).Scan(&after); e != nil {
			t.Fatal("journal")
		}
		expected, delta := "cancelled", 1
		if rollback {
			expected, delta = "active", 0
		}
		if state != expected || after != before+delta || reserved != rollback || quota != 2-delta {
			t.Fatalf("rollback=%v state=%s journal delta=%d", rollback, state, after-before)
		}
		// Same IDs and bytes must be available to the restarted process's relays.
		if e = withDrainedAPI(ctx, cfg, pool, func(p *apiprocess.Process) error {
			var cursor, end int64
			if e := pool.QueryRow(ctx, `SELECT position FROM app.subscription_relay WHERE singleton`).Scan(&cursor); e != nil {
				return errors.New("cursor")
			}
			if e := pool.QueryRow(ctx, `SELECT position FROM app.api_journal_counter WHERE singleton`).Scan(&end); e != nil {
				return errors.New("journal end")
			}
			if cursor != end {
				return errors.New("subscription replay incomplete")
			}
			if !rollback {
				var original []byte
				if e := pool.QueryRow(ctx, `SELECT body FROM app.api_journal ORDER BY position DESC LIMIT 1`).Scan(&original); e != nil {
					return errors.New("cancel bytes")
				}
				return consumeCopies(ctx, cfg, f.Active.ID, original, 1)
			}
			return nil
		}); e != nil {
			t.Fatal("restart:", e)
		}
	}
	t.Log("delivery ack interruption")
	// Pause the SQL ack after Kafka committed the delivery. TERM must leave it
	// unacknowledged; restart republishes exactly the persisted key/value.
	var key, value []byte
	e := withDrainedAPI(ctx, cfg, pool, func(p *apiprocess.Process) error {
		tx, e := pool.Begin(ctx)
		if e != nil {
			return errors.New("begin ack lock")
		}
		defer tx.Rollback(ctx)
		if _, e = tx.Exec(ctx, `SELECT token FROM app.delivery_relay WHERE singleton FOR UPDATE`); e != nil {
			return errors.New("lease lock")
		}
		f, e := seedDrainFixture(ctx, pool)
		if e != nil {
			return errors.New("seed during runtime")
		}
		key, value = f.DeliveryKey, f.DeliveryValue
		ackTx, e := pool.Begin(ctx)
		if e != nil {
			return errors.New("begin ack blocker")
		}
		defer ackTx.Rollback(ctx)
		if _, e = ackTx.Exec(ctx, `INSERT INTO app.delivery_published(subscription_id) VALUES($1)`, f.Delivery.ID); e != nil {
			return errors.New("ack blocker insert")
		}
		if e = tx.Rollback(ctx); e != nil {
			return errors.New("release lease lock")
		}

		if e = waitLock("INSERT INTO app.delivery_published"); e != nil {
			return errors.New("relay did not reach post-Kafka ack")
		}
		if e = signal(p); e != nil {
			return e
		}
		if e = waitExit(p); e != nil {
			return e
		}
		var ack bool
		if e = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.delivery_published WHERE subscription_id=$1)`, f.Delivery.ID).Scan(&ack); e != nil || ack {
			return errors.New("interrupted ack persisted")
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	e = withDrainedAPI(ctx, cfg, pool, func(p *apiprocess.Process) error {
		if e := consumeCopies(ctx, cfg, string(key), value, 2); e != nil {
			return e
		}
		var ack bool
		if e = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app.delivery_published WHERE subscription_id=$1)`, string(key)).Scan(&ack); e != nil || !ack {
			return errors.New("recovered ack missing")
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	if e = waitRelayLeases(ctx, pool); e != nil {
		t.Fatal("lease recovery", e)
	}
}

func consumeCopies(ctx context.Context, cfg apiprocess.Config, key string, value []byte, want int) error {
	consumer, e := kgo.NewClient(kgo.SeedBrokers(cfg.Env["KAFKA_BROKERS"]), kgo.ConsumeTopics(cfg.Env["DELIVERY_REQUESTS_TOPIC"], cfg.Env["SUBSCRIPTION_CHANGES_TOPIC"]), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchMaxWait(100*time.Millisecond))
	if e != nil {
		return e
	}
	defer consumer.Close()
	count := 0
	deadline, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	for count < want {
		fetch := consumer.PollFetches(deadline)
		if len(fetch.Errors()) > 0 {
			return errors.New("recovery consumer")
		}
		for _, r := range fetch.Records() {
			if string(r.Key) == key && bytes.Equal(r.Value, value) {
				count++
			}
		}
	}
	return nil
}

// Seed reserves one fixed test chat ID. Give each fixture a distinct binding
// before creating the next, preserving the production unique constraint.
func seedDrainFixture(ctx context.Context, pool *pgxpool.Pool) (apiprocess.Fixture, error) {
	f, err := apiprocess.Seed(ctx, pool)
	if err == nil {
		_, err = pool.Exec(ctx, `UPDATE app.users SET telegram_chat_id=123456+(SELECT count(*) FROM app.users) WHERE id=$1`, f.OwnerID)
	}
	return f, err
}

// A cancelled claim can have an ambiguous SQL response. Respect the existing
// 30-second lease rather than mutating it to make recovery artificially fast.
func waitRelayLeases(ctx context.Context, pool *pgxpool.Pool) error {
	return eventually(ctx, func() bool {
		var free bool
		e := pool.QueryRow(ctx, `SELECT (s.lease_until IS NULL OR s.lease_until<=clock_timestamp()) AND (d.lease_until IS NULL OR d.lease_until<=clock_timestamp()) FROM app.subscription_relay s CROSS JOIN app.delivery_relay d WHERE s.singleton AND d.singleton`).Scan(&free)
		return e == nil && free
	})
}
func withDrainedAPI(ctx context.Context, cfg apiprocess.Config, pool *pgxpool.Pool, check func(*apiprocess.Process) error) error {
	if err := waitRelayLeases(ctx, pool); err != nil {
		return err
	}
	return apiprocess.With(ctx, cfg, check)
}
