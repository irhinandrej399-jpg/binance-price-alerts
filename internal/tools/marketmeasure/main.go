// Infrastructure phase driver for OPS-022. Production behavior is unchanged.
package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/price"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/publish"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/relay"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/storage"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/trade"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafka"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/postgres"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/kafkafault"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}
func require(ok bool, message string) {
	if !ok {
		panic(message)
	}
}
func value[T any](v T, e error) T { check(e); return v }

type candidate struct {
	Q     priceobserved.Observed
	M     event.Envelope
	Bytes int
}
type driver struct {
	pool       *pgxpool.Pool
	repo       *storage.Repository
	cfg        kafka.Config
	topic      string
	gen        uint64
	candidates []candidate
	work       storage.Work
	phases     []map[string]any
}

func (d *driver) setup(ctx context.Context) {
	d.pool = value(postgres.Open(ctx, os.Getenv("MARKET_DATA_DATABASE_URL")))
	var role string
	check(d.pool.QueryRow(ctx, "SELECT current_user").Scan(&role))
	require(role == "market_data_runtime", "wrong SQL role")
	d.repo = storage.New(d.pool)
	d.cfg = kafka.Config{Brokers: []string{os.Getenv("KAFKA_BROKERS")}, TransactionalID: os.Getenv("KAFKA_PREFIX") + "-market-relay"}
	d.topic = os.Getenv("KAFKA_PREFIX") + "-prices"
	p := value(publish.New(ctx, d.cfg, d.topic))
	p.Close()
	tm := time.Date(2026, 11, 5, 10, 0, 0, 123456789, time.UTC)
	for i, id := range []string{"10", "12", "13", "14", "15"} {
		p := value(price.Parse("123.000000000000000001"))
		q := priceobserved.Observed{SchemaVersion: 1, Pair: "BTCUSDT", Price: &p, SourceID: id, Revision: priceobserved.Revision{StreamID: trade.StreamID, Generation: 1, Sequence: uint64(i + 1)}, TradeTime: tm, ReceivedAt: tm, SourceEventTime: &tm, StreamState: priceobserved.Live, GapDetected: i > 0}
		if i >= 3 {
			q.Revision.Generation = 2
			q.Revision.Sequence = 1
			q.StreamState = priceobserved.Unconfirmed
		}
		m := event.Envelope{EventID: fmt.Sprintf("ops022-price-%d", i+1), Type: priceobserved.Type, Version: 1, OccurredAt: tm, CorrelationID: "ops022-source", OrderingKey: q.Pair}
		b := value(priceobserved.Encode(m, q))
		target := []int{len(b), 4095, 4096, 4096, 4097}[i]
		m.CorrelationID += strings.Repeat("x", target-len(b))
		b = value(priceobserved.Encode(m, q))
		require(len(b) == target, "bad size")
		d.candidates = append(d.candidates, candidate{q, m, len(b)})
	}
}
func (d *driver) accept(ctx context.Context, i int) map[string]any {
	c := d.candidates[i]
	ok, e := d.repo.Accept(ctx, d.gen, c.Q, c.M)
	if i == 4 {
		require(!ok && errors.Is(e, storage.ErrTooLarge), "oversize accepted")
	} else {
		check(e)
		require(ok, "not accepted")
	}
	return map[string]any{"operations": 1, "event_id": c.M.EventID, "bytes": c.Bytes, "accepted": ok}
}
func (d *driver) operation(ctx context.Context, phase string) map[string]any {
	out := map[string]any{"operations": 1}
	switch phase {
	case "empty":
		out["operations"] = 0
	case "begin", "begin_again":
		d.gen = value(d.repo.BeginGeneration(ctx, "BTCUSDT"))
		out["generation"] = d.gen
	case "status_live":
		check(d.repo.SetStatus(ctx, "BTCUSDT", d.gen, priceobserved.Live, false))
	case "status_gap":
		check(d.repo.SetStatus(ctx, "BTCUSDT", d.gen, priceobserved.Unconfirmed, true))
	case "status_sticky":
		check(d.repo.SetStatus(ctx, "BTCUSDT", d.gen, priceobserved.Live, false))
		q := value(d.repo.Latest(ctx, "BTCUSDT"))
		require(q.GapDetected, "gap cleared")
		out["gap"] = q.GapDetected
	case "accept_small":
		return d.accept(ctx, 0)
	case "accept_4095_gap":
		return d.accept(ctx, 1)
	case "accept_4096":
		return d.accept(ctx, 2)
	case "accept_generation2":
		return d.accept(ctx, 3)
	case "reject_4097":
		return d.accept(ctx, 4)
	case "repeat", "old":
		c := d.candidates[0]
		if phase == "old" {
			c.Q.SourceID = "9"
		}
		c.M.EventID = "ops022-not-persisted"
		ok, e := d.repo.Accept(ctx, d.gen, c.Q, c.M)
		check(e)
		require(!ok, "repeat accepted")
		out["accepted"] = ok
	case "stale_generation":
		c := d.candidates[3]
		ok, e := d.repo.Accept(ctx, 1, c.Q, c.M)
		require(!ok && errors.Is(e, storage.ErrStale), "stale accepted")
		out["stale"] = true
	case "claim_first", "claim_lost", "reclaim":
		old := d.work
		// Lost-publish lease expires by database clock; no direct fixture UPDATE.
		for {
			w, e := d.repo.Claim(ctx, 60*time.Second)
			if phase == "reclaim" && errors.Is(e, storage.ErrNotFound) {
				select {
				case <-ctx.Done():
					panic(ctx.Err())
				case <-time.After(50 * time.Millisecond):
				}
				continue
			}
			check(e)
			d.work = w
			break
		}
		if phase == "reclaim" {
			require(d.work.EventID == old.EventID && d.work.Token > old.Token && string(d.work.Data) == string(old.Data), "changed reclaim")
		}
		out["event_id"] = d.work.EventID
		out["token"] = d.work.Token
		out["bytes"] = len(d.work.Data)
	case "publish_first", "publish_lost", "publish_retry":
		cfg := d.cfg
		var proxy *kafkafault.Proxy
		if phase == "publish_lost" {
			proxy = kafkafault.New(cfg.Brokers[0])
			defer proxy.Close()
			cfg.Dialer = proxy.DialContext
		}
		p := value(publish.New(ctx, cfg, d.topic))
		defer p.Close()
		if proxy != nil {
			proxy.Arm()
		}
		c, cancel := context.WithTimeout(ctx, 4*time.Second)
		defer cancel()
		e := p.Publish(c, d.work.Data)
		if proxy != nil {
			require(errors.Is(e, kafka.ErrCommitUnknown), "expected unknown commit")
			select {
			case code := <-proxy.Observed:
				require(code == 0, "broker rejected commit")
				out["broker_endtxn_code"] = code
			case <-time.After(time.Second):
				panic("no broker commit")
			}
			out["commit"] = "unknown-to-client-broker-committed"
		} else {
			check(e)
			out["commit"] = "confirmed"
		}
		out["event_id"] = d.work.EventID
		out["bytes"] = len(d.work.Data)
	case "mark_first", "mark_retry":
		check(d.repo.MarkPublished(ctx, d.work.EventID, d.work.Token))
		out["event_id"] = d.work.EventID
	case "relay_tail":
		c, cancel := context.WithCancel(ctx)
		defer cancel()
		n := 0
		e := relay.Run(c, d.repo, d.cfg, d.topic, func(s relay.Status) {
			if s.Err != nil {
				panic(s.Err)
			}
			require(s.InFlight <= 1 && s.Bytes <= 4096, "relay bounds")
			if s.Phase == "committed" {
				n++
				if n == 2 {
					cancel()
				}
			}
		})
		require(errors.Is(e, context.Canceled) && n == 2, "relay did not finish")
		out["operations"] = n
		out["max_batch"] = 1
	case "claim_empty":
		_, e := d.repo.Claim(ctx, time.Minute)
		require(errors.Is(e, storage.ErrNotFound), "unfinished outbox")
		out["empty"] = true
	default:
		panic("unknown phase: " + phase)
	}
	return out
}
func (d *driver) verify(ctx context.Context) any {
	var raw []byte
	check(d.pool.QueryRow(ctx, `SELECT json_agg(x ORDER BY position) FROM (SELECT position,event_id,pair,encode(data,'hex') body_hex,published,claim_token FROM app.price_outbox) x`).Scan(&raw))
	var rows []struct {
		EventID   string `json:"event_id"`
		Body      string `json:"body_hex"`
		Published bool   `json:"published"`
	}
	check(json.Unmarshal(raw, &rows))
	require(len(rows) == 4, "wrong outbox count")
	expected := map[string]string{}
	for i, r := range rows {
		require(r.Published, "not marked")
		want := value(priceobserved.Encode(d.candidates[i].M, d.candidates[i].Q))
		require(r.Body == hex.EncodeToString(want), "changed SQL bytes")
		expected[r.EventID] = r.Body
	}
	consumer := value(kgo.NewClient(kgo.SeedBrokers(d.cfg.Brokers...), kgo.ConsumeTopics(d.topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxWait(100*time.Millisecond)))
	defer consumer.Close()
	records := []map[string]any{}
	seen := map[string]int{}
	for len(records) < 5 {
		fs := consumer.PollFetches(ctx)
		if es := fs.Errors(); len(es) > 0 {
			panic(es[0].Err)
		}
		for _, r := range fs.Records() {
			m, q, e := priceobserved.Decode(r.Value)
			check(e)
			require(expected[m.EventID] == hex.EncodeToString(r.Value) && string(r.Key) == q.Pair, "changed Kafka bytes/key")
			seen[m.EventID]++
			records = append(records, map[string]any{"event_id": m.EventID, "body_hex": hex.EncodeToString(r.Value), "partition": r.Partition, "offset": r.Offset, "key": string(r.Key)})
		}
	}
	require(len(records) == 5 && seen["ops022-price-2"] == 2 && len(seen) == 4, "bad duplicate")
	q := value(d.repo.Latest(ctx, "BTCUSDT"))
	require(q.Revision.Generation == 2 && q.Revision.Sequence == 1 && q.SourceID == "14" && q.GapDetected, "bad latest")
	return map[string]any{"format_version": 1, "source_id": "OPS-022-market-data-v1", "sql_role": "market_data_runtime", "phases": d.phases, "outbox": json.RawMessage(raw), "read_committed": records, "latest": q, "max_batch": 1}
}
func main() {
	d := &driver{}
	defer func() {
		if d.pool != nil {
			d.pool.Close()
		}
	}()
	scan := bufio.NewScanner(os.Stdin)
	enc := json.NewEncoder(os.Stdout)
	for scan.Scan() {
		var req struct {
			Phase string `json:"phase_id"`
		}
		check(json.Unmarshal(scan.Bytes(), &req))
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
		var evidence any
		switch req.Phase {
		case "setup":
			d.setup(ctx)
		case "verification":
			evidence = d.verify(ctx)
		default:
			out := d.operation(ctx, req.Phase)
			d.phases = append(d.phases, map[string]any{"phase_id": req.Phase, "result": out})
		}
		cancel()
		check(enc.Encode(map[string]any{"phase_id": req.Phase, "status": "ok", "evidence": evidence}))
	}
	check(scan.Err())
}
