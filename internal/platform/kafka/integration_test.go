package kafka

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/kafkafault"
	"github.com/twmb/franz-go/pkg/kgo"
)

func testConfig(t *testing.T, suffix string) Config {
	t.Helper()
	if os.Getenv("KAFKA_INTEGRATION") != "1" {
		t.Skip("real Kafka: run bash infra/kafka/verify-go.sh (KAFKA_INTEGRATION=1)")
	}
	broker, prefix := os.Getenv("KAFKA_TEST_BROKER"), os.Getenv("KAFKA_TEST_PREFIX")
	if broker == "" || prefix == "" {
		t.Fatal("opt-in requires KAFKA_TEST_BROKER and KAFKA_TEST_PREFIX")
	}
	return Config{Brokers: []string{broker}, TransactionalID: prefix + "-" + suffix}
}
func bounded(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}
func producer(t *testing.T, cfg Config) *Producer {
	t.Helper()
	p, e := New(bounded(t, 15*time.Second), cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(p.Close)
	return p
}
func probe(value string) Record {
	return Record{Topic: os.Getenv("KAFKA_TEST_PREFIX") + "-subscription-changes", Key: []byte("probe-key"), Value: []byte(value)}
}

// Read a whole partition until a unique committed barrier. This verifies absence
// of aborted/open records without guessing a sleep interval for fetch completion.
func readUntil(t *testing.T, cfg Config, barrier string, isolation kgo.IsolationLevel) []string {
	t.Helper()
	c, e := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{probe("").Topic: {0: kgo.NewOffset().AtStart()}}), kgo.FetchIsolationLevel(isolation), kgo.FetchMaxWait(100*time.Millisecond))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	ctx := bounded(t, 20*time.Second)
	var values []string
	for {
		fs := c.PollFetches(ctx)
		if es := fs.Errors(); len(es) > 0 {
			t.Fatal(es)
		}
		for _, r := range fs.Records() {
			v := string(r.Value)
			values = append(values, v)
			if v == barrier {
				return values
			}
		}
	}
}

// Check while the transaction is still open, not only after abort.
func assertOpenInvisible(t *testing.T, cfg Config) {
	t.Helper()
	c, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{probe("").Topic: {0: kgo.NewOffset().AtStart()}}), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxWait(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := bounded(t, time.Second)
	for ctx.Err() == nil {
		fs := c.PollFetches(ctx)
		for _, e := range fs.Errors() {
			if ctx.Err() == nil {
				t.Fatal(e)
			}
		}
		for _, r := range fs.Records() {
			if string(r.Value) == "open" || string(r.Value) == "aborted" {
				t.Fatalf("uncommitted record visible: %s", r.Value)
			}
		}
	}
}

func contains(vs []string, s string) bool {
	for _, v := range vs {
		if v == s {
			return true
		}
	}
	return false
}

func TestIntegrationCommitAbortIsolation(t *testing.T) {
	cfg := testConfig(t, "isolation")
	p := producer(t, cfg)
	// Raw-client path confirms chosen client's explicit abort and open transaction
	// semantics; application publishing uses the smaller Publish interface.
	raw, e := kgo.NewClient(clientOptions(Config{Brokers: cfg.Brokers, TransactionalID: cfg.TransactionalID + "-raw"})...)
	if e != nil {
		t.Fatal(e)
	}
	defer raw.Close()
	ctx := bounded(t, 30*time.Second)
	send := func(value string) {
		t.Helper()
		r := probe(value)
		if e := raw.ProduceSync(ctx, &kgo.Record{Topic: r.Topic, Key: r.Key, Value: r.Value}).FirstErr(); e != nil {
			t.Fatal(e)
		}
	}
	if e := raw.BeginTransaction(); e != nil {
		t.Fatal(e)
	}
	send("aborted")
	if e := raw.EndTransaction(ctx, kgo.TryAbort); e != nil {
		t.Fatal(e)
	}
	if e := raw.BeginTransaction(); e != nil {
		t.Fatal(e)
	}
	send("open")
	// read_uncommitted demonstrably sees the open record.
	if vs := readUntil(t, cfg, "open", kgo.ReadUncommitted()); !contains(vs, "aborted") {
		t.Fatal("aborted record absent from physical log")
	}
	assertOpenInvisible(t, cfg)
	if e := raw.EndTransaction(ctx, kgo.TryAbort); e != nil {
		t.Fatal(e)
	}
	if e := p.Publish(ctx, probe("isolation-barrier")); e != nil {
		t.Fatal(e)
	}
	vs := readUntil(t, cfg, "isolation-barrier", kgo.ReadCommitted())
	if contains(vs, "open") || contains(vs, "aborted") {
		t.Fatalf("uncommitted visible: %v", vs)
	}
}

func TestIntegrationFencing(t *testing.T) {
	cfg := testConfig(t, "fencing")
	old := producer(t, cfg)
	// Make the old epoch active before replacement initialization.
	ctx := bounded(t, 20*time.Second)
	if e := old.client.BeginTransaction(); e != nil {
		t.Fatal(e)
	}
	r := probe("fenced-uncommitted")
	if e := old.client.ProduceSync(ctx, &kgo.Record{Topic: r.Topic, Value: r.Value}).FirstErr(); e != nil {
		t.Fatal(e)
	}
	next := producer(t, cfg)
	if e := safeError(old.client.EndTransaction(ctx, kgo.TryCommit), true); !errors.Is(e, ErrFenced) {
		t.Fatalf("old producer must be fenced: %v", e)
	}
	if e := next.Publish(ctx, probe("fencing-barrier")); e != nil {
		t.Fatal(e)
	}
	if vs := readUntil(t, cfg, "fencing-barrier", kgo.ReadCommitted()); contains(vs, "fenced-uncommitted") {
		t.Fatal(vs)
	}
}

func TestIntegrationPublishFenced(t *testing.T) {
	cfg := testConfig(t, "wrapper-fenced")
	old := producer(t, cfg)
	if e := old.Publish(bounded(t, 10*time.Second), probe("before-fence")); e != nil {
		t.Fatal(e)
	}
	replacement := producer(t, cfg)
	if e := old.Publish(bounded(t, 10*time.Second), probe("after-fence")); !errors.Is(e, ErrFenced) {
		t.Fatalf("public Publish must report fencing: %v", e)
	}
	if e := old.Publish(bounded(t, time.Second), probe("after-fence")); !errors.Is(e, ErrRetired) {
		t.Fatalf("fenced client not retired: %v", e)
	}
	if e := replacement.Publish(bounded(t, 10*time.Second), probe("wrapper-fence-barrier")); e != nil {
		t.Fatal(e)
	}
	if vs := readUntil(t, cfg, "wrapper-fence-barrier", kgo.ReadCommitted()); contains(vs, "after-fence") {
		t.Fatal(vs)
	}
}

func TestIntegrationLostCommitResponse(t *testing.T) {
	cfg := testConfig(t, "lost-response")
	proxy := kafkafault.New(cfg.Brokers[0])
	defer proxy.Close()
	faultCfg := cfg
	faultCfg.Dialer = proxy.DialContext
	p := producer(t, faultCfg)
	proxy.Arm()
	ctx := bounded(t, 4*time.Second)
	original := probe("stable-probe-identity")
	err := p.Publish(ctx, original)
	if !errors.Is(err, ErrCommitUnknown) || !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrFenced) {
		t.Fatalf("expected unknown commit: %v", err)
	}
	select {
	case code := <-proxy.Observed:
		if code != 0 {
			t.Fatalf("EndTxn broker error %d", code)
		}
	case <-time.After(time.Second):
		t.Fatal("no actual broker EndTxn success captured")
	}
	if e := p.Publish(bounded(t, time.Second), original); !errors.Is(e, ErrRetired) {
		t.Fatalf("uncertain client reused: %v", e)
	}
	// Independent read_committed proves first commit happened despite lost reply.
	readUntil(t, cfg, "stable-probe-identity", kgo.ReadCommitted())
	p.Close()
	replacement := producer(t, cfg)
	if e := replacement.Publish(bounded(t, 15*time.Second), original, probe("retry-barrier")); e != nil {
		t.Fatal(e)
	}
	vs := readUntil(t, cfg, "retry-barrier", kgo.ReadCommitted())
	count := 0
	for _, v := range vs {
		if v == "stable-probe-identity" {
			count++
		}
	}
	if count != 2 {
		t.Fatalf("expected two copies with original identity: %v", vs)
	}
	t.Log("broker confirmed EndTxn; client got unknown outcome; read_committed saw two identical probe identities after replacement retry")
}

func TestIntegrationUnavailable(t *testing.T) {
	cfg := testConfig(t, "unavailable")
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	addr := l.Addr().String()
	l.Close()
	cfg.Brokers = []string{addr}
	_, e = New(bounded(t, 500*time.Millisecond), cfg)
	if e == nil || errors.Is(e, ErrFenced) || errors.Is(e, ErrCommitUnknown) {
		t.Fatalf("unavailable initialization: %v", e)
	}
}
