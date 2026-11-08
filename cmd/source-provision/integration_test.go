package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/intake"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/postgres"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/sourceprovision"
	"github.com/twmb/franz-go/pkg/kgo"
)

const wire = `{"event_id":"outbox-1","type":"alert.triggered","version":1,"occurred_at":"2026-10-27T10:00:01Z","correlation_id":"request-1","ordering_key":"sub-1","payload":{"schema_version":1,"intent":"delivery_requested","trigger_id":"workflow-1","workflow_version":9007199254740993,"user_id":"user-1","subscription_id":"sub-1","subscription_version":9007199254740994,"pair":"BTCUSDT","threshold":"999999999999999999.123456789012345677","direction":"up","candidate_quote":{"schema_version":1,"pair":"BTCUSDT","price":"999999999999999999.123456789012345678","source_id":"9007199254740993","revision":{"stream_id":"binance-spot-trade","generation":2,"sequence":9007199254740993},"trade_time":"2026-10-27T10:00:00.123456Z","received_at":"2026-10-27T10:00:01.123456Z","stream_state":"unconfirmed","gap_detected":true}}}`

// Stop after durable inventory creation, before SQL mutation.
type interruptedBootstrap struct{ *intake.Store }

func (interruptedBootstrap) Bootstrap(context.Context, intake.Binding, []int32) error {
	return errors.New("interrupted before SQL")
}

func TestDisposableProvision(t *testing.T) {
	if os.Getenv("E2E_PROCESS_INTEGRATION") != "1" {
		t.Skip("run check_e2e_environment.py --package ./cmd/source-provision")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "inventory.json")
	secret := filepath.Join(t.TempDir(), "dsn")
	if err := os.WriteFile(secret, []byte(os.Getenv("NOTIFICATIONS_DATABASE_URL")), 0600); err != nil {
		t.Fatal("secret file")
	}
	get := func(k string) string {
		switch k {
		case "NOTIFICATIONS_DATABASE_URL_FILE":
			return secret
		case "KAFKA_BROKERS":
			return os.Getenv("KAFKA_TEST_BROKER")
		}
		return ""
	}
	var output bytes.Buffer
	call := func(mode string) error {
		output.Reset()
		args := []string{"--mode", mode, "--inventory", path}
		if mode == "init" {
			args = append(args, "--prefix", os.Getenv("KAFKA_TEST_PREFIX"))
		}
		return run(args, get, &output)
	}

	// An interruption leaves a usable plan but no SQL proof: verify must fail.
	initialPool, err := postgres.Open(ctx, os.Getenv("NOTIFICATIONS_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	initialClient, err := kgo.NewClient(kgo.SeedBrokers(os.Getenv("KAFKA_TEST_BROKER")))
	if err != nil {
		t.Fatal(err)
	}
	pending := filepath.Join(t.TempDir(), "pending.json")
	err = sourceprovision.Run(ctx, "init", pending, os.Getenv("KAFKA_TEST_PREFIX"), sourceprovision.Kafka{Client: initialClient}, interruptedBootstrap{intake.NewStore(initialPool)})
	initialClient.Close()
	initialPool.Close()
	if err == nil {
		t.Fatal("ignored interruption")
	}
	if run([]string{"--inventory", pending}, get, &output) == nil {
		t.Fatal("accepted missing SQL bootstrap")
	}
	if err := call("init"); err != nil {
		t.Fatal(err)
	}
	inv, err := sourceprovision.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	var env map[string]string
	if json.Unmarshal(output.Bytes(), &env) != nil || !reflect.DeepEqual(env, inv.Env()) {
		t.Fatal("runtime env")
	}
	if bytes.Contains(output.Bytes(), []byte(os.Getenv("NOTIFICATIONS_DATABASE_URL"))) {
		t.Fatal("secret disclosed")
	}
	pool, err := postgres.Open(ctx, os.Getenv("NOTIFICATIONS_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := intake.NewStore(pool)
	// A foreign generation and a missing SQL bootstrap are rejected read-only.
	foreign := inv
	foreign.Binding.Generation = "foreign"
	data, _ := json.Marshal(foreign)
	wrong := filepath.Join(t.TempDir(), "foreign.json")
	if os.WriteFile(wrong, data, 0600) != nil {
		t.Fatal("write")
	}
	if run([]string{"--inventory", wrong}, get, &output) == nil {
		t.Fatal("accepted wrong SQL generation")
	}
	// Actual broker history and matching durable intake progress.
	client, err := kgo.NewClient(kgo.SeedBrokers(os.Getenv("KAFKA_TEST_BROKER")), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	record := &kgo.Record{Topic: inv.Binding.Source.Topic, Partition: 0, Key: []byte("sub-1"), Value: []byte(wire)}
	if err = client.ProduceSync(ctx, record).FirstErr(); err != nil {
		t.Fatal("produce failed")
	}
	if err = store.AcceptBound(ctx, inv.Binding, 0, 0, record.Offset, record.Key, record.Value); err != nil {
		t.Fatal("durable accept failed")
	}
	for _, mode := range []string{"verify", "resume"} {
		if err = call(mode); err != nil {
			t.Fatal(err)
		}
	}
	progress, err := store.ReadProgress(ctx, inv.Binding, 0)
	if err != nil || progress.Next != record.Offset+1 {
		t.Fatal("progress reset")
	}
	after, _ := sourceprovision.Load(path)
	if !reflect.DeepEqual(inv, after) {
		t.Fatal("inventory changed")
	}
	// Changed Kafka identity is rejected even though topic names still exist.
	foreign = inv
	foreign.Sources = map[string]kafkasource.Identity{}
	for k, v := range inv.Sources {
		foreign.Sources[k] = v
	}
	source := foreign.Sources["prices"]
	source.TopicID[0] ^= 0xff
	foreign.Sources["prices"] = source
	data, _ = json.Marshal(foreign)
	if os.WriteFile(wrong, data, 0600) != nil {
		t.Fatal("write")
	}
	if run([]string{"--inventory", wrong}, get, &output) == nil {
		t.Fatal("accepted Kafka mismatch")
	}
	t.Log("fresh inventory, runtime role/secret file, durable nonzero resume and mismatches PASS")
}
