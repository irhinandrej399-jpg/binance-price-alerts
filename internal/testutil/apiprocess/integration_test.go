package apiprocess

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/deliveryrelay"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/session"
	store "github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/alerttriggered"
	domain "github.com/irhinandrej399-jpg/binance-price-alerts/internal/domain/subscription"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafka"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/postgrestest"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestDisposableFixture(t *testing.T) {
	if os.Getenv("API_PROCESS_INTEGRATION") != "1" {
		t.Skip("run python3 scripts/check_api_process.py")
	}
	pool := postgrestest.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	f, err := Seed(ctx, pool)
	if err != nil {
		t.Fatal("seed failed", err)
	}
	owner, err := session.New(pool, nil).Verify(ctx, f.Token)
	if err != nil || owner != f.OwnerID {
		t.Fatal("session invalid")
	}
	active, err := store.New(pool).ByID(ctx, f.OwnerID, f.Active.ID)
	if err != nil || active.State != domain.Active {
		t.Fatal("active subscription missing")
	}
	var quota, reserved int
	if err = pool.QueryRow(ctx, `SELECT active_subscriptions,(SELECT count(*) FROM app.subscriptions WHERE owner_id=$1 AND quota_reserved) FROM app.users WHERE id=$1`, f.OwnerID).Scan(&quota, &reserved); err != nil || quota != 2 || reserved != 2 {
		t.Fatal("quota mismatch", err)
	}
	meta, work, err := alerttriggered.Decode(f.DeliveryValue)
	if err != nil || meta.EventID != f.DeliveryEventID || work.UserID != f.OwnerID || work.SubscriptionID != f.Delivery.ID || work.TriggerID != f.Delivery.Workflow.TriggerID {
		t.Fatal("delivery identity mismatch")
	}
	cfg := childConfig(t, "serve")
	cfg.Env["DATABASE_URL"] = os.Getenv("DATABASE_URL")
	pid := 0
	if err = With(ctx, cfg, func(p *Process) error { pid = p.PID(); return nil }); err != nil {
		t.Fatal(err)
	}
	assertDead(t, pid)
	broker, prefix := os.Getenv("KAFKA_TEST_BROKER"), os.Getenv("KAFKA_TEST_PREFIX")
	if broker == "" || prefix == "" {
		t.Fatal("missing Kafka opt-in config")
	}
	topic := prefix + "-delivery-requests"
	relay, err := deliveryrelay.New(pool, kafka.Config{Brokers: []string{broker}, TransactionalID: prefix + "-delivery-relay"}, topic)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := relay.Step(ctx); err != nil || !ok {
		t.Fatal("fixture relay failed", err)
	}
	consumer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxWait(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	for {
		fetch := consumer.PollFetches(ctx)
		if len(fetch.Errors()) > 0 {
			t.Fatal("consumer failed")
		}
		records := fetch.Records()
		if len(records) == 0 {
			continue
		}
		if len(records) != 1 || !bytes.Equal(records[0].Key, f.DeliveryKey) || !bytes.Equal(records[0].Value, f.DeliveryValue) {
			t.Fatal("changed key/value")
		}
		got, _, err := alerttriggered.Decode(records[0].Value)
		if err != nil || got.EventID != f.DeliveryEventID {
			t.Fatal("changed event ID")
		}
		break
	}
}
