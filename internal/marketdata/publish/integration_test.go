package publish

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafka"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/kafkafault"
	"github.com/twmb/franz-go/pkg/kgo"
)

func config(t *testing.T, suffix string) (kafka.Config, string, []byte) {
	t.Helper()
	if os.Getenv("KAFKA_INTEGRATION") != "1" {
		t.Skip("real Kafka: run bash infra/kafka/verify-go.sh")
	}
	broker, prefix := os.Getenv("KAFKA_TEST_BROKER"), os.Getenv("KAFKA_TEST_PREFIX")
	if broker == "" || prefix == "" {
		t.Fatal("opt-in requires KAFKA_TEST_BROKER and KAFKA_TEST_PREFIX")
	}
	payload := bytes.Replace(observation(), []byte("stable-event"), []byte(prefix+"-"+suffix), 1)
	return kafka.Config{Brokers: []string{broker}, TransactionalID: prefix + "-price-" + suffix}, prefix + "-prices", payload
}
func bounded(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}
func newProducer(t *testing.T, cfg kafka.Config, topic string) *Producer {
	t.Helper()
	p, err := New(bounded(t, 15*time.Second), cfg, topic)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

// An independent read_committed consumer checks actual payloads on the prices
// topic (16 partitions), rather than acknowledging a sequence of mock calls.
func readCopies(t *testing.T, cfg kafka.Config, topic string, payload []byte, want int) {
	t.Helper()
	c, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxWait(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	target, _, err := priceobserved.Decode(payload)
	if err != nil {
		t.Fatal(err)
	}
	ctx := bounded(t, 20*time.Second)
	count := 0
	var partition int32 = -1
	for count < want {
		fs := c.PollFetches(ctx)
		if es := fs.Errors(); len(es) > 0 {
			t.Fatal(es)
		}
		for _, r := range fs.Records() {
			meta, observed, err := priceobserved.Decode(r.Value)
			if err != nil {
				t.Fatal(err)
			}
			if meta.EventID != target.EventID {
				continue
			}
			if !bytes.Equal(r.Value, payload) || string(r.Key) != observed.Pair {
				t.Fatalf("changed wire identity/key: %+v", r)
			}
			if partition >= 0 && partition != r.Partition {
				t.Fatal("retry changed partition")
			}
			partition = r.Partition
			count++
		}
	}
	if count != want {
		t.Fatalf("copies: got %d want %d", count, want)
	}
}
func TestIntegrationCommit(t *testing.T) {
	cfg, topic, payload := config(t, "commit")
	p := newProducer(t, cfg, topic)
	if err := p.Publish(bounded(t, 10*time.Second), payload); err != nil {
		t.Fatal(err)
	}
	readCopies(t, cfg, topic, payload, 1)
}
func TestIntegrationLostAckRetry(t *testing.T) {
	cfg, topic, payload := config(t, "lost-ack")
	proxy := kafkafault.New(cfg.Brokers[0])
	defer proxy.Close()
	faultCfg := cfg
	faultCfg.Dialer = proxy.DialContext
	p := newProducer(t, faultCfg, topic)
	proxy.Arm()
	err := p.Publish(bounded(t, 4*time.Second), payload)
	if !errors.Is(err, kafka.ErrCommitUnknown) || !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, kafka.ErrFenced) {
		t.Fatalf("unknown commit required: %v", err)
	}
	select {
	case code := <-proxy.Observed:
		if code != 0 {
			t.Fatalf("broker EndTxn code %d", code)
		}
	case <-time.After(time.Second):
		t.Fatal("broker commit success not captured")
	}
	readCopies(t, cfg, topic, payload, 1)
	if err := p.Publish(bounded(t, time.Second), payload); !errors.Is(err, kafka.ErrRetired) {
		t.Fatal(err)
	}
	p.Close()
	replacement := newProducer(t, cfg, topic)
	if err := replacement.Publish(bounded(t, 10*time.Second), payload); err != nil {
		t.Fatal(err)
	}
	readCopies(t, cfg, topic, payload, 2)
}
func TestIntegrationFenced(t *testing.T) {
	cfg, topic, payload := config(t, "fenced")
	old := newProducer(t, cfg, topic)
	if err := old.Publish(bounded(t, 10*time.Second), payload); err != nil {
		t.Fatal(err)
	}
	replacement := newProducer(t, cfg, topic)
	if err := old.Publish(bounded(t, 10*time.Second), payload); !errors.Is(err, kafka.ErrFenced) {
		t.Fatalf("fencing required: %v", err)
	}
	if err := replacement.Publish(bounded(t, 10*time.Second), payload); err != nil {
		t.Fatal(err)
	}
	readCopies(t, cfg, topic, payload, 2)
}
func TestIntegrationBrokerFailure(t *testing.T) {
	cfg, topic, payload := config(t, "failure")
	proxy := kafkafault.New(cfg.Brokers[0])
	defer proxy.Close()
	cfg.Dialer = proxy.DialContext
	p := newProducer(t, cfg, topic)
	proxy.Close()
	if err := p.Publish(bounded(t, time.Second), payload); !errors.Is(err, kafka.ErrPublish) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("broker failure reported as %v", err)
	}
}
func TestIntegrationUnavailable(t *testing.T) {
	cfg, topic, _ := config(t, "unavailable")
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Brokers = []string{l.Addr().String()}
	l.Close()
	if _, err := New(bounded(t, 500*time.Millisecond), cfg, topic); !errors.Is(err, kafka.ErrPublish) {
		t.Fatalf("unavailable broker: %v", err)
	}
}
