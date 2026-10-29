package pricefeed

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/kafkafault"
	"github.com/twmb/franz-go/pkg/kgo"
)

func bounded(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}
func integration(t *testing.T, partition int32, buffer int) (Config, *kgo.Client) {
	t.Helper()
	if os.Getenv("KAFKA_INTEGRATION") != "1" {
		t.Skip("real Kafka: bash infra/kafka/verify-go.sh")
	}
	broker, prefix := os.Getenv("KAFKA_TEST_BROKER"), os.Getenv("KAFKA_PRICEFEED_TEST_PREFIX")
	if broker == "" || prefix == "" {
		t.Fatal("opt-in requires KAFKA_TEST_BROKER and KAFKA_PRICEFEED_TEST_PREFIX")
	}
	cfg := Config{Brokers: []string{broker}, Topic: prefix + "-prices", Partitions: []int32{partition}, Buffer: buffer}
	p, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.TransactionalID(prefix+"-feed-"+t.Name()), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.ProducerBatchCompression(kgo.NoCompression()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return cfg, p
}
func source(t *testing.T, cfg Config) *Source {
	t.Helper()
	s, err := New(bounded(t, 30*time.Second), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}
func registration(t *testing.T, s *Source, partition int32) *Registration {
	t.Helper()
	r, err := s.Register(bounded(t, 25*time.Second), "BTCUSDT", partition)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	return r
}
func produce(t *testing.T, p *kgo.Client, cfg Config, payload []byte, key string, commit bool) *kgo.Record {
	t.Helper()
	ctx := bounded(t, 8*time.Second)
	if err := p.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	r := &kgo.Record{Topic: cfg.Topic, Partition: cfg.Partitions[0], Key: []byte(key), Value: payload}
	if err := p.ProduceSync(ctx, r).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := p.EndTransaction(ctx, kgo.TransactionEndTry(commit)); err != nil {
		t.Fatal(err)
	}
	return r
}
func next(t *testing.T, r *Registration) Update {
	t.Helper()
	u, err := r.Next(bounded(t, 8*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	return u
}
func TestIntegrationRegistrationIdentityAndDuplicates(t *testing.T) {
	cfg, p := integration(t, 0, 0)
	s := source(t, cfg)
	r := registration(t, s, 0)
	for i := range 2 {
		sent := produce(t, p, cfg, fixture(), "BTCUSDT", true)
		var u Update
		if i == 0 {
			u = next(t, r)
		} else {
			ctx := bounded(t, 8*time.Second)
			for {
				var ok bool
				var err error
				u, ok, err = r.TryNext()
				if err != nil {
					t.Fatal(err)
				}
				if ok {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(time.Millisecond):
				}
			}
		}
		if u.Envelope.EventID != "original-event" || u.Quote.Revision.Generation != 2 || u.Quote.Revision.Sequence != 17 || u.Quote.SourceID != "9007199254740993" || u.Quote.Price.String() != "123.456789012345678901" || u.Key != "BTCUSDT" || u.Position != (Position{cfg.Topic, 0, sent.Offset}) || u.Position.Offset < r.Start().Offset {
			t.Fatalf("identity changed: %+v", u)
		}
	}
	// Direct assignment has no group or offset-commit protocol. Assert the actual
	// client configuration as well as continued reading after a cancelled wait.
	if v := s.client.OptValue(kgo.ConsumerGroup); v != "" {
		t.Fatalf("group: %v", v)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	produce(t, p, cfg, fixture(), "BTCUSDT", true)
	next(t, r)
	r.Close()
	if _, err := r.Next(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	// A fresh barrier can replace a closed registration on the same source.
	replacement := registration(t, s, 0)
	produce(t, p, cfg, fixture(), "BTCUSDT", true)
	next(t, replacement)
}
func TestIntegrationOpenAbortAndCommitAfterRegistration(t *testing.T) {
	cfg, p := integration(t, 1, 0)
	s := source(t, cfg)
	ctx := bounded(t, 8*time.Second)
	if err := p.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	open := &kgo.Record{Topic: cfg.Topic, Partition: 1, Key: []byte("BTCUSDT"), Value: fixture()}
	if err := p.ProduceSync(ctx, open).FirstErr(); err != nil {
		t.Fatal(err)
	}
	r := registration(t, s, 1)
	if r.Start().Offset > open.Offset {
		t.Fatal("registration skipped open transaction")
	}
	wait, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := r.Next(wait); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("open visible: %v", err)
	}
	if err := p.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}
	if u := next(t, r); u.Position.Offset != open.Offset {
		t.Fatal(u)
	}
	produce(t, p, cfg, bytes.ReplaceAll(fixture(), []byte("original-event"), []byte("aborted-event")), "BTCUSDT", false)
	barrier := produce(t, p, cfg, fixture(), "BTCUSDT", true)
	if u := next(t, r); u.Envelope.EventID != "original-event" || u.Position.Offset != barrier.Offset {
		t.Fatalf("aborted visible: %+v", u)
	}
}
func TestIntegrationConcurrentRegistration(t *testing.T) {
	cfg, p := integration(t, 2, 0)
	s := source(t, cfg)
	// A transaction concurrent with the broker barrier can be on either side;
	// every subsequent publication must be present, in original offset order.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); produce(t, p, cfg, fixture(), "BTCUSDT", true) }()
	r := registration(t, s, 2)
	wg.Wait()
	barrier := produce(t, p, cfg, fixture(), "BTCUSDT", true)
	for {
		u := next(t, r)
		if u.Position.Offset == barrier.Offset {
			break
		}
		if u.Position.Offset < r.Start().Offset || u.Position.Offset > barrier.Offset {
			t.Fatal(u)
		}
	}
}
func TestIntegrationOverflow(t *testing.T) {
	cfg, p := integration(t, 3, 1)
	s := source(t, cfg)
	r := registration(t, s, 3)
	produce(t, p, cfg, fixture(), "BTCUSDT", true)
	produce(t, p, cfg, fixture(), "BTCUSDT", true)
	select {
	case <-s.done:
	case <-time.After(8 * time.Second):
		t.Fatal("overflow did not retire source")
	}
	if _, err := r.Next(context.Background()); !errors.Is(err, ErrOverflow) || !errors.Is(err, ErrContinuity) {
		t.Fatal(err)
	}
	if _, err := s.Register(context.Background(), "ETHUSDT", 3); !errors.Is(err, ErrOverflow) {
		t.Fatal(err)
	}
}
func TestIntegrationInvalidContracts(t *testing.T) {
	for i, payload := range [][]byte{[]byte("{"), bytes.Replace(fixture(), []byte(`"version":1`), []byte(`"version":99`), 1), bytes.Replace(fixture(), []byte(`"schema_version":1`), []byte(`"schema_version":99`), 1), fixture(), append(fixture(), bytes.Repeat([]byte(" "), 4096)...)} {
		t.Run(string(rune('a'+i)), func(t *testing.T) {
			cfg, p := integration(t, int32(4+i), 0)
			s := source(t, cfg)
			r := registration(t, s, cfg.Partitions[0])
			key := "BTCUSDT"
			if i == 3 {
				key = "ETHUSDT"
			}
			produce(t, p, cfg, payload, key, true)
			if _, err := r.Next(bounded(t, 8*time.Second)); !errors.Is(err, ErrContract) || !errors.Is(err, ErrContinuity) {
				t.Fatalf("contract not rejected: %v", err)
			}
		})
	}
}
func TestIntegrationCancellationRevocationClose(t *testing.T) {
	cfg, _ := integration(t, 9, 0)
	s := source(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	r, err := s.Register(ctx, "BTCUSDT", 9)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err = r.Next(bounded(t, time.Second)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	r = registration(t, s, 9)
	s.Revoke()
	if _, err = r.Next(context.Background()); !errors.Is(err, ErrRevoked) || !errors.Is(err, ErrContinuity) {
		t.Fatal(err)
	}
	s.Close()
}
func TestIntegrationUnavailableAndCursor(t *testing.T) {
	cfg, _ := integration(t, 10, 0)
	t.Run("cursor", func(t *testing.T) {
		bad := cfg
		bad.Topic = strings.TrimSuffix(cfg.Topic, "-prices") + "-subscription-changes"
		bad.Partitions = []int32{1}
		if s, err := New(bounded(t, 5*time.Second), bad); !errors.Is(err, ErrPosition) {
			if s != nil {
				s.Close()
			}
			t.Fatalf("missing partition: %v", err)
		}
	})
	t.Run("broker", func(t *testing.T) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		bad := cfg
		bad.Brokers = []string{l.Addr().String()}
		l.Close()
		if s, err := New(bounded(t, time.Second), bad); !errors.Is(err, ErrPosition) {
			if s != nil {
				s.Close()
			}
			t.Fatal(err)
		}
	})
	t.Run("loss", func(t *testing.T) {
		proxy := kafkafault.New(cfg.Brokers[0])
		defer proxy.Close()
		bad := cfg
		bad.Dialer = proxy.DialContext
		s := source(t, bad)
		r := registration(t, s, 10)
		proxy.Close()
		if _, err := r.Next(bounded(t, 8*time.Second)); !errors.Is(err, ErrContinuity) {
			t.Fatalf("loss not signalled: %v", err)
		}
	})
}

func TestIntegrationOutOfRange(t *testing.T) {
	cfg, p := integration(t, 11, 0)
	s := source(t, cfg)
	r := registration(t, s, 11)
	produce(t, p, cfg, fixture(), "BTCUSDT", true)
	next(t, r)
	// Inject a stale/unavailable cursor into the actual assigned consumer. Kafka
	// must report the error; resetting to end/start would silently lose continuity.
	s.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{cfg.Topic: {11: {Epoch: -1, Offset: 1 << 60}}})
	if _, err := r.Next(bounded(t, 8*time.Second)); !errors.Is(err, ErrPosition) || !errors.Is(err, ErrContinuity) {
		t.Fatalf("cursor silently reset: %v", err)
	}
}
