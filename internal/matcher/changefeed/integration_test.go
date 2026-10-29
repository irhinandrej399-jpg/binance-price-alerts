package changefeed

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
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
func integration(t *testing.T, limit int) (Config, *kgo.Client) {
	t.Helper()
	if os.Getenv("KAFKA_INTEGRATION") != "1" {
		t.Skip("real Kafka: bash infra/kafka/verify-go.sh")
	}
	broker, prefix := os.Getenv("KAFKA_TEST_BROKER"), os.Getenv("KAFKA_CHANGEFEED_TEST_PREFIX")
	if broker == "" || prefix == "" {
		t.Fatal("opt-in requires broker and changefeed prefix")
	}
	cfg := Config{Brokers: []string{broker}, Topic: prefix + "-subscription-changes", Buffer: limit}
	p, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.TransactionalID(prefix+"-changes-"+t.Name()), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.ProducerBatchCompression(kgo.NoCompression()))
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
func registration(t *testing.T, s *Source) *Registration {
	t.Helper()
	r, err := s.Register(bounded(t, 25*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	return r
}
func publish(t *testing.T, p *kgo.Client, cfg Config, commit bool, rs ...*kgo.Record) {
	t.Helper()
	ctx := bounded(t, 8*time.Second)
	if err := p.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	for _, r := range rs {
		r.Topic = cfg.Topic
		r.Partition = 0
	}
	if err := p.ProduceSync(ctx, rs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := p.EndTransaction(ctx, kgo.TransactionEndTry(commit)); err != nil {
		t.Fatal(err)
	}
}
func next(t *testing.T, r *Registration) Transaction {
	t.Helper()
	tx, err := r.Next(bounded(t, 8*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	return tx
}
func ack(t *testing.T, r *Registration, tx Transaction) {
	t.Helper()
	if err := r.Confirm(tx.NextOffset); err != nil {
		t.Fatal(err)
	}
}
func TestIntegrationWholeIdentityDuplicatesAndGap(t *testing.T) {
	cfg, p := integration(t, 0)
	s := source(t, cfg)
	r := registration(t, s)
	if s.client.OptValue(kgo.ConsumerGroup) != "" {
		t.Fatal("unexpected broker commits")
	}
	for range 2 {
		a, b := record(0, 9007199254740993, 9007199254740994), record(0, 9007199254740994, 9007199254740994)
		publish(t, p, cfg, true, a, b)
		tx := next(t, r)
		if len(tx.Records) != 2 || tx.First != 9007199254740993 || tx.C != 9007199254740994 || tx.Records[0].Position.Offset != a.Offset || tx.Records[1].Position.Offset != b.Offset || !bytes.Equal(tx.Records[0].JSON, a.Value) || tx.Records[0].Envelope.EventID != "event-9007199254740993" || tx.Records[0].Key != "sub-1" || tx.NextOffset <= b.Offset+1 {
			t.Fatalf("identity: %+v", tx)
		}
		if r.Confirmed() == tx.NextOffset {
			t.Fatal("automatic confirmation")
		}
		ack(t, r, tx)
	}
	publish(t, p, cfg, true, record(0, 9007199254740999, 9007199254740999))
	tx := next(t, r)
	if tx.First != 9007199254740999 {
		t.Fatal(tx)
	}
	ack(t, r, tx)
}
func TestIntegrationOpenAbortAndCommitAfterRegistration(t *testing.T) {
	cfg, p := integration(t, 0)
	s := source(t, cfg)
	ctx := bounded(t, 8*time.Second)
	if err := p.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	a, b := record(0, 41, 42), record(0, 42, 42)
	a.Topic = cfg.Topic
	b.Topic = cfg.Topic
	if err := p.ProduceSync(ctx, a, b).FirstErr(); err != nil {
		t.Fatal(err)
	}
	r := registration(t, s)
	if r.Start().Offset > a.Offset {
		t.Fatal("open transaction skipped")
	}
	wait, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if _, err := r.Next(wait); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("open visible", err)
	}
	if err := p.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}
	tx := next(t, r)
	if len(tx.Records) != 2 || tx.Records[0].Position.Offset != a.Offset {
		t.Fatal(tx)
	}
	ack(t, r, tx)
	publish(t, p, cfg, false, record(0, 43, 43))
	last := record(0, 44, 44)
	publish(t, p, cfg, true, last)
	tx = next(t, r)
	if tx.First != 44 || tx.Records[0].Position.Offset != last.Offset {
		t.Fatal("aborted visible", tx)
	}
	ack(t, r, tx)
}
func TestIntegrationConcurrentRegistration(t *testing.T) {
	cfg, p := integration(t, 0)
	s := source(t, cfg)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); publish(t, p, cfg, true, record(0, 1, 1)) }()
	r := registration(t, s)
	wg.Wait()
	last := record(0, 2, 2)
	publish(t, p, cfg, true, last)
	for {
		tx := next(t, r)
		if tx.Records[0].Position.Offset < r.Start().Offset {
			t.Fatal(tx)
		}
		ack(t, r, tx)
		if tx.Records[0].Position.Offset == last.Offset {
			break
		}
	}
}
func TestIntegrationInvalidBoundaryContractAndLimits(t *testing.T) {
	cases := []struct {
		name  string
		limit int
		rs    []*kgo.Record
		want  error
	}{
		{"partial", 0, []*kgo.Record{record(0, 1, 2)}, ErrBoundary},
		{"order", 0, []*kgo.Record{record(0, 1, 3), record(0, 3, 3)}, ErrBoundary},
		{"two-packages", 0, []*kgo.Record{record(0, 1, 1), record(0, 2, 2)}, ErrBoundary},
		{"package-limit", 1, []*kgo.Record{record(0, 1, 2), record(0, 2, 2)}, ErrOverflow},
	}
	badKey := record(0, 1, 1)
	badKey.Key = []byte("other")
	badVersion := record(0, 1, 1)
	badVersion.Value = bytes.Replace(badVersion.Value, []byte(`"version":1`), []byte(`"version":99`), 1)
	badSchema := record(0, 1, 1)
	badSchema.Value = bytes.Replace(badSchema.Value, []byte(`"schema_version":1`), []byte(`"schema_version":99`), 1)
	corrupt := record(0, 1, 1)
	corrupt.Value = []byte("{")
	large := record(0, 1, 1)
	large.Value = append(large.Value, bytes.Repeat([]byte(" "), MaxPayload)...)
	for i, v := range []*kgo.Record{badKey, badVersion, badSchema, corrupt, large} {
		want := ErrContract
		if i == 4 {
			want = ErrOverflow
		}
		cases = append(cases, struct {
			name  string
			limit int
			rs    []*kgo.Record
			want  error
		}{string(rune('a' + i)), 0, []*kgo.Record{v}, want})
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cfg, p := integration(t, tt.limit)
			r := registration(t, source(t, cfg))
			publish(t, p, cfg, true, tt.rs...)
			if _, err := r.Next(bounded(t, 8*time.Second)); !errors.Is(err, tt.want) || !errors.Is(err, ErrContinuity) {
				t.Fatal(err)
			}
		})
	}
}
func TestIntegrationQueueOverflowAfterDelivery(t *testing.T) {
	cfg, p := integration(t, 1)
	r := registration(t, source(t, cfg))
	publish(t, p, cfg, true, record(0, 1, 1))
	next(t, r)
	publish(t, p, cfg, true, record(0, 2, 2))
	select {
	case <-r.Done():
	case <-time.After(8 * time.Second):
		t.Fatal("no overflow")
	}
	if !errors.Is(r.Err(), ErrOverflow) {
		t.Fatal(r.Err())
	}
	if err := r.Confirm(r.Start().Offset + 2); !errors.Is(err, ErrContinuity) {
		t.Fatal(err)
	}
}
func TestIntegrationCancellationRevocationAndIdleLoss(t *testing.T) {
	cfg, _ := integration(t, 0)
	t.Run("registration-cancel", func(t *testing.T) {
		s := source(t, cfg)
		ctx, cancel := context.WithCancel(context.Background())
		r, err := s.Register(ctx)
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		if _, err = r.Next(bounded(t, 2*time.Second)); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})
	t.Run("owner-cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		s, err := New(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		r := registration(t, s)
		cancel()
		if _, err = r.Next(bounded(t, 2*time.Second)); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})
	t.Run("revoke", func(t *testing.T) {
		s := source(t, cfg)
		r := registration(t, s)
		s.Revoke()
		if !errors.Is(r.Err(), ErrRevoked) {
			t.Fatal(r.Err())
		}
		s.Close()
	})
	t.Run("loss", func(t *testing.T) {
		proxy := kafkafault.New(cfg.Brokers[0])
		defer proxy.Close()
		bad := cfg
		bad.Dialer = proxy.DialContext
		r := registration(t, source(t, bad))
		proxy.Close()
		if _, err := r.Next(bounded(t, 8*time.Second)); !errors.Is(err, ErrContinuity) {
			t.Fatal(err)
		}
	})
}
func TestIntegrationUnavailableAndOutOfRange(t *testing.T) {
	cfg, p := integration(t, 0)
	t.Run("unavailable", func(t *testing.T) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		bad := cfg
		bad.Brokers = []string{l.Addr().String()}
		l.Close()
		s := source(t, bad)
		if _, err := s.Register(bounded(t, time.Second)); !errors.Is(err, ErrPosition) {
			t.Fatal(err)
		}
	})
	t.Run("not-single-partition", func(t *testing.T) {
		bad := cfg
		bad.Topic = os.Getenv("KAFKA_CHANGEFEED_TEST_PREFIX") + "-prices"
		s := source(t, bad)
		if _, err := s.Register(bounded(t, 5*time.Second)); !errors.Is(err, ErrPosition) {
			t.Fatal(err)
		}
	})
	t.Run("cursor", func(t *testing.T) {
		s := source(t, cfg)
		r := registration(t, s)
		publish(t, p, cfg, true, record(0, 1, 1))
		ack(t, r, next(t, r))
		s.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{cfg.Topic: {0: {Epoch: -1, Offset: 1 << 60}}})
		if _, err := r.Next(bounded(t, 8*time.Second)); !errors.Is(err, ErrPosition) {
			t.Fatal(err)
		}
	})
}
func TestIntegrationMaximumPackage(t *testing.T) {
	cfg, p := integration(t, 0)
	r := registration(t, source(t, cfg))
	rs := make([]*kgo.Record, MaxBuffer)
	for i := range rs {
		rs[i] = record(0, uint64(i+1), MaxBuffer)
		rs[i].Value = append(rs[i].Value, bytes.Repeat([]byte(" "), MaxPayload-len(rs[i].Value))...)
	}
	publish(t, p, cfg, true, rs...)
	tx := next(t, r)
	if len(tx.Records) != MaxBuffer || tx.C != MaxBuffer {
		t.Fatal("maximum package truncated")
	}
	ack(t, r, tx)
}
