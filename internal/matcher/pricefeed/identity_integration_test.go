package pricefeed

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func replaceGeneration(t *testing.T, p *kgo.Client, cfg GroupConfig) {
	t.Helper()
	req := kmsg.NewPtrDeleteTopicsRequest()
	req.TopicNames = []string{cfg.Topic}
	req.Topics = []kmsg.DeleteTopicsRequestTopic{{Topic: &cfg.Topic}}
	res, err := req.RequestWith(bounded(t, 5*time.Second), p)
	if err != nil || len(res.Topics) != 1 || res.Topics[0].ErrorCode != 0 {
		t.Fatal(res, err)
	}
	create := kmsg.NewPtrCreateTopicsRequest()
	v := kmsg.NewCreateTopicsRequestTopic()
	v.Topic = cfg.Topic
	v.NumPartitions = 16
	v.ReplicationFactor = 1
	create.Topics = []kmsg.CreateTopicsRequestTopic{v}
	created, err := create.RequestWith(bounded(t, 5*time.Second), p)
	if err != nil || len(created.Topics) != 1 || created.Topics[0].ErrorCode != 0 {
		t.Fatal(created, err)
	}
}
func TestIntegrationDirectSourceGeneration(t *testing.T) {
	cfg, p := groupFixture(t, 16, 0)
	cfg.Config.Partitions = []int32{0}
	for _, field := range []string{"cluster", "topic-id"} {
		bad := cfg.Config
		if field == "cluster" {
			bad.Source.ClusterID = "unknown"
		} else {
			bad.Source.TopicID[0] ^= 1
		}
		if _, err := New(bounded(t, 5*time.Second), bad); !errors.Is(err, kafkasource.ErrMismatch) {
			t.Fatal(field, err)
		}
	}
	s := source(t, cfg.Config)
	r := registration(t, s, 0)
	produce(t, p, cfg.Config, fixture(), "BTCUSDT", true)
	original := next(t, r)
	replaceGeneration(t, p, cfg)
	fresh, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.TransactionalID(cfg.Topic+"-replacement"), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	sent := produce(t, fresh, cfg.Config, fixture(), "BTCUSDT", true)
	if sent.Offset != original.Position.Offset {
		t.Fatal("fixture must reuse offset", sent.Offset, original.Position.Offset)
	}
	select {
	case <-r.Done():
	case <-bounded(t, 6*time.Second).Done():
		t.Fatal("old reader survived replacement")
	}
	if _, err := r.Next(context.Background()); !errors.Is(err, ErrContinuity) {
		t.Fatal("replacement escaped", err)
	}
	if _, err := s.Register(context.Background(), "ETHUSDT", 0); !errors.Is(err, ErrContinuity) {
		t.Fatal(err)
	}
	if _, err := New(bounded(t, 5*time.Second), cfg.Config); !errors.Is(err, kafkasource.ErrMismatch) {
		t.Fatal("restart adopted new source", err)
	}
}
func TestIntegrationGroupSourceGeneration(t *testing.T) {
	cfg, p := groupFixture(t, 16, 0)
	bad := cfg
	bad.Source.TopicID[0] ^= 1
	if _, err := NewGroup(bounded(t, 5*time.Second), bad); !errors.Is(err, kafkasource.ErrMismatch) {
		t.Fatal(err)
	}
	g := group(t, cfg)
	a := assignment(t, g, nil)
	groupProduce(t, p, cfg, pairAt(0), true)
	original, err := a.Next(bounded(t, 5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	replaceGeneration(t, p, cfg)
	fresh, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.TransactionalID(cfg.Topic+"-replacement"))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	groupProduce(t, fresh, cfg, pairAt(0), true)
	select {
	case <-g.Done():
	case <-bounded(t, 6*time.Second).Done():
		t.Fatal("group survived replacement")
	}
	if _, err := a.Next(context.Background()); !errors.Is(err, ErrContinuity) {
		t.Fatal(err)
	}
	if err := a.Confirm(context.Background(), original.Position); !errors.Is(err, ErrContinuity) {
		t.Fatal("old delivery confirmed", err)
	}
	if _, err := g.NextAssignment(context.Background(), a); !errors.Is(err, ErrContinuity) {
		t.Fatal(err)
	}
}

func TestIntegrationGroupResumeIdentity(t *testing.T) {
	for _, mode := range []string{"unbound", "foreign", "wrong-partition", "wrong-topic"} {
		t.Run(mode, func(t *testing.T) {
			cfg, _ := groupFixture(t, 16, 0)
			prepare := cfg.PrepareSource
			cfg.PrepareSource = func(ctx context.Context, parts []int32) (map[int32]Position, error) {
				out, err := prepare(ctx, parts)
				if err != nil {
					return nil, err
				}
				p := parts[0]
				pos := out[p]
				switch mode {
				case "unbound":
					pos.Source = kafkasource.Identity{}
				case "foreign":
					pos.Source.TopicID[0] ^= 1
				case "wrong-partition":
					pos.Partition = 16
				case "wrong-topic":
					pos.Topic = "another-topic"
				}
				out[p] = pos
				return out, nil
			}
			g := group(t, cfg)
			a, err := g.NextAssignment(bounded(t, 8*time.Second), nil)
			expected := kafkasource.ErrMismatch
			if mode == "unbound" {
				expected = kafkasource.ErrIdentity
			}
			if a != nil || !errors.Is(err, expected) {
				t.Fatal("unproven cursor published", a, err)
			}
		})
	}
}
