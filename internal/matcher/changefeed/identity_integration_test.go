package changefeed

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func generationTopic(t *testing.T, c *kgo.Client, topic string, create bool) {
	t.Helper()
	ctx := bounded(t, 5*time.Second)
	if create {
		req := kmsg.NewPtrCreateTopicsRequest()
		v := kmsg.NewCreateTopicsRequestTopic()
		v.Topic = topic
		v.NumPartitions = 1
		v.ReplicationFactor = 1
		req.Topics = []kmsg.CreateTopicsRequestTopic{v}
		res, err := req.RequestWith(ctx, c)
		if err != nil || len(res.Topics) != 1 || res.Topics[0].ErrorCode != 0 {
			t.Fatal(res, err)
		}
	} else {
		req := kmsg.NewPtrDeleteTopicsRequest()
		req.TopicNames = []string{topic}
		req.Topics = []kmsg.DeleteTopicsRequestTopic{{Topic: &topic}}
		res, err := req.RequestWith(ctx, c)
		if err != nil || len(res.Topics) != 1 || res.Topics[0].ErrorCode != 0 {
			t.Fatal(res, err)
		}
	}
}
func TestIntegrationSourceGeneration(t *testing.T) {
	cfg, p := integration(t, 0)
	cfg.Topic += fmt.Sprintf("-source-generation-%d", time.Now().UnixNano())
	generationTopic(t, p, cfg.Topic, true)
	var err error
	cfg.Source, err = newGeneration(bounded(t, 5*time.Second), p, cfg.Topic, kafkasource.Identity{})
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"cluster", "topic-id"} {
		bad := cfg
		if field == "cluster" {
			bad.Source.ClusterID = "unknown"
		} else {
			bad.Source.TopicID[0] ^= 1
		}
		s := source(t, bad)
		if _, err := s.Register(bounded(t, 5*time.Second)); !errors.Is(err, kafkasource.ErrMismatch) {
			t.Fatal(field, err)
		}
	}
	s := source(t, cfg)
	r := registration(t, s)
	publish(t, p, cfg, true, record(0, 1, 1))
	tx := next(t, r)
	pos := r.Start()
	pos.Offset = tx.NextOffset
	// Keep an unconfirmed delivery across replacement. It must become unconfirmable.
	generationTopic(t, p, cfg.Topic, false)
	generationTopic(t, p, cfg.Topic, true)
	replacement, err := newGeneration(bounded(t, 5*time.Second), p, cfg.Topic, cfg.Source)
	if err != nil || replacement == cfg.Source {
		t.Fatal(replacement, err)
	}
	// A fresh producer avoids its own cached ID for the deleted topic.
	fresh, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.TransactionalID(cfg.Topic+"-replacement"))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	publish(t, fresh, cfg, true, record(0, 2, 2))
	select {
	case <-r.Done():
	case <-bounded(t, 6*time.Second).Done():
		t.Fatal("old reader survived replacement")
	}
	if !errors.Is(r.Err(), ErrContinuity) {
		t.Fatal(r.Err())
	}
	if _, err := r.Next(context.Background()); err == nil {
		t.Fatal("new generation escaped")
	}
	if err := r.Confirm(tx.NextOffset); err == nil {
		t.Fatal("old position confirmed")
	}
	old := source(t, cfg)
	if _, err := old.RegisterAt(bounded(t, 5*time.Second), pos); !errors.Is(err, kafkasource.ErrMismatch) {
		t.Fatal("old resume", err)
	}
	newCfg := cfg
	newCfg.Source = replacement
	current := source(t, newCfg)
	if _, err := current.RegisterAt(bounded(t, 5*time.Second), pos); !errors.Is(err, kafkasource.ErrMismatch) {
		t.Fatal("relabeled checkpoint", err)
	}
	pos.Source = kafkasource.Identity{}
	if _, err := current.RegisterAt(bounded(t, 5*time.Second), pos); !errors.Is(err, kafkasource.ErrIdentity) {
		t.Fatal("unbound checkpoint", err)
	}
}

// newGeneration is only for provisioning disposable topics, never resuming an
// existing position. A successful controller create is not broker readiness.
func newGeneration(ctx context.Context, c *kgo.Client, topic string, previous kafkasource.Identity) (kafkasource.Identity, error) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		id, parts, err := kafkasource.Metadata(ctx, c, topic)
		if err == nil && id != previous && (previous.ClusterID == "" || id.ClusterID == previous.ClusterID) && len(parts) == 1 && parts[0].Partition == 0 && parts[0].Leader >= 0 {
			for _, replica := range parts[0].ISR {
				if replica == parts[0].Leader {
					return id, nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return kafkasource.Identity{}, ctx.Err()
		case <-ticker.C:
		}
	}
}
