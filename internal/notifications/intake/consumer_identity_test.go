package intake

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestLegacyConsumerRejected(t *testing.T) {
	c, err := New(Config{Brokers: []string{"localhost:9092"}, Topic: "delivery", Group: "notifications"}, &Store{pool: &pgxpool.Pool{}})
	if c != nil {
		c.Close()
	}
	if !errors.Is(err, ErrConfig) {
		t.Fatalf("unbound transport accepted: %v", err)
	}
}

func TestFetchIdentityBeforeDecodeAcceptAndControl(t *testing.T) {
	id := kafkasource.Identity{ClusterID: "cluster", Topic: "delivery", TopicID: [16]byte{1}}
	for _, control := range []bool{false, true} {
		for _, zero := range []bool{false, true} {
			t.Run(fmt.Sprintf("control=%v/zero=%v", control, zero), func(t *testing.T) {
				attrs := kgo.NewRecordAttrs(kgo.RecordAttrsOpts{Control: control, Transactional: true})
				topicID := [16]byte{2}
				want := kafkasource.ErrMismatch
				if zero {
					topicID = [16]byte{}
					want = kafkasource.ErrIdentity
				}
				c := &Consumer{cfg: Config{Topic: id.Topic, Binding: Binding{Source: id}}, cursors: map[int32]int64{0: 0}}
				fs := kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: id.Topic, TopicID: topicID, Partitions: []kgo.FetchPartition{{Partition: 0, Records: []*kgo.Record{{Topic: id.Topic, Partition: 0, Offset: 0, Attrs: attrs, Key: []byte("sub-1"), Value: wire("foreign")}}}}}}}}
				if e := c.acceptFetches(context.Background(), fs, false); !errors.Is(e, want) {
					t.Fatal(e)
				}
				if c.cursors[0] != 0 {
					t.Fatal("foreign cursor advanced")
				}
			})
		}
	}
	// Same-topic responses must not merge away the foreign second response.
	c := &Consumer{cfg: Config{Binding: Binding{Source: id}}}
	fs := kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: id.Topic, TopicID: id.TopicID}}}, {Topics: []kgo.FetchTopic{{Topic: id.Topic, TopicID: [16]byte{2}}}}}
	if e := c.acceptFetches(context.Background(), fs, false); !errors.Is(e, kafkasource.ErrMismatch) {
		t.Fatal(e)
	}
	if e := c.acceptFetches(context.Background(), kgo.NewErrFetch(context.DeadlineExceeded), true); e != nil {
		t.Fatal(e)
	}
}

func TestConsumerBindingConfig(t *testing.T) {
	cfg := Config{Brokers: []string{"localhost:9092"}, Topic: "delivery", Group: "notifications", Binding: Binding{ConsumerID: "logical", Generation: "bootstrap", Source: kafkasource.Identity{ClusterID: "cluster", Topic: "delivery", TopicID: [16]byte{1}}}}
	for _, field := range []string{"consumer", "generation", "cluster", "id", "topic"} {
		bad := cfg
		want := ErrConfig
		switch field {
		case "consumer":
			bad.Binding.ConsumerID = ""
		case "generation":
			bad.Binding.Generation = " "
		case "cluster":
			bad.Binding.Source.ClusterID = ""
			want = kafkasource.ErrIdentity
		case "id":
			bad.Binding.Source.TopicID = [16]byte{}
			want = kafkasource.ErrIdentity
		case "topic":
			bad.Binding.Source.Topic = "other"
			want = kafkasource.ErrMismatch
		}
		c, e := New(bad, &Store{pool: &pgxpool.Pool{}})
		if c != nil {
			c.Close()
		}
		if !errors.Is(e, want) {
			t.Fatalf("%s: %v", field, e)
		}
	}
}

func TestPollDeadlineDoesNotHideBrokerPartitionFailure(t *testing.T) {
	id := kafkasource.Identity{ClusterID: "cluster", Topic: "delivery", TopicID: [16]byte{1}}
	c := &Consumer{cfg: Config{Binding: Binding{Source: id}}}
	fs := kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: id.Topic, TopicID: id.TopicID, Partitions: []kgo.FetchPartition{{Partition: 0, Err: context.DeadlineExceeded}}}}}}
	if err := c.acceptFetches(context.Background(), fs, true); !errors.Is(err, ErrPosition) {
		t.Fatal("broker failure hidden by poll deadline", err)
	}
}
