package sourceprovision

import (
	"context"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type Kafka struct{ Client *kgo.Client }

func (k Kafka) Metadata(ctx context.Context, topic string) (kafkasource.Identity, []int32, error) {
	id, parts, err := kafkasource.Metadata(ctx, k.Client, topic)
	if err != nil {
		return id, nil, err
	}
	ps := make([]int32, len(parts))
	for n, p := range parts {
		ps[n] = p.Partition
	}
	return id, ps, nil
}
func (k Kafka) Empty(ctx context.Context, topic string, ps []int32) error {
	for _, timestamp := range []int64{-2, -1} {
		req := kmsg.NewPtrListOffsetsRequest()
		req.IsolationLevel = 1
		t := kmsg.NewListOffsetsRequestTopic()
		t.Topic = topic
		for _, p := range ps {
			v := kmsg.NewListOffsetsRequestTopicPartition()
			v.Partition = p
			v.Timestamp = timestamp
			t.Partitions = append(t.Partitions, v)
		}
		req.Topics = []kmsg.ListOffsetsRequestTopic{t}
		res, err := req.RequestWith(ctx, k.Client)
		if err != nil {
			return ErrInvalid
		}
		if len(res.Topics) != 1 || res.Topics[0].Topic != topic || len(res.Topics[0].Partitions) != len(ps) {
			return ErrInvalid
		}
		seen := map[int32]bool{}
		expected := map[int32]bool{}
		for _, p := range ps {
			expected[p] = true
		}
		for _, p := range res.Topics[0].Partitions {
			if p.ErrorCode != 0 || p.Offset != 0 || !expected[p.Partition] || seen[p.Partition] {
				return ErrInvalid
			}
			seen[p.Partition] = true
		}
	}
	return nil
}
