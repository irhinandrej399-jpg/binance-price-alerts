package cleaner

import (
	"context"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// BrokerRejection is a decoded broker error for this one partition. No broker
// message or endpoint is included; the durable reservation is still retained.
type BrokerRejection struct{ Code int16 }

func (e *BrokerRejection) Error() string { return "DeleteRecords rejected by broker" }
func (e *BrokerRejection) Unwrap() error { return ErrDelete }

type KafkaBroker struct{ Client *kgo.Client }

func (b KafkaBroker) Bounds(parent context.Context, source kafkasource.Identity, partition int32) (int64, int64, error) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	if b.Client == nil {
		return 0, 0, ErrBroker
	}
	parts, err := source.Verify(ctx, b.Client)
	if err != nil {
		return 0, 0, ErrBroker
	}
	found := false
	for _, p := range parts {
		if p.Partition == partition {
			found = true
		}
	}
	if !found {
		return 0, 0, ErrBroker
	}
	offset := func(timestamp int64) (int64, error) {
		req := kmsg.NewPtrListOffsetsRequest()
		req.ReplicaID = -1
		req.IsolationLevel = 1
		p := kmsg.NewListOffsetsRequestTopicPartition()
		p.Partition = partition
		p.Timestamp = timestamp
		req.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: source.Topic, Partitions: []kmsg.ListOffsetsRequestTopicPartition{p}}}
		out, err := req.RequestWith(ctx, b.Client)
		if err != nil || len(out.Topics) != 1 || out.Topics[0].Topic != source.Topic || len(out.Topics[0].Partitions) != 1 {
			return 0, ErrBroker
		}
		v := out.Topics[0].Partitions[0]
		if v.Partition != partition || v.ErrorCode != 0 || v.Offset < 0 {
			return 0, ErrBroker
		}
		return v.Offset, nil
	}
	start, err := offset(-2)
	if err != nil {
		return 0, 0, err
	}
	end, err := offset(-1)
	if err != nil || end < start {
		return 0, 0, ErrBroker
	}
	return start, end, nil
}
func (b KafkaBroker) Delete(parent context.Context, source kafkasource.Identity, partition int32, target int64) (int64, error) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	if b.Client == nil || target < 0 {
		return 0, ErrDelete
	}
	// This check cannot fence an administrator recreating a topic by name.
	// Topic lifecycle exclusion is an explicit operational precondition (ADR).
	parts, err := source.Verify(ctx, b.Client)
	if err != nil {
		return 0, ErrDelete
	}
	found := false
	for _, p := range parts {
		if p.Partition == partition {
			found = true
		}
	}
	if !found {
		return 0, ErrDelete
	}
	req := kmsg.NewPtrDeleteRecordsRequest()
	req.TimeoutMillis = 3000
	req.Topics = []kmsg.DeleteRecordsRequestTopic{{Topic: source.Topic, Partitions: []kmsg.DeleteRecordsRequestTopicPartition{{Partition: partition, Offset: target}}}}
	out, err := req.RequestWith(ctx, b.Client)
	if err != nil || len(out.Topics) != 1 || out.Topics[0].Topic != source.Topic || len(out.Topics[0].Partitions) != 1 {
		return 0, ErrDelete
	}
	v := out.Topics[0].Partitions[0]
	if v.Partition != partition {
		return 0, ErrDelete
	}
	if v.ErrorCode != 0 {
		return 0, &BrokerRejection{Code: v.ErrorCode}
	}
	if v.LowWatermark < 0 {
		return 0, ErrDelete
	}
	return v.LowWatermark, nil
}
