package cleanerprocess

import (
	"context"
	"errors"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// LogStart uses an independent direct client, never the faulted caller.
func LogStart(ctx context.Context, broker, topic string, partition int32) (int64, error) {
	c, e := kgo.NewClient(kgo.SeedBrokers(broker))
	if e != nil {
		return 0, e
	}
	defer c.Close()
	q := kmsg.NewPtrListOffsetsRequest()
	q.ReplicaID = -1
	q.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: topic, Partitions: []kmsg.ListOffsetsRequestTopicPartition{{Partition: partition, Timestamp: -2, CurrentLeaderEpoch: -1}}}}
	r, e := q.RequestWith(ctx, c)
	if e != nil {
		return 0, e
	}
	if len(r.Topics) != 1 || len(r.Topics[0].Partitions) != 1 || r.Topics[0].Partitions[0].ErrorCode != 0 {
		return 0, errors.New("log-start unavailable")
	}
	return r.Topics[0].Partitions[0].Offset, nil
}

// ReadCommitted observes exactly count data records from an absolute position.
// Caller supplies a finite context; insufficient retained records are an error.
func ReadCommitted(ctx context.Context, broker, topic string, partition int32, from int64, count int) ([]*kgo.Record, error) {
	if from < 0 || count < 1 || count > 1024 {
		return nil, errors.New("invalid observer bounds")
	}
	c, e := kgo.NewClient(kgo.SeedBrokers(broker), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {partition: kgo.NewOffset().At(from)}}), kgo.FetchMaxBytes(1<<20))
	if e != nil {
		return nil, e
	}
	defer c.Close()
	var out []*kgo.Record
	for len(out) < count {
		fs := c.PollRecords(ctx, count-len(out))
		if e := fs.Err(); e != nil {
			return nil, e
		}
		out = append(out, fs.Records()...)
	}
	return out, nil
}
