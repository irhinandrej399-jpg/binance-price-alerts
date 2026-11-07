// Package kafkasource verifies the broker incarnation of a Kafka topic.
package kafkasource

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var (
	ErrIdentity = errors.New("kafka source: missing or unsupported identity")
	ErrMismatch = errors.New("kafka source: identity mismatch")
	ErrMetadata = errors.New("kafka source: metadata unavailable")
)

// Identity is a broker incarnation, not a consumer identity. Partition and
// offset belong to a position separately. Storage clones retaining both IDs
// cannot be distinguished. A zero identity never disables verification.
type Identity struct {
	ClusterID string
	TopicID   [16]byte
	Topic     string
}

func (id Identity) Validate() error {
	if strings.TrimSpace(id.ClusterID) == "" || strings.TrimSpace(id.Topic) == "" || id.TopicID == ([16]byte{}) {
		return ErrIdentity
	}
	return nil
}

// Metadata explicitly reads one named topic without auto-creation, with a
// three-second bound. Discovery is for provisioning/new sources: it must never
// be used to guess the origin of an existing checkpoint.
func Metadata(parent context.Context, client *kgo.Client, topic string) (Identity, []kmsg.MetadataResponseTopicPartition, error) {
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	req := kmsg.NewPtrMetadataRequest()
	req.AllowAutoTopicCreation = false
	t := kmsg.NewMetadataRequestTopic()
	t.Topic = &topic
	req.Topics = []kmsg.MetadataRequestTopic{t}
	resp, err := req.RequestWith(ctx, client)
	if err != nil {
		return Identity{}, nil, errors.Join(ErrMetadata, ctx.Err())
	}
	if resp.ClusterID == nil || len(resp.Topics) != 1 {
		return Identity{}, nil, ErrMetadata
	}
	v := resp.Topics[0]
	if v.ErrorCode != 0 || v.Topic == nil || *v.Topic != topic || len(v.Partitions) == 0 {
		return Identity{}, nil, ErrMetadata
	}
	id := Identity{ClusterID: *resp.ClusterID, TopicID: v.TopicID, Topic: topic}
	if err := id.Validate(); err != nil {
		return Identity{}, nil, err
	}
	for _, p := range v.Partitions {
		if p.ErrorCode != 0 {
			return Identity{}, nil, ErrMetadata
		}
	}
	return id, v.Partitions, nil
}
func (id Identity) Verify(ctx context.Context, client *kgo.Client) ([]kmsg.MetadataResponseTopicPartition, error) {
	if err := id.Validate(); err != nil {
		return nil, err
	}
	actual, parts, err := Metadata(ctx, client, id.Topic)
	if err != nil {
		return nil, err
	}
	if actual != id {
		return nil, ErrMismatch
	}
	return parts, nil
}

// CheckFetches must precede Records/EachRecord and decoding, including control
// records. Walk the original responses rather than EachTopic's merged view.
func (id Identity) CheckFetches(fs kgo.Fetches) error {
	if err := id.Validate(); err != nil {
		return err
	}
	for _, f := range fs {
		for _, t := range f.Topics {
			// PollRecords synthesizes this empty topic when the poll context expires.
			if t.Topic == "" && len(t.Partitions) == 1 && t.Partitions[0].Partition == -1 && len(t.Partitions[0].Records) == 0 && errors.Is(t.Partitions[0].Err, context.DeadlineExceeded) {
				continue
			}
			if t.TopicID == ([16]byte{}) {
				return ErrIdentity
			}
			if t.Topic != id.Topic || t.TopicID != id.TopicID {
				return ErrMismatch
			}
		}
	}
	return nil
}
