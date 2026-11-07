package kafkasource

import (
	"context"
	"errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"testing"
)

func TestIdentityAndFetch(t *testing.T) {
	id := Identity{ClusterID: "cluster", Topic: "prices", TopicID: [16]byte{1}}
	if err := id.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []Identity{{}, {ClusterID: "cluster", Topic: "prices"}, {Topic: "prices", TopicID: id.TopicID}, {ClusterID: "cluster", TopicID: id.TopicID}} {
		if !errors.Is(bad.Validate(), ErrIdentity) {
			t.Fatal(bad)
		}
	}
	for _, control := range []bool{false, true} {
		r := &kgo.Record{Topic: "prices", Attrs: kgo.NewRecordAttrs(kgo.RecordAttrsOpts{Control: control})}
		fs := kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: id.Topic, TopicID: id.TopicID, Partitions: []kgo.FetchPartition{{Records: []*kgo.Record{r}}}}}}}
		if err := id.CheckFetches(fs); err != nil {
			t.Fatal(err)
		}
		fs[0].Topics[0].TopicID = [16]byte{2}
		if !errors.Is(id.CheckFetches(fs), ErrMismatch) {
			t.Fatal("foreign data/control fetch accepted")
		}
		fs[0].Topics[0].TopicID = [16]byte{}
		if !errors.Is(id.CheckFetches(fs), ErrIdentity) {
			t.Fatal("zero fetch ID accepted")
		}
	}
}

func TestFetchesValidateEveryResponseBeforeDispatch(t *testing.T) {
	id := Identity{ClusterID: "cluster", Topic: "prices", TopicID: [16]byte{1}}
	fs := kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: id.Topic, TopicID: id.TopicID}}}, {Topics: []kgo.FetchTopic{{Topic: id.Topic, TopicID: [16]byte{2}}}}}
	if !errors.Is(id.CheckFetches(fs), ErrMismatch) {
		t.Fatal("later broker response was missed")
	}
	fs[1].Topics[0].TopicID = id.TopicID
	fs[1].Topics[0].Topic = "other"
	if !errors.Is(id.CheckFetches(fs), ErrMismatch) {
		t.Fatal("foreign name accepted")
	}
	if err := id.CheckFetches(kgo.NewErrFetch(context.DeadlineExceeded)); err != nil {
		t.Fatal("poll wait treated as broker generation", err)
	}
}
