package kafkafault

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

func TestIntegrationOffsetCommitDeleteRecords(t *testing.T) {
	if os.Getenv("KAFKA_INTEGRATION") != "1" {
		t.Skip("real Kafka: bash infra/kafka/verify-go.sh")
	}
	broker, prefix := os.Getenv("KAFKA_TEST_BROKER"), os.Getenv("KAFKA_TEST_PREFIX")
	if broker == "" || prefix == "" {
		t.Fatal("opt-in requires broker and prefix")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	p := New(broker)
	defer p.Close()
	client := func(proxy bool, extra ...kgo.Opt) *kgo.Client {
		opts := []kgo.Opt{kgo.SeedBrokers(broker), kgo.MaxVersions(kversion.V2_8_0()), kgo.RequestTimeoutOverhead(time.Second)}
		if proxy {
			opts = append(opts, kgo.Dialer(p.DialContext))
		}
		opts = append(opts, extra...)
		c, e := kgo.NewClient(opts...)
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(c.Close)
		return c
	}
	direct, c := client(false), client(true)
	if err := direct.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	topic := prefix + "-fault-boundary"
	create := kmsg.NewPtrCreateTopicsRequest()
	create.TimeoutMillis = 5000
	create.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}}
	created, err := create.RequestWith(ctx, direct)
	if err != nil || len(created.Topics) != 1 || created.Topics[0].ErrorCode != 0 {
		t.Fatalf("create isolated topic: %+v %v", created, err)
	}
	records := []*kgo.Record{{Topic: topic, Partition: 0, Key: []byte("prefix"), Value: []byte("processed")}, {Topic: topic, Partition: 0, Key: []byte("retained-key"), Value: []byte{0, 1, 255, 2}, Headers: []kgo.RecordHeader{{Key: "identity", Value: []byte("canonical")}}}}
	if e := direct.ProduceSync(ctx, records...).FirstErr(); e != nil {
		t.Fatal(e)
	}
	boundary := records[1].Offset
	group := prefix + "-fault-group"
	commit := kmsg.NewPtrOffsetCommitRequest()
	commit.Group = group
	commit.Topics = []kmsg.OffsetCommitRequestTopic{{Topic: topic, Partitions: []kmsg.OffsetCommitRequestTopicPartition{{Partition: 0, Offset: boundary, LeaderEpoch: -1}}}}
	offset := func() int64 {
		r := kmsg.NewPtrOffsetFetchRequest()
		r.Group = group
		r.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: topic, Partitions: []int32{0}}}
		v, e := r.RequestWith(ctx, direct)
		if e != nil {
			t.Fatal(e)
		}
		if v.ErrorCode != 0 || len(v.Topics) != 1 || len(v.Topics[0].Partitions) != 1 || v.Topics[0].Partitions[0].ErrorCode != 0 {
			t.Fatalf("offset fetch: %+v", v)
		}
		return v.Topics[0].Partitions[0].Offset
	}
	start := func() int64 {
		r := kmsg.NewPtrListOffsetsRequest()
		r.ReplicaID = -1
		r.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: topic, Partitions: []kmsg.ListOffsetsRequestTopicPartition{{Partition: 0, Timestamp: -2, CurrentLeaderEpoch: -1}}}}
		v, e := r.RequestWith(ctx, direct)
		if e != nil {
			t.Fatal(e)
		}
		if len(v.Topics) != 1 || len(v.Topics[0].Partitions) != 1 || v.Topics[0].Partitions[0].ErrorCode != 0 {
			t.Fatalf("list offsets: %+v", v)
		}
		return v.Topics[0].Partitions[0].Offset
	}
	event := func(f *Fault) Event {
		select {
		case e := <-f.Events:
			return e
		case <-ctx.Done():
			t.Fatal(ctx.Err())
			return Event{}
		}
	}
	run := func(r kmsg.Request) (<-chan error, context.CancelFunc) {
		call, stop := context.WithTimeout(ctx, 2*time.Second)
		done := make(chan error, 1)
		go func() { _, e := c.Request(call, r); done <- e }()
		return done, stop
	}
	failed := func(done <-chan error, stop context.CancelFunc) {
		t.Helper()
		defer stop()
		select {
		case e := <-done:
			if e == nil {
				t.Fatal("caller received success during fault")
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	f, e := p.Intercept(ctx, 8, AfterResponse)
	if e != nil {
		t.Fatal(e)
	}
	done, stop := run(commit)
	ev := event(f)
	resp := ev.Response.(*kmsg.OffsetCommitResponse)
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 || resp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("commit result: %+v", resp)
	}
	if offset() != boundary {
		t.Fatal("broker did not commit")
	}
	f.Fail()
	failed(done, stop)
	// A second operation while still armed must also lose its actual success.
	done, stop = run(commit)
	event(f)
	failed(done, stop)
	f.Release()
	f.Release()
	if _, e := commit.RequestWith(ctx, c); e != nil {
		t.Fatal(e)
	}
	initial := start()
	del := kmsg.NewPtrDeleteRecordsRequest()
	del.TimeoutMillis = 1000
	del.Topics = []kmsg.DeleteRecordsRequestTopic{{Topic: topic, Partitions: []kmsg.DeleteRecordsRequestTopicPartition{{Partition: 0, Offset: boundary}}}}
	f, e = p.Intercept(ctx, 21, BeforeRequest)
	if e != nil {
		t.Fatal(e)
	}
	done, stop = run(del)
	ev = event(f)
	if ev.Response != nil {
		t.Fatal("before request has response")
	}
	if start() != initial {
		t.Fatal("delete escaped gate")
	}
	f.Fail()
	failed(done, stop)
	if start() != initial {
		t.Fatal("rejected deletion changed start")
	}
	f.Release()
	f, e = p.Intercept(ctx, 21, AfterResponse)
	if e != nil {
		t.Fatal(e)
	}
	done, stop = run(del)
	ev = event(f)
	dr := ev.Response.(*kmsg.DeleteRecordsResponse)
	if len(dr.Topics) != 1 || len(dr.Topics[0].Partitions) != 1 || dr.Topics[0].Partitions[0].ErrorCode != 0 || dr.Topics[0].Partitions[0].LowWatermark != boundary {
		t.Fatalf("delete result: %+v", dr)
	}
	if start() != boundary {
		t.Fatal("lost response did not advance log-start")
	}
	f.Fail()
	failed(done, stop)
	f.Release()
	dr, e = del.RequestWith(ctx, c)
	if e != nil || dr.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("retry: %+v %v", dr, e)
	}
	if start() != boundary || offset() != boundary {
		t.Fatal("retry changed boundary")
	}
	reader := client(false, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().At(boundary)}}), kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	fs := reader.PollFetches(ctx)
	if len(fs.Errors()) > 0 {
		t.Fatal(fs.Errors())
	}
	got := fs.Records()
	if len(got) != 1 || got[0].Offset != boundary || !bytes.Equal(got[0].Key, records[1].Key) || !bytes.Equal(got[0].Value, records[1].Value) || len(got[0].Headers) != 1 || got[0].Headers[0].Key != "identity" || !bytes.Equal(got[0].Headers[0].Value, records[1].Headers[0].Value) {
		t.Fatalf("retained canonical bytes: %+v", got)
	}
	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	t.Log("actual OffsetCommit success lost; pre-delete fault preserves start; actual DeleteRecords success lost; same-boundary retry preserves read_committed key/value/header bytes")
}
