package changefeed

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// A narrow wire fixture for the registration/fetch boundary, using Config.Dialer.
// Explicit Verify requests see the expected generation. The client's later
// metadata lookup and first fetch see a different incarnation with offset zero.
// No group/producer/fault framework is implemented here.
type generationWire struct {
	mu               sync.Mutex
	metadata         int
	control          bool
	metadataMode     string
	metadataResponse func(int, *kmsg.MetadataResponse)
	conns            []net.Conn
	wg               sync.WaitGroup
	closed           bool
}

var wireIdentity = kafkasource.Identity{ClusterID: "cluster", Topic: "changes", TopicID: [16]byte{1}}

func (w *generationWire) close() {
	w.mu.Lock()
	w.closed = true
	for _, c := range w.conns {
		c.Close()
	}
	w.mu.Unlock()
	w.wg.Wait()
}
func wireFrame(r io.Reader) ([]byte, error) {
	var h [4]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(h[:])
	if n > 1<<20 {
		return nil, io.ErrShortBuffer
	}
	b := make([]byte, n)
	_, err := io.ReadFull(r, b)
	return b, err
}
func (w *generationWire) dial(context.Context, string, string) (net.Conn, error) {
	a, b := net.Pipe()
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		a.Close()
		b.Close()
		return nil, net.ErrClosed
	}
	w.conns = append(w.conns, b)
	w.wg.Add(1)
	w.mu.Unlock()
	go func() {
		defer w.wg.Done()
		defer b.Close()
		for {
			req, err := wireFrame(b)
			if err != nil || len(req) < 10 {
				return
			}
			key := int16(binary.BigEndian.Uint16(req))
			version := int16(binary.BigEndian.Uint16(req[2:]))
			correlation := req[4:8]
			var resp kmsg.Response
			switch key {
			case 18:
				v := kmsg.NewPtrApiVersionsResponse()
				v.ApiKeys = []kmsg.ApiVersionsResponseApiKey{{ApiKey: 18, MinVersion: 0, MaxVersion: 3}, {ApiKey: 19, MinVersion: 0, MaxVersion: 7}, {ApiKey: 3, MinVersion: 0, MaxVersion: 12}, {ApiKey: 2, MinVersion: 0, MaxVersion: 7}, {ApiKey: 1, MinVersion: 0, MaxVersion: 13}}
				resp = v
			case 19:
				v := kmsg.NewPtrCreateTopicsResponse()
				v.Topics = []kmsg.CreateTopicsResponseTopic{{Topic: wireIdentity.Topic, TopicID: [16]byte{2}}}
				resp = v
			case 3:
				off := 10 + int(int16(binary.BigEndian.Uint16(req[8:10])))
				if version >= 9 {
					off++
				}
				r := kmsg.NewPtrMetadataRequest()
				r.SetVersion(version)
				if err := r.ReadFrom(req[off:]); err != nil || r.AllowAutoTopicCreation {
					return
				}
				w.mu.Lock()
				w.metadata++
				n := w.metadata
				w.mu.Unlock()
				id := wireIdentity.TopicID
				if w.metadataMode == "" && n >= 3 {
					id = [16]byte{2}
				}
				v := kmsg.NewPtrMetadataResponse()
				cluster := "cluster"
				topic := "changes"
				v.ClusterID = &cluster
				v.ControllerID = 0
				v.Brokers = []kmsg.MetadataResponseBroker{{NodeID: 0, Host: "localhost", Port: 9092}}
				p := kmsg.NewMetadataResponseTopicPartition()
				p.Partition = 0
				p.Leader = 0
				p.Replicas = []int32{0}
				p.ISR = []int32{0}
				v.Topics = []kmsg.MetadataResponseTopic{{Topic: &topic, TopicID: id, Partitions: []kmsg.MetadataResponseTopicPartition{p}}}
				switch w.metadataMode {
				case "empty":
					v.Topics = nil
				case "cluster":
					v.ClusterID = nil
				case "zero":
					v.Topics[0].TopicID = [16]byte{}
				case "error":
					v.Topics[0].ErrorCode = 29
				case "cancel":
					<-time.After(100 * time.Millisecond)
					return
				}
				if w.metadataResponse != nil {
					w.metadataResponse(n, v)
				}
				resp = v
			case 2:
				v := kmsg.NewPtrListOffsetsResponse()
				p := kmsg.NewListOffsetsResponseTopicPartition()
				p.Partition = 0
				p.Offset = 0
				p.LeaderEpoch = 0
				v.Topics = []kmsg.ListOffsetsResponseTopic{{Topic: "changes", Partitions: []kmsg.ListOffsetsResponseTopicPartition{p}}}
				resp = v
			case 1:
				v := kmsg.NewPtrFetchResponse()
				p := kmsg.NewFetchResponseTopicPartition()
				p.Partition = 0
				p.HighWatermark = 1
				p.LastStableOffset = 1
				p.LogStartOffset = 0
				r := kmsg.Record{Key: []byte("invalid-data"), Value: []byte(`{}`)}
				attrs := int16(32)
				if w.control {
					attrs |= 64
					r.Key = []byte{0, 0, 0, 1}
					r.Value = []byte{0, 0, 0, 0, 0, 0}
				}
				r.Length = int32(len(r.AppendTo(nil)) - 1)
				batch := kmsg.RecordBatch{Magic: 2, Attributes: attrs, ProducerID: 7, NumRecords: 1, Records: r.AppendTo(nil)}
				raw := batch.AppendTo(nil)
				binary.BigEndian.PutUint32(raw[8:12], uint32(len(raw)-12))
				binary.BigEndian.PutUint32(raw[17:21], crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli)))
				p.RecordBatches = raw
				v.Topics = []kmsg.FetchResponseTopic{{TopicID: [16]byte{2}, Partitions: []kmsg.FetchResponseTopicPartition{p}}}
				resp = v
			default:
				return
			}
			resp.SetVersion(version)
			out := append([]byte(nil), correlation...)
			if resp.IsFlexible() && key != 18 {
				out = append(out, 0)
			}
			out = resp.AppendTo(out)
			h := make([]byte, 4)
			binary.BigEndian.PutUint32(h, uint32(len(out)))
			if _, err := b.Write(append(h, out...)); err != nil {
				return
			}
		}
	}()
	return a, nil
}
func TestFetchGenerationBeforeDataAndControlDispatch(t *testing.T) {
	for _, control := range []bool{false, true} {
		t.Run(map[bool]string{false: "data", true: "commit-marker"}[control], func(t *testing.T) {
			w := &generationWire{control: control}
			defer w.close()
			cfg := Config{Brokers: []string{"localhost:9092"}, Topic: "changes", Source: wireIdentity, Dialer: w.dial}
			s := source(t, cfg)
			defer s.Close()
			r, err := s.Register(bounded(t, 5*time.Second))
			if err != nil {
				t.Fatal("register", err)
			}
			select {
			case <-r.Done():
			case <-bounded(t, 5*time.Second).Done():
				t.Fatal("fetch generation not rejected")
			}
			if !errors.Is(r.Err(), kafkasource.ErrMismatch) {
				t.Fatal(r.Err())
			}
			r.mu.Lock()
			last := r.lastOffset
			r.mu.Unlock()
			if last != -1 {
				t.Fatal("foreign record reached accept", last)
			}
		})
	}
}
func TestMetadataIdentityFailuresAndCancellation(t *testing.T) {
	for _, mode := range []string{"empty", "cluster", "zero", "error", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			w := &generationWire{metadataMode: mode}
			defer w.close()
			s := source(t, Config{Brokers: []string{"localhost:9092"}, Topic: "changes", Source: wireIdentity, Dialer: w.dial})
			defer s.Close()
			ctx := bounded(t, 3*time.Second)
			var cancel context.CancelFunc
			if mode == "cancel" {
				ctx, cancel = context.WithTimeout(ctx, 20*time.Millisecond)
				defer cancel()
			}
			if _, err := s.Register(ctx); err == nil {
				t.Fatal("unverified metadata accepted")
			} else if mode == "cancel" && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
		})
	}
}

// Exercise fixture readiness through the existing Kafka wire boundary. These
// retries provision a new test source; they never retry a rejected reader.
func TestNewGenerationReadiness(t *testing.T) {
	for _, mode := range []string{"missing", "old", "zero", "partition-error", "no-leader", "no-isr", "ready"} {
		t.Run(mode, func(t *testing.T) {
			w := &generationWire{metadataMode: "stable"}
			w.metadataResponse = func(n int, v *kmsg.MetadataResponse) {
				v.Topics[0].TopicID = [16]byte{2}
				if n > 1 {
					return
				}
				switch mode {
				case "missing":
					v.Topics[0].ErrorCode = 3
					v.Topics[0].Partitions = nil
				case "old":
					v.Topics[0].TopicID = wireIdentity.TopicID
				case "zero":
					v.Topics[0].TopicID = [16]byte{}
				case "partition-error":
					v.Topics[0].Partitions[0].ErrorCode = 5
				case "no-leader":
					v.Topics[0].Partitions[0].Leader = -1
				case "no-isr":
					v.Topics[0].Partitions[0].ISR = nil
				}
			}
			defer w.close()
			c, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"), kgo.Dialer(w.dial))
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			generationTopic(t, c, wireIdentity.Topic, true)
			// CreateTopics locates the controller with its own metadata request.
			w.mu.Lock()
			w.metadata = 0
			w.mu.Unlock()
			id, err := newGeneration(bounded(t, time.Second), c, wireIdentity.Topic, wireIdentity)
			if err != nil || id.TopicID != ([16]byte{2}) {
				t.Fatal(id, err)
			}
			w.mu.Lock()
			n := w.metadata
			w.mu.Unlock()
			if mode != "ready" && n < 2 {
				t.Fatal("unready response accepted", mode, n)
			}
		})
	}
}
func TestNewGenerationReadinessBounded(t *testing.T) {
	w := &generationWire{metadataMode: "stable"}
	defer w.close()
	c, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"), kgo.Dialer(w.dial))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	id, err := newGeneration(ctx, c, wireIdentity.Topic, wireIdentity)
	if !errors.Is(err, context.DeadlineExceeded) || id != (kafkasource.Identity{}) {
		t.Fatal("old generation accepted or deadline lost", id, err)
	}
}

func TestCreatedTopicMetadataMayBeUnavailable(t *testing.T) {
	w := &generationWire{metadataMode: "stable"}
	w.metadataResponse = func(n int, v *kmsg.MetadataResponse) {
		v.Topics[0].TopicID = [16]byte{2}
		if n == 1 {
			v.Topics[0].ErrorCode = 3 // UNKNOWN_TOPIC_OR_PARTITION after successful create.
			v.Topics[0].Partitions = nil
		}
	}
	defer w.close()
	c, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"), kgo.Dialer(w.dial))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	generationTopic(t, c, wireIdentity.Topic, true)
	// CreateTopics locates the controller with its own metadata request.
	w.mu.Lock()
	w.metadata = 0
	w.mu.Unlock()
	id, _, err := kafkasource.Metadata(bounded(t, time.Second), c, wireIdentity.Topic)
	if !errors.Is(err, kafkasource.ErrMetadata) || id != (kafkasource.Identity{}) {
		t.Fatal(id, err)
	}
	id, err = newGeneration(bounded(t, time.Second), c, wireIdentity.Topic, wireIdentity)
	if err != nil || id.TopicID != ([16]byte{2}) {
		t.Fatal(id, err)
	}
}
