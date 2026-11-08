package intake

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

// Narrow metadata/fetch wire fixture, following the existing changefeed fixture.
// Direct assignment isolates Step from group negotiation. The first Verify and
// post-poll Verify see the expected ID; internal lookup/fetch see a replacement.
type generationWire struct {
	idle         bool
	failAfter    int
	mu           sync.Mutex
	metadata     int
	control      bool
	metadataMode string
	conns        []net.Conn
	wg           sync.WaitGroup
	closed       bool
}

var wireIdentity = kafkasource.Identity{ClusterID: "cluster", Topic: "delivery", TopicID: [16]byte{1}}

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
				v.ApiKeys = []kmsg.ApiVersionsResponseApiKey{{ApiKey: 18, MinVersion: 0, MaxVersion: 3}, {ApiKey: 3, MinVersion: 0, MaxVersion: 12}, {ApiKey: 2, MinVersion: 0, MaxVersion: 7}, {ApiKey: 1, MinVersion: 0, MaxVersion: 13}}
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
				if w.metadataMode == "" && !w.idle && n == 2 {
					id = [16]byte{2}
				}
				v := kmsg.NewPtrMetadataResponse()
				cluster := "cluster"
				topic := "delivery"
				v.ClusterID = &cluster
				v.ControllerID = 0
				v.Brokers = []kmsg.MetadataResponseBroker{{NodeID: 0, Host: "localhost", Port: 9092}}
				p := kmsg.NewMetadataResponseTopicPartition()
				p.Partition = 0
				p.Leader = 0
				p.Replicas = []int32{0}
				p.ISR = []int32{0}
				v.Topics = []kmsg.MetadataResponseTopic{{Topic: &topic, TopicID: id, Partitions: []kmsg.MetadataResponseTopicPartition{p}}}
				mode := w.metadataMode
				if w.failAfter > 0 && n >= w.failAfter {
					mode = "error"
				}
				switch mode {
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
				resp = v
			case 2:
				v := kmsg.NewPtrListOffsetsResponse()
				p := kmsg.NewListOffsetsResponseTopicPartition()
				p.Partition = 0
				p.Offset = 0
				p.LeaderEpoch = 0
				v.Topics = []kmsg.ListOffsetsResponseTopic{{Topic: "delivery", Partitions: []kmsg.ListOffsetsResponseTopicPartition{p}}}
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
				if w.idle {
					p.RecordBatches = nil
					p.HighWatermark = 0
					p.LastStableOffset = 0
				}
				id := [16]byte{2}
				if w.idle {
					id = wireIdentity.TopicID
				}
				v.Topics = []kmsg.FetchResponseTopic{{TopicID: id, Partitions: []kmsg.FetchResponseTopicPartition{p}}}
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

func wireConsumer(t *testing.T, w *generationWire) *Consumer {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cl, e := kgo.NewClient(kgo.WithContext(ctx), kgo.SeedBrokers("localhost:9092"), kgo.Dialer(w.dial), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{"delivery": {0: kgo.NewOffset().At(0)}}), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.KeepControlRecords(), kgo.FetchMaxWait(50*time.Millisecond), kgo.RequestRetries(0))
	if e != nil {
		cancel()
		t.Fatal(e)
	}
	c := &Consumer{client: cl, cancel: cancel, cfg: Config{Topic: "delivery", Binding: Binding{ConsumerID: "logical", Generation: "bootstrap", Source: wireIdentity}}, cursors: map[int32]int64{0: 0}}
	c.ack = func(context.Context, *kgo.Record) error { t.Error("unexpected ack"); return ErrAck }
	t.Cleanup(c.Close)
	return c
}
func TestWireFetchBeforeSQLDataAndControl(t *testing.T) {
	for _, control := range []bool{false, true} {
		t.Run(map[bool]string{false: "data", true: "control"}[control], func(t *testing.T) {
			w := &generationWire{control: control}
			defer w.close()
			c := wireConsumer(t, w)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if e := c.Step(ctx); !errors.Is(e, kafkasource.ErrMismatch) {
				t.Fatal(e)
			}
			if c.cursors[0] != 0 {
				t.Fatal("foreign cursor advanced")
			}
			if e := c.Step(ctx); !errors.Is(e, kafkasource.ErrMismatch) {
				t.Fatal("retired consumer resumed", e)
			}
		})
	}
}
func TestWireMetadataFailureAndIdle(t *testing.T) {
	for _, mode := range []string{"empty", "cluster", "zero", "error", "cancel", "idle"} {
		t.Run(mode, func(t *testing.T) {
			w := &generationWire{metadataMode: mode}
			if mode == "idle" {
				w.metadataMode = ""
				w.idle = true
				w.failAfter = 3
			}
			defer w.close()
			c := wireConsumer(t, w)
			limit := 5 * time.Second
			if mode == "cancel" {
				limit = 20 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), limit)
			defer cancel()
			e := c.Step(ctx)
			if e == nil {
				t.Fatal("metadata failure accepted")
			}
			if mode == "cancel" && !errors.Is(e, context.DeadlineExceeded) {
				t.Fatal(e)
			}
			if c.cursors[0] != 0 {
				t.Fatal("cursor advanced")
			}
			if mode == "idle" {
				w.mu.Lock()
				n := w.metadata
				w.mu.Unlock()
				if n < 3 {
					t.Fatal("no idle verification", n)
				}
			}
		})
	}
}
