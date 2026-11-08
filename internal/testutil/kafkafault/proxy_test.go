package kafkafault

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestBoundaries(t *testing.T) {
	for _, version := range []int16{0, 2} {
		for _, boundary := range []Boundary{BeforeRequest, AfterResponse} {
			t.Run(string(rune('0'+version))+string(boundary), func(t *testing.T) {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				broker := make(chan bool, 1)
				go func() {
					c, e := listener.Accept()
					if e != nil {
						return
					}
					defer c.Close()
					b, e := readFrame(c)
					if e != nil {
						return
					}
					broker <- true
					r := kmsg.NewPtrDeleteRecordsResponse()
					r.SetVersion(version)
					r.Topics = []kmsg.DeleteRecordsResponseTopic{{Topic: "t", Partitions: []kmsg.DeleteRecordsResponseTopicPartition{{Partition: 0, LowWatermark: 7, ErrorCode: 3}}}}
					response := binary.BigEndian.AppendUint32(nil, binary.BigEndian.Uint32(b[4:]))
					if r.IsFlexible() {
						response = append(response, 0)
					}
					writeFrame(c, r.AppendTo(response))
				}()
				p := New(listener.Addr().String())
				defer p.Close()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				f, err := p.Intercept(ctx, 21, boundary)
				if err != nil {
					t.Fatal(err)
				}
				c, err := p.DialContext(ctx, "tcp", "")
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				c.SetDeadline(time.Now().Add(time.Second * 2))
				r := kmsg.NewPtrDeleteRecordsRequest()
				r.SetVersion(version)
				r.Topics = []kmsg.DeleteRecordsRequestTopic{{Topic: "t", Partitions: []kmsg.DeleteRecordsRequestTopicPartition{{Partition: 0, Offset: 7}}}}
				b := binary.BigEndian.AppendUint16(nil, 21)
				b = binary.BigEndian.AppendUint16(b, uint16(version))
				b = binary.BigEndian.AppendUint32(b, 42)
				b = append(b, 255, 255)
				if r.IsFlexible() {
					b = append(b, 0)
				}
				b = r.AppendTo(b)
				if err := writeFrame(c, b); err != nil {
					t.Fatal(err)
				}
				select {
				case e := <-f.Events:
					if e.CorrelationID != 42 || e.Request.(*kmsg.DeleteRecordsRequest).Topics[0].Partitions[0].Offset != 7 {
						t.Fatal("wrong request")
					}
					if boundary == AfterResponse {
						if e.Response.(*kmsg.DeleteRecordsResponse).Topics[0].Partitions[0].ErrorCode != 3 {
							t.Fatal("lost real error")
						}
					} else {
						select {
						case <-broker:
							t.Fatal("request escaped gate")
						default:
						}
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				f.Fail()
				f.Fail()
				if _, err := readFrame(c); err == nil {
					t.Fatal("fault delivered response")
				}
				f.Release()
				f.Release()
			})
		}
	}
}

func requestFrame(r kmsg.Request, id uint32) []byte {
	b := binary.BigEndian.AppendUint16(nil, uint16(r.Key()))
	b = binary.BigEndian.AppendUint16(b, uint16(r.GetVersion()))
	b = binary.BigEndian.AppendUint32(b, id)
	b = append(b, 0, 3, 'c', 'u', 'a')
	if r.IsFlexible() {
		b = append(b, 1, 7, 2, 0, 1)
	} // Nonempty flexible header tags.
	return r.AppendTo(b)
}

func TestParallelGateLifecycle(t *testing.T) {
	for _, action := range []string{"cancel", "deadline", "close", "release"} {
		t.Run(action, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			var brokerWG sync.WaitGroup
			brokerWG.Add(1)
			go func() {
				defer brokerWG.Done()
				for {
					c, e := listener.Accept()
					if e != nil {
						return
					}
					brokerWG.Add(1)
					go func() {
						defer brokerWG.Done()
						defer c.Close()
						b, e := readFrame(c)
						if e != nil {
							return
						}
						r := kmsg.NewPtrDeleteRecordsResponse()
						writeFrame(c, r.AppendTo(binary.BigEndian.AppendUint32(nil, binary.BigEndian.Uint32(b[4:]))))
					}()
				}
			}()
			p := New(listener.Addr().String())
			defer func() { p.Close(); listener.Close(); brokerWG.Wait() }()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			gateCtx, gateCancel := context.WithCancel(ctx)
			defer gateCancel()
			if action == "deadline" {
				gateCtx, gateCancel = context.WithTimeout(ctx, 300*time.Millisecond)
				defer gateCancel()
			}
			f, err := p.Intercept(gateCtx, 21, BeforeRequest)
			if err != nil {
				t.Fatal(err)
			}
			results := make(chan error, 8)
			for i := 0; i < 8; i++ {
				c, e := p.DialContext(ctx, "tcp", "")
				if e != nil {
					t.Fatal(e)
				}
				go func(c net.Conn, id uint32) {
					defer c.Close()
					c.SetDeadline(time.Now().Add(2 * time.Second))
					e := writeFrame(c, requestFrame(kmsg.NewPtrDeleteRecordsRequest(), id))
					if e == nil {
						_, e = readFrame(c)
					}
					results <- e
				}(c, uint32(i))
			}
			for i := 0; i < 8; i++ {
				select {
				case <-f.Events:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			switch action {
			case "cancel":
				gateCancel()
			case "deadline":
			case "close":
				p.Close()
				p.Close()
			case "release":
				f.Release()
				f.Release()
			}
			for i := 0; i < 8; i++ {
				select {
				case e := <-results:
					if (action == "release") != (e == nil) {
						t.Fatalf("%s: %v", action, e)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			f.Release()
			f.Release()
		})
	}
}

func TestOffsetCommitHeadersAndPartitionErrors(t *testing.T) {
	for _, version := range []int16{0, 8, 9, 10} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			listener, e := net.Listen("tcp", "127.0.0.1:0")
			if e != nil {
				t.Fatal(e)
			}
			defer listener.Close()
			go func() {
				c, e := listener.Accept()
				if e != nil {
					return
				}
				defer c.Close()
				b, e := readFrame(c)
				if e != nil {
					return
				}
				r := kmsg.NewPtrOffsetCommitResponse()
				r.SetVersion(version)
				r.Topics = []kmsg.OffsetCommitResponseTopic{{Topic: "a", TopicID: [16]byte{1}, Partitions: []kmsg.OffsetCommitResponseTopicPartition{{Partition: 1, ErrorCode: 0}, {Partition: 2, ErrorCode: 29}}}, {Topic: "b", TopicID: [16]byte{2}, Partitions: []kmsg.OffsetCommitResponseTopicPartition{{Partition: 7, ErrorCode: 3}}}}
				b = binary.BigEndian.AppendUint32(nil, binary.BigEndian.Uint32(b[4:]))
				if r.IsFlexible() {
					b = append(b, 1, 8, 1, 0)
				}
				writeFrame(c, r.AppendTo(b))
			}()
			p := New(listener.Addr().String())
			defer p.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
			defer cancel()
			f, e := p.Intercept(ctx, 8, AfterResponse)
			if e != nil {
				t.Fatal(e)
			}
			c, e := p.DialContext(ctx, "tcp", "")
			if e != nil {
				t.Fatal(e)
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(time.Second))
			r := kmsg.NewPtrOffsetCommitRequest()
			r.SetVersion(version)
			r.Group = "g"
			if e = writeFrame(c, requestFrame(r, 99)); e != nil {
				t.Fatal(e)
			}
			select {
			case ev := <-f.Events:
				response := ev.Response.(*kmsg.OffsetCommitResponse)
				if ev.CorrelationID != 99 || ev.Request.(*kmsg.OffsetCommitRequest).Group != "g" || len(response.Topics) != 2 || response.Topics[0].Partitions[1].ErrorCode != 29 || response.Topics[1].Partitions[0].ErrorCode != 3 {
					t.Fatal("wrong decoded result")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			f.Release()
			if _, e = readFrame(c); e != nil {
				t.Fatal(e)
			}
		})
	}
}

func TestMalformedFrames(t *testing.T) {
	for _, b := range [][]byte{nil, {0, 0}, {255, 255, 255, 255}, {0, 0, 0, 2, 1}} {
		if _, e := readFrame(bytes.NewReader(b)); e == nil {
			t.Fatal("accepted invalid frame")
		}
	}
	for _, b := range [][]byte{nil, make([]byte, 10), append(make([]byte, 8), 255, 254), append(make([]byte, 8), 0, 10)} {
		if _, e := decodeRequest(b, 21, 2); e == nil {
			t.Fatal("accepted invalid request")
		}
	}
	for _, version := range []int16{-1, 100} {
		if _, e := decodeRequest(make([]byte, 20), 21, version); e == nil {
			t.Fatal("accepted invalid version")
		}
	}
	for _, b := range [][]byte{nil, {128}, {1, 0}, {1, 0, 10, 1}} {
		if _, e := skipTags(b); e == nil {
			t.Fatal("accepted invalid tags")
		}
	}
	p := New("")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := p.Intercept(ctx, 21, BeforeRequest); e == nil {
		t.Fatal("canceled context accepted")
	}
	for _, pair := range []struct {
		key int16
		b   Boundary
	}{{8, BeforeRequest}, {26, AfterResponse}, {21, "invalid"}} {
		if _, e := p.Intercept(context.Background(), pair.key, pair.b); e == nil {
			t.Fatal("invalid interception accepted")
		}
	}
	p.Close()
	if _, e := p.Intercept(context.Background(), 21, BeforeRequest); e == nil {
		t.Fatal("closed proxy accepted")
	}
}

func TestCorrelationAndAdjacentAPI(t *testing.T) {
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer listener.Close()
	neighbor := binary.BigEndian.AppendUint32(nil, 100)
	neighbor = append(neighbor, 1, 2, 3)
	go func() {
		c, e := listener.Accept()
		if e != nil {
			return
		}
		defer c.Close()
		if _, e = readFrame(c); e != nil {
			return
		}
		if _, e = readFrame(c); e != nil {
			return
		}
		if writeFrame(c, neighbor) != nil {
			return
		}
		r := kmsg.NewPtrDeleteRecordsResponse()
		writeFrame(c, r.AppendTo(binary.BigEndian.AppendUint32(nil, 99)))
	}()
	p := New(listener.Addr().String())
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	f, e := p.Intercept(ctx, 21, AfterResponse)
	if e != nil {
		t.Fatal(e)
	}
	c, e := p.DialContext(ctx, "tcp", "")
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(time.Second))
	if e = writeFrame(c, requestFrame(kmsg.NewPtrDeleteRecordsRequest(), 99)); e != nil {
		t.Fatal(e)
	}
	if e = writeFrame(c, requestFrame(kmsg.NewPtrMetadataRequest(), 100)); e != nil {
		t.Fatal(e)
	}
	got, e := readFrame(c)
	if e != nil || !bytes.Equal(got, neighbor) {
		t.Fatalf("adjacent API corrupted: %v", e)
	}
	select {
	case ev := <-f.Events:
		if ev.CorrelationID != 99 {
			t.Fatal("wrong correlation")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	f.Release()
	got, e = readFrame(c)
	if e != nil || binary.BigEndian.Uint32(got) != 99 {
		t.Fatalf("release: %v", e)
	}
}

func TestInvalidResponseClosesConnection(t *testing.T) {
	for _, body := range [][]byte{{0, 0, 0, 8}, {0, 0, 0, 7, 128}} {
		t.Run(fmt.Sprint(body), func(t *testing.T) {
			listener, e := net.Listen("tcp", "127.0.0.1:0")
			if e != nil {
				t.Fatal(e)
			}
			defer listener.Close()
			go func() {
				c, e := listener.Accept()
				if e != nil {
					return
				}
				defer c.Close()
				if _, e = readFrame(c); e == nil {
					writeFrame(c, body)
				}
			}()
			p := New(listener.Addr().String())
			defer p.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
			defer cancel()
			f, e := p.Intercept(ctx, 21, AfterResponse)
			if e != nil {
				t.Fatal(e)
			}
			c, e := p.DialContext(ctx, "tcp", "")
			if e != nil {
				t.Fatal(e)
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(time.Second))
			r := kmsg.NewPtrDeleteRecordsRequest()
			r.SetVersion(2)
			if e = writeFrame(c, requestFrame(r, 7)); e != nil {
				t.Fatal(e)
			}
			if _, e = readFrame(c); e == nil {
				t.Fatal("invalid response delivered")
			}
			select {
			case <-f.Events:
				t.Fatal("invalid response observed as valid")
			default:
			}
		})
	}
}

func TestCancellationWhileAwaitingBroker(t *testing.T) {
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer listener.Close()
	received := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		c, e := listener.Accept()
		if e != nil {
			return
		}
		defer c.Close()
		if _, e = readFrame(c); e != nil {
			return
		}
		close(received)
		var b [1]byte
		c.Read(b[:])
	}()
	p := New(listener.Addr().String())
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	gateCtx, stop := context.WithCancel(ctx)
	defer stop()
	if _, e = p.Intercept(gateCtx, 21, AfterResponse); e != nil {
		t.Fatal(e)
	}
	c, e := p.DialContext(ctx, "tcp", "")
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(time.Second))
	if e = writeFrame(c, requestFrame(kmsg.NewPtrDeleteRecordsRequest(), 7)); e != nil {
		t.Fatal(e)
	}
	select {
	case <-received:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	stop()
	if _, e = readFrame(c); e == nil {
		t.Fatal("canceled request returned frame")
	}
	select {
	case <-exited:
	case <-ctx.Done():
		t.Fatal("upstream connection leaked")
	}
}
