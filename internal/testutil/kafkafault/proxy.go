// Package kafkafault injects a lost EndTxn response on real plaintext Kafka
// connections. Use only on a disposable, single-broker test cluster.
package kafkafault

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// Proxy redirects every dial (including advertised metadata addresses) to Target.
// Once armed, all EndTxn responses are suppressed, including retries. Requests
// still reach Kafka. Observed receives the broker's decoded EndTxn error code.
type Proxy struct {
	Target   string
	Observed chan int16
	mu       sync.Mutex
	armed    bool
	closed   bool
	conns    []net.Conn
	wg       sync.WaitGroup
}

func New(target string) *Proxy { return &Proxy{Target: target, Observed: make(chan int16, 16)} }
func (p *Proxy) Arm()          { p.mu.Lock(); p.armed = true; p.mu.Unlock() }

func (p *Proxy) Close() {
	p.mu.Lock()
	p.closed = true
	for _, c := range p.conns {
		c.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
}

// DialContext is supplied as kgo.Dialer. Both directions are framed and bounded;
// closing the client or proxy releases all connections and goroutines.
func (p *Proxy) DialContext(ctx context.Context, network, _ string) (net.Conn, error) {
	upstream, err := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, network, p.Target)
	if err != nil {
		return nil, err
	}
	client, relay := net.Pipe()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		upstream.Close()
		relay.Close()
		client.Close()
		return nil, net.ErrClosed
	}
	p.conns = append(p.conns, upstream, relay)
	p.wg.Add(2)
	p.mu.Unlock()
	type request struct{ key, version int16 }
	var mu sync.Mutex
	pending := map[uint32]request{}
	closeBoth := func() { upstream.Close(); relay.Close() }
	go func() {
		defer p.wg.Done()
		defer closeBoth()
		for {
			b, e := readFrame(relay)
			if e != nil {
				return
			}
			if len(b) < 8 {
				return
			}
			mu.Lock()
			pending[binary.BigEndian.Uint32(b[4:8])] = request{int16(binary.BigEndian.Uint16(b)), int16(binary.BigEndian.Uint16(b[2:]))}
			mu.Unlock()
			if writeFrame(upstream, b) != nil {
				return
			}
		}
	}()
	go func() {
		defer p.wg.Done()
		defer closeBoth()
		for {
			b, e := readFrame(upstream)
			if e != nil {
				return
			}
			if len(b) < 4 {
				return
			}
			id := binary.BigEndian.Uint32(b)
			mu.Lock()
			req, ok := pending[id]
			delete(pending, id)
			mu.Unlock()
			if !ok {
				return
			}
			p.mu.Lock()
			drop := p.armed && req.key == 26
			p.mu.Unlock()
			if drop {
				body := b[4:]
				if req.version >= 3 {
					body, e = skipTags(body)
					if e != nil {
						return
					}
				}
				resp := kmsg.NewPtrEndTxnResponse()
				resp.SetVersion(req.version)
				if resp.ReadFrom(body) != nil {
					return
				}
				select {
				case p.Observed <- resp.ErrorCode:
				default:
				}
				continue
			}
			if writeFrame(relay, b) != nil {
				return
			}
		}
	}()
	return client, nil
}

const maxFrame = 16 << 20

func readFrame(r io.Reader) ([]byte, error) {
	var header [4]byte
	if _, e := io.ReadFull(r, header[:]); e != nil {
		return nil, e
	}
	n := binary.BigEndian.Uint32(header[:])
	if n > maxFrame {
		return nil, errors.New("kafkafault: oversized frame")
	}
	b := make([]byte, n)
	_, e := io.ReadFull(r, b)
	return b, e
}
func writeFrame(w io.Writer, b []byte) error {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(b)))
	for _, buf := range [][]byte{header[:], b} {
		for len(buf) > 0 {
			n, e := w.Write(buf)
			if e != nil {
				return e
			}
			if n == 0 {
				return io.ErrShortWrite
			}
			buf = buf[n:]
		}
	}
	return nil
}
func skipTags(b []byte) ([]byte, error) {
	count, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, io.ErrUnexpectedEOF
	}
	b = b[n:]
	for ; count > 0; count-- {
		_, n = binary.Uvarint(b)
		if n <= 0 {
			return nil, io.ErrUnexpectedEOF
		}
		b = b[n:]
		size, m := binary.Uvarint(b)
		if m <= 0 {
			return nil, io.ErrUnexpectedEOF
		}
		b = b[m:]
		if size > uint64(len(b)) {
			return nil, io.ErrUnexpectedEOF
		}
		b = b[size:]
	}
	return b, nil
}
