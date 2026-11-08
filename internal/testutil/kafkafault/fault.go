package kafkafault

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"

	"github.com/twmb/franz-go/pkg/kmsg"
)

type Boundary string

const (
	BeforeRequest Boundary = "before-request"
	AfterResponse Boundary = "after-response"
)

// Event contains the actual decoded frame, including every partition error.
// Response is nil at BeforeRequest. Treat the values as read-only.
type Event struct {
	CorrelationID uint32
	Request       kmsg.Request
	Response      kmsg.Response
}

// Fault applies to every request of one API on every connection, including
// retries. Events are bounded: a full channel backpressures intercepted traffic.
type Fault struct {
	Events                chan Event
	p                     *Proxy
	ctx                   context.Context
	key                   int16
	boundary              Boundary
	released, failed      chan struct{}
	releaseOnce, failOnce sync.Once
}

// Intercept installs one fault at a time. Only DeleteRecords (21) supports
// BeforeRequest; OffsetCommit (8) and DeleteRecords support AfterResponse.
// The context bounds all gates; cancellation fails held traffic. DialContext's
// context only bounds dialing, following net.Dialer semantics.
func (p *Proxy) Intercept(ctx context.Context, key int16, b Boundary) (*Fault, error) {
	if ctx == nil || (key != 21 && key != 8) || (b != AfterResponse && !(key == 21 && b == BeforeRequest)) {
		return nil, errors.New("kafkafault: invalid interception")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errors.New("kafkafault: closed")
	}
	if p.fault != nil && p.fault.ctx.Err() == nil {
		return nil, errors.New("kafkafault: interception active")
	}
	f := &Fault{Events: make(chan Event, 32), p: p, ctx: ctx, key: key, boundary: b, released: make(chan struct{}), failed: make(chan struct{})}
	p.fault = f
	return f, nil
}

// Fail drops held and subsequent matching operations without disarming. It
// closes the affected connection, so pipelined calls on it can also fail.
func (f *Fault) Fail() { f.failOnce.Do(func() { close(f.failed) }) }

// Release disarms and lets gates that have not failed proceed. Both operations
// are idempotent. Failed operations require a caller retry after Release.
func (f *Fault) Release() {
	f.releaseOnce.Do(func() {
		f.p.mu.Lock()
		if f.p.fault == f {
			f.p.fault = nil
		}
		f.p.mu.Unlock()
		close(f.released)
	})
}
func (f *Fault) canProceed() bool {
	select {
	case <-f.failed:
		return false
	case <-f.ctx.Done():
		return false
	default:
		return true
	}
}
func (f *Fault) wait(e Event, done <-chan struct{}) bool {
	select {
	case <-f.released:
		return f.canProceed()
	default:
	}
	// Keep observations of retries when space is available even after Fail.
	select {
	case f.Events <- e:
	default:
		select {
		case f.Events <- e:
		case <-f.failed:
			return false
		case <-f.ctx.Done():
			return false
		case <-done:
			return false
		case <-f.released:
			return f.canProceed()
		}
	}
	select {
	case <-f.failed:
		return false
	case <-f.ctx.Done():
		return false
	case <-done:
		return false
	case <-f.released:
		return f.canProceed()
	}
}
func decodeRequest(b []byte, key, version int16) (kmsg.Request, error) {
	r := kmsg.RequestForKey(key)
	if r == nil || version < 0 || version > r.MaxVersion() {
		return nil, errors.New("kafkafault: unsupported version")
	}
	r.SetVersion(version)
	if len(b) < 10 {
		return nil, io.ErrUnexpectedEOF
	}
	n := int(int16(binary.BigEndian.Uint16(b[8:10])))
	if n < -1 {
		return nil, errors.New("kafkafault: invalid client header")
	}
	pos := 10
	if n >= 0 {
		pos += n
	}
	if pos > len(b) {
		return nil, io.ErrUnexpectedEOF
	}
	body := b[pos:]
	var err error
	if r.IsFlexible() {
		body, err = skipTags(body)
		if err != nil {
			return nil, err
		}
	}
	if err = r.ReadFrom(body); err != nil {
		return nil, errors.New("kafkafault: invalid request body")
	}
	return r, nil
}
