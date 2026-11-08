// Package binancefake provides an explicitly controlled, loopback-only Binance
// fixture for parent tests driving real service processes. It never dials out.
package binancefake

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

//go:embed testdata/exchange_info.json
var catalogFixture []byte

var ErrClosed = errors.New("binancefake: closed")

// Fake owns its HTTP listener and all upgraded sockets. Call Close after use.
type Fake struct {
	server *httptest.Server
	next   chan *Connection
	done   chan struct{}
	mu     sync.Mutex
	closed bool
	conns  map[*Connection]struct{}
	wg     sync.WaitGroup
	once   sync.Once
}

func New() *Fake {
	f := &Fake{next: make(chan *Connection), done: make(chan struct{}), conns: make(map[*Connection]struct{})}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	return f
}
func (f *Fake) URL() string          { return f.server.URL }
func (f *Fake) Client() *http.Client { return f.server.Client() }

// StreamURL selects a raw stream present in the fixture (BTCUSDT or SOLUSDT).
func (f *Fake) StreamURL(symbol string) string {
	return "ws" + strings.TrimPrefix(f.URL(), "http") + "/ws/" + strings.ToLower(symbol) + "@trade"
}

// Next waits for the next accepted socket, including reconnects. No prices are
// sent automatically. Callers control each socket independently.
func (f *Fake) Next(ctx context.Context) (*Connection, error) {
	select {
	case c := <-f.next:
		return c, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-f.done:
		return nil, ErrClosed
	}
}
func (f *Fake) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if r.URL.Path == "/api/v3/exchangeInfo" {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(catalogFixture)
		return
	}
	symbol := ""
	switch r.URL.Path {
	case "/ws/btcusdt@trade":
		symbol = "BTCUSDT"
	case "/ws/solusdt@trade":
		symbol = "SOLUSDT"
	default:
		http.NotFound(w, r)
		return
	}
	// Add and shutdown share the mutex: Close cannot miss a hijacked socket or
	// race Wait against the first Add.
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	ws, err := (&websocket.Upgrader{HandshakeTimeout: time.Second}).Upgrade(w, r, nil)
	if err != nil {
		f.mu.Unlock()
		return
	}
	c := &Connection{Symbol: symbol, ws: ws, done: make(chan struct{}), pongs: make(chan string, 16)}
	f.conns[c] = struct{}{}
	f.wg.Add(1)
	f.mu.Unlock()
	defer f.wg.Done()
	defer func() { _ = ws.Close(); close(c.done); f.mu.Lock(); delete(f.conns, c); f.mu.Unlock() }()
	ws.SetReadLimit(1 << 20)
	ws.SetPongHandler(func(p string) error {
		select {
		case c.pongs <- p:
			return nil
		default:
			return errors.New("binancefake: pong queue full")
		}
	})
	// Publish readiness separately so the reader can process control frames even
	// before Next is consumed. This goroutine is joined before handler exit.
	announced := make(chan struct{})
	publishStop := make(chan struct{})
	go func() {
		defer close(announced)
		select {
		case f.next <- c:
		case <-publishStop:
		case <-f.done:
		}
	}()
	defer func() { close(publishStop); <-announced }()
	for {
		if _, _, err := ws.ReadMessage(); err != nil {
			return
		}
	}
}

// Close is idempotent and joins handlers, including idle/unclaimed sockets.
func (f *Fake) Close() {
	f.once.Do(func() {
		f.mu.Lock()
		f.closed = true
		close(f.done)
		for c := range f.conns {
			_ = c.ws.Close()
		}
		f.mu.Unlock()
		f.server.Close()
		f.wg.Wait()
	})
}

// Trade is a raw trade fixture; timestamps are explicit Unix milliseconds.
// IDs/prices are never generated or converted through float64.
type Trade struct {
	ID                   uint64
	Price                string
	TradeTime, EventTime int64
}

// Connection identifies one accepted raw stream. Writes are serialized; one
// Ping at a time is allowed. After a disconnect acquire a new socket with Next.
type Connection struct {
	Symbol  string
	ws      *websocket.Conn
	done    chan struct{}
	pongs   chan string
	writeMu sync.Mutex
	pingMu  sync.Mutex
}

func (c *Connection) write(ctx context.Context, kind int, b []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-c.done:
		return ErrClosed
	default:
	}
	deadline := time.Now().Add(time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := c.ws.SetWriteDeadline(deadline); err != nil {
		return err
	}
	return c.ws.WriteMessage(kind, b)
}
func (c *Connection) Trade(ctx context.Context, t Trade) error {
	payload := struct {
		Kind      string `json:"e"`
		Symbol    string `json:"s"`
		ID        uint64 `json:"t"`
		Price     string `json:"p"`
		TradeTime int64  `json:"T"`
		EventTime int64  `json:"E"`
	}{"trade", c.Symbol, t.ID, t.Price, t.TradeTime, t.EventTime}
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return c.write(ctx, websocket.TextMessage, b)
}

// Ping sends an explicit payload and waits for a matching client pong. The
// caller supplies a deadline; no wall-clock ping schedule is installed.
func (c *Connection) Ping(ctx context.Context, payload string) error {
	c.pingMu.Lock()
	defer c.pingMu.Unlock()
	if len(payload) > 125 {
		return errors.New("binancefake: ping exceeds 125 bytes")
	}
	if err := c.write(ctx, websocket.PingMessage, []byte(payload)); err != nil {
		return err
	}
	for {
		select {
		case p := <-c.pongs:
			if p == payload {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-c.done:
			return ErrClosed
		}
	}
}

// Disconnect drops TCP without a close frame (client observes abnormal close).
func (c *Connection) Disconnect() error { return c.ws.Close() }

// Close sends an explicit WebSocket close frame, then closes the socket.
func (c *Connection) Close(ctx context.Context, code int, reason string) error {
	err := c.write(ctx, websocket.CloseMessage, websocket.FormatCloseMessage(code, reason))
	_ = c.ws.Close()
	return err
}
