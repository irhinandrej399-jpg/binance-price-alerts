// Package binancefake provides an explicitly controlled Binance
// fixture for parent tests driving real service processes. It never dials out.
package binancefake

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/marketdata/catalog"
)

//go:embed testdata/exchange_info.json
var catalogFixture []byte

var ErrClosed = errors.New("binancefake: closed")
var ErrNotSubscribed = errors.New("binancefake: pair not subscribed")

// Fake owns its HTTP listener and all upgraded sockets. Call Close after use.
type Fake struct {
	server  *httptest.Server
	origin  string
	next    chan *Connection
	done    chan struct{}
	mu      sync.Mutex
	closed  bool
	catalog []byte
	conns   map[*Connection]struct{}
	wg      sync.WaitGroup
	once    sync.Once
}

func New() *Fake {
	f := &Fake{next: make(chan *Connection), done: make(chan struct{}), conns: make(map[*Connection]struct{}), catalog: catalogFixture}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	return f
}

// NewWithListener serves the existing fixture on an already-open listener.
// Ownership transfers on entry, including validation failure. origin is the
// caller-selected HTTP origin reachable by its clients, never a wildcard URL.
// Only isolated test scenarios may opt in to a non-loopback listener.
// Listen/bind errors belong to the caller; this function validates configuration
// before starting any goroutine. The caller must not reuse or close the listener.
func NewWithListener(listener net.Listener, origin string) (*Fake, error) {
	if listener == nil {
		return nil, errors.New("binancefake: nil listener")
	}
	u, err := url.Parse(origin)
	valid := err == nil && u.Scheme == "http" && u.Hostname() != "" && u.Hostname() != "*" && u.User == nil && u.Path == "" && u.RawQuery == "" && !u.ForceQuery && u.Fragment == "" && u.RawFragment == "" && u.Opaque == ""
	if valid {
		port, e := strconv.Atoi(u.Port())
		ip := net.ParseIP(u.Hostname())
		valid = e == nil && port > 0 && port <= 65535 && (ip == nil || !ip.IsUnspecified())
	}
	if !valid {
		_ = listener.Close()
		return nil, errors.New("binancefake: invalid client origin")
	}
	f := &Fake{next: make(chan *Connection), done: make(chan struct{}), conns: make(map[*Connection]struct{}), catalog: catalogFixture}
	f.origin = origin
	f.server = &httptest.Server{Listener: listener, Config: &http.Server{Handler: http.HandlerFunc(f.serve)}}
	f.server.Start()
	return f, nil
}

// WSURL returns the bare endpoint for catalog-driven subscriptions.
func (f *Fake) WSURL() string { return "ws" + strings.TrimPrefix(f.URL(), "http") + "/ws" }

// SetCatalog atomically replaces exchangeInfo with explicit tradable pairs.
// It copies the input before returning; nil represents an empty catalog.
// Existing socket subscriptions remain controlled by client commands.
func (f *Fake) SetCatalog(pairs []catalog.Pair) error {
	type symbol struct {
		Symbol string `json:"symbol"`
		Base   string `json:"baseAsset"`
		Quote  string `json:"quoteAsset"`
		Status string `json:"status"`
		Spot   bool   `json:"isSpotTradingAllowed"`
	}
	payload := struct {
		Symbols []symbol `json:"symbols"`
	}{Symbols: make([]symbol, 0, len(pairs))}
	for _, p := range pairs {
		payload.Symbols = append(payload.Symbols, symbol{p.Symbol, p.BaseAsset, p.QuoteAsset, "TRADING", true})
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return ErrClosed
	}
	f.catalog = b
	return nil
}

func (f *Fake) URL() string {
	if f.origin != "" {
		return f.origin
	}
	return f.server.URL
}
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
		f.mu.Lock()
		body := f.catalog
		f.mu.Unlock()
		_, _ = w.Write(body)
		return
	}
	symbol := ""
	switch r.URL.Path {
	case "/ws":
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
	c := &Connection{Symbol: symbol, ws: ws, done: make(chan struct{}), pongs: make(chan string, 16), streams: make(map[string]struct{})}
	if symbol != "" {
		c.streams[strings.ToLower(symbol)+"@trade"] = struct{}{}
	}
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
		_, b, err := ws.ReadMessage()
		if err != nil {
			return
		}
		if symbol == "" {
			if err := c.command(b); err != nil {
				return
			}
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
		f.server.CloseClientConnections()
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

// Connection identifies one accepted socket. Symbol is empty on bare /ws.
// Writes and subscription changes are serialized; one
// Ping at a time is allowed. After a disconnect acquire a new socket with Next.
type Connection struct {
	Symbol  string
	ws      *websocket.Conn
	done    chan struct{}
	pongs   chan string
	writeMu sync.Mutex
	pingMu  sync.Mutex
	streams map[string]struct{} // guarded by writeMu, including command/trade ordering
}

func (c *Connection) write(ctx context.Context, kind int, b []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.writeLocked(ctx, kind, b)
}
func (c *Connection) writeLocked(ctx context.Context, kind int, b []byte) error {
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
	return c.TradePair(ctx, c.Symbol, t)
}

// Streams returns a sorted independent snapshot of this socket's subscriptions.
func (c *Connection) Streams() []string {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	names := make([]string, 0, len(c.streams))
	for name := range c.streams {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func (c *Connection) command(b []byte) error {
	var cmd struct {
		Method string          `json:"method"`
		Params []string        `json:"params"`
		ID     json.RawMessage `json:"id"`
	}
	if err := json.Unmarshal(b, &cmd); err != nil {
		return err
	}
	if (cmd.Method != "SUBSCRIBE" && cmd.Method != "UNSUBSCRIBE") || len(cmd.ID) == 0 || string(cmd.ID) == "null" || len(cmd.Params) == 0 {
		return errors.New("binancefake: invalid command")
	}
	for _, name := range cmd.Params {
		if !strings.HasSuffix(name, "@trade") || len(name) <= len("@trade") || name != strings.ToLower(name) {
			return errors.New("binancefake: unsupported stream")
		}
	}
	ack, err := json.Marshal(struct {
		Result any             `json:"result"`
		ID     json.RawMessage `json:"id"`
	}{nil, cmd.ID})
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	for _, name := range cmd.Params {
		if cmd.Method == "SUBSCRIBE" {
			c.streams[name] = struct{}{}
		} else {
			delete(c.streams, name)
		}
	}
	return c.writeLocked(context.Background(), websocket.TextMessage, ack)
}

// TradePair emits a raw trade only if this socket owns the selected pair.
// An absent subscription returns ErrNotSubscribed and writes no frame.
func (c *Connection) TradePair(ctx context.Context, symbol string, t Trade) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := c.streams[strings.ToLower(symbol)+"@trade"]; !ok {
		return ErrNotSubscribed
	}
	payload := struct {
		Kind      string `json:"e"`
		Symbol    string `json:"s"`
		ID        uint64 `json:"t"`
		Price     string `json:"p"`
		TradeTime int64  `json:"T"`
		EventTime int64  `json:"E"`
	}{"trade", symbol, t.ID, t.Price, t.TradeTime, t.EventTime}
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return c.writeLocked(ctx, websocket.TextMessage, b)
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
