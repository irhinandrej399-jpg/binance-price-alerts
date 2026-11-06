package marketprocess

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// RawFeed serves exactly one raw trade on each connection to btcusdt@trade.
// It holds the connection open until peer close or Close. It does not model
// Binance subscriptions, ping policy, catalog, rotation or failure injection.
type RawFeed struct {
	URL    string
	server *httptest.Server
	mu     sync.Mutex
	closed bool
	conns  map[*websocket.Conn]struct{}
}

func NewRawFeed(raw []byte) *RawFeed {
	payload := append([]byte(nil), raw...)
	f := &RawFeed{conns: make(map[*websocket.Conn]struct{})}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ws/btcusdt@trade" {
			http.NotFound(w, r)
			return
		}
		c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		f.mu.Lock()
		if f.closed {
			f.mu.Unlock()
			return
		}
		f.conns[c] = struct{}{}
		f.mu.Unlock()
		defer func() { f.mu.Lock(); delete(f.conns, c); f.mu.Unlock() }()
		_ = c.SetWriteDeadline(time.Now().Add(3 * time.Second))
		if c.WriteMessage(websocket.TextMessage, payload) != nil {
			return
		}
		for {
			if _, _, err = c.ReadMessage(); err != nil {
				return
			}
		}
	}))
	f.URL = "ws" + strings.TrimPrefix(f.server.URL, "http") + "/ws/btcusdt@trade"
	return f
}

func (f *RawFeed) Close() {
	f.mu.Lock()
	f.closed = true
	for c := range f.conns {
		_ = c.Close()
	}
	f.mu.Unlock()
	f.server.Close()
}
