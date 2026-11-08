package binancefake_test

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/binancefake"
)

// Simulate a wildcard listener address without exposing the test on LAN.
// Traffic still reaches the real loopback socket through the explicit origin.
type wildcardAddress struct{ net.Listener }

func (l wildcardAddress) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4zero, Port: l.Listener.Addr().(*net.TCPAddr).Port}
}

func newFixture(t *testing.T, explicit bool) *binancefake.Fake {
	t.Helper()
	if !explicit {
		return binancefake.New()
	}
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	origin := "http://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(l.Addr().(*net.TCPAddr).Port))
	f, err := binancefake.NewWithListener(wildcardAddress{l}, origin)
	if err != nil {
		t.Fatal(err)
	}
	if f.URL() != origin {
		t.Fatalf("client URL %s want %s", f.URL(), origin)
	}
	return f
}

func TestListenerValidationAndOwnership(t *testing.T) {
	for _, origin := range []string{"", "http://*:123", "http://localhost", "http://localhost:0", "http://localhost:65536", "http://localhost:123#fragment", "http://0.0.0.0:123", "http://[::]:123", "https://127.0.0.1:123", "http://127.0.0.1:123/path", "http://user@localhost:123", "http://localhost:123?x=y"} {
		t.Run(origin, func(t *testing.T) {
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			f, err := binancefake.NewWithListener(l, origin)
			if err == nil {
				f.Close()
				t.Fatal("accepted invalid origin")
			}
			l.(*net.TCPListener).SetDeadline(time.Now().Add(time.Second))
			if _, err := l.Accept(); !errors.Is(err, net.ErrClosed) {
				t.Fatal("listener remains open on error")
			}
		})
	}
	if _, err := binancefake.NewWithListener(nil, "http://localhost:123"); err == nil {
		t.Fatal("accepted nil listener")
	}
	f := newFixture(t, true)
	f.Close()
	f.Close()
	u, _ := url.Parse(f.URL())
	l, err := net.Listen("tcp4", net.JoinHostPort("127.0.0.1", u.Port()))
	if err != nil {
		t.Fatalf("Close did not release listener: %v", err)
	}
	l.Close()
}

func TestListenerCloseIncompleteHTTP(t *testing.T) {
	f := newFixture(t, true)
	defer f.Close()
	c, err := net.DialTimeout("tcp", strings.TrimPrefix(f.URL(), "http://"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := fmt.Fprint(c, "GET /api/v3/exchangeInfo HTTP/1.1\r\nHost: fixture\r\n"); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { f.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		c.Close()
		<-done
		t.Fatal("Close waited for incomplete HTTP")
	}
	c.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := c.Read(make([]byte, 1)); err == nil {
		t.Fatal("HTTP connection remains open")
	}
}
