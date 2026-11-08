package telegramfake_test

import (
	"context"
	"errors"
	"net"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/telegram"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/telegramfake"
)

// Simulate a wildcard listener address without exposing the test on LAN.
// Traffic still reaches the real loopback socket through the explicit origin.
type wildcardAddress struct{ net.Listener }

func (l wildcardAddress) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4zero, Port: l.Listener.Addr().(*net.TCPAddr).Port}
}

func newFixture(t *testing.T, explicit bool) *telegramfake.Fake {
	t.Helper()
	if !explicit {
		return telegramfake.New()
	}
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	origin := "http://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(l.Addr().(*net.TCPAddr).Port))
	f, err := telegramfake.NewWithListener(wildcardAddress{l}, origin)
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
			f, err := telegramfake.NewWithListener(l, origin)
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
	if _, err := telegramfake.NewWithListener(nil, "http://localhost:123"); err == nil {
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

func TestListenerCloseUnclaimedSend(t *testing.T) {
	f := newFixture(t, true)
	defer f.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client, err := telegram.New(telegram.Config{Token: telegramfake.Token, BaseURL: f.URL(), Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := client.Send(ctx, 1, "unclaimed"); done <- err }()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for len(f.Requests()) == 0 {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("request not accepted")
		}
	}
	// Leave Next untouched: Close must release the queued call and its HTTP handler.
	f.Close()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("unclaimed send leaked")
	}
	if _, err := f.Next(ctx); err == nil {
		t.Fatal("Next returned a queued call after Close")
	}
}
