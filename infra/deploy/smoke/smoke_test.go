package smoke_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/serviceauth"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/binancefake"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/testutil/telegramfake"
	"google.golang.org/grpc"
)

// This is only the bounded OPS-074 Compose scenario. The host controller handles
// container lifecycle; the existing Go fakes remain manually driven here.
func TestCompose(t *testing.T) {
	if os.Getenv("DEPLOY_SMOKE") != "1" {
		t.Skip("run python3 infra/deploy/smoke.py")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute)
	defer cancel()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	listen := func(port string) net.Listener {
		l, err := net.Listen("tcp", "0.0.0.0:"+port)
		must(err)
		return l
	}
	b, err := binancefake.NewWithListener(listen("18080"), "http://smoke:18080")
	must(err)
	defer b.Close()
	tg, err := telegramfake.NewWithListener(listen("18081"), "http://127.0.0.1:18081")
	must(err)
	defer tg.Close()
	signal := func(name string) { must(os.WriteFile("/state/"+name, []byte("ok"), 0600)) }
	signal("fixtures")
	wait := func(label string, check func() bool) {
		t.Helper()
		deadline := time.NewTimer(140 * time.Second)
		defer deadline.Stop()
		for !check() {
			select {
			case <-ctx.Done():
				t.Fatal(label, "context expired")
			case <-deadline.C:
				t.Fatal(label, "deadline exceeded")
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	read := func(name string) []byte {
		v, e := os.ReadFile("/run/pki/" + name)
		must(e)
		return v
	}
	creds, err := serviceauth.ClientCredentials(read("client.pem"), read("client.key"), read("ca.pem"))
	must(err)
	rpc, err := grpc.NewClient("market-data:9080", grpc.WithAuthority("services.internal"), grpc.WithTransportCredentials(creds))
	must(err)
	defer rpc.Close()
	var connection *binancefake.Connection
	feed := func() {
		t.Helper()
		connection, err = b.Next(ctx)
		must(err)
		wait("subscription ACK", func() bool { return len(connection.Streams()) == 2 })
	}
	var seq uint64
	trade := func(price string) {
		t.Helper()
		seq++
		now := time.Now().UnixMilli()
		must(connection.TradePair(ctx, "BTCUSDT", binancefake.Trade{ID: seq, Price: price, TradeTime: now, EventTime: now}))
		wait("real Latest quote", func() bool {
			c, done := context.WithTimeout(ctx, time.Second)
			defer done()
			v, e := pb.NewQuotesClient(rpc).Latest(c, &pb.LatestRequest{Pair: "BTCUSDT"})
			return e == nil && v.Quote != nil && v.Quote.SourceId == fmt.Sprint(seq) && v.Quote.Price == price && v.Quote.StreamState == "unconfirmed"
		})
	}
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil}}
	defer client.CloseIdleConnections()
	base := "http://api:8080"
	if os.Getenv("DEPLOY_INGRESS") == "1" {
		pem, e := os.ReadFile("/run/ingress/tls.pem")
		must(e)
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			t.Fatal("bad ingress CA")
		}
		client.Transport = &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
		base = "https://alerts.test"
	}
	token := ""
	request := func(method, path, body string, want int, dst any) {
		t.Helper()
		req, e := http.NewRequestWithContext(ctx, method, base+path, strings.NewReader(body))
		must(e)
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		if path == "/v1/telegram/webhook" {
			req.Header.Set("X-Telegram-Bot-Api-Secret-Token", telegramfake.Secret)
		}
		res, e := client.Do(req)
		must(e)
		defer res.Body.Close()
		if res.StatusCode != want {
			t.Fatalf("%s %s status %d want %d", method, path, res.StatusCode, want)
		}
		if dst != nil {
			must(json.NewDecoder(io.LimitReader(res.Body, 65536)).Decode(dst))
		}
	}
	feed()
	trade("99")
	// Observe all four HTTP endpoints, preserving the raw-feed readiness contract.
	for _, name := range []string{"market-data", "api", "matcher", "notifications"} {
		wait(name+" readiness", func() bool {
			res, e := client.Get("http://" + name + ":8080/readyz")
			if e != nil {
				return false
			}
			defer res.Body.Close()
			if name == "market-data" {
				body, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
				return res.StatusCode == 503 && strings.Contains(string(body), "unconfirmed")
			}
			return res.StatusCode == 200
		})
	}
	waitIngress := func() {
		if os.Getenv("DEPLOY_INGRESS") != "1" {
			return
		}
		wait("TLS ingress", func() bool {
			res, e := client.Get(base + "/healthz")
			if e != nil {
				return false
			}
			defer res.Body.Close()
			return res.StatusCode == 404
		})
	}
	if os.Getenv("DEPLOY_INGRESS") == "1" {
		wait("host ingress checks", func() bool { _, e := os.Stat("/state/ingress_checked"); return e == nil })
	}
	waitIngress()
	credentials := `{"email":"compose@example.test","password":"test-only-password"}`
	request("POST", "/v1/accounts", credentials, 201, nil)
	var session struct {
		Token string `json:"access_token"`
	}
	request("POST", "/v1/sessions", credentials, 200, &session)
	token = session.Token
	var code struct {
		Code string `json:"code"`
	}
	request("POST", "/v1/telegram/link-codes", "", 201, &code)
	// Link is intentionally loopback-only; issue the ordinary test webhook over
	// this isolated bridge without changing the helper's access boundary.
	request("POST", "/v1/telegram/webhook", fmt.Sprintf(`{"update_id":1,"message":{"message_id":1,"chat":{"id":90001,"type":"private"},"text":"/start %s"}}`, code.Code), 200, nil)
	create := func(threshold string) string {
		t.Helper()
		trade("99")
		var v struct {
			ID       string   `json:"id"`
			Price    string   `json:"creation_price"`
			Warnings []string `json:"warnings"`
		}
		request("POST", "/v1/subscriptions", fmt.Sprintf(`{"pair":"BTCUSDT","price":"%s","direction":"up"}`, threshold), 201, &v)
		if v.ID == "" || v.Price != "99" || !strings.Contains(strings.Join(v.Warnings, ","), "stream_unconfirmed") {
			t.Fatal("creation snapshot/warning missing")
		}
		return v.ID
	}
	first, second := create("100"), create("200")
	list := func() []string {
		var v struct {
			Subscriptions []struct {
				ID string `json:"id"`
			} `json:"subscriptions"`
		}
		request("GET", "/v1/subscriptions", "", 200, &v)
		var ids []string
		for _, s := range v.Subscriptions {
			ids = append(ids, s.ID)
		}
		return ids
	}
	deliver := func(price, threshold string, messageID int64) {
		t.Helper()
		trade(price)
		c, done := context.WithTimeout(ctx, 20*time.Second)
		defer done()
		call, e := tg.Next(c)
		must(e)
		if call.Request.ChatID != 90001 || !strings.Contains(call.Request.Text, "Фактическая цена: "+price+"\n") || !strings.Contains(call.Request.Text, "Порог: "+threshold) {
			t.Fatal("wrong Telegram recipient/price/threshold")
		}
		call.Reply(messageID)
	}
	deliver("101", "100", 501)
	wait("canonical completion and retained subscription", func() bool {
		ids := list()
		return len(ids) == 1 && ids[0] == second && ids[0] != first
	})
	t.Log("registration/link/two subscriptions/real raw trade/confirmed delivery PASS")
	signal("restart")
	wait("host restart", func() bool { _, e := os.Stat("/state/restarted"); return e == nil })
	waitIngress()
	feed()
	trade("101")
	ids := list()
	if len(ids) != 1 || ids[0] != second {
		t.Fatal("subscription/session lost after restart")
	}
	deliver("201", "200", 502)
	wait("completion after restart", func() bool { return len(list()) == 0 })
	if len(tg.Requests()) != 2 {
		t.Fatal("unexpected duplicate Telegram request")
	}
	t.Log("same-volume stop/start: saved session/subscription and second delivery PASS")
	signal("done")
}
