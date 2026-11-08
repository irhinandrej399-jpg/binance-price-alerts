package main

import (
	"context"
	"net"
	"strings"
	"testing"
)

func validEnv() map[string]string {
	return map[string]string{"HTTP_ADDR": "127.0.0.1:8081", "RPC_ADDR": "127.0.0.1:8082", "RPC_CERT_FILE": "cert.pem", "RPC_KEY_FILE": "key.pem", "RPC_CA_FILE": "ca.pem", "API_SERVICE_URI": "spiffe://local/api", "MATCHER_SERVICE_URI": "spiffe://local/matcher", "CATALOG_BASE_URL": "http://127.0.0.1:9000", "MARKET_DATA_DATABASE_URL": "postgres://market_data_runtime:secret@127.0.0.1:5432/market_data", "RAW_TRADE_URL": "ws://127.0.0.1:9000/ws", "KAFKA_BROKERS": "127.0.0.1:9092", "PRICES_TOPIC": "local-prices", "MARKET_DATA_RELAY_TRANSACTIONAL_ID": "local-market-data-relay-shard-0"}
}
func TestConfig(t *testing.T) {
	env := validEnv()
	if _, err := loadConfig(func(k string) string { return env[k] }); err != nil {
		t.Fatal(err)
	}
	for k := range env {
		t.Run("missing_"+k, func(t *testing.T) {
			v := env[k]
			delete(env, k)
			defer func() { env[k] = v }()
			if _, err := loadConfig(func(k string) string { return env[k] }); err == nil {
				t.Fatal("accepted missing setting")
			}
		})
	}
	for k, values := range map[string][]string{
		"HTTP_ADDR":                          {"bad", ":8081", "127.0.0.1:0"},
		"RPC_ADDR":                           {"bad", ":9081", "127.0.0.1:0"},
		"API_SERVICE_URI":                    {"bad", "spiffe://local/matcher", "spiffe://local/api?secret=x"},
		"MATCHER_SERVICE_URI":                {"bad", "spiffe://local/api"},
		"CATALOG_BASE_URL":                   {"ftp://localhost", "http://localhost/path", "http://localhost?", "http://user:secret@localhost"},
		"MARKET_DATA_DATABASE_URL":           {"postgres://api_runtime:secret@localhost/api", "secret"},
		"RAW_TRADE_URL":                      {"https://localhost/ws/btcusdt@trade", "ws://user:secret@localhost/ws/btcusdt@trade", "ws://localhost/ws/ethusdt@trade", "ws://localhost/ws/btcusdt@trade?timeUnit=MICROSECOND"},
		"CATALOG_REFRESH_INTERVAL":           {"bad", "0s", "10ms", "2h"},
		"KAFKA_BROKERS":                      {"localhost:0", "localhost:9092,"},
		"PRICES_TOPIC":                       {"..", "has space"},
		"MARKET_DATA_RELAY_TRANSACTIONAL_ID": {".", strings.Repeat("x", 250)},
	} {
		for _, v := range values {
			t.Run(k+v, func(t *testing.T) {
				old := env[k]
				env[k] = v
				defer func() { env[k] = old }()
				_, err := loadConfig(func(k string) string { return env[k] })
				if err == nil || strings.Contains(err.Error(), "secret") {
					t.Fatal("invalid config or credential leak")
				}
			})
		}
	}
}
func TestStartupFailureReleasesListener(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	if err = run(context.Background(), config{addr: addr}); err == nil {
		t.Fatal("bind failure hidden")
	}
	l.Close()
	if err = run(context.Background(), config{addr: addr, dsn: "invalid secret"}); err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatal("startup error not sanitized")
	}
	l, err = net.Listen("tcp", addr)
	if err != nil {
		t.Fatal("listener leaked")
	}
	l.Close()
}

func TestRPCStartupFailureReleasesListeners(t *testing.T) {
	env := validEnv()
	_, addr := configureRPC(t, env)
	env["MARKET_DATA_DATABASE_URL"] = "postgres://market_data_runtime:secret@127.0.0.1:1/market_data"
	c, err := loadConfig(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	http, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	c.addr = http.Addr().String()
	http.Close()
	if err := run(context.Background(), c); err == nil || !strings.Contains(err.Error(), "database startup failed") {
		t.Fatal("DB startup failure hidden")
	}
	for _, a := range []string{c.addr, addr} {
		l, err := net.Listen("tcp", a)
		if err != nil {
			t.Fatal("startup listener leaked")
		}
		l.Close()
	}
}
