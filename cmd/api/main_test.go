package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func validEnv() map[string]string {
	return map[string]string{
		"TELEGRAM_WEBHOOK_SECRET":   "test-runtime-secret",
		"NOTIFICATIONS_SERVICE_URI": "spiffe://local/notifications", "ADMISSION_PERMIT_TTL": "10s",
		"RPC_ADDR": "127.0.0.1:8081", "RPC_CERT_FILE": "cert.pem", "RPC_KEY_FILE": "key.pem", "RPC_CA_FILE": "ca.pem", "MATCHER_SERVICE_URI": "spiffe://local/matcher",
		"MARKET_RPC_ADDR": "127.0.0.1:8082", "MARKET_RPC_SERVER_NAME": "market.test", "MARKET_RPC_CERT_FILE": "client.pem", "MARKET_RPC_KEY_FILE": "client-key.pem", "MARKET_RPC_CA_FILE": "market-ca.pem",
		"HTTP_ADDR": "127.0.0.1:8080", "DATABASE_URL": "postgres://api_runtime:secret@127.0.0.1:5432/api",
		"KAFKA_BROKERS": "127.0.0.1:9092", "SUBSCRIPTION_CHANGES_TOPIC": "local-subscription-changes", "DELIVERY_REQUESTS_TOPIC": "local-delivery-requests",
		"SUBSCRIPTION_RELAY_TRANSACTIONAL_ID": "local-api-subscriptions-0", "DELIVERY_RELAY_TRANSACTIONAL_ID": "local-api-delivery-0",
	}
}
func TestConfiguration(t *testing.T) {
	env := validEnv()
	if _, err := loadConfig(func(k string) string { return env[k] }); err != nil {
		t.Fatal(err)
	}
	for key := range env {
		t.Run(key, func(t *testing.T) {
			e := validEnv()
			e[key] = ""
			if _, err := loadConfig(func(k string) string { return e[k] }); err == nil || strings.Contains(err.Error(), "secret") {
				t.Fatal("invalid configuration accepted or leaked")
			}
		})
	}
	for _, pair := range [][2]string{{"TRUSTED_PROXY_IP", "0.0.0.0/0"}, {"TRUSTED_PROXY_IP", "proxy"}, {"TELEGRAM_WEBHOOK_SECRET", "invalid secret"}, {"TELEGRAM_WEBHOOK_SECRET", strings.Repeat("x", 257)}, {"NOTIFICATIONS_SERVICE_URI", "bad"}, {"NOTIFICATIONS_SERVICE_URI", "spiffe://local/notifications?forged=1"}, {"NOTIFICATIONS_SERVICE_URI", env["MATCHER_SERVICE_URI"]}, {"ADMISSION_PERMIT_TTL", "0s"}, {"ADMISSION_PERMIT_TTL", "61s"}, {"ADMISSION_PERMIT_TTL", "1ns"}, {"ADMISSION_PERMIT_TTL", "bad"}, {"MATCHER_SERVICE_URI", "bad"}, {"MATCHER_SERVICE_URI", "spiffe://local/matcher?forged=1"}, {"RPC_ADDR", "bad"}, {"MARKET_RPC_ADDR", "dns:///market"}, {"MARKET_RPC_SERVER_NAME", "bad/name"}, {"HTTP_ADDR", "bad"}, {"DATABASE_URL", "postgres://secret%"}, {"DATABASE_URL", "postgres://notifications_runtime@localhost/api"}, {"DATABASE_URL", "postgres://api_runtime@localhost/notifications"}, {"KAFKA_BROKERS", "localhost:bad"}, {"DELIVERY_RELAY_TRANSACTIONAL_ID", env["SUBSCRIPTION_RELAY_TRANSACTIONAL_ID"]}, {"DELIVERY_REQUESTS_TOPIC", env["SUBSCRIPTION_CHANGES_TOPIC"]}, {"SUBSCRIPTION_CHANGES_TOPIC", "bad topic"}} {
		e := validEnv()
		e[pair[0]] = pair[1]
		if _, err := loadConfig(func(k string) string { return e[k] }); err == nil {
			t.Fatalf("accepted invalid %s", pair[0])
		}
	}
}
func TestSupervision(t *testing.T) {
	terminal := errors.New("terminal")
	for failed := range 3 {
		t.Run(string(rune('0'+failed)), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			started := make(chan struct{}, 3)
			stopped := make(chan struct{}, 2)
			fire := make(chan struct{})
			runs := make([]func(context.Context) error, 3)
			for i := range runs {
				runs[i] = func(ctx context.Context) error {
					started <- struct{}{}
					if i == failed {
						<-fire
						return terminal
					}
					<-ctx.Done()
					stopped <- struct{}{}
					return ctx.Err()
				}
			}
			result := make(chan error, 1)
			go func() { result <- supervise(ctx, runs...) }()
			for range 3 {
				<-started
			}
			close(fire)
			if err := <-result; !errors.Is(err, terminal) {
				t.Fatal(err)
			}
			if len(stopped) != 2 {
				t.Fatal("siblings not joined")
			}
		})
	}
}
