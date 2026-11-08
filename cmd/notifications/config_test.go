package main

import (
	"strings"
	"testing"
)

func validEnv() map[string]string {
	return map[string]string{
		"HTTP_ADDR": "127.0.0.1:8083", "NOTIFICATIONS_DATABASE_URL": "postgres://notifications_runtime:secret@localhost/notifications",
		"KAFKA_BROKERS": "localhost:9092", "DELIVERY_REQUESTS_TOPIC": "local-delivery", "NOTIFICATIONS_GROUP": "local-notifications",
		"NOTIFICATIONS_CONSUMER_ID": "logical-notifications", "NOTIFICATIONS_GENERATION": "bootstrap-1",
		"KAFKA_CLUSTER_ID": "cluster-1", "DELIVERY_REQUESTS_TOPIC_ID": "0102030405060708090a0b0c0d0e0f10",
		"API_RPC_ADDR": "localhost:8085", "RPC_SERVER_NAME": "market.test", "RPC_IDENTITY": "spiffe://process.test/notifications", "RPC_CA_FILE": "ca.pem", "RPC_CERT_FILE": "cert.pem", "RPC_KEY_FILE": "key.pem",
		"TELEGRAM_BASE_URL": "http://127.0.0.1:12345", "TELEGRAM_BOT_TOKEN": "123:fake-secret", "TELEGRAM_TIMEOUT": "1s", "BOT_RATE": "25", "CHAT_INTERVAL": "1s",
		"RETRY_BASE_DELAY": "100ms", "RETRY_MAX_DELAY": "2s", "CONDITION_INTERVAL": "100ms", "MAX_FAILURES": "5",
		"LEASE_TTL": "5s", "RECOVERY_INTERVAL": "1s", "REPORT_LIMIT": "10", "WORKER_CONCURRENCY": "2", "WORKER_INTERVAL": "20ms", "WORKER_TIMEOUT": "3s", "WRITE_TIMEOUT": "1s",
	}
}
func TestConfig(t *testing.T) {
	env := validEnv()
	if _, e := loadConfig(func(k string) string { return env[k] }); e != nil {
		t.Fatal(e)
	}
	for k := range env {
		t.Run("missing_"+k, func(t *testing.T) {
			m := validEnv()
			delete(m, k)
			if _, e := loadConfig(func(k string) string { return m[k] }); e == nil {
				t.Fatal("missing config accepted")
			}
		})
	}
	for _, tc := range []struct{ k, v string }{
		{"NOTIFICATIONS_CONSUMER_ID", " "}, {"NOTIFICATIONS_GENERATION", " "}, {"KAFKA_CLUSTER_ID", " "}, {"DELIVERY_REQUESTS_TOPIC_ID", "00000000000000000000000000000000"}, {"DELIVERY_REQUESTS_TOPIC_ID", "xx"}, {"DELIVERY_REQUESTS_TOPIC_ID", "01"},
		{"NOTIFICATIONS_DATABASE_URL", "postgres://api_runtime:secret@localhost/api"}, {"HTTP_ADDR", ":0"}, {"API_RPC_ADDR", "http://localhost:8085"}, {"KAFKA_BROKERS", "localhost:9092,"}, {"NOTIFICATIONS_GROUP", ".."}, {"RPC_IDENTITY", "invalid"},
		{"TELEGRAM_BASE_URL", "https://secret@host/path"}, {"TELEGRAM_BASE_URL", "https://host/botsecret"}, {"TELEGRAM_BOT_TOKEN", "0:secret"}, {"BOT_RATE", "0"}, {"BOT_RATE", "1000001"}, {"CHAT_INTERVAL", "1ns"},
		{"RETRY_MAX_DELAY", "1ms"}, {"MAX_FAILURES", "4294967296"}, {"LEASE_TTL", "61s"}, {"RECOVERY_INTERVAL", "1ms"}, {"REPORT_LIMIT", "1001"}, {"WORKER_CONCURRENCY", "1001"}, {"WORKER_INTERVAL", "0s"}, {"WORKER_TIMEOUT", "6s"}, {"TELEGRAM_TIMEOUT", "4s"},
	} {
		t.Run(tc.k+"_invalid", func(t *testing.T) {
			m := validEnv()
			m[tc.k] = tc.v
			_, e := loadConfig(func(k string) string { return m[k] })
			if e == nil {
				t.Fatal("invalid config accepted")
			}
			if strings.Contains(e.Error(), "secret") {
				t.Fatal("secret leaked")
			}
		})
	}
}
