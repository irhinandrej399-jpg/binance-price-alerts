package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func validEnv() map[string]string {
	offsets := map[int32]int64{}
	for p := int32(0); p < 16; p++ {
		offsets[p] = 17
	}
	b, _ := json.Marshal(offsets)
	return map[string]string{"KAFKA_CLUSTER_ID": "test-cluster", "PRICES_TOPIC_ID": "01000000000000000000000000000000", "SUBSCRIPTION_CHANGES_TOPIC_ID": "02000000000000000000000000000000", "HTTP_ADDR": "127.0.0.1:8082", "MATCHER_DATABASE_URL": "postgres://matcher_runtime:secret@localhost/matcher", "KAFKA_BROKERS": "localhost:9092", "PRICES_TOPIC": "local-prices", "SUBSCRIPTION_CHANGES_TOPIC": "local-changes", "MATCHER_ENVIRONMENT": "local", "MATCHER_GROUP": "local-matcher", "MATCHER_REPLICA_ID": "one", "PRICES_BOOTSTRAP": string(b), "API_RPC_ADDR": "localhost:8085", "LATEST_RPC_ADDR": "localhost:8086", "RPC_SERVER_NAME": "market.test", "RPC_CA_FILE": "ca.pem", "RPC_CERT_FILE": "cert.pem", "RPC_KEY_FILE": "key.pem", "SNAPSHOT_TTL": "1h", "REPLAY_TTL": "1h", "HEAD_INTERVAL": "1s", "SNAPSHOT_PAGE_SIZE": "1000", "REPLAY_PAGE_SIZE": "100"}
}
func TestConfig(t *testing.T) {
	env := validEnv()
	c, err := loadConfig(func(k string) string { return env[k] })
	if err != nil || len(c.bootstrap) != 16 || c.bootstrap[15] != 17 {
		t.Fatal("valid explicit bootstrap rejected", err)
	}
	for k := range env {
		t.Run("missing_"+k, func(t *testing.T) {
			copy := validEnv()
			delete(copy, k)
			if _, err := loadConfig(func(k string) string { return copy[k] }); err == nil {
				t.Fatal("missing required config accepted")
			}
		})
	}
	for _, tc := range []struct{ k, v string }{
		{"KAFKA_CLUSTER_ID", " "}, {"PRICES_TOPIC_ID", "00000000000000000000000000000000"}, {"SUBSCRIPTION_CHANGES_TOPIC_ID", "bad"}, {"HTTP_ADDR", ":0"}, {"KAFKA_BROKERS", "localhost:9092,"}, {"PRICES_TOPIC", ".."}, {"SUBSCRIPTION_CHANGES_TOPIC", "local-prices"},
		{"MATCHER_DATABASE_URL", "postgres://api_runtime:secret@localhost/api"}, {"API_RPC_ADDR", "http://localhost:8080"},
		{"MATCHER_REPLICA_ID", " "}, {"REPLAY_TTL", "0s"}, {"SNAPSHOT_TTL", "25h"}, {"HEAD_INTERVAL", "0s"}, {"REPLAY_PAGE_SIZE", "1001"},
		{"PRICES_BOOTSTRAP", "{}"},
		{"PRICES_BOOTSTRAP", strings.Replace(env["PRICES_BOOTSTRAP"], `"0":17`, `"0":null`, 1)}, {"PRICES_BOOTSTRAP", strings.Replace(env["PRICES_BOOTSTRAP"], `"0":17`, `"0":-1`, 1)},
		{"PRICES_BOOTSTRAP", strings.Replace(env["PRICES_BOOTSTRAP"], `"0":17`, `"0":17,"0":18`, 1)},
		{"PRICES_BOOTSTRAP", env["PRICES_BOOTSTRAP"] + ` {}`},
	} {
		t.Run(tc.k+tc.v[:1], func(t *testing.T) {
			copy := validEnv()
			copy[tc.k] = tc.v
			_, err := loadConfig(func(k string) string { return copy[k] })
			if err == nil {
				t.Fatal("invalid configuration accepted")
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatal("credential leaked")
			}
		})
	}
}
