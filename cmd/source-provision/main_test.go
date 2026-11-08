package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestBadInputsDoNotDiscloseSecrets(t *testing.T) {
	for _, args := range [][]string{{"--dsn=secret-token"}, {"--mode=reset", "--inventory=x"}, {"--inventory=x", "secret-token"}, {"--inventory=x"}} {
		var out bytes.Buffer
		err := run(args, func(k string) string {
			if k == "NOTIFICATIONS_DATABASE_URL" {
				return "postgres://secret-token@invalid"
			}
			return ""
		}, &out)
		if err == nil || strings.Contains(err.Error(), "secret-token") || out.Len() != 0 {
			t.Fatal("invalid input or secret disclosure")
		}
	}
}
