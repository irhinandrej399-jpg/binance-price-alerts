package kafka

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestConfigAndDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, cfg := range []Config{{}, {Brokers: []string{"127.0.0.1:1"}}, {TransactionalID: "probe"}} {
		if _, err := New(ctx, cfg); !errors.Is(err, ErrConfig) {
			t.Fatalf("invalid config: %v", err)
		}
	}
	if _, err := New(context.Background(), Config{Brokers: []string{"127.0.0.1:1"}, TransactionalID: "probe"}); !errors.Is(err, ErrDeadline) {
		t.Fatalf("missing deadline: %v", err)
	}
}
