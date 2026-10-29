package pricefeed

import (
	"context"
	"errors"
	"testing"
)

func TestGroupInvalidConfig(t *testing.T) {
	for _, cfg := range []GroupConfig{{}, {Config: Config{Brokers: []string{"localhost:1"}, Topic: "prices"}, Group: "g"}} {
		if _, err := NewGroup(context.Background(), cfg); !errors.Is(err, ErrConfig) {
			t.Fatalf("invalid config: %v", err)
		}
	}
}
