package pricefeed

import (
	"context"
	"errors"
	"testing"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafkasource"
)

func TestGroupInvalidConfig(t *testing.T) {
	for _, cfg := range []GroupConfig{{}, {Config: Config{Brokers: []string{"localhost:1"}, Topic: "prices"}, Group: "g"}} {
		if _, err := NewGroup(context.Background(), cfg); !errors.Is(err, ErrConfig) {
			t.Fatalf("invalid config: %v", err)
		}
	}
}

func TestLegacyPrepareCannotAssignUnboundOffsets(t *testing.T) {
	cfg := GroupConfig{Config: Config{Brokers: []string{"localhost:1"}, Topic: "prices", Source: kafkasource.Identity{ClusterID: "cluster", Topic: "prices", TopicID: [16]byte{1}}}, Group: "owners",
		Prepare: func(context.Context, []int32) (map[int32]int64, error) {
			t.Fatal("legacy Prepare called")
			return nil, nil
		}}
	if _, err := NewGroup(context.Background(), cfg); !errors.Is(err, ErrConfig) {
		t.Fatal(err)
	}
}
