package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
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

// Check actual franz-go option compatibility without connecting to a broker.
func TestSingleRecordOptionsAreValid(t *testing.T) {
	cl, err := kgo.NewClient(clientOptions(Config{Brokers: []string{"127.0.0.1:1"}, TransactionalID: "bounded", SingleRecord: true})...)
	if err != nil {
		t.Fatal(err)
	}
	cl.Close()
}
