package priceconnection

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/event"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/contracts/priceobserved"
	pb "github.com/irhinandrej399-jpg/binance-price-alerts/internal/gen/marketdata/v1"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/matcher/pricefeed"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestIntegrationGroupRegisterLatest(t *testing.T) {
	if os.Getenv("KAFKA_INTEGRATION") != "1" {
		t.Skip("real Kafka: bash infra/kafka/verify-go.sh")
	}
	broker, prefix := os.Getenv("KAFKA_TEST_BROKER"), os.Getenv("KAFKA_PRICEFEED_TEST_PREFIX")
	if broker == "" || prefix == "" {
		t.Fatal("opt-in requires broker and prefix")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	topic := prefix + "-group-latest"
	p, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.TransactionalID(topic))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	req := kmsg.NewPtrCreateTopicsRequest()
	v := kmsg.NewCreateTopicsRequestTopic()
	v.Topic = topic
	v.NumPartitions = 16
	v.ReplicationFactor = 1
	req.Topics = []kmsg.CreateTopicsRequestTopic{v}
	created, err := req.RequestWith(ctx, p)
	if err != nil || len(created.Topics) != 1 || created.Topics[0].ErrorCode != 0 {
		t.Fatal(created, err)
	}
	g, err := pricefeed.NewGroup(ctx, pricefeed.GroupConfig{Config: pricefeed.Config{Brokers: []string{broker}, Topic: topic}, Group: topic,
		Prepare: func(_ context.Context, parts []int32) (map[int32]int64, error) {
			out := map[int32]int64{}
			for _, p := range parts {
				out[p] = 0
			}
			return out, nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	a, err := g.NextAssignment(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	part := pricefeed.PairPartition("BTCUSDT")
	var registration *pricefeed.Registration
	var delivered pricefeed.Update
	c, err := Connect(ctx, "BTCUSDT", part, func(ctx context.Context, pair string, part int32) (Registration, error) {
		r, e := a.Register(ctx, pair, part)
		registration = r
		return r, e
	}, quoteFunc(func(readCtx context.Context, _ string) (*pb.LatestResponse, error) {
		if registration == nil {
			return nil, errors.New("Latest preceded registration")
		}
		payload, e := priceobserved.Encode(event.Envelope{EventID: "during-latest", Type: priceobserved.Type, Version: 1, OccurredAt: now, CorrelationID: "group-bootstrap", OrderingKey: "BTCUSDT"}, observed(2, "101"))
		if e != nil {
			return nil, e
		}
		if e = p.BeginTransaction(); e != nil {
			return nil, e
		}
		if e = p.ProduceSync(readCtx, &kgo.Record{Topic: topic, Key: []byte("BTCUSDT"), Value: payload}).FirstErr(); e != nil {
			return nil, e
		}
		if e = p.EndTransaction(readCtx, kgo.TryCommit); e != nil {
			return nil, e
		}
		// The one dispatcher must put this record into BOTH the complete stream
		// and the pair registration before the stale Latest response is merged.
		delivered, e = a.Next(readCtx)
		if e != nil {
			return nil, e
		}
		return response(observed(1, "99")), nil
	}), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	q, err := c.Current()
	assertQuote(t, q, err, 2, "101")
	if delivered.Position.Offset < registration.Start().Offset || delivered.Position.Partition != part {
		t.Fatal(delivered)
	}
	g.Close()
	if _, err := c.Current(); !errors.Is(err, pricefeed.ErrClosed) {
		t.Fatal("readiness survived group close", err)
	}
}
