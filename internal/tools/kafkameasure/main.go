// Command kafkameasure drives the existing transactional producer on a disposable
// broker. Line protocol keeps producer initialization outside measured phases.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

const payload = `{"event_id":"ops020-probe-1","kind":"physical-measurement","version":1}`

func read(broker, topic string, n int, group string) ([]map[string]any, error) {
	opts := []kgo.Opt{kgo.SeedBrokers(broker), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxWait(100 * time.Millisecond)}
	if group == "" {
		opts = append(opts, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}}))
	} else {
		opts = append(opts, kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit())
	}
	c, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows := make([]map[string]any, 0, n)
	for len(rows) < n {
		fs := c.PollFetches(ctx)
		if es := fs.Errors(); len(es) > 0 {
			return nil, es[0].Err
		}
		for _, r := range fs.Records() {
			rows = append(rows, map[string]any{"partition": r.Partition, "offset": r.Offset, "key": string(r.Key), "value": string(r.Value)})
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	if group != "" {
		if err := c.CommitUncommittedOffsets(ctx); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func run() error {
	if len(os.Args) != 3 {
		return fmt.Errorf("usage: kafkameasure broker prefix")
	}
	broker, prefix := os.Args[1], os.Args[2]
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	p, err := kafka.New(ctx, kafka.Config{Brokers: []string{broker}, TransactionalID: prefix + "-measure"})
	cancel()
	if err != nil {
		return err
	}
	defer p.Close()
	publish := func(value string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		return p.Publish(ctx, kafka.Record{Topic: prefix + "-subscription-changes", Key: []byte("probe"), Value: []byte(value)})
	}
	// Warm transaction coordinator and offsets topic before the baseline.
	if err := publish("prepare"); err != nil {
		return err
	}
	if _, err := read(broker, prefix+"-subscription-changes", 1, prefix+"-group"); err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	if err := enc.Encode(map[string]any{"operation": "prepared", "payload": payload, "transactional_id": prefix + "-measure"}); err != nil {
		return err
	}
	scanner := bufio.NewScanner(os.Stdin)
	count := 1
	for scanner.Scan() {
		op := scanner.Text()
		switch op {
		case "publish":
			if err := publish(payload); err != nil {
				return err
			}
			count++
		case "empty":
		case "read":
			rows, err := read(broker, prefix+"-subscription-changes", count, "")
			if err != nil {
				return err
			}
			if len(rows) != count {
				return fmt.Errorf("unexpected record count")
			}
			for i, r := range rows {
				want := payload
				if i == 0 {
					want = "prepare"
				}
				if r["value"] != want || r["key"] != "probe" {
					return fmt.Errorf("unexpected committed bytes")
				}
			}
			if err := enc.Encode(map[string]any{"operation": op, "isolation": "read_committed", "records": rows}); err != nil {
				return err
			}
			continue
		default:
			return fmt.Errorf("unknown operation")
		}
		if err := enc.Encode(map[string]any{"operation": op, "committed_records": count}); err != nil {
			return err
		}
	}
	return scanner.Err()
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
