// kafka-cleaner is an explicit local maintenance command, not a fifth service.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/cleaner"
	"github.com/twmb/franz-go/pkg/kgo"
)

type config struct {
	Brokers   []string                    `json:"brokers"`
	Endpoints map[string]cleaner.Endpoint `json:"endpoints"`
}

func decode(path string, out any) error {
	f, e := os.Open(path)
	if e != nil {
		return cleaner.ErrConfig
	}
	defer f.Close()
	data, e := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if e != nil || len(data) > 1<<20 {
		return cleaner.ErrConfig
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil || d.Decode(new(any)) != io.EOF {
		return cleaner.ErrConfig
	}
	return nil
}
func run(parent context.Context, args []string, out, errOut io.Writer) int {
	fs := flag.NewFlagSet("kafka-cleaner", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	operation := fs.String("operation", "", "init, inspect, register, retire or clean")
	dir := fs.String("state-dir", "", "authoritative private durable directory")
	configFile := fs.String("config", "", "Kafka/RPC client configuration")
	inventory := fs.String("inventory", "", "complete initial inventory JSON")
	consumer := fs.String("consumer", "", "exact consumer descriptor JSON")
	limit := fs.Int64("max-offsets", 1000, "maximum numeric offsets per clean (1..1000000)")
	timeout := fs.Duration("timeout", 15*time.Second, "whole command budget (up to 30s)")
	bad := func() int { _, _ = io.WriteString(errOut, "invalid cleaner configuration\n"); return 2 }
	if fs.Parse(args) != nil || fs.NArg() != 0 || *dir == "" || *timeout <= 0 || *timeout > 30*time.Second {
		return bad()
	}
	ctx, cancel := context.WithTimeout(parent, *timeout)
	defer cancel()
	store := cleaner.NewStore(*dir)
	if *operation == "inspect" {
		state, e := store.Inspect(ctx)
		if e != nil {
			_, _ = io.WriteString(errOut, "cleaner registry unavailable\n")
			return 1
		}
		if json.NewEncoder(out).Encode(state) != nil {
			return 1
		}
		return 0
	}
	if *operation != "init" && *operation != "register" && *operation != "retire" && *operation != "clean" {
		return bad()
	}
	if *limit < 1 || *limit > 1_000_000 {
		return bad()
	}
	var cfg config
	if decode(*configFile, &cfg) != nil || len(cfg.Brokers) < 1 || len(cfg.Brokers) > 16 {
		return bad()
	}
	for _, b := range cfg.Brokers {
		if b == "" {
			return bad()
		}
	}
	reader, e := cleaner.NewRPCReader(cfg.Endpoints)
	if e != nil {
		return bad()
	}
	defer reader.Close()
	broker, e := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.DialTimeout(3*time.Second), kgo.RequestTimeoutOverhead(time.Second))
	if e != nil {
		return bad()
	}
	defer broker.Close()
	engine := cleaner.New(store, cleaner.KafkaBroker{Client: broker}, reader)
	switch *operation {
	case "init":
		var in cleaner.Inventory
		if decode(*inventory, &in) != nil {
			return bad()
		}
		e = engine.Init(ctx, in)
	case "register", "retire":
		var c cleaner.Consumer
		if decode(*consumer, &c) != nil {
			return bad()
		}
		if *operation == "register" {
			e = engine.Register(ctx, c)
		} else {
			e = engine.Retire(ctx, c)
		}
	case "clean":
		var result cleaner.Result
		result, e = engine.Clean(ctx, *limit)
		if json.NewEncoder(out).Encode(result) != nil {
			return 1
		}
	}
	if e != nil {
		_, _ = io.WriteString(errOut, "cleaner operation failed; registry reservation retained\n")
		return 1
	}
	if *operation != "clean" {
		_, _ = io.WriteString(out, "{\"status\":\"ok\"}\n")
	}
	return 0
}
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}
