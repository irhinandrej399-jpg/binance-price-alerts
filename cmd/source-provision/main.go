// source-provision is an explicit operator command; service runtimes never call it.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"strings"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/notifications/intake"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/postgres"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/sourceprovision"
	"github.com/twmb/franz-go/pkg/kgo"
)

func run(args []string, get func(string) string, out io.Writer) error {
	flags := flag.NewFlagSet("source-provision", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	mode := flags.String("mode", "verify", "")
	path := flags.String("inventory", "", "")
	prefix := flags.String("prefix", "", "")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *path == "" || (*mode != "init" && *mode != "resume" && *mode != "verify") {
		return errors.New("invalid options")
	}
	dsn := get("NOTIFICATIONS_DATABASE_URL")
	secretFile := get("NOTIFICATIONS_DATABASE_URL_FILE")
	if (dsn == "") == (secretFile == "") {
		return errors.New("provide exactly one notifications database secret input")
	}
	if secretFile != "" {
		b, err := os.ReadFile(secretFile)
		if err != nil || len(b) > 65536 {
			return errors.New("database secret file unavailable")
		}
		dsn = strings.TrimSpace(string(b))
	}
	brokers := strings.Split(get("KAFKA_BROKERS"), ",")
	for _, b := range brokers {
		if strings.TrimSpace(b) == "" {
			return errors.New("Kafka brokers required")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pool, err := postgres.Open(ctx, dsn)
	if err != nil {
		return errors.New("notifications database unavailable")
	}
	defer pool.Close()
	var role, db string
	if pool.QueryRow(ctx, "SELECT current_user,current_database()").Scan(&role, &db) != nil || role != "notifications_runtime" || db != "notifications" {
		return errors.New("notifications runtime database role required")
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return errors.New("Kafka configuration unavailable")
	}
	defer client.Close()
	if err = sourceprovision.Run(ctx, *mode, *path, *prefix, sourceprovision.Kafka{Client: client}, intake.NewStore(pool)); err != nil {
		// Driver/server errors may contain credentials or query data; never print them.
		return errors.New("provisioning refused; keep inventory, stop services, check sources and SQL provenance before resume")
	}
	inv, err := sourceprovision.Load(*path)
	if err != nil {
		return errors.New("inventory unavailable")
	}
	return json.NewEncoder(out).Encode(inv.Env())
}
func main() {
	if run(os.Args[1:], os.Getenv, os.Stdout) != nil {
		_, _ = io.WriteString(os.Stderr, "source provisioning failed; check options, secret inputs, runtime role, metadata and persisted provenance; preserve existing inventory\n")
		os.Exit(1)
	}
}
