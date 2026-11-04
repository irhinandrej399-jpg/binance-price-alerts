// Command pgmeasure writes synthetic accounts through the existing API adapter.
// It is a measurement fixture, never a service runtime or a load qualification.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/api/account"
	"github.com/irhinandrej399-jpg/binance-price-alerts/internal/platform/postgres"
)

func main() {
	rows := flag.Int("rows", 1000, "number of distinct synthetic accounts")
	flag.Parse()
	if *rows < 1 || *rows > 100000 {
		fmt.Fprintln(os.Stderr, "rows must be 1..100000")
		os.Exit(1)
	}
	if err := run(*rows); err != nil {
		fmt.Fprintln(os.Stderr, "measurement adapter failed")
		os.Exit(1)
	}
}

func run(rows int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	db, err := postgres.Open(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	defer db.Close()
	var role, database string
	if err := db.QueryRow(ctx, "SELECT current_user, current_database()").Scan(&role, &database); err != nil {
		return err
	}
	if role != "api_runtime" || database != "api" {
		return fmt.Errorf("wrong owner")
	}
	repo := account.New(db)
	for i := 0; i < rows; i++ {
		if _, err := repo.Create(ctx, fmt.Sprintf("physical-%08d@example.invalid", i), "synthetic-measurement-hash"); err != nil {
			return err
		}
	}
	// Flush this session's counters before closing. No stats reset and no privilege elevation.
	if _, err := db.Exec(ctx, "SELECT pg_stat_force_next_flush()"); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"rows": rows, "role": role, "database": database, "adapter": "account.Repository.Create"})
}
