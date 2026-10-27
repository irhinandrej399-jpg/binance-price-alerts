// Package postgres opens runtime PostgreSQL pools. Migrations run separately.
package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Open parses a PostgreSQL URL or keyword DSN and verifies connectivity before
// returning. The caller supplies a context deadline and must close the pool.
// Driver configuration/connection errors can contain the DSN, so they are not
// exposed. Query errors after Open remain the caller's responsibility.
func Open(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	if dsn == "" {
		return nil, errors.New("PostgreSQL configuration is required")
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("invalid PostgreSQL configuration")
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, errors.New("cannot create PostgreSQL pool")
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, errors.New("cannot connect to PostgreSQL")
	}
	return pool, nil
}
