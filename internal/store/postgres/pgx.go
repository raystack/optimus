package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/goto/optimus/config"
)

// Open will connect to the DB with custom configuration
func Open(config config.DBConfig) (*pgxpool.Pool, error) {
	pgxConf, err := pgxpool.ParseConfig(config.DSN)
	if err != nil {
		return nil, err
	}

	if config.MaxOpenConnection > 0 {
		pgxConf.MaxConns = int32(config.MaxOpenConnection)
	}
	if config.MinOpenConnection > 0 {
		pgxConf.MinConns = int32(config.MinOpenConnection)
	}

	pgxConf.ConnConfig.Tracer = newTracer()

	dbPool, err := pgxpool.NewWithConfig(context.Background(), pgxConf)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	return dbPool, nil // cleanup to be done with dbPool.Close()
}
