package setup

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/internal/store/postgres"
)

var (
	dbPool     *pgxpool.Pool
	initDBOnce sync.Once
)

func TestPool() *pgxpool.Pool {
	initDBOnce.Do(migrateDB)

	return dbPool
}

func mustReadDBConfig() string {
	dbURL, ok := os.LookupEnv("TEST_OPTIMUS_DB_URL")
	if ok {
		return dbURL
	}

	// Did not find a suitable way to read db config
	panic("unable to find config for optimus test db")
}

// migrateDB takes around 700ms to drop and recreate db + run migrations
func migrateDB() {
	dbURL := mustReadDBConfig()

	dbConf := config.DBConfig{
		DSN:               dbURL,
		MinOpenConnection: 1,
		MaxOpenConnection: 2,
	}

	pool, err := postgres.Open(dbConf)
	if err != nil {
		panic(err)
	}
	dbPool = pool

	m, err := postgres.NewMigrator(dbURL)
	if err != nil {
		panic(err)
	}

	cleanDB(m, pool)

	if err = postgres.Migrate(dbURL); err != nil {
		panic(err)
	}
}

func cleanDB(m *migrate.Migrate, pool *pgxpool.Pool) {
	shouldDrop := false
	_, ok := os.LookupEnv("TEST_OPTIMUS_DROP_DB")
	if !ok {
		shouldDrop = true
	}

	if shouldDrop {
		if err := m.Drop(); err != nil {
			panic(err)
		}
		return
	}

	if err := dropTables(pool); err != nil {
		panic(err)
	}
}

func dropTables(db *pgxpool.Pool) error {
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancelFunc()

	tablesToDelete := []string{
		"instance",
		"hook_run",
		"sensor_run",
		"task_run",
		"job_run_old",
		"job_run",
		"backup",
		"backup_old",
		"secret",
		"secret_old",
		"job_deployment",
		"job_source",
		"replay",
		"schema_migrations",
		"job_old",
		"job_upstream",
		"job",
		"resource",
		"resource_old",
		"namespace",
		"namespace_old",
		"project",
		"project_old",
		"migration_steps",
	}
	var errMsgs []string
	for _, table := range tablesToDelete {
		if _, err := db.Exec(ctx, "drop table if exists "+table); err != nil {
			toleratedErrMsg := fmt.Sprintf("table %q does not exist", table)
			if !strings.Contains(err.Error(), toleratedErrMsg) {
				errMsgs = append(errMsgs, err.Error())
			}
		}
	}
	if len(errMsgs) > 0 {
		return fmt.Errorf("error encountered when dropping tables: %s", strings.Join(errMsgs, ","))
	}
	return nil
}

func TruncateTablesWith(pool *pgxpool.Pool) {
	ctx := context.Background()
	pool.Exec(ctx, "TRUNCATE TABLE backup_old, resource_old CASCADE")
	pool.Exec(ctx, "TRUNCATE TABLE backup CASCADE")
	pool.Exec(ctx, "TRUNCATE TABLE replay CASCADE")
	pool.Exec(ctx, "TRUNCATE TABLE resource CASCADE")

	pool.Exec(ctx, "TRUNCATE TABLE job_run CASCADE")
	pool.Exec(ctx, "TRUNCATE TABLE sensor_run CASCADE")
	pool.Exec(ctx, "TRUNCATE TABLE task_run CASCADE")
	pool.Exec(ctx, "TRUNCATE TABLE hook_run CASCADE")

	pool.Exec(ctx, "TRUNCATE TABLE job CASCADE")

	pool.Exec(ctx, "TRUNCATE TABLE secret CASCADE")
	pool.Exec(ctx, "TRUNCATE TABLE namespace CASCADE")
	pool.Exec(ctx, "TRUNCATE TABLE project CASCADE")
	pool.Exec(ctx, "TRUNCATE TABLE project_old, namespace_old, secret_old CASCADE")

	pool.Exec(ctx, "TRUNCATE TABLE job_deployment CASCADE")

	pool.Exec(ctx, "TRUNCATE TABLE job_upstream CASCADE")
}
