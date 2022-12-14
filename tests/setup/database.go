package setup

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/odpf/salt/log"
	"gorm.io/gorm"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/internal/store/postgres"
)

var (
	optimusDB  *gorm.DB
	dbPool     *pgxpool.Pool
	initDBOnce sync.Once
)

func TestDB() *gorm.DB {
	initDBOnce.Do(migrateDB)

	return optimusDB
}

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
	dbConn, err := postgres.Connect(dbConf, os.Stdout)
	if err != nil {
		panic(err)
	}
	if err := dropTables(dbConn); err != nil {
		panic(err)
	}

	logger := log.NewLogrus(log.LogrusWithWriter(os.Stdout))
	optimusVersion := "integration_test"
	m, err := postgres.NewMigration(logger, optimusVersion, dbURL)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	if err := m.Up(ctx); err != nil {
		panic(err)
	}

	pool, err := postgres.Open(dbConf)
	if err != nil {
		panic(err)
	}
	dbPool = pool

	optimusDB = dbConn
}

func dropTables(db *gorm.DB) error {
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
		if err := db.Exec(fmt.Sprintf("drop table if exists %s", table)).Error; err != nil {
			toleratedErrMsg := fmt.Sprintf("table \"%s\" does not exist", table)
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

func TruncateTables(db *gorm.DB) {
	db.Exec("TRUNCATE TABLE backup_old, resource_old CASCADE")
	db.Exec("TRUNCATE TABLE backup CASCADE")
	db.Exec("TRUNCATE TABLE replay CASCADE")
	db.Exec("TRUNCATE TABLE resource CASCADE")

	db.Exec("TRUNCATE TABLE job_run CASCADE")
	db.Exec("TRUNCATE TABLE sensor_run CASCADE")
	db.Exec("TRUNCATE TABLE task_run CASCADE")
	db.Exec("TRUNCATE TABLE hook_run CASCADE")

	db.Exec("TRUNCATE TABLE job CASCADE")

	db.Exec("TRUNCATE TABLE secret CASCADE")
	db.Exec("TRUNCATE TABLE namespace CASCADE")
	db.Exec("TRUNCATE TABLE project CASCADE")

	db.Exec("TRUNCATE TABLE job_deployment CASCADE")

	db.Exec("TRUNCATE TABLE job_upstream CASCADE")
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
	pool.Exec(ctx, "TRUNCATE TABLE project_old, namespace_old CASCADE")

	pool.Exec(ctx, "TRUNCATE TABLE job_deployment CASCADE")

	pool.Exec(ctx, "TRUNCATE TABLE job_upstream CASCADE")
}
