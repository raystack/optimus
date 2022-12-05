//go:build !unit_test
// +build !unit_test

package postgres_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/odpf/salt/log"
	"gorm.io/gorm"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/internal/store/postgres"
)

var (
	optimusDB  *gorm.DB
	initDBOnce sync.Once
)

func setupDB() *gorm.DB {
	initDBOnce.Do(migrateDB)

	return optimusDB
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
		MaxIdleConnection: 1,
		MaxOpenConnection: 1,
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
		"secret",
		"job_deployment",
		"job_source",
		"replay",
		"schema_migrations",
		"job",
		"resource",
		"namespace",
		"project",
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

func truncateTables(db *gorm.DB) {
	db.Exec("TRUNCATE TABLE backup CASCADE")
	db.Exec("TRUNCATE TABLE replay CASCADE")
	db.Exec("TRUNCATE TABLE resource CASCADE")

	db.Exec("TRUNCATE TABLE job_run CASCADE")
	db.Exec("TRUNCATE TABLE sensor_run CASCADE")
	db.Exec("TRUNCATE TABLE task_run CASCADE")
	db.Exec("TRUNCATE TABLE hook_run CASCADE")

	db.Exec("TRUNCATE TABLE job_run_old CASCADE")
	db.Exec("TRUNCATE TABLE instance CASCADE")

	db.Exec("TRUNCATE TABLE job CASCADE")

	db.Exec("TRUNCATE TABLE secret CASCADE")
	db.Exec("TRUNCATE TABLE namespace CASCADE")
	db.Exec("TRUNCATE TABLE project CASCADE")

	db.Exec("TRUNCATE TABLE job_deployment CASCADE")

	db.Exec("TRUNCATE TABLE job_source CASCADE")
}
