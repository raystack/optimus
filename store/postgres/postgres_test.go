//go:build !unit_test
// +build !unit_test

package postgres

import (
	"os"
	"sync"

	"gorm.io/gorm"
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

	dbConn, err := Connect(dbURL, 1, 1, os.Stdout)
	if err != nil {
		panic(err)
	}
	m, err := NewHTTPFSMigrator(dbURL)
	if err != nil {
		panic(err)
	}
	if err := m.Drop(); err != nil {
		panic(err)
	}
	if err := Migrate(dbURL); err != nil {
		panic(err)
	}

	optimusDB = dbConn
}

func truncateTables(db *gorm.DB) {
	db.Exec("TRUNCATE TABLE backup CASCADE")
	db.Exec("TRUNCATE TABLE replay CASCADE")
	db.Exec("TRUNCATE TABLE resource CASCADE")

	db.Exec("TRUNCATE TABLE job_run CASCADE")
	db.Exec("TRUNCATE TABLE instance CASCADE")

	db.Exec("TRUNCATE TABLE job CASCADE")

	db.Exec("TRUNCATE TABLE secret CASCADE")
	db.Exec("TRUNCATE TABLE namespace CASCADE")
	db.Exec("TRUNCATE TABLE project CASCADE")
}
