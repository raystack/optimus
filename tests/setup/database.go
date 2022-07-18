package setup

import (
	"os"
	"sync"

	"gorm.io/gorm"

	"github.com/odpf/optimus/store/postgres"
)

var (
	optimusDB  *gorm.DB
	initDBOnce sync.Once
)

func TestDB() *gorm.DB {
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

	dbConn, err := postgres.Connect(dbURL, 1, 1, os.Stdout)
	if err != nil {
		panic(err)
	}
	m, err := postgres.NewHTTPFSMigrator(dbURL)
	if err != nil {
		panic(err)
	}
	if err := m.Drop(); err != nil {
		panic(err)
	}
	if err := postgres.Migrate(dbURL); err != nil {
		panic(err)
	}

	optimusDB = dbConn
}

func TruncateTables(db *gorm.DB) {
	db.Exec("TRUNCATE TABLE backup CASCADE")
	db.Exec("TRUNCATE TABLE replay CASCADE")
	db.Exec("TRUNCATE TABLE resource CASCADE")

	db.Exec("TRUNCATE TABLE job_run CASCADE")
	db.Exec("TRUNCATE TABLE job_run_old CASCADE")
	db.Exec("TRUNCATE TABLE instance CASCADE")

	db.Exec("TRUNCATE TABLE job CASCADE")

	db.Exec("TRUNCATE TABLE secret CASCADE")
	db.Exec("TRUNCATE TABLE namespace CASCADE")
	db.Exec("TRUNCATE TABLE project CASCADE")

	db.Exec("TRUNCATE TABLE job_deployment CASCADE")

	db.Exec("TRUNCATE TABLE job_source CASCADE")
}
