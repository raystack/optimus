package postgres

import (
	"embed"
	"fmt"
	"net/http"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/source/httpfs"
	"github.com/jinzhu/gorm"
	"github.com/pkg/errors"

	_ "embed"

	_ "github.com/golang-migrate/migrate/v4/database/postgres" // required for postgres migrate driver
	_ "github.com/lib/pq"
)

//go:embed migrations
var migrationFs embed.FS

const (
	resourcePath = "migrations"
)

// NewHTTPFSMigrator reads the migrations from httpfs and returns the migrate.Migrate
func NewHTTPFSMigrator(DBConnURL string) (*migrate.Migrate, error) {
	src, err := httpfs.New(http.FS(migrationFs), resourcePath)
	if err != nil {
		return &migrate.Migrate{}, fmt.Errorf("db migrator: %v", err)
	}
	return migrate.NewWithSourceInstance("httpfs", src, DBConnURL)
}

// Connect connect to the DB with custom configuration.
func Connect(connURL string, maxIdleConnections, maxOpenConnections int) (*gorm.DB, error) {
	var db *gorm.DB
	var err error

	if db, err = gorm.Open("postgres", connURL); err != nil {
		return nil, err
	}

	db.DB().SetMaxIdleConns(maxIdleConnections)
	db.DB().SetMaxOpenConns(maxOpenConnections)
	db.SingularTable(true)
	return db, nil
}

// Migrate to run up migrations
func Migrate(connURL string) error {
	m, err := NewHTTPFSMigrator(connURL)
	if err != nil {
		return errors.Wrap(err, "db migrator")
	}
	defer m.Close()

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return errors.Wrap(err, "db migrator")
	}
	return nil
}
