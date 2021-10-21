// postgres implementation relies on gorm for queries which is very very
// inefficient at the moment, we are trading convenience with performance
// for example in lot of select stmts, we pull all related relations as well
// even when we don't really need to, most of the times these relation
// queries even in update gets executed for no reason even if user didn't
// intend to update them.
package postgres

import (
	"embed"
	"fmt"
	"net/http"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres" // required for postgres migrate driver
	"github.com/golang-migrate/migrate/v4/source/httpfs"
	"github.com/pkg/errors"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
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
	db, err := gorm.Open(postgres.Open(connURL), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true,
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize postgres db connection")
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxIdleConns(maxIdleConnections)
	sqlDB.SetMaxOpenConns(maxOpenConnections)
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
