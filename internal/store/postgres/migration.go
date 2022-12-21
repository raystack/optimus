package postgres

import (
	"embed"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres" // required for postgres migrate driver
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed migrations
var migrationFs embed.FS

const (
	resourcePath = "migrations"
)

func NewMigrator(dbConnURL string) (*migrate.Migrate, error) {
	sourceDriver, err := iofs.New(migrationFs, resourcePath)
	if err != nil {
		return nil, fmt.Errorf("error initializing source driver: %w", err)
	}

	return migrate.NewWithSourceInstance("iofs", sourceDriver, dbConnURL)
}

// Migrate to run up migrations
func Migrate(connURL string) error {
	m, err := NewMigrator(connURL)
	if err != nil {
		return fmt.Errorf("db migrator: %w", err)
	}
	defer m.Close()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("db migrator: %w", err)
	}
	return nil
}

// Rollback to run up migrations
func Rollback(connURL string, count int) error {
	m, err := NewMigrator(connURL)
	if err != nil {
		return fmt.Errorf("db migrator: %w", err)
	}
	defer m.Close()

	if count < 1 {
		return fmt.Errorf("invalid value[%d] for rollback", count)
	}

	err = m.Steps(count * -1)
	if err != nil {
		return fmt.Errorf("db migrator: %w", err)
	}
	return nil
}

func ToVersion(version uint, connURL string) error {
	m, err := NewMigrator(connURL)
	if err != nil {
		return fmt.Errorf("db migrator: %w", err)
	}
	defer m.Close()

	err = m.Migrate(version)
	if err != nil {
		return fmt.Errorf("db migrator: %w", err)
	}
	return nil
}
