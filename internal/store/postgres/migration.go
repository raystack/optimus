package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/odpf/salt/log"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/odpf/optimus/internal/store"
)

//go:embed migrations
var migrationFs embed.FS

type migrationStep struct {
	CurrentOptimusVersion   string
	CurrentMigrationVersion uint
	PreviousOptimusVersion  string
	CreatedAt               time.Time
}

type migration struct {
	incomingOptimusVersion string
	dbConnURL              string

	logger log.Logger
}

// NewMigration initializes migration mechanism specific for postgres
func NewMigration(logger log.Logger, incomingOptimusVersion, dbConnURL string) (store.Migration, error) {
	if logger == nil {
		return nil, errors.New("logger is nil")
	}
	if incomingOptimusVersion == "" {
		return nil, errors.New("incoming optimus version is empty")
	}
	if dbConnURL == "" {
		return nil, errors.New("database connection url is empty")
	}
	return &migration{
		incomingOptimusVersion: incomingOptimusVersion,
		dbConnURL:              dbConnURL,
		logger:                 logger,
	}, nil
}

func (m *migration) Up(ctx context.Context) error {
	dbClient, dbClientCleanup, err := m.newDBClient()
	if err != nil {
		return fmt.Errorf("error initializing db client: %w", err)
	}
	defer dbClientCleanup()

	if err := dbClient.WithContext(ctx).AutoMigrate(&migrationStep{}); err != nil {
		return fmt.Errorf("error setting up migration_steps: %w", err)
	}

	latestStep, err := m.getLatestMigrationStep(ctx, dbClient)
	if err != nil {
		return fmt.Errorf("error getting the latest migration step: %w", err)
	}
	if m.incomingOptimusVersion < latestStep.CurrentOptimusVersion {
		return fmt.Errorf("optimus version [%s] should be higher or equal than existing [%s]", m.incomingOptimusVersion, latestStep.CurrentOptimusVersion)
	}
	if m.incomingOptimusVersion == latestStep.CurrentOptimusVersion {
		m.logger.Warn(fmt.Sprintf("migration up is skipped because optimus version [%s] is the same as current one", m.incomingOptimusVersion))
		return nil
	}

	migrationClient, migrationClientCleanup, err := m.newMigrationClient()
	if err != nil {
		return fmt.Errorf("error initializing migration client: %w", err)
	}
	defer migrationClientCleanup()

	if err := migrationClient.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("error executing migration up: %w", err)
	}
	newVersion, _, err := migrationClient.Version()
	if err != nil {
		return fmt.Errorf("error getting current migration version: %w", err)
	}

	newMigrationVersion := &migrationStep{
		CurrentOptimusVersion:   m.incomingOptimusVersion,
		CurrentMigrationVersion: newVersion,
		PreviousOptimusVersion:  latestStep.CurrentOptimusVersion,
		CreatedAt:               time.Now(),
	}
	return m.addMigrationStep(ctx, dbClient, newMigrationVersion)
}

func (m *migration) Rollback(ctx context.Context) error {
	dbClient, dbClientCleanup, err := m.newDBClient()
	if err != nil {
		return fmt.Errorf("error initializing db client: %w", err)
	}
	defer dbClientCleanup()

	if err := dbClient.WithContext(ctx).AutoMigrate(&migrationStep{}); err != nil {
		return fmt.Errorf("error setting up migration_steps: %w", err)
	}

	latestStep, err := m.getLatestMigrationStep(ctx, dbClient)
	if err != nil {
		return err
	}
	if m.incomingOptimusVersion != latestStep.CurrentOptimusVersion {
		return fmt.Errorf("expecting optimus with version [%s] but got [%s]", latestStep.CurrentOptimusVersion, m.incomingOptimusVersion)
	}

	previousMigrationVersion, err := m.getMigrationVersion(ctx, dbClient, latestStep.PreviousOptimusVersion)
	if err != nil {
		return err
	}
	if previousMigrationVersion == 0 {
		m.logger.Warn("migration rollback is skipped because previous migration version is not registered")
		return nil
	}

	migrationClient, migrationClientCleanup, err := m.newMigrationClient()
	if err != nil {
		return fmt.Errorf("error initializing migration client: %w", err)
	}
	defer migrationClientCleanup()

	if err := migrationClient.Migrate(previousMigrationVersion); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("error migrating to version [%d]: %w", previousMigrationVersion, err)
	}
	return m.removeMigrationStep(ctx, dbClient, latestStep)
}

func (m *migration) newMigrationClient() (*migrate.Migrate, func(), error) {
	path := "migrations"
	sourceDriver, err := iofs.New(migrationFs, path)
	if err != nil {
		return nil, nil, fmt.Errorf("error initializing source driver: %w", err)
	}
	name := "iofs"
	migrationClient, err := migrate.NewWithSourceInstance(name, sourceDriver, m.dbConnURL)
	if err != nil {
		return nil, nil, fmt.Errorf("error initializing migration client: %w", err)
	}
	cleanup := func() {
		sourceErr, databaseErr := migrationClient.Close()
		if sourceErr != nil {
			m.logger.Error("source driver error encountered when closing migration connection: %w", sourceErr)
		}
		if databaseErr != nil {
			m.logger.Error("database error encountered when closing migration connection: %w", databaseErr)
		}
	}
	return migrationClient, cleanup, nil
}

func (m *migration) newDBClient() (*gorm.DB, func(), error) {
	dbClient, err := gorm.Open(postgres.Open(m.dbConnURL))
	if err != nil {
		return nil, nil, fmt.Errorf("error initializing db client: %w", err)
	}
	cleanup := func() {
		db, err := dbClient.DB()
		if err != nil {
			m.logger.Error("error getting db: %w", err)
			return
		}
		if err := db.Close(); err != nil {
			m.logger.Error("error encountered when closing db connection: %w", err)
		}
	}
	return dbClient, cleanup, nil
}

func (*migration) removeMigrationStep(ctx context.Context, db *gorm.DB, oldStep *migrationStep) error {
	return db.WithContext(ctx).
		Where("current_optimus_version = ? and current_migration_version = ? and previous_optimus_version = ?",
			oldStep.CurrentOptimusVersion, oldStep.CurrentMigrationVersion, oldStep.PreviousOptimusVersion).
		Delete(&migrationStep{}).Error
}

func (*migration) getMigrationVersion(ctx context.Context, db *gorm.DB, optimusVersion string) (uint, error) {
	var rst migrationStep
	if err := db.WithContext(ctx).
		Select("current_optimus_version, current_migration_version, previous_optimus_version, created_at").
		Table("migration_steps").
		Where("current_optimus_version = ?", optimusVersion).
		Order("created_at desc limit 1").
		Find(&rst).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, fmt.Errorf("error getting migration version for optimus version [%s]: %w", optimusVersion, err)
	}
	return rst.CurrentMigrationVersion, nil
}

func (m *migration) addMigrationStep(ctx context.Context, db *gorm.DB, newStep *migrationStep) error {
	var existingSteps []migrationStep
	if err := db.WithContext(ctx).
		Where(newStep).
		First(&existingSteps).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("error getting existing steps: %w", err)
	}
	if len(existingSteps) > 0 {
		m.logger.Warn("migration step is not added because it already exists")
		return nil
	}
	return db.WithContext(ctx).Create(newStep).Error
}

func (*migration) getLatestMigrationStep(ctx context.Context, db *gorm.DB) (*migrationStep, error) {
	var rst migrationStep
	if err := db.WithContext(ctx).
		Select("m.current_optimus_version, m.current_migration_version, m.previous_optimus_version, m.created_at").
		Table("migration_steps m").
		Joins("right join schema_migrations s on m.current_migration_version = s.version").
		Order("m.created_at desc limit 1").
		Find(&rst).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) && !strings.Contains(err.Error(), "42P01") {
		return nil, fmt.Errorf("error getting existing step: %w", err)
	}
	return &rst, nil
}
