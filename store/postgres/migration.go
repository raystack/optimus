package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/odpf/optimus/store"
	"github.com/odpf/salt/log"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
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
	logger                 log.Logger
	incomingOptimusVersion string

	db      *gorm.DB
	migrate *migrate.Migrate
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
	m, err := newMigrate(dbConnURL)
	if err != nil {
		return nil, fmt.Errorf("error initializing migrate: %w", err)
	}
	db, err := gorm.Open(postgres.Open(dbConnURL))
	if err != nil {
		return nil, fmt.Errorf("error initializing gorm: %w", err)
	}
	return &migration{
		incomingOptimusVersion: incomingOptimusVersion,
		db:                     db,
		migrate:                m,
		logger:                 logger,
	}, nil
}

func newMigrate(dbConnURL string) (*migrate.Migrate, error) {
	path := "migrations"
	src, err := iofs.New(migrationFs, path)
	if err != nil {
		return nil, fmt.Errorf("error initializing source: %w", err)
	}
	name := "iofs"
	return migrate.NewWithSourceInstance(name, src, dbConnURL)
}

func (m *migration) Up(ctx context.Context) error {
	if err := m.db.AutoMigrate(&migrationStep{}); err != nil {
		return fmt.Errorf("error auto-migrate migration_version: %w", err)
	}

	existingStep, err := m.getLatestMigrationStep(ctx)
	if err != nil {
		return err
	}
	if m.incomingOptimusVersion < existingStep.CurrentOptimusVersion {
		return fmt.Errorf("optimus version [%s] should be higher or equal than existing [%s]",
			m.incomingOptimusVersion, existingStep.CurrentOptimusVersion,
		)
	}
	if m.incomingOptimusVersion == existingStep.CurrentOptimusVersion {
		m.logger.Warn(
			fmt.Sprintf("migration up is skipped because optimus version [%s] is the same as current one",
				m.incomingOptimusVersion,
			),
		)
		return nil
	}

	if err := m.migrate.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("error migrating up: %w", err)
	}
	newVersion, _, err := m.migrate.Version()
	if err != nil {
		return fmt.Errorf("error getting current migration version: %w", err)
	}

	newMigrationVersion := &migrationStep{
		CurrentOptimusVersion:   m.incomingOptimusVersion,
		CurrentMigrationVersion: newVersion,
		PreviousOptimusVersion:  existingStep.CurrentOptimusVersion,
		CreatedAt:               time.Now(),
	}
	return m.addMigrationStep(ctx, newMigrationVersion)
}

func (m *migration) Rollback(ctx context.Context) error {
	existingStep, err := m.getLatestMigrationStep(ctx)
	if err != nil {
		return err
	}
	if m.incomingOptimusVersion != existingStep.CurrentOptimusVersion {
		return fmt.Errorf("expecting optimus with version [%s] but got [%s]",
			existingStep.CurrentOptimusVersion, m.incomingOptimusVersion,
		)
	}

	previousMigrationVersion, err := m.getMigrationVersion(ctx, existingStep.PreviousOptimusVersion)
	if err != nil {
		return err
	}
	if previousMigrationVersion == 0 {
		m.logger.Warn("migration rollback is skipped because previous migration version is zero")
		return nil
	}

	if err := m.migrate.Migrate(previousMigrationVersion); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("error migrating to version [%d]: %w", previousMigrationVersion, err)
	}
	return m.removeMigrationStep(ctx, existingStep)
}

func (m *migration) removeMigrationStep(ctx context.Context, oldStep *migrationStep) error {
	return m.db.WithContext(ctx).
		Where("current_optimus_version = ? and current_migration_version = ? and previous_optimus_version = ?",
			oldStep.CurrentOptimusVersion, oldStep.CurrentMigrationVersion, oldStep.PreviousOptimusVersion).
		Delete(&migrationStep{}).Error
}

func (m *migration) getMigrationVersion(ctx context.Context, optimusVersion string) (uint, error) {
	var rst migrationStep
	if err := m.db.WithContext(ctx).
		Select("current_optimus_version, current_migration_version, previous_optimus_version, created_at").
		Table("migration_steps").
		Where("current_optimus_version = ?", optimusVersion).
		Order("created_at desc limit 1").
		Find(&rst).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, fmt.Errorf("error getting migration version for optimus version [%s]: %w", optimusVersion, err)
	}
	return rst.CurrentMigrationVersion, nil
}

func (m *migration) addMigrationStep(ctx context.Context, newStep *migrationStep) error {
	var existingSteps []migrationStep
	if err := m.db.WithContext(ctx).
		Where(newStep).
		First(&existingSteps).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("error getting existing steps: %w", err)
	}
	if len(existingSteps) > 0 {
		m.logger.Warn("migration step is not added because it already exists")
		return nil
	}
	return m.db.WithContext(ctx).Create(newStep).Error
}

func (m *migration) getLatestMigrationStep(ctx context.Context) (*migrationStep, error) {
	var rst migrationStep
	if err := m.db.WithContext(ctx).
		Select("m.current_optimus_version, m.current_migration_version, m.previous_optimus_version, m.created_at").
		Table("migration_steps m").
		Joins("right join schema_migrations s on m.current_migration_version = s.version").
		Order("m.created_at desc limit 1").
		Find(&rst).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("error getting existing step: %w", err)
	}
	return &rst, nil
}
