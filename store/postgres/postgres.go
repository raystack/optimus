// postgres implementation relies on gorm for queries which is very very
// inefficient at the moment, we are trading convenience with performance
// for example in lot of select stmts, we pull all related relations as well
// even when we don't really need to, most of the times these relation
// queries even in update gets executed for no reason even if user didn't
// intend to update them.
package postgres

import (
	"errors"
	"fmt"
	"io"
	stdlog "log"
	"net/http"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres" // required for postgres migrate driver
	"github.com/golang-migrate/migrate/v4/source/httpfs"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

const (
	resourcePath   = "migrations"
	tracingSpanKey = "otel:span"
)

var tracer = otel.Tracer("optimus/store/postgres")

// NewHTTPFSMigrator reads the migrations from httpfs and returns the migrate.Migrate
func NewHTTPFSMigrator(dbConnURL string) (*migrate.Migrate, error) {
	src, err := httpfs.New(http.FS(migrationFs), resourcePath)
	if err != nil {
		return &migrate.Migrate{}, fmt.Errorf("db migrator: %w", err)
	}
	return migrate.NewWithSourceInstance("httpfs", src, dbConnURL)
}

// Connect connect to the DB with custom configuration.
func Connect(connURL string, maxIdleConnections, maxOpenConnections int, writer io.Writer) (*gorm.DB, error) {
	db, err := gorm.Open(postgres.Open(connURL), &gorm.Config{
		Logger: logger.New(
			stdlog.New(writer, "\r\n", stdlog.LstdFlags), // io writer
			logger.Config{
				SlowThreshold:             time.Second,
				LogLevel:                  logger.Warn,
				IgnoreRecordNotFoundError: true,
				Colorful:                  true,
			},
		),
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize postgres db connection: %w", err)
	}

	if err := InitTrace(db); err != nil {
		return nil, fmt.Errorf("failed to initialize tracing for postgresql: %w", err)
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
		return fmt.Errorf("db migrator: %w", err)
	}
	defer m.Close()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("db migrator: %w", err)
	}
	return nil
}

func InitTrace(db *gorm.DB) error {
	// create
	if err := db.Callback().Create().Before("gorm:create").Register("otel:before_create", beforeCallback("db:create")); err != nil {
		return err
	}
	if err := db.Callback().Create().After("gorm:create").Register("otel:after_create", afterCallback); err != nil {
		return err
	}

	// query
	if err := db.Callback().Query().Before("gorm:query").Register("otel:before_query", beforeCallback("db:query")); err != nil {
		return err
	}
	if err := db.Callback().Query().After("gorm:query").Register("otel:after_query", afterCallback); err != nil {
		return err
	}

	// update
	if err := db.Callback().Update().Before("gorm:update").Register("otel:before_update", beforeCallback("db:update")); err != nil {
		return err
	}
	if err := db.Callback().Update().After("gorm:update").Register("otel:after_update", afterCallback); err != nil {
		return err
	}

	// delete
	if err := db.Callback().Delete().Before("gorm:delete").Register("otel:before_delete", beforeCallback("db:delete")); err != nil {
		return err
	}
	if err := db.Callback().Delete().After("gorm:delete").Register("otel:after_delete", afterCallback); err != nil {
		return err
	}

	// row
	if err := db.Callback().Row().Before("gorm:row").Register("otel:before_row", beforeCallback("db:row")); err != nil {
		return err
	}
	if err := db.Callback().Row().After("gorm:row").Register("otel:after_row", afterCallback); err != nil {
		return err
	}

	// raw
	if err := db.Callback().Raw().Before("gorm:raw").Register("otel:before_raw", beforeCallback("db:raw")); err != nil {
		return err
	}

	return db.Callback().Raw().After("gorm:raw").Register("otel:after_raw", afterCallback)
}

func beforeCallback(operation string) func(db *gorm.DB) {
	return func(db *gorm.DB) {
		if db == nil || db.Statement == nil || db.Statement.Context == nil {
			return
		}
		// if not tracing
		if !trace.SpanFromContext(db.Statement.Context).IsRecording() {
			return
		}
		_, span := tracer.Start(db.Statement.Context, operation)
		db.InstanceSet(tracingSpanKey, span)
	}
}

func afterCallback(db *gorm.DB) {
	if db == nil || db.Statement == nil || db.Statement.Context == nil {
		return
	}
	// extract sp from db context
	v, ok := db.InstanceGet(tracingSpanKey)
	if !ok || v == nil {
		return
	}
	sp, ok := v.(trace.Span)
	if !ok || sp == nil {
		return
	}
	defer sp.End()

	sp.SetAttributes(
		attribute.String("table", db.Statement.Table),
		attribute.Int64("rows_affected", db.Statement.RowsAffected),
		attribute.String("sql", db.Statement.SQL.String()),
	)
}
