//go:build !unit_test
// +build !unit_test

package bench

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	tmock "github.com/stretchr/testify/mock"
	"gorm.io/gorm"

	"github.com/odpf/optimus/ext/datastore/bigquery"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/postgres"
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

	db.Exec("TRUNCATE TABLE job_dependency CASCADE")
}

func getBigQueryDataStore() *bigquery.BigQuery {
	bQClient := new(bigquery.BqClientMock)

	bQClientFactory := new(bigquery.BQClientFactoryMock)
	bQClientFactory.On("New", tmock.Anything, tmock.Anything).Return(bQClient, nil)

	return &bigquery.BigQuery{
		ClientFac: bQClientFactory,
	}
}

type bqPlugin struct{}

func (b bqPlugin) PluginInfo() (*models.PluginInfoResponse, error) {
	return &models.PluginInfoResponse{
		Name:          "bq2bq",
		Description:   "BigQuery to BigQuery transformation task",
		PluginType:    models.PluginTypeTask,
		PluginVersion: "dev",
		APIVersion:    nil,
		DependsOn:     nil,
		HookType:      "",
		Image:         "gcr.io/bq-plugin:dev",
		SecretPath:    "/tmp/auth.json",
		PluginMods:    []models.PluginMod{models.ModTypeDependencyResolver},
	}, nil
}

func (b bqPlugin) GenerateDestination(ctx2 context.Context, request models.GenerateDestinationRequest) (*models.GenerateDestinationResponse, error) {
	time.Sleep(time.Millisecond * 20) // Simulate the delay
	proj, ok1 := request.Config.Get("PROJECT")
	dataset, ok2 := request.Config.Get("DATASET")
	tab, ok3 := request.Config.Get("TABLE")
	if ok1 && ok2 && ok3 {
		return &models.GenerateDestinationResponse{
			Destination: fmt.Sprintf("%s:%s.%s", proj.Value, dataset.Value, tab.Value),
			Type:        models.DestinationTypeBigquery,
		}, nil
	}
	return nil, errors.New("missing config key required to generate destination")
}

func (b bqPlugin) GenerateDependencies(_ context.Context, _ models.GenerateDependenciesRequest) (*models.GenerateDependenciesResponse, error) {
	time.Sleep(time.Millisecond * 100) // Simulate the delay
	return &models.GenerateDependenciesResponse{Dependencies: []string{}}, nil
}

func inMemoryPluginRegistry() models.PluginRepository {
	bq2bq := bqPlugin{}

	transporterHook := "transporter"
	hookUnit := new(mock.BasePlugin)
	hookUnit.On("PluginInfo").Return(&models.PluginInfoResponse{
		Name:     transporterHook,
		HookType: models.HookTypePre,
		Image:    "example.io/namespace/hook-image:latest",
	}, nil)

	pluginRepo := new(mock.SupportedPluginRepo)
	pluginRepo.On("GetByName", "bq2bq").Return(&models.Plugin{
		Base:          bq2bq,
		DependencyMod: bq2bq,
	}, nil)
	pluginRepo.On("GetByName", "transporter").Return(&models.Plugin{
		Base: hookUnit,
	}, nil)
	return pluginRepo
}
