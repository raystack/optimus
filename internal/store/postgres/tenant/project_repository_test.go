//go:build !unit_test

package tenant_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/store"
	postgres "github.com/odpf/optimus/internal/store/postgres/tenant"
	"github.com/odpf/optimus/tests/setup"
)

func TestProjectRepository(t *testing.T) {
	transporterKafkaBrokerKey := "KAFKA_BROKERS"

	proj, _ := tenant.NewProject("t-optimus-1",
		map[string]string{
			"bucket":                  "gs://some_folder-2",
			transporterKafkaBrokerKey: "10.12.12.12:6668,10.12.12.13:6668",
		})

	ctx := context.Background()
	dbSetup := func() *gorm.DB {
		dbConn := setup.TestDB()
		setup.TruncateTables(dbConn)

		return dbConn
	}

	t.Run("Save", func(t *testing.T) {
		t.Run("creates projects with unique name", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewProjectRepository(db)

			err := repo.Save(ctx, proj)
			assert.Nil(t, err)

			savedProj, err := repo.GetByName(ctx, proj.Name())
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus-1", savedProj.Name().String())

			proj2, _ := tenant.NewProject("t-optimus-2", proj.GetConfigs())
			err = repo.Save(ctx, proj2)
			assert.Nil(t, err)

			savedProj2, err := repo.GetByName(ctx, proj2.Name())
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus-2", savedProj2.Name().String())

			config, err := savedProj2.GetConfig(transporterKafkaBrokerKey)
			assert.Nil(t, err)
			assert.Equal(t, "10.12.12.12:6668,10.12.12.13:6668", config)
		})
		t.Run("overwrites the existing resource", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewProjectRepository(db)

			err := repo.Save(ctx, proj)
			assert.Nil(t, err)

			savedProj, err := repo.GetByName(ctx, proj.Name())
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus-1", savedProj.Name().String())

			conf := proj.GetConfigs()
			conf["STORAGE"] = "gs://some_place"
			proj2, _ := tenant.NewProject(proj.Name().String(), conf)

			err = repo.Save(ctx, proj2)
			assert.Nil(t, err)

			savedProj, err = repo.GetByName(ctx, proj2.Name())
			assert.Nil(t, err)
			config, err := savedProj.GetConfig("STORAGE")
			assert.Nil(t, err)
			assert.Equal(t, "gs://some_place", config)
		})
		t.Run("skips update if config is empty", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewProjectRepository(db)

			err := repo.Save(ctx, proj)
			assert.Nil(t, err)
			savedProj, err := repo.GetByName(ctx, proj.Name())
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus-1", savedProj.Name().String())

			proj2, _ := tenant.NewProject(proj.Name().String(), map[string]string{})
			err = repo.Save(ctx, proj2)
			assert.Equal(t, store.ErrEmptyConfig, err)
		})
	})
	t.Run("GetAll", func(t *testing.T) {
		t.Run("returns all the projects in db", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewProjectRepository(db)

			err := repo.Save(ctx, proj)
			assert.Nil(t, err)

			proj2, _ := tenant.NewProject("t-optimus-2", proj.GetConfigs())
			err = repo.Save(ctx, proj2)
			assert.Nil(t, err)

			projs, err := repo.GetAll(ctx)
			assert.Nil(t, err)
			assert.Equal(t, 2, len(projs))

			assert.Equal(t, proj.Name().String(), projs[0].Name().String())
			assert.Equal(t, proj2.Name().String(), projs[1].Name().String())
		})
	})
	t.Run("GetByName", func(t *testing.T) {
		t.Run("return error when record is not found", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewProjectRepository(db)

			_, err := repo.GetByName(ctx, proj.Name())
			assert.NotNil(t, err)
			assert.EqualError(t, err, "not found for entity project: record not found")
		})
		t.Run("returns the saved project with same name", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewProjectRepository(db)

			err := repo.Save(ctx, proj)
			assert.Nil(t, err)

			p, err := repo.GetByName(ctx, proj.Name())
			assert.Nil(t, err)

			assert.Equal(t, proj.Name(), p.Name())
		})
	})
}
