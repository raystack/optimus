//go:build !unit_test

package tenant_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/tenant"
	postgres "github.com/goto/optimus/internal/store/postgres/tenant"
	"github.com/goto/optimus/tests/setup"
)

func TestPostgresProjectRepository(t *testing.T) {
	transporterKafkaBrokerKey := "KAFKA_BROKERS"
	proj, _ := tenant.NewProject("t-optimus-1",
		map[string]string{
			"bucket":                     "gs://some_folder-2",
			transporterKafkaBrokerKey:    "10.12.12.12:6668,10.12.12.13:6668",
			tenant.ProjectSchedulerHost:  "host",
			tenant.ProjectStoragePathKey: "gs://location",
		})

	ctx := context.Background()
	dbSetup := func() *pgxpool.Pool {
		dbPool := setup.TestPool()
		setup.TruncateTablesWith(dbPool)

		return dbPool
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

			updatedProj, err := repo.GetByName(ctx, proj2.Name())
			assert.Nil(t, err)
			config, err := updatedProj.GetConfig("STORAGE")
			assert.Nil(t, err)
			assert.Equal(t, "gs://some_place", config)
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
			assert.EqualError(t, err, "not found for entity project: no project for t-optimus-1")
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
