//go:build !unit_test

package tenant_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	"github.com/odpf/optimus/core/tenant"
	postgres "github.com/odpf/optimus/internal/store/postgres/tenant"
	"github.com/odpf/optimus/tests/setup"
)

func TestNamespaceRepository(t *testing.T) {
	transporterKafkaBrokerKey := "KAFKA_BROKERS"
	proj, _ := tenant.NewProject("t-optimus-1",
		map[string]string{
			"bucket":                     "gs://some_folder-2",
			transporterKafkaBrokerKey:    "10.12.12.12:6668,10.12.12.13:6668",
			tenant.ProjectSchedulerHost:  "host",
			tenant.ProjectStoragePathKey: "gs://location",
		})
	ns, _ := tenant.NewNamespace("n-optimus-1", proj.Name(),
		map[string]string{
			"bucket": "gs://ns_bucket",
		})

	ctx := context.Background()
	dbSetup := func() *gorm.DB {
		dbConn := setup.TestDB()
		setup.TruncateTables(dbConn)

		prjRepo := postgres.NewProjectRepository(dbConn)
		err := prjRepo.Save(ctx, proj)
		if err != nil {
			panic(err)
		}

		return dbConn
	}

	t.Run("Save", func(t *testing.T) {
		t.Run("creates namespace with unique name", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewNamespaceRepository(db)

			err := repo.Save(ctx, ns)
			assert.Nil(t, err)

			savedNs, err := repo.GetByName(ctx, proj.Name(), ns.Name())
			assert.Nil(t, err)
			assert.Equal(t, "n-optimus-1", savedNs.Name().String())

			ns2, _ := tenant.NewNamespace("n-optimus-2", proj.Name(), ns.GetConfigs())
			err = repo.Save(ctx, ns2)
			assert.Nil(t, err)

			savedNS2, err := repo.GetByName(ctx, proj.Name(), ns2.Name())
			assert.Nil(t, err)
			assert.Equal(t, "n-optimus-2", savedNS2.Name().String())

			config, err := savedNS2.GetConfig("bucket")
			assert.Nil(t, err)
			assert.Equal(t, "gs://ns_bucket", config)
		})
		t.Run("overwrites the existing resource", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewNamespaceRepository(db)

			err := repo.Save(ctx, ns)
			assert.Nil(t, err)

			savedNS, err := repo.GetByName(ctx, proj.Name(), ns.Name())
			assert.Nil(t, err)
			assert.Equal(t, "n-optimus-1", savedNS.Name().String())

			conf := proj.GetConfigs()
			conf["STORAGE"] = "gs://some_place"
			ns2, _ := tenant.NewNamespace(ns.Name().String(), ns.ProjectName(), conf)

			err = repo.Save(ctx, ns2)
			assert.Nil(t, err)

			updatedNS, err := repo.GetByName(ctx, ns.ProjectName(), ns2.Name())
			assert.Nil(t, err)
			config, err := updatedNS.GetConfig("STORAGE")
			assert.Nil(t, err)
			assert.Equal(t, "gs://some_place", config)
		})
		t.Run("should not update if config is empty", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewNamespaceRepository(db)

			err := repo.Save(ctx, ns)
			assert.Nil(t, err)

			savedNS, err := repo.GetByName(ctx, proj.Name(), ns.Name())
			assert.Nil(t, err)
			assert.Equal(t, "n-optimus-1", savedNS.Name().String())

			ns2, _ := tenant.NewNamespace(ns.Name().String(), ns.ProjectName(), map[string]string{})

			err = repo.Save(ctx, ns2)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "empty config")
		})
	})
	t.Run("GetAll", func(t *testing.T) {
		t.Run("returns all the namespaces in db", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewNamespaceRepository(db)

			err := repo.Save(ctx, ns)
			assert.Nil(t, err)

			ns2, _ := tenant.NewNamespace("t-optimus-2", proj.Name(), ns.GetConfigs())
			err = repo.Save(ctx, ns2)
			assert.Nil(t, err)

			nss, err := repo.GetAll(ctx, proj.Name())
			assert.Nil(t, err)
			assert.Equal(t, 2, len(nss))

			assert.Equal(t, proj.Name().String(), nss[0].ProjectName().String())
			assert.Equal(t, ns2.Name().String(), nss[1].Name().String())
		})
	})
	t.Run("GetByName", func(t *testing.T) {
		t.Run("return error when record is not found", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewNamespaceRepository(db)

			_, err := repo.GetByName(ctx, proj.Name(), ns.Name())
			assert.NotNil(t, err)
			assert.EqualError(t, err, "not found for entity namespace: no record for n-optimus-1")
		})
		t.Run("returns the saved namespace with same name", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewNamespaceRepository(db)

			err := repo.Save(ctx, ns)
			assert.Nil(t, err)

			n, err := repo.GetByName(ctx, proj.Name(), ns.Name())
			assert.Nil(t, err)

			assert.Equal(t, ns.Name(), n.Name())
		})
	})
}
