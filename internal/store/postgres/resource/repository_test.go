//go:build !unit_test
// +build !unit_test

package resource_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	serviceResource "github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	repoResource "github.com/odpf/optimus/internal/store/postgres/resource"
	"github.com/odpf/optimus/tests/setup"
)

func TestIntegrationResource(t *testing.T) {
	dbSetup := func() *gorm.DB {
		dbConn := setup.TestDB()
		setup.TruncateTables(dbConn)
		return dbConn
	}

	t.Run("Create", func(t *testing.T) {
		t.Run("returns error if resource with the provided full name is already defined within project and namespace", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			ctx := context.Background()
			tnnt, err := tenant.NewTenant("project_test", "namespace_test")
			if err != nil {
				panic(err)
			}
			fullName := "project.dataset"
			meta := &serviceResource.Metadata{}
			spec := map[string]any{
				"key": "value",
			}
			resourceToCreate, err := serviceResource.NewResource(fullName, serviceResource.KindDataset, serviceResource.BigQuery, tnnt, meta, spec)
			if err != nil {
				panic(err)
			}

			actualFirstError := repository.Create(ctx, resourceToCreate)
			actualSecondError := repository.Create(ctx, resourceToCreate)

			assert.NoError(t, actualFirstError)
			assert.Error(t, actualSecondError)
		})

		t.Run("returns nil if no error is encountered", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			ctx := context.Background()
			tnnt, err := tenant.NewTenant("project_test", "namespace_test")
			if err != nil {
				panic(err)
			}
			fullName := "project.dataset"
			meta := &serviceResource.Metadata{}
			spec := map[string]any{
				"key": "value",
			}
			resourceToCreate, err := serviceResource.NewResource(fullName, serviceResource.KindDataset, serviceResource.BigQuery, tnnt, meta, spec)
			if err != nil {
				panic(err)
			}

			actualError := repository.Create(ctx, resourceToCreate)

			assert.NoError(t, actualError)
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Run("returns error if resource does not exist", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			ctx := context.Background()
			tnnt, err := tenant.NewTenant("project_test", "namespace_test")
			if err != nil {
				panic(err)
			}
			fullName := "project.dataset"
			meta := &serviceResource.Metadata{}
			spec := map[string]any{
				"key": "value",
			}
			resourceToCreate, err := serviceResource.NewResource(fullName, serviceResource.KindDataset, serviceResource.BigQuery, tnnt, meta, spec)
			if err != nil {
				panic(err)
			}

			actualError := repository.Update(ctx, resourceToCreate)

			assert.Error(t, actualError)
		})

		t.Run("returns nil if no error is encountered", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			ctx := context.Background()
			tnnt, err := tenant.NewTenant("project_test", "namespace_test")
			if err != nil {
				panic(err)
			}
			fullName := "project.dataset"
			meta := &serviceResource.Metadata{}
			spec := map[string]any{
				"key": "value",
			}
			resourceToCreate, err := serviceResource.NewResource(fullName, serviceResource.KindDataset, serviceResource.BigQuery, tnnt, meta, spec)
			if err != nil {
				panic(err)
			}
			if err := repository.Create(ctx, resourceToCreate); err != nil {
				panic(err)
			}
			resourceToUpdate := serviceResource.FromExisting(resourceToCreate, serviceResource.ReplaceStatus(serviceResource.StatusSuccess))

			actualError := repository.Update(ctx, resourceToUpdate)

			assert.NoError(t, actualError)
		})
	})

	t.Run("ReadByFullName", func(t *testing.T) {
		t.Run("returns nil and error if resource does not exist", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			ctx := context.Background()
			tnnt, err := tenant.NewTenant("project_test", "namespace_test")
			if err != nil {
				panic(err)
			}
			store := serviceResource.BigQuery
			fullName := "project.dataset"

			actualResource, actualError := repository.ReadByFullName(ctx, tnnt, store, fullName)

			assert.Nil(t, actualResource)
			assert.Error(t, actualError)
		})

		t.Run("returns resource and nil if no error is encountered", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			ctx := context.Background()
			tnnt, err := tenant.NewTenant("project_test", "namespace_test")
			if err != nil {
				panic(err)
			}
			store := serviceResource.BigQuery
			fullName := "project.dataset"
			meta := &serviceResource.Metadata{}
			spec := map[string]any{
				"key": "value",
			}
			resourceToCreate, err := serviceResource.NewResource(fullName, serviceResource.KindDataset, serviceResource.BigQuery, tnnt, meta, spec)
			if err != nil {
				panic(err)
			}
			if err := repository.Create(ctx, resourceToCreate); err != nil {
				panic(err)
			}

			actualResource, actualError := repository.ReadByFullName(ctx, tnnt, store, fullName)

			assert.NotNil(t, actualResource)
			assert.NoError(t, actualError)
		})
	})

	t.Run("ReadAll", func(t *testing.T) {
		t.Run("returns empty and nil if no resource is found", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			ctx := context.Background()
			tnnt, err := tenant.NewTenant("project_test", "namespace_test")
			if err != nil {
				panic(err)
			}
			store := serviceResource.BigQuery

			actualResources, actualError := repository.ReadAll(ctx, tnnt, store)

			assert.Empty(t, actualResources)
			assert.NoError(t, actualError)
		})

		t.Run("returns resource and nil if no error is encountered", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			ctx := context.Background()
			tnnt, err := tenant.NewTenant("project_test", "namespace_test")
			if err != nil {
				panic(err)
			}
			store := serviceResource.BigQuery
			fullName := "project.dataset"
			meta := &serviceResource.Metadata{}
			spec := map[string]any{
				"key": "value",
			}
			resourceToCreate, err := serviceResource.NewResource(fullName, serviceResource.KindDataset, serviceResource.BigQuery, tnnt, meta, spec)
			if err != nil {
				panic(err)
			}
			if err := repository.Create(ctx, resourceToCreate); err != nil {
				panic(err)
			}

			actualResources, actualError := repository.ReadAll(ctx, tnnt, store)

			assert.NotEmpty(t, actualResources)
			assert.NoError(t, actualError)
		})
	})

	t.Run("UpdateAll", func(t *testing.T) {
		t.Run("returns error if error is encountered when updating one or more resources", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			ctx := context.Background()
			tnnt, err := tenant.NewTenant("project_test", "namespace_test")
			if err != nil {
				panic(err)
			}
			fullName := "project.dataset"
			meta := &serviceResource.Metadata{}
			spec := map[string]any{
				"key": "value",
			}
			resourceToCreate, err := serviceResource.NewResource(fullName, serviceResource.KindDataset, serviceResource.BigQuery, tnnt, meta, spec)
			if err != nil {
				panic(err)
			}
			resourcesToUpdate := []*serviceResource.Resource{resourceToCreate}

			actualError := repository.UpdateAll(ctx, resourcesToUpdate)

			assert.Error(t, actualError)
		})

		t.Run("returns nil if no error is encountered", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			ctx := context.Background()
			tnnt, err := tenant.NewTenant("project_test", "namespace_test")
			if err != nil {
				panic(err)
			}
			fullName := "project.dataset"
			meta := &serviceResource.Metadata{}
			spec := map[string]any{
				"key": "value",
			}
			resourceToCreate, err := serviceResource.NewResource(fullName, serviceResource.KindDataset, serviceResource.BigQuery, tnnt, meta, spec)
			if err != nil {
				panic(err)
			}
			if err := repository.Create(ctx, resourceToCreate); err != nil {
				panic(err)
			}
			resourceToUpdate := serviceResource.FromExisting(resourceToCreate, serviceResource.ReplaceStatus(serviceResource.StatusSuccess))
			resourcesToUpdate := []*serviceResource.Resource{resourceToUpdate}

			actualError := repository.UpdateAll(ctx, resourcesToUpdate)

			assert.NoError(t, actualError)
		})
	})
}
