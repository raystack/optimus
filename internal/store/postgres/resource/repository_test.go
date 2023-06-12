//go:build !unit_test

package resource_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	serviceResource "github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	repoResource "github.com/goto/optimus/internal/store/postgres/resource"
	tenantPostgres "github.com/goto/optimus/internal/store/postgres/tenant"
	"github.com/goto/optimus/tests/setup"
)

func TestPostgresResourceRepository(t *testing.T) {
	ctx := context.Background()
	tnnt, err := tenant.NewTenant("t-optimus-1", "n-optimus-1")
	assert.NoError(t, err)
	spec := map[string]any{
		"description": "spec for test",
	}
	meta := &serviceResource.Metadata{
		Version:     1,
		Description: "metadata for test",
		Labels: map[string]string{
			"orchestrator": "optimus",
		},
	}
	store := serviceResource.Bigquery
	kindDataset := "dataset"

	t.Run("Create", func(t *testing.T) {
		t.Run("returns error if resource with the provided full name is already defined within project and namespace", func(t *testing.T) {
			pool := dbSetup()
			repository := repoResource.NewRepository(pool)

			resourceToCreate, err := serviceResource.NewResource("project.dataset", kindDataset, store, tnnt, meta, spec)
			assert.NoError(t, err)
			resourceToCreate.UpdateURN("bigquery://project:dataset")

			actualFirstError := repository.Create(ctx, resourceToCreate)
			assert.NoError(t, actualFirstError)
			actualSecondError := repository.Create(ctx, resourceToCreate)
			assert.ErrorContains(t, actualSecondError, "error creating resource to database")
		})

		t.Run("stores resource to database and returns nil if no error is encountered", func(t *testing.T) {
			pool := dbSetup()
			repository := repoResource.NewRepository(pool)

			resourceToCreate, err := serviceResource.NewResource("project.dataset", kindDataset, store, tnnt, meta, spec)
			assert.NoError(t, err)
			err = resourceToCreate.UpdateURN("bigquery://project:dataset")
			assert.NoError(t, err)

			actualError := repository.Create(ctx, resourceToCreate)
			assert.NoError(t, actualError)

			storedResource, err := repository.ReadByFullName(ctx, tnnt, store, "project.dataset")
			assert.NoError(t, err)
			assert.EqualValues(t, resourceToCreate, storedResource)
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Run("returns error if resource does not exist", func(t *testing.T) {
			pool := dbSetup()
			repository := repoResource.NewRepository(pool)

			resourceToUpdate, err := serviceResource.NewResource("project.dataset", kindDataset, store, tnnt, meta, spec)
			assert.NoError(t, err)
			resourceToUpdate.UpdateURN("bigquery://project:dataset")

			actualError := repository.Update(ctx, resourceToUpdate)
			assert.ErrorContains(t, actualError, "not found for entity resource")
		})

		t.Run("updates resource and returns nil if no error is encountered", func(t *testing.T) {
			pool := dbSetup()
			repository := repoResource.NewRepository(pool)

			resourceToCreate, err := serviceResource.NewResource("project.dataset", kindDataset, store, tnnt, meta, spec)
			assert.NoError(t, err)
			resourceToCreate.UpdateURN("bigquery://project:dataset")

			err = repository.Create(ctx, resourceToCreate)
			assert.NoError(t, err)

			resourceToUpdate := serviceResource.FromExisting(resourceToCreate, serviceResource.ReplaceStatus(serviceResource.StatusSuccess))
			actualError := repository.Update(ctx, resourceToUpdate)
			assert.NoError(t, actualError)

			storedResources, err := repository.ReadAll(ctx, tnnt, store)
			assert.NoError(t, err)
			assert.Len(t, storedResources, 1)
			assert.EqualValues(t, resourceToUpdate, storedResources[0])
		})
	})

	t.Run("ChangeNamespace", func(t *testing.T) {
		newNamespaceName := "n-optimus-2"
		newTenant, err := tenant.NewTenant("t-optimus-1", newNamespaceName)
		assert.Nil(t, err)
		t.Run("returns error if resource does not exist", func(t *testing.T) {
			pool := dbSetup()
			repository := repoResource.NewRepository(pool)

			resourceToUpdate, err := serviceResource.NewResource("project.dataset", kindDataset, store, tnnt, meta, spec)
			assert.NoError(t, err)
			resourceToUpdate.UpdateURN("bigquery://project:dataset")

			actualError := repository.ChangeNamespace(ctx, resourceToUpdate, newTenant)
			assert.ErrorContains(t, actualError, "not found for entity resource")
		})

		t.Run("updates resource and returns nil if no error is encountered", func(t *testing.T) {
			pool := dbSetup()
			repository := repoResource.NewRepository(pool)

			resourceToCreate, err := serviceResource.NewResource("project.dataset", kindDataset, store, tnnt, meta, spec)
			assert.NoError(t, err)
			resourceToCreate.UpdateURN("bigquery://project:dataset")

			err = repository.Create(ctx, resourceToCreate)
			assert.NoError(t, err)

			resourceToUpdate := serviceResource.FromExisting(resourceToCreate, serviceResource.ReplaceStatus(serviceResource.StatusSuccess))
			actualError := repository.ChangeNamespace(ctx, resourceToUpdate, newTenant)
			assert.NoError(t, actualError)

			storedResources, err := repository.GetResources(ctx, tnnt, store, []string{resourceToUpdate.FullName()})
			assert.NoError(t, err)
			assert.Len(t, storedResources, 0)
			storedNewResources, err := repository.GetResources(ctx, newTenant, store, []string{resourceToUpdate.FullName()})
			assert.NoError(t, err)
			assert.Len(t, storedNewResources, 1)
		})
	})

	t.Run("ReadByFullName", func(t *testing.T) {
		t.Run("returns nil and error if resource does not exist", func(t *testing.T) {
			pool := dbSetup()
			repository := repoResource.NewRepository(pool)

			actualResource, actualError := repository.ReadByFullName(ctx, tnnt, store, "project.dataset")
			assert.Nil(t, actualResource)
			assert.ErrorContains(t, actualError, "not found for entity resource")
		})

		t.Run("returns resource and nil if no error is encountered", func(t *testing.T) {
			pool := dbSetup()
			repository := repoResource.NewRepository(pool)

			fullName := "project.dataset"
			resourceToCreate, err := serviceResource.NewResource(fullName, kindDataset, store, tnnt, meta, spec)
			assert.NoError(t, err)

			err = repository.Create(ctx, resourceToCreate)
			assert.NoError(t, err)

			actualResource, actualError := repository.ReadByFullName(ctx, tnnt, store, fullName)
			assert.NotNil(t, actualResource)
			assert.NoError(t, actualError)
			assert.EqualValues(t, resourceToCreate, actualResource)
		})
	})

	t.Run("ReadAll", func(t *testing.T) {
		t.Run("returns empty and nil if no resource is found", func(t *testing.T) {
			pool := dbSetup()
			repository := repoResource.NewRepository(pool)

			actualResources, actualError := repository.ReadAll(ctx, tnnt, store)
			assert.Empty(t, actualResources)
			assert.NoError(t, actualError)
		})

		t.Run("returns resource and nil if no error is encountered", func(t *testing.T) {
			pool := dbSetup()
			repository := repoResource.NewRepository(pool)

			resourceToCreate, err := serviceResource.NewResource("project.dataset", kindDataset, store, tnnt, meta, spec)
			assert.NoError(t, err)

			err = repository.Create(ctx, resourceToCreate)
			assert.NoError(t, err)

			actualResources, actualError := repository.ReadAll(ctx, tnnt, store)
			assert.NotEmpty(t, actualResources)
			assert.NoError(t, actualError)
			assert.EqualValues(t, []*serviceResource.Resource{resourceToCreate}, actualResources)
		})
	})

	t.Run("GetResources", func(t *testing.T) {
		t.Run("gets the resources with given full_names", func(t *testing.T) {
			pool := dbSetup()
			repository := repoResource.NewRepository(pool)

			name1 := "project.dataset"
			resourceToCreate1, err := serviceResource.NewResource(name1, kindDataset, store, tnnt, meta, spec)
			assert.NoError(t, err)
			resourceToCreate1.UpdateURN("bigquery://project:dataset1")

			err = repository.Create(ctx, resourceToCreate1)
			assert.NoError(t, err)

			viewSpec := map[string]any{
				"view_query": "select * from `project.dataset.table`",
			}
			name2 := "project.dataset.view"
			resourceToCreate2, err := serviceResource.NewResource(name2, "view", store, tnnt, meta, viewSpec)
			assert.NoError(t, err)
			resourceToCreate2.UpdateURN("bigquery://project:dataset.view")

			err = repository.Create(ctx, resourceToCreate2)
			assert.NoError(t, err)

			actualResources, actualError := repository.GetResources(ctx, tnnt, store, []string{name1, name2})
			assert.NoError(t, actualError)
			assert.NotEmpty(t, actualResources)
			assert.EqualValues(t, []*serviceResource.Resource{resourceToCreate1, resourceToCreate2}, actualResources)
		})
	})

	t.Run("UpdateStatus", func(t *testing.T) {
		t.Run("updates status and return error for partial update success", func(t *testing.T) {
			pool := dbSetup()
			repository := repoResource.NewRepository(pool)

			existingResource, err := serviceResource.NewResource("project.dataset1", kindDataset, store, tnnt, meta, spec)
			assert.NoError(t, err)
			err = repository.Create(ctx, existingResource)
			assert.NoError(t, err)
			nonExistingResource, err := serviceResource.NewResource("project.dataset2", kindDataset, store, tnnt, meta, spec)
			assert.NoError(t, err)

			resourcesToUpdate := []*serviceResource.Resource{
				serviceResource.FromExisting(existingResource, serviceResource.ReplaceStatus(serviceResource.StatusSuccess)),
				serviceResource.FromExisting(nonExistingResource, serviceResource.ReplaceStatus(serviceResource.StatusSuccess)),
			}
			actualError := repository.UpdateStatus(ctx, resourcesToUpdate...)
			assert.Error(t, actualError)
			assert.ErrorContains(t, actualError, "error updating status for project.dataset2")

			storedResources, err := repository.ReadAll(ctx, tnnt, store)
			assert.NoError(t, err)
			assert.Len(t, storedResources, 1)
			assert.EqualValues(t, serviceResource.StatusSuccess, storedResources[0].Status())
		})

		t.Run("updates only status and returns nil if no error is encountered", func(t *testing.T) {
			pool := dbSetup()
			repository := repoResource.NewRepository(pool)

			existingResource1, err := serviceResource.NewResource("project.dataset1", kindDataset, store, tnnt, meta, spec)
			assert.NoError(t, err)
			existingResource1.UpdateURN("bigquery://project:dataset1")
			existingResource2, err := serviceResource.NewResource("project.dataset2", kindDataset, store, tnnt, meta, spec)
			assert.NoError(t, err)
			existingResource2.UpdateURN("bigquery://project:dataset2")
			err = repository.Create(ctx, existingResource1)
			assert.NoError(t, err)
			err = repository.Create(ctx, existingResource2)
			assert.NoError(t, err)

			newSpec := map[string]any{
				"Description": "spec for testing update status",
			}
			modifiedResource1, err := serviceResource.NewResource("project.dataset1", kindDataset, store, tnnt, meta, newSpec)
			assert.NoError(t, err)
			modifiedResource1.MarkExistInStore()
			modifiedResource2, err := serviceResource.NewResource("project.dataset2", kindDataset, store, tnnt, meta, newSpec)
			assert.NoError(t, err)
			resourcesToUpdate := []*serviceResource.Resource{
				serviceResource.FromExisting(modifiedResource1, serviceResource.ReplaceStatus(serviceResource.StatusSuccess)),
				serviceResource.FromExisting(modifiedResource2, serviceResource.ReplaceStatus(serviceResource.StatusSuccess)),
			}
			actualError := repository.UpdateStatus(ctx, resourcesToUpdate...)
			assert.NoError(t, actualError)

			storedResources, err := repository.ReadAll(ctx, tnnt, store)
			assert.NoError(t, err)
			assert.Len(t, storedResources, 2)
			assert.EqualValues(t, existingResource1.Spec(), storedResources[0].Spec())
			assert.EqualValues(t, serviceResource.StatusSuccess, storedResources[0].Status())
			assert.EqualValues(t, existingResource2.Spec(), storedResources[1].Spec())
			assert.EqualValues(t, serviceResource.StatusSuccess, storedResources[1].Status())
		})
	})
}

func dbSetup() *pgxpool.Pool {
	ctx := context.Background()
	pool := setup.TestPool()
	setup.TruncateTablesWith(pool)
	proj, _ := tenant.NewProject("t-optimus-1",
		map[string]string{
			"bucket":                     "gs://some_folder-2",
			tenant.ProjectSchedulerHost:  "host",
			tenant.ProjectStoragePathKey: "gs://location",
		})
	projRepo := tenantPostgres.NewProjectRepository(pool)
	err := projRepo.Save(ctx, proj)
	if err != nil {
		panic(err)
	}

	namespaceRepo := tenantPostgres.NewNamespaceRepository(pool)

	ns, _ := tenant.NewNamespace("n-optimus-1", proj.Name(),
		map[string]string{
			"bucket": "gs://ns_bucket",
		})
	err = namespaceRepo.Save(ctx, ns)
	if err != nil {
		panic(err)
	}

	ns2, _ := tenant.NewNamespace("n-optimus-2", proj.Name(),
		map[string]string{
			"bucket": "gs://ns_bucket",
		})
	err = namespaceRepo.Save(ctx, ns2)
	if err != nil {
		panic(err)
	}

	return pool
}
