//go:build !unit_test
// +build !unit_test

package resource_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	serviceResource "github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	repoResource "github.com/odpf/optimus/internal/store/postgres/resource"
	tenantPostgres "github.com/odpf/optimus/internal/store/postgres/tenant"
	"github.com/odpf/optimus/tests/setup"
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
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			resourceToCreate, err := serviceResource.NewResource("project.dataset", kindDataset, store, tnnt, meta, spec)
			assert.NoError(t, err)
			resourceToCreate.UpdateURN("bigquery://project:dataset")

			actualFirstError := repository.Create(ctx, resourceToCreate)
			assert.NoError(t, actualFirstError)
			actualSecondError := repository.Create(ctx, resourceToCreate)
			assert.ErrorContains(t, actualSecondError, "error creating resource to database")
		})

		t.Run("stores resource to database and returns nil if no error is encountered", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			resourceToCreate, err := serviceResource.NewResource("project.dataset", kindDataset, store, tnnt, meta, spec)
			assert.NoError(t, err)
			err = resourceToCreate.UpdateURN("bigquery://project:dataset")
			assert.NoError(t, err)

			actualError := repository.Create(ctx, resourceToCreate)
			assert.NoError(t, actualError)

			storedResources, err := readAllFromDB(db)
			assert.NoError(t, err)
			assert.Len(t, storedResources, 1)
			assert.EqualValues(t, resourceToCreate, storedResources[0])
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Run("returns error if resource does not exist", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			resourceToUpdate, err := serviceResource.NewResource("project.dataset", kindDataset, store, tnnt, meta, spec)
			assert.NoError(t, err)
			resourceToUpdate.UpdateURN("bigquery://project:dataset")

			actualError := repository.Update(ctx, resourceToUpdate)
			assert.ErrorContains(t, actualError, "not found for entity resource")
		})

		t.Run("updates resource and returns nil if no error is encountered", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			resourceToCreate, err := serviceResource.NewResource("project.dataset", kindDataset, store, tnnt, meta, spec)
			assert.NoError(t, err)
			resourceToCreate.UpdateURN("bigquery://project:dataset")

			err = insertAllToDB(db, []*serviceResource.Resource{resourceToCreate})
			assert.NoError(t, err)

			resourceToUpdate := serviceResource.FromExisting(resourceToCreate, serviceResource.ReplaceStatus(serviceResource.StatusSuccess))
			actualError := repository.Update(ctx, resourceToUpdate)
			assert.NoError(t, actualError)

			storedResources, err := readAllFromDB(db)
			assert.NoError(t, err)
			assert.Len(t, storedResources, 1)
			assert.EqualValues(t, resourceToUpdate, storedResources[0])
		})
	})

	t.Run("ReadByFullName", func(t *testing.T) {
		t.Run("returns nil and error if resource does not exist", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			actualResource, actualError := repository.ReadByFullName(ctx, tnnt, store, "project.dataset")
			assert.Nil(t, actualResource)
			assert.ErrorContains(t, actualError, "not found for entity resource")
		})

		t.Run("returns resource and nil if no error is encountered", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			fullName := "project.dataset"
			resourceToCreate, err := serviceResource.NewResource(fullName, kindDataset, store, tnnt, meta, spec)
			assert.NoError(t, err)

			err = insertAllToDB(db, []*serviceResource.Resource{resourceToCreate})
			assert.NoError(t, err)

			actualResource, actualError := repository.ReadByFullName(ctx, tnnt, store, fullName)
			assert.NotNil(t, actualResource)
			assert.NoError(t, actualError)
			assert.EqualValues(t, resourceToCreate, actualResource)
		})
	})

	t.Run("ReadAll", func(t *testing.T) {
		t.Run("returns empty and nil if no resource is found", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			actualResources, actualError := repository.ReadAll(ctx, tnnt, store)
			assert.Empty(t, actualResources)
			assert.NoError(t, actualError)
		})

		t.Run("returns resource and nil if no error is encountered", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

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
			db := dbSetup()
			repository := repoResource.NewRepository(db)

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
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			existingResource, err := serviceResource.NewResource("project.dataset1", kindDataset, store, tnnt, meta, spec)
			assert.NoError(t, err)
			err = insertAllToDB(db, []*serviceResource.Resource{existingResource})
			assert.NoError(t, err)
			nonExistingResource, err := serviceResource.NewResource("project.dataset2", kindDataset, store, tnnt, meta, spec)
			assert.NoError(t, err)

			resourcesToUpdate := []*serviceResource.Resource{
				serviceResource.FromExisting(existingResource, serviceResource.ReplaceStatus(serviceResource.StatusSuccess)),
				serviceResource.FromExisting(nonExistingResource, serviceResource.ReplaceStatus(serviceResource.StatusSuccess)),
			}
			actualError := repository.UpdateStatus(ctx, resourcesToUpdate...)
			assert.Error(t, actualError)

			storedResources, err := readAllFromDB(db)
			assert.NoError(t, err)
			assert.Len(t, storedResources, 1)
			assert.EqualValues(t, serviceResource.StatusSuccess, storedResources[0].Status())
		})

		t.Run("updates only status and returns nil if no error is encountered", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			existingResource1, err := serviceResource.NewResource("project.dataset1", kindDataset, store, tnnt, meta, spec)
			assert.NoError(t, err)
			existingResource1.UpdateURN("bigquery://project:dataset1")
			existingResource2, err := serviceResource.NewResource("project.dataset2", kindDataset, store, tnnt, meta, spec)
			assert.NoError(t, err)
			existingResource2.UpdateURN("bigquery://project:dataset2")
			err = insertAllToDB(db, []*serviceResource.Resource{existingResource1, existingResource2})
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

			storedResources, err := readAllFromDB(db)
			assert.NoError(t, err)
			assert.Len(t, storedResources, 2)
			assert.EqualValues(t, existingResource1.Spec(), storedResources[0].Spec())
			assert.EqualValues(t, serviceResource.StatusSuccess, storedResources[0].Status())
			assert.EqualValues(t, existingResource2.Spec(), storedResources[1].Spec())
			assert.EqualValues(t, serviceResource.StatusSuccess, storedResources[1].Status())
		})
	})
}

func readAllFromDB(db *gorm.DB) ([]*serviceResource.Resource, error) {
	var rs []*repoResource.Resource
	if err := db.Find(&rs).Error; err != nil {
		return nil, err
	}
	output := make([]*serviceResource.Resource, len(rs))
	for i, r := range rs {
		o, err := fromModelToResource(r)
		if err != nil {
			return nil, err
		}
		output[i] = o
	}
	return output, nil
}

func insertAllToDB(db *gorm.DB, rs []*serviceResource.Resource) error {
	for _, r := range rs {
		resourceToCreate := fromResourceToModel(r)
		if err := db.Create(resourceToCreate).Error; err != nil {
			return err
		}
	}
	return nil
}

func fromResourceToModel(r *serviceResource.Resource) *repoResource.Resource {
	metadata, _ := json.Marshal(r.Metadata())
	spec, _ := json.Marshal(r.Spec())
	return &repoResource.Resource{
		FullName:      r.FullName(),
		Kind:          r.Kind(),
		Store:         r.Store().String(),
		ProjectName:   r.Tenant().ProjectName().String(),
		NamespaceName: r.Tenant().NamespaceName().String(),
		Metadata:      metadata,
		Spec:          spec,
		URN:           r.URN(),
		Status:        r.Status().String(),
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
}

func fromModelToResource(r *repoResource.Resource) (*serviceResource.Resource, error) {
	store, err := serviceResource.FromStringToStore(r.Store)
	if err != nil {
		return nil, err
	}
	tnnt, err := tenant.NewTenant(r.ProjectName, r.NamespaceName)
	if err != nil {
		return nil, err
	}
	var meta *serviceResource.Metadata
	if err := json.Unmarshal(r.Metadata, &meta); err != nil {
		return nil, err
	}
	var spec map[string]any
	if err := json.Unmarshal(r.Spec, &spec); err != nil {
		return nil, err
	}
	output, err := serviceResource.NewResource(r.FullName, r.Kind, store, tnnt, meta, spec)
	if err != nil {
		return nil, err
	}
	status := serviceResource.FromStringToStatus(r.Status)
	res := serviceResource.FromExisting(output, serviceResource.ReplaceStatus(status))
	res.UpdateURN(r.URN)
	return res, nil
}

func dbSetup() *gorm.DB {
	dbConn := setup.TestDB()
	setup.TruncateTables(dbConn)

	pool := setup.TestPool()
	ctx := context.Background()
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

	return dbConn
}
