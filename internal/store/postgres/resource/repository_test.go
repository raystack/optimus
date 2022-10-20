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
	"github.com/odpf/optimus/tests/setup"
)

func TestPostgresResourceRepository(t *testing.T) {
	ctx := context.Background()
	tnnt, err := tenant.NewTenant("project_test", "namespace_test")
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

	t.Run("Create", func(t *testing.T) {
		t.Run("returns error if resource with the provided full name is already defined within project and namespace", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			resourceToCreate, err := serviceResource.NewResource("project.dataset", serviceResource.KindDataset, serviceResource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)

			actualFirstError := repository.Create(ctx, resourceToCreate)
			assert.NoError(t, actualFirstError)
			actualSecondError := repository.Create(ctx, resourceToCreate)
			assert.ErrorContains(t, actualSecondError, "error creating resource to database")
		})

		t.Run("stores resource to database and returns nil if no error is encountered", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			resourceToCreate, err := serviceResource.NewResource("project.dataset", serviceResource.KindDataset, serviceResource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)

			actualError := repository.Create(ctx, resourceToCreate)
			assert.NoError(t, actualError)

			storedResources, err := readAllFromDb(db)
			assert.NoError(t, err)
			assert.Len(t, storedResources, 1)
			assert.EqualValues(t, resourceToCreate, storedResources[0])
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Run("returns error if resource does not exist", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			resourceToUpdate, err := serviceResource.NewResource("project.dataset", serviceResource.KindDataset, serviceResource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)

			actualError := repository.Update(ctx, resourceToUpdate)
			assert.ErrorContains(t, actualError, "error reading from database")
		})

		t.Run("updates resource and returns nil if no error is encountered", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			resourceToCreate, err := serviceResource.NewResource("project.dataset", serviceResource.KindDataset, serviceResource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)

			err = insertAllToDB(db, []*serviceResource.Resource{resourceToCreate})
			assert.NoError(t, err)

			resourceToUpdate := serviceResource.FromExisting(resourceToCreate, serviceResource.ReplaceStatus(serviceResource.StatusSuccess))
			actualError := repository.Update(ctx, resourceToUpdate)
			assert.NoError(t, actualError)

			storedResources, err := readAllFromDb(db)
			assert.NoError(t, err)
			assert.Len(t, storedResources, 1)
			assert.EqualValues(t, resourceToUpdate, storedResources[0])
		})
	})

	t.Run("ReadByFullName", func(t *testing.T) {
		t.Run("returns nil and error if resource does not exist", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			actualResource, actualError := repository.ReadByFullName(ctx, tnnt, serviceResource.Bigquery, "project.dataset")
			assert.Nil(t, actualResource)
			assert.ErrorContains(t, actualError, "error reading from database")
		})

		t.Run("returns resource and nil if no error is encountered", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			fullName := "project.dataset"
			resourceToCreate, err := serviceResource.NewResource(fullName, serviceResource.KindDataset, serviceResource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)

			err = insertAllToDB(db, []*serviceResource.Resource{resourceToCreate})
			assert.NoError(t, err)

			actualResource, actualError := repository.ReadByFullName(ctx, tnnt, serviceResource.Bigquery, fullName)
			assert.NotNil(t, actualResource)
			assert.NoError(t, actualError)
			assert.EqualValues(t, resourceToCreate, actualResource)
		})
	})

	t.Run("ReadAll", func(t *testing.T) {
		t.Run("returns empty and nil if no resource is found", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			actualResources, actualError := repository.ReadAll(ctx, tnnt, serviceResource.Bigquery)
			assert.Empty(t, actualResources)
			assert.NoError(t, actualError)
		})

		t.Run("returns resource and nil if no error is encountered", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			resourceToCreate, err := serviceResource.NewResource("project.dataset", serviceResource.KindDataset, serviceResource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)

			err = repository.Create(ctx, resourceToCreate)
			assert.NoError(t, err)

			actualResources, actualError := repository.ReadAll(ctx, tnnt, serviceResource.Bigquery)
			assert.NotEmpty(t, actualResources)
			assert.NoError(t, actualError)
			assert.EqualValues(t, []*serviceResource.Resource{resourceToCreate}, actualResources)
		})
	})

	t.Run("UpdateAll", func(t *testing.T) {
		t.Run("does not do any update and returns error if error is encountered when updating one or more resources", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			existingResource, err := serviceResource.NewResource("project.dataset1", serviceResource.KindDataset, serviceResource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)
			err = insertAllToDB(db, []*serviceResource.Resource{existingResource})
			assert.NoError(t, err)
			nonExistingResource, err := serviceResource.NewResource("project.dataset2", serviceResource.KindDataset, serviceResource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)

			resourcesToUpdate := []*serviceResource.Resource{
				serviceResource.FromExisting(existingResource, serviceResource.ReplaceStatus(serviceResource.StatusSuccess)),
				serviceResource.FromExisting(nonExistingResource, serviceResource.ReplaceStatus(serviceResource.StatusSuccess)),
			}
			actualError := repository.UpdateAll(ctx, resourcesToUpdate)
			assert.Error(t, actualError)

			storedResources, err := readAllFromDb(db)
			assert.NoError(t, err)
			assert.Len(t, storedResources, 1)
			assert.EqualValues(t, existingResource, storedResources[0])
		})

		t.Run("returns nil if no error is encountered", func(t *testing.T) {
			db := dbSetup()
			repository := repoResource.NewRepository(db)

			existingResource1, err := serviceResource.NewResource("project.dataset1", serviceResource.KindDataset, serviceResource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)
			existingResource2, err := serviceResource.NewResource("project.dataset2", serviceResource.KindDataset, serviceResource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)

			resourcesToCreate := []*serviceResource.Resource{existingResource1, existingResource2}
			err = insertAllToDB(db, resourcesToCreate)
			assert.NoError(t, err)

			resourcesToUpdate := []*serviceResource.Resource{
				serviceResource.FromExisting(existingResource1, serviceResource.ReplaceStatus(serviceResource.StatusSuccess)),
				serviceResource.FromExisting(existingResource2, serviceResource.ReplaceStatus(serviceResource.StatusSuccess)),
			}
			actualError := repository.UpdateAll(ctx, resourcesToUpdate)
			assert.NoError(t, actualError)

			storedResources, err := readAllFromDb(db)
			assert.NoError(t, err)
			assert.Len(t, storedResources, 2)
			assert.EqualValues(t, resourcesToUpdate[0], storedResources[0])
			assert.EqualValues(t, resourcesToUpdate[1], storedResources[1])
		})
	})
}

func readAllFromDb(db *gorm.DB) ([]*serviceResource.Resource, error) {
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
	var namespaceName string
	if name, err := r.Tenant().NamespaceName(); err == nil {
		namespaceName = name.String()
	}
	metadata, _ := json.Marshal(r.Metadata())
	spec, _ := json.Marshal(r.Spec())
	return &repoResource.Resource{
		FullName:      r.FullName(),
		Kind:          r.Kind().String(),
		Store:         r.Dataset().Store.String(),
		ProjectName:   r.Tenant().ProjectName().String(),
		NamespaceName: namespaceName,
		Metadata:      metadata,
		Spec:          spec,
		URN:           r.URN(),
		Status:        r.Status().String(),
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
}

func fromModelToResource(r *repoResource.Resource) (*serviceResource.Resource, error) {
	kind, err := serviceResource.FromStringToKind(r.Kind)
	if err != nil {
		return nil, err
	}
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
	output, err := serviceResource.NewResource(r.FullName, kind, store, tnnt, meta, spec)
	if err != nil {
		return nil, err
	}
	status := serviceResource.FromStringToStatus(r.Status)
	return serviceResource.FromExisting(output, serviceResource.ReplaceStatus(status)), nil
}

func dbSetup() *gorm.DB {
	dbConn := setup.TestDB()
	setup.TruncateTables(dbConn)
	return dbConn
}
