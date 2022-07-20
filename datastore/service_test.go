package datastore_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/datastore"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

func TestService(t *testing.T) {
	projectName := "a-data-project"
	projectSpec := models.ProjectSpec{
		ID:   models.ProjectID(uuid.New()),
		Name: projectName,
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
	}
	ctx := context.Background()

	namespaceSpec := models.NamespaceSpec{
		ID:          uuid.New(),
		Name:        "dev-team-1",
		ProjectSpec: projectSpec,
	}

	t.Run("GetAll", func(t *testing.T) {
		t.Run("should return nil and error if encountered error from persistent repository", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			dsRepo.On("GetByName", "bq").Return(nil, errors.New("random error"))
			defer dsRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)

			service := datastore.NewService(resourceRepoFac, dsRepo)
			res, err := service.GetAll(ctx, namespaceSpec, "bq")

			assert.Error(t, err)
			assert.Nil(t, res)
		})

		t.Run("should successfully read resources from persistent repository", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			dsRepo.On("GetByName", "bq").Return(datastorer, nil)
			defer dsRepo.AssertExpectations(t)

			resourceSpec1 := models.ResourceSpec{
				Version:   1,
				Name:      "proj.datas",
				Type:      models.ResourceTypeDataset,
				Datastore: datastorer,
			}

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("GetAll", ctx).Return([]models.ResourceSpec{resourceSpec1}, nil)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, dsRepo)
			res, err := service.GetAll(ctx, namespaceSpec, "bq")
			assert.Nil(t, err)
			assert.Equal(t, []models.ResourceSpec{resourceSpec1}, res)
		})
	})

	t.Run("CreateResource", func(t *testing.T) {
		t.Run("should return error if encountered unknown error when getting existing spec", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			resourceSpec := models.ResourceSpec{
				Version:   1,
				Name:      "proj.datas",
				Type:      models.ResourceTypeDataset,
				Datastore: datastorer,
			}

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("GetByName", ctx, resourceSpec.Name).Return(models.ResourceSpec{}, errors.New("random error"))
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, nil)
			err := service.CreateResource(ctx, namespaceSpec, []models.ResourceSpec{resourceSpec}, nil)
			assert.Error(t, err)
		})

		t.Run("should not proceed with store and deployment if incoming spec is the same as existing spec", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			resourceSpec := models.ResourceSpec{
				Version:   1,
				Name:      "proj.datas",
				Type:      models.ResourceTypeDataset,
				Datastore: datastorer,
			}

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("GetByName", ctx, resourceSpec.Name).Return(resourceSpec, nil)
			resourceRepo.AssertNotCalled(t, "Save", ctx, resourceSpec)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, nil)
			err := service.CreateResource(ctx, namespaceSpec, []models.ResourceSpec{resourceSpec}, nil)
			assert.NoError(t, err)
		})

		t.Run("should not call create in datastore if failed to save in repository", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			existingSpec := models.ResourceSpec{
				Version:   1,
				Name:      "proj.datas",
				Type:      models.ResourceTypeDataset,
				Labels:    map[string]string{},
				Datastore: datastorer,
			}
			incomingSpec := models.ResourceSpec{
				Version: 1,
				Name:    "proj.datas",
				Type:    models.ResourceTypeDataset,
				Labels: map[string]string{
					"created_by": "test",
				},
				Datastore: datastorer,
			}

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("GetByName", ctx, incomingSpec.Name).Return(existingSpec, nil)
			resourceRepo.On("Save", ctx, incomingSpec).Return(errors.New("random error"))
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, nil)
			err := service.CreateResource(ctx, namespaceSpec, []models.ResourceSpec{incomingSpec}, nil)
			assert.Error(t, err)
		})

		t.Run("should successfully call datastore create and save in persistent repository if no existing spec available", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			incomingSpec := models.ResourceSpec{
				Version: 1,
				Name:    "proj.datas",
				Type:    models.ResourceTypeDataset,
				Labels: map[string]string{
					"created_by": "test",
				},
				Datastore: datastorer,
			}
			datastorer.On("CreateResource", ctx, models.CreateResourceRequest{
				Resource: incomingSpec,
				Project:  projectSpec,
			}).Return(nil)

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("GetByName", ctx, incomingSpec.Name).Return(models.ResourceSpec{}, store.ErrResourceNotFound)
			resourceRepo.On("Save", ctx, incomingSpec).Return(nil)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, dsRepo)
			err := service.CreateResource(ctx, namespaceSpec, []models.ResourceSpec{incomingSpec}, nil)
			assert.NoError(t, err)
		})

		t.Run("should successfully call datastore create resource individually for each resource and save in persistent repository", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			existingSpec := models.ResourceSpec{
				Version:   1,
				Name:      "proj.datas",
				Type:      models.ResourceTypeDataset,
				Labels:    map[string]string{},
				Datastore: datastorer,
			}
			incomingSpec := models.ResourceSpec{
				Version: 1,
				Name:    "proj.datas",
				Type:    models.ResourceTypeDataset,
				Labels: map[string]string{
					"created_by": "test",
				},
				Datastore: datastorer,
			}
			datastorer.On("CreateResource", ctx, models.CreateResourceRequest{
				Resource: incomingSpec,
				Project:  projectSpec,
			}).Return(nil)

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("GetByName", ctx, incomingSpec.Name).Return(existingSpec, nil)
			resourceRepo.On("Save", ctx, incomingSpec).Return(nil)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, dsRepo)
			err := service.CreateResource(ctx, namespaceSpec, []models.ResourceSpec{incomingSpec}, nil)
			assert.NoError(t, err)
		})
	})

	t.Run("UpdateResource", func(t *testing.T) {
		t.Run("should return error if encountered unknown error when getting existing spec", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			resourceSpec := models.ResourceSpec{
				Version:   1,
				Name:      "proj.datas",
				Type:      models.ResourceTypeDataset,
				Datastore: datastorer,
			}

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("GetByName", ctx, resourceSpec.Name).Return(models.ResourceSpec{}, errors.New("random error"))
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, nil)
			err := service.UpdateResource(ctx, namespaceSpec, []models.ResourceSpec{resourceSpec}, nil, nil)
			assert.Error(t, err)
		})

		t.Run("should not proceed with store and deployment if incoming spec is the same as existing spec", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			resourceSpec := models.ResourceSpec{
				Version:   1,
				Name:      "proj.datas",
				Type:      models.ResourceTypeDataset,
				Datastore: datastorer,
			}

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("GetByName", ctx, resourceSpec.Name).Return(resourceSpec, nil)
			resourceRepo.AssertNotCalled(t, "Save", ctx, resourceSpec)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, nil)
			err := service.UpdateResource(ctx, namespaceSpec, []models.ResourceSpec{resourceSpec}, nil, nil)
			assert.NoError(t, err)
		})

		t.Run("should not call update in datastore if failed to save in repository", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			existingSpec := models.ResourceSpec{
				Version:   1,
				Name:      "proj.datas",
				Type:      models.ResourceTypeDataset,
				Labels:    map[string]string{},
				Datastore: datastorer,
			}
			incomingSpec := models.ResourceSpec{
				Version: 1,
				Name:    "proj.datas",
				Type:    models.ResourceTypeDataset,
				Labels: map[string]string{
					"created_by": "test",
				},
				Datastore: datastorer,
			}

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("GetByName", ctx, incomingSpec.Name).Return(existingSpec, nil)
			resourceRepo.On("Save", ctx, incomingSpec).Return(errors.New("random error"))
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, nil)
			err := service.UpdateResource(ctx, namespaceSpec, []models.ResourceSpec{incomingSpec}, nil, nil)
			assert.Error(t, err)
		})

		t.Run("should successfully call datastore update and save in persistent repository if no existing spec available", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			incomingSpec := models.ResourceSpec{
				Version: 1,
				Name:    "proj.datas",
				Type:    models.ResourceTypeDataset,
				Labels: map[string]string{
					"created_by": "test",
				},
				Datastore: datastorer,
			}
			datastorer.On("UpdateResource", ctx, models.UpdateResourceRequest{
				Resource: incomingSpec,
				Project:  projectSpec,
			}).Return(nil)

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("GetByName", ctx, incomingSpec.Name).Return(models.ResourceSpec{}, store.ErrResourceNotFound)
			resourceRepo.On("Save", ctx, incomingSpec).Return(nil)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, dsRepo)
			err := service.UpdateResource(ctx, namespaceSpec, []models.ResourceSpec{incomingSpec}, nil, nil)
			assert.NoError(t, err)
		})

		t.Run("should successfully call datastore update resource individually for each resource and save in persistent repository", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			existingSpec := models.ResourceSpec{
				Version:   1,
				Name:      "proj.datas",
				Type:      models.ResourceTypeDataset,
				Labels:    map[string]string{},
				Datastore: datastorer,
			}
			incomingSpec := models.ResourceSpec{
				Version: 1,
				Name:    "proj.datas",
				Type:    models.ResourceTypeDataset,
				Labels: map[string]string{
					"created_by": "test",
				},
				Datastore: datastorer,
			}
			datastorer.On("UpdateResource", ctx, models.UpdateResourceRequest{
				Resource: incomingSpec,
				Project:  projectSpec,
			}).Return(nil)

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("GetByName", ctx, incomingSpec.Name).Return(existingSpec, nil)
			resourceRepo.On("Save", ctx, incomingSpec).Return(nil)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, dsRepo)
			err := service.UpdateResource(ctx, namespaceSpec, []models.ResourceSpec{incomingSpec}, nil, nil)
			assert.NoError(t, err)
		})
	})

	t.Run("ReadResource", func(t *testing.T) {
		t.Run("should return empty and error when encountered error from persistent repository", func(t *testing.T) {
			dsRepo := new(mock.SupportedDatastoreRepo)
			dsRepo.On("GetByName", "bq").Return(nil, errors.New("random error"))

			resourceSpec1 := models.ResourceSpec{
				Version: 1,
				Name:    "proj.datas",
				Type:    models.ResourceTypeDataset,
			}

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)

			service := datastore.NewService(resourceRepoFac, dsRepo)
			resp, err := service.ReadResource(ctx, namespaceSpec, "bq", resourceSpec1.Name)

			assert.Error(t, err)
			assert.Empty(t, resp)
		})

		t.Run("should return empty and error if encountered error when reading frm datastore", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			dsRepo.On("GetByName", "bq").Return(datastorer, nil)
			defer dsRepo.AssertExpectations(t)

			resourceSpec1 := models.ResourceSpec{
				Version:   1,
				Name:      "proj.datas",
				Type:      models.ResourceTypeDataset,
				Datastore: datastorer,
			}
			datastorer.On("ReadResource", ctx, models.ReadResourceRequest{
				Project:  projectSpec,
				Resource: resourceSpec1,
			}).Return(models.ReadResourceResponse{}, errors.New("random error"))

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("GetByName", ctx, resourceSpec1.Name).Return(resourceSpec1, nil)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, dsRepo)
			resp, err := service.ReadResource(ctx, namespaceSpec, "bq", resourceSpec1.Name)

			assert.Error(t, err)
			assert.Empty(t, resp)
		})

		t.Run("should successfully call datastore read operation by reading from persistent repository", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			dsRepo.On("GetByName", "bq").Return(datastorer, nil)
			defer dsRepo.AssertExpectations(t)

			resourceSpec1 := models.ResourceSpec{
				Version:   1,
				Name:      "proj.datas",
				Type:      models.ResourceTypeDataset,
				Datastore: datastorer,
			}
			datastorer.On("ReadResource", ctx, models.ReadResourceRequest{
				Project:  projectSpec,
				Resource: resourceSpec1,
			}).Return(models.ReadResourceResponse{Resource: resourceSpec1}, nil)

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("GetByName", ctx, resourceSpec1.Name).Return(resourceSpec1, nil)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, dsRepo)
			resp, err := service.ReadResource(ctx, namespaceSpec, "bq", resourceSpec1.Name)
			assert.Nil(t, err)
			assert.Equal(t, resourceSpec1, resp)
		})

		t.Run("should not call read in datastore if failed to read from repository", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			dsRepo.On("GetByName", "bq").Return(datastorer, nil)
			defer dsRepo.AssertExpectations(t)

			resourceSpec1 := models.ResourceSpec{
				Version:   1,
				Name:      "proj.datas",
				Type:      models.ResourceTypeDataset,
				Datastore: datastorer,
			}

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("GetByName", ctx, resourceSpec1.Name).Return(resourceSpec1, errors.New("not found"))
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, dsRepo)
			_, err := service.ReadResource(ctx, namespaceSpec, "bq", resourceSpec1.Name)
			assert.NotNil(t, err)
		})
	})

	t.Run("DeleteResource", func(t *testing.T) {
		t.Run("should return error if encountered error when reading from persistent datastore repository", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			dsRepo.On("GetByName", "bq").Return(nil, errors.New("random error"))
			defer dsRepo.AssertExpectations(t)

			resourceSpec1 := models.ResourceSpec{
				Version:   1,
				Name:      "proj.datas",
				Type:      models.ResourceTypeDataset,
				Datastore: datastorer,
			}
			resourceRepoFac := new(mock.ResourceSpecRepoFactory)

			service := datastore.NewService(resourceRepoFac, dsRepo)
			err := service.DeleteResource(ctx, namespaceSpec, "bq", resourceSpec1.Name)

			assert.Error(t, err)
		})

		t.Run("should return error if encountered error when reading from resource spec repository", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			dsRepo.On("GetByName", "bq").Return(datastorer, nil)
			defer dsRepo.AssertExpectations(t)

			resourceSpec1 := models.ResourceSpec{
				Version:   1,
				Name:      "proj.datas",
				Type:      models.ResourceTypeDataset,
				Datastore: datastorer,
			}

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("GetByName", ctx, resourceSpec1.Name).Return(models.ResourceSpec{}, errors.New("random error"))
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, dsRepo)
			err := service.DeleteResource(ctx, namespaceSpec, "bq", resourceSpec1.Name)

			assert.Error(t, err)
		})

		t.Run("should successfully call datastore delete operation and then from persistent repository", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			dsRepo.On("GetByName", "bq").Return(datastorer, nil)
			defer dsRepo.AssertExpectations(t)

			resourceSpec1 := models.ResourceSpec{
				Version:   1,
				Name:      "proj.datas",
				Type:      models.ResourceTypeDataset,
				Datastore: datastorer,
			}
			datastorer.On("DeleteResource", ctx, models.DeleteResourceRequest{
				Project:  projectSpec,
				Resource: resourceSpec1,
			}).Return(nil)

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("GetByName", ctx, resourceSpec1.Name).Return(resourceSpec1, nil)
			resourceRepo.On("Delete", ctx, resourceSpec1.Name).Return(nil)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, dsRepo)
			err := service.DeleteResource(ctx, namespaceSpec, "bq", resourceSpec1.Name)
			assert.Nil(t, err)
		})

		t.Run("should not call delete in datastore if failed to delete from repository", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			dsRepo.On("GetByName", "bq").Return(datastorer, nil)
			defer dsRepo.AssertExpectations(t)

			resourceSpec1 := models.ResourceSpec{
				Version:   1,
				Name:      "proj.datas",
				Type:      models.ResourceTypeDataset,
				Datastore: datastorer,
			}
			datastorer.On("DeleteResource", ctx, models.DeleteResourceRequest{
				Project:  projectSpec,
				Resource: resourceSpec1,
			}).Return(errors.New("failed to delete"))

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("GetByName", ctx, resourceSpec1.Name).Return(resourceSpec1, nil)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, dsRepo)
			err := service.DeleteResource(ctx, namespaceSpec, "bq", resourceSpec1.Name)
			assert.NotNil(t, err)
		})
	})
}
