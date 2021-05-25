package datastore_test

import (
	"context"
	"github.com/pkg/errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/datastore"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

func TestService(t *testing.T) {
	projectName := "a-data-project"
	projectSpec := models.ProjectSpec{
		ID:   uuid.Must(uuid.NewRandom()),
		Name: projectName,
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
	}

	namespaceSpec := models.NamespaceSpec{
		ID:          uuid.Must(uuid.NewRandom()),
		Name:        "dev-team-1",
		ProjectSpec: projectSpec,
	}

	t.Run("GetAll", func(t *testing.T) {
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
			resourceRepo.On("GetAll").Return([]models.ResourceSpec{resourceSpec1}, nil)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, dsRepo)
			res, err := service.GetAll(namespaceSpec, "bq")
			assert.Nil(t, err)
			assert.Equal(t, []models.ResourceSpec{resourceSpec1}, res)
		})
	})

	t.Run("CreateResource", func(t *testing.T) {
		t.Run("should successfully call datastore create resource individually for reach resource and save in persistent repository", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			resourceSpec1 := models.ResourceSpec{
				Version:   1,
				Name:      "proj.datas",
				Type:      models.ResourceTypeDataset,
				Datastore: datastorer,
			}
			resourceSpec2 := models.ResourceSpec{
				Version:   1,
				Name:      "proj.batas",
				Type:      models.ResourceTypeDataset,
				Datastore: datastorer,
			}
			datastorer.On("CreateResource", context.TODO(), models.CreateResourceRequest{
				Project:  projectSpec,
				Resource: resourceSpec1,
			}).Return(nil)
			datastorer.On("CreateResource", context.TODO(), models.CreateResourceRequest{
				Project:  projectSpec,
				Resource: resourceSpec2,
			}).Return(nil)

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("Save", resourceSpec1).Return(nil)
			resourceRepo.On("Save", resourceSpec2).Return(nil)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, dsRepo)
			err := service.CreateResource(context.TODO(), namespaceSpec, []models.ResourceSpec{resourceSpec1, resourceSpec2}, nil)
			assert.Nil(t, err)
		})
		t.Run("should not call create in datastore if failed to save in repository", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			resourceSpec1 := models.ResourceSpec{
				Version:   1,
				Name:      "proj.datas",
				Type:      models.ResourceTypeDataset,
				Datastore: datastorer,
			}
			resourceSpec2 := models.ResourceSpec{
				Version:   1,
				Name:      "proj.batas",
				Type:      models.ResourceTypeDataset,
				Datastore: datastorer,
			}
			datastorer.On("CreateResource", context.TODO(), models.CreateResourceRequest{
				Project:  projectSpec,
				Resource: resourceSpec2,
			}).Return(nil)

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("Save", resourceSpec1).Return(errors.New("cant save, too busy"))
			resourceRepo.On("Save", resourceSpec2).Return(nil)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, dsRepo)
			err := service.CreateResource(context.TODO(), namespaceSpec, []models.ResourceSpec{resourceSpec1, resourceSpec2}, nil)
			assert.NotNil(t, err)
		})
	})
	t.Run("UpdateResource", func(t *testing.T) {
		t.Run("should successfully call datastore update resource individually for reach resource and save in persistent repository", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			resourceSpec1 := models.ResourceSpec{
				Version:   1,
				Name:      "proj.datas",
				Type:      models.ResourceTypeDataset,
				Datastore: datastorer,
			}
			resourceSpec2 := models.ResourceSpec{
				Version:   1,
				Name:      "proj.batas",
				Type:      models.ResourceTypeDataset,
				Datastore: datastorer,
			}
			datastorer.On("UpdateResource", context.TODO(), models.UpdateResourceRequest{
				Project:  projectSpec,
				Resource: resourceSpec1,
			}).Return(nil)
			datastorer.On("UpdateResource", context.TODO(), models.UpdateResourceRequest{
				Project:  projectSpec,
				Resource: resourceSpec2,
			}).Return(nil)

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("Save", resourceSpec1).Return(nil)
			resourceRepo.On("Save", resourceSpec2).Return(nil)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, dsRepo)
			err := service.UpdateResource(context.TODO(), namespaceSpec, []models.ResourceSpec{resourceSpec1, resourceSpec2}, nil)
			assert.Nil(t, err)
		})
		t.Run("should not call update in datastore if failed to save in repository", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			resourceSpec1 := models.ResourceSpec{
				Version:   1,
				Name:      "proj.datas",
				Type:      models.ResourceTypeDataset,
				Datastore: datastorer,
			}
			resourceSpec2 := models.ResourceSpec{
				Version:   1,
				Name:      "proj.batas",
				Type:      models.ResourceTypeDataset,
				Datastore: datastorer,
			}
			datastorer.On("UpdateResource", context.TODO(), models.UpdateResourceRequest{
				Project:  projectSpec,
				Resource: resourceSpec2,
			}).Return(nil)

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("Save", resourceSpec1).Return(errors.New("cant save, too busy"))
			resourceRepo.On("Save", resourceSpec2).Return(nil)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, dsRepo)
			err := service.UpdateResource(context.TODO(), namespaceSpec, []models.ResourceSpec{resourceSpec1, resourceSpec2}, nil)
			assert.NotNil(t, err)
		})
	})
	t.Run("ReadResource", func(t *testing.T) {
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
			datastorer.On("ReadResource", context.TODO(), models.ReadResourceRequest{
				Project:  projectSpec,
				Resource: resourceSpec1,
			}).Return(models.ReadResourceResponse{Resource: resourceSpec1}, nil)

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("GetByName", resourceSpec1.Name).Return(resourceSpec1, nil)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, dsRepo)
			resp, err := service.ReadResource(context.TODO(), namespaceSpec, "bq", resourceSpec1.Name)
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
			resourceRepo.On("GetByName", resourceSpec1.Name).Return(resourceSpec1, errors.New("not found"))
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, dsRepo)
			_, err := service.ReadResource(context.TODO(), namespaceSpec, "bq", resourceSpec1.Name)
			assert.NotNil(t, err)
		})
	})
	t.Run("DeleteResource", func(t *testing.T) {
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
			datastorer.On("DeleteResource", context.TODO(), models.DeleteResourceRequest{
				Project:  projectSpec,
				Resource: resourceSpec1,
			}).Return(nil)

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("GetByName", resourceSpec1.Name).Return(resourceSpec1, nil)
			resourceRepo.On("Delete", resourceSpec1.Name).Return(nil)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, dsRepo)
			err := service.DeleteResource(context.TODO(), namespaceSpec, "bq", resourceSpec1.Name)
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
			datastorer.On("DeleteResource", context.TODO(), models.DeleteResourceRequest{
				Project:  projectSpec,
				Resource: resourceSpec1,
			}).Return(errors.New("failed to delete"))

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("GetByName", resourceSpec1.Name).Return(resourceSpec1, nil)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, dsRepo)
			err := service.DeleteResource(context.TODO(), namespaceSpec, "bq", resourceSpec1.Name)
			assert.NotNil(t, err)
		})
	})
}
