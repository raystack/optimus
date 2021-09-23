package datastore_test

import (
	"context"
	"testing"
	"time"

	"github.com/odpf/optimus/store"

	"github.com/pkg/errors"

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
	t.Run("BackupResourceDryRun", func(t *testing.T) {
		jobTask := models.JobSpecTask{
			Config: models.JobSpecConfigs{
				{
					Name:  "do",
					Value: "this",
				},
			},
			Priority: 2000,
			Window: models.JobSpecTaskWindow{
				Size:       time.Hour,
				Offset:     0,
				TruncateTo: "d",
			},
		}
		jobAssets := *models.JobAssets{}.New(
			[]models.JobSpecAsset{
				{
					Name:  "query.sql",
					Value: "select * from 1",
				},
			})
		t.Run("should return list of resources without dependents to be backed up", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			depMod := new(mock.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			jobTask.Unit = &models.Plugin{Base: execUnit, DependencyMod: depMod}
			jobSpec := models.JobSpec{
				ID:     uuid.Must(uuid.NewRandom()),
				Name:   "job-1",
				Task:   jobTask,
				Assets: jobAssets,
			}

			unitData := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobSpec.Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobSpec.Assets),
			}
			destination := &models.GenerateDestinationResponse{
				Destination: "project.dataset.table",
				Type:        models.DestinationTypeBigquery,
			}
			depMod.On("GenerateDestination", context.TODO(), unitData).Return(destination, nil)

			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)

			resourceSpec := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.table",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("GetByURN", destination.URN()).Return(resourceSpec, nil)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			backupResourceReq := models.BackupResourceRequest{
				Resource: resourceSpec,
				Project:  projectSpec,
				DryRun:   true,
			}
			datastorer.On("BackupResource", context.TODO(), backupResourceReq).Return(nil)

			service := datastore.NewService(resourceRepoFac, dsRepo)
			resp, err := service.BackupResourceDryRun(context.TODO(), projectSpec, namespaceSpec, []models.JobSpec{jobSpec})
			assert.Nil(t, err)
			assert.Equal(t, []string{destination.Destination}, resp)
		})
		t.Run("should return list of resources with dependents to be backed up", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			depMod := new(mock.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)

			jobTask.Unit = &models.Plugin{Base: execUnit, DependencyMod: depMod}
			jobDownstream := models.JobSpec{
				ID:     uuid.Must(uuid.NewRandom()),
				Name:   "job-2",
				Task:   jobTask,
				Assets: jobAssets,
			}
			dependencies := make(map[string]models.JobSpecDependency)
			dependencies[jobDownstream.GetName()] = models.JobSpecDependency{
				Job: &jobDownstream,
			}
			jobRoot := models.JobSpec{
				ID:           uuid.Must(uuid.NewRandom()),
				Name:         "job-1",
				Task:         jobTask,
				Assets:       jobAssets,
				Dependencies: dependencies,
			}

			unitRoot := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobRoot.Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobRoot.Assets),
			}
			destinationRoot := &models.GenerateDestinationResponse{
				Destination: "project.dataset.table",
				Type:        models.DestinationTypeBigquery,
			}
			depMod.On("GenerateDestination", context.TODO(), unitRoot).Return(destinationRoot, nil).Once()

			unitDownstream := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobDownstream.Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobDownstream.Assets),
			}
			destinationDownstream := &models.GenerateDestinationResponse{
				Destination: "project.dataset.downstream",
				Type:        models.DestinationTypeBigquery,
			}
			depMod.On("GenerateDestination", context.TODO(), unitDownstream).Return(destinationDownstream, nil).Once()

			resourceRoot := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.root",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			resourceDownstream := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.downstream",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("GetByURN", destinationRoot.URN()).Return(resourceRoot, nil).Once()
			resourceRepo.On("GetByURN", destinationDownstream.URN()).Return(resourceDownstream, nil).Once()
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			backupResourceReqRoot := models.BackupResourceRequest{
				Resource: resourceRoot,
				Project:  projectSpec,
				DryRun:   true,
			}
			datastorer.On("BackupResource", context.TODO(), backupResourceReqRoot).Return(nil).Once()

			backupResourceReqDownstream := models.BackupResourceRequest{
				Resource: resourceDownstream,
				Project:  projectSpec,
				DryRun:   true,
			}
			datastorer.On("BackupResource", context.TODO(), backupResourceReqDownstream).Return(nil).Once()

			service := datastore.NewService(resourceRepoFac, dsRepo)
			resp, err := service.BackupResourceDryRun(context.TODO(), projectSpec, namespaceSpec, []models.JobSpec{jobRoot, jobDownstream})
			assert.Nil(t, err)
			assert.Equal(t, []string{destinationRoot.Destination, destinationDownstream.Destination}, resp)
		})
		t.Run("should return error when unable to generate destination", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			depMod := new(mock.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			jobTask.Unit = &models.Plugin{Base: execUnit, DependencyMod: depMod}
			jobSpec := models.JobSpec{
				ID:     uuid.Must(uuid.NewRandom()),
				Name:   "job-1",
				Task:   jobTask,
				Assets: jobAssets,
			}

			unitData := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobSpec.Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobSpec.Assets),
			}

			errorMsg := "unable to generate destination"
			depMod.On("GenerateDestination", context.TODO(), unitData).Return(&models.GenerateDestinationResponse{}, errors.New(errorMsg))

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			service := datastore.NewService(nil, dsRepo)
			resp, err := service.BackupResourceDryRun(context.TODO(), projectSpec, namespaceSpec, []models.JobSpec{jobSpec})
			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, resp)
		})
		t.Run("should return error when unable to get datastorer", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			depMod := new(mock.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			jobTask.Unit = &models.Plugin{Base: execUnit, DependencyMod: depMod}
			jobSpec := models.JobSpec{
				ID:     uuid.Must(uuid.NewRandom()),
				Name:   "job-1",
				Task:   jobTask,
				Assets: jobAssets,
			}

			unitData := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobSpec.Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobSpec.Assets),
			}
			destination := &models.GenerateDestinationResponse{
				Destination: "project.dataset.table",
				Type:        models.DestinationTypeBigquery,
			}

			depMod.On("GenerateDestination", context.TODO(), unitData).Return(destination, nil)

			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)
			errorMsg := "unable to get datastorer"
			dsRepo.On("GetByName", destination.Type.String()).Return(datastorer, errors.New(errorMsg))

			service := datastore.NewService(nil, dsRepo)
			resp, err := service.BackupResourceDryRun(context.TODO(), projectSpec, namespaceSpec, []models.JobSpec{jobSpec})
			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, resp)
		})
		t.Run("should return error when unable to do backup dry run", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			depMod := new(mock.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			jobTask.Unit = &models.Plugin{Base: execUnit, DependencyMod: depMod}
			jobSpec := models.JobSpec{
				ID:     uuid.Must(uuid.NewRandom()),
				Name:   "job-1",
				Task:   jobTask,
				Assets: jobAssets,
			}

			unitData := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobSpec.Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobSpec.Assets),
			}
			destination := &models.GenerateDestinationResponse{
				Destination: "project.dataset.table",
				Type:        models.DestinationTypeBigquery,
			}
			depMod.On("GenerateDestination", context.TODO(), unitData).Return(destination, nil)

			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)

			resourceSpec := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.table",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("GetByURN", destination.URN()).Return(resourceSpec, nil)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			backupResourceReq := models.BackupResourceRequest{
				Resource: resourceSpec,
				Project:  projectSpec,
				DryRun:   true,
			}
			errorMsg := "unable to do backup dry run"
			datastorer.On("BackupResource", context.TODO(), backupResourceReq).Return(errors.New(errorMsg))

			service := datastore.NewService(resourceRepoFac, dsRepo)
			resp, err := service.BackupResourceDryRun(context.TODO(), projectSpec, namespaceSpec, []models.JobSpec{jobSpec})
			assert.Equal(t, errorMsg, err.Error())
			assert.Nil(t, resp)
		})
		t.Run("should return error when unable to get resource", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			depMod := new(mock.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			jobTask.Unit = &models.Plugin{Base: execUnit, DependencyMod: depMod}
			jobSpec := models.JobSpec{
				ID:     uuid.Must(uuid.NewRandom()),
				Name:   "job-1",
				Task:   jobTask,
				Assets: jobAssets,
			}

			unitData := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobSpec.Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobSpec.Assets),
			}
			destination := &models.GenerateDestinationResponse{
				Destination: "project.dataset.table",
				Type:        models.DestinationTypeBigquery,
			}
			depMod.On("GenerateDestination", context.TODO(), unitData).Return(destination, nil)

			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)

			resourceRepo := new(mock.ResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)
			errorMsg := "unable to get resource"
			resourceRepo.On("GetByURN", destination.URN()).Return(models.ResourceSpec{}, errors.New(errorMsg))

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			defer resourceRepoFac.AssertExpectations(t)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)

			service := datastore.NewService(resourceRepoFac, dsRepo)
			resp, err := service.BackupResourceDryRun(context.TODO(), projectSpec, namespaceSpec, []models.JobSpec{jobSpec})
			assert.Equal(t, errorMsg, err.Error())
			assert.Nil(t, resp)
		})
		t.Run("should return error when unable to generate destination for downstream", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			depMod := new(mock.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)

			jobTask.Unit = &models.Plugin{Base: execUnit, DependencyMod: depMod}
			jobDownstream := models.JobSpec{
				ID:     uuid.Must(uuid.NewRandom()),
				Name:   "job-2",
				Task:   jobTask,
				Assets: jobAssets,
			}
			dependencies := make(map[string]models.JobSpecDependency)
			dependencies[jobDownstream.GetName()] = models.JobSpecDependency{
				Job: &jobDownstream,
			}
			jobRoot := models.JobSpec{
				ID:           uuid.Must(uuid.NewRandom()),
				Name:         "job-1",
				Task:         jobTask,
				Assets:       jobAssets,
				Dependencies: dependencies,
			}

			unitRoot := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobRoot.Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobRoot.Assets),
			}
			destinationRoot := &models.GenerateDestinationResponse{
				Destination: "project.dataset.table",
				Type:        models.DestinationTypeBigquery,
			}
			depMod.On("GenerateDestination", context.TODO(), unitRoot).Return(destinationRoot, nil).Once()

			unitDownstream := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobDownstream.Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobDownstream.Assets),
			}
			errorMsg := "unable to generate destination"
			depMod.On("GenerateDestination", context.TODO(), unitDownstream).Return(&models.GenerateDestinationResponse{}, errors.New(errorMsg)).Once()

			resourceRoot := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.root",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("GetByURN", destinationRoot.URN()).Return(resourceRoot, nil).Once()
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			backupResourceReqRoot := models.BackupResourceRequest{
				Resource: resourceRoot,
				Project:  projectSpec,
				DryRun:   true,
			}
			datastorer.On("BackupResource", context.TODO(), backupResourceReqRoot).Return(nil).Once()

			service := datastore.NewService(resourceRepoFac, dsRepo)
			resp, err := service.BackupResourceDryRun(context.TODO(), projectSpec, namespaceSpec, []models.JobSpec{jobRoot, jobDownstream})

			assert.Equal(t, errorMsg, err.Error())
			assert.Nil(t, resp)
		})
		t.Run("should not return error when one of the resources is not found", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			depMod := new(mock.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)

			jobTask.Unit = &models.Plugin{Base: execUnit, DependencyMod: depMod}
			jobDownstream := models.JobSpec{
				ID:     uuid.Must(uuid.NewRandom()),
				Name:   "job-2",
				Task:   jobTask,
				Assets: jobAssets,
			}
			dependencies := make(map[string]models.JobSpecDependency)
			dependencies[jobDownstream.GetName()] = models.JobSpecDependency{
				Job: &jobDownstream,
			}
			jobRoot := models.JobSpec{
				ID:           uuid.Must(uuid.NewRandom()),
				Name:         "job-1",
				Task:         jobTask,
				Assets:       jobAssets,
				Dependencies: dependencies,
			}

			unitRoot := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobRoot.Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobRoot.Assets),
			}
			destinationRoot := &models.GenerateDestinationResponse{
				Destination: "project.dataset.table",
				Type:        models.DestinationTypeBigquery,
			}
			depMod.On("GenerateDestination", context.TODO(), unitRoot).Return(destinationRoot, nil).Once()

			unitDownstream := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobDownstream.Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobDownstream.Assets),
			}
			destinationDownstream := &models.GenerateDestinationResponse{
				Destination: "project.dataset.downstream",
				Type:        models.DestinationTypeBigquery,
			}
			depMod.On("GenerateDestination", context.TODO(), unitDownstream).Return(destinationDownstream, nil).Once()

			resourceRoot := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.root",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			resourceRepo := new(mock.ResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)
			resourceRepo.On("GetByURN", destinationRoot.URN()).Return(resourceRoot, nil).Once()
			resourceRepo.On("GetByURN", destinationDownstream.URN()).Return(models.ResourceSpec{}, store.ErrResourceNotFound).Once()

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			backupResourceReqRoot := models.BackupResourceRequest{
				Resource: resourceRoot,
				Project:  projectSpec,
				DryRun:   true,
			}
			datastorer.On("BackupResource", context.TODO(), backupResourceReqRoot).Return(nil).Once()

			service := datastore.NewService(resourceRepoFac, dsRepo)
			resp, err := service.BackupResourceDryRun(context.TODO(), projectSpec, namespaceSpec, []models.JobSpec{jobRoot, jobDownstream})
			assert.Nil(t, err)
			assert.Equal(t, []string{destinationRoot.Destination}, resp)
		})
		t.Run("should not return error when one of the resources is not supported", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			depMod := new(mock.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)

			jobTask.Unit = &models.Plugin{Base: execUnit, DependencyMod: depMod}
			jobDownstream := models.JobSpec{
				ID:     uuid.Must(uuid.NewRandom()),
				Name:   "job-2",
				Task:   jobTask,
				Assets: jobAssets,
			}
			dependencies := make(map[string]models.JobSpecDependency)
			dependencies[jobDownstream.GetName()] = models.JobSpecDependency{
				Job: &jobDownstream,
			}
			jobRoot := models.JobSpec{
				ID:           uuid.Must(uuid.NewRandom()),
				Name:         "job-1",
				Task:         jobTask,
				Assets:       jobAssets,
				Dependencies: dependencies,
			}

			unitRoot := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobRoot.Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobRoot.Assets),
			}
			destinationRoot := &models.GenerateDestinationResponse{
				Destination: "project.dataset.table",
				Type:        models.DestinationTypeBigquery,
			}
			depMod.On("GenerateDestination", context.TODO(), unitRoot).Return(destinationRoot, nil).Once()

			unitDownstream := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobDownstream.Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobDownstream.Assets),
			}
			destinationDownstream := &models.GenerateDestinationResponse{
				Destination: "project.dataset.downstream",
				Type:        models.DestinationTypeBigquery,
			}
			depMod.On("GenerateDestination", context.TODO(), unitDownstream).Return(destinationDownstream, nil).Once()

			resourceRoot := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.root",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			resourceDownstream := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.downstream",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("GetByURN", destinationRoot.URN()).Return(resourceRoot, nil).Once()
			resourceRepo.On("GetByURN", destinationDownstream.URN()).Return(resourceDownstream, nil).Once()
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			backupResourceReqRoot := models.BackupResourceRequest{
				Resource: resourceRoot,
				Project:  projectSpec,
				DryRun:   true,
			}
			datastorer.On("BackupResource", context.TODO(), backupResourceReqRoot).Return(nil).Once()

			backupResourceReqDownstream := models.BackupResourceRequest{
				Resource: resourceDownstream,
				Project:  projectSpec,
				DryRun:   true,
			}
			datastorer.On("BackupResource", context.TODO(), backupResourceReqDownstream).Return(models.ErrUnsupportedResource).Once()

			service := datastore.NewService(resourceRepoFac, dsRepo)
			resp, err := service.BackupResourceDryRun(context.TODO(), projectSpec, namespaceSpec, []models.JobSpec{jobRoot, jobDownstream})
			assert.Nil(t, err)
			assert.Equal(t, []string{destinationRoot.Destination}, resp)
		})
	})
}
