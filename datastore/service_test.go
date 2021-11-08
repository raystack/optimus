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
	ctx := context.Background()

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
			resourceRepo.On("GetAll", ctx).Return([]models.ResourceSpec{resourceSpec1}, nil)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, nil, dsRepo, nil, nil)
			res, err := service.GetAll(ctx, namespaceSpec, "bq")
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
			datastorer.On("CreateResource", ctx, models.CreateResourceRequest{
				Project:  projectSpec,
				Resource: resourceSpec1,
			}).Return(nil)
			datastorer.On("CreateResource", ctx, models.CreateResourceRequest{
				Project:  projectSpec,
				Resource: resourceSpec2,
			}).Return(nil)

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("Save", ctx, resourceSpec1).Return(nil)
			resourceRepo.On("Save", ctx, resourceSpec2).Return(nil)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, nil, dsRepo, nil, nil)
			err := service.CreateResource(ctx, namespaceSpec, []models.ResourceSpec{resourceSpec1, resourceSpec2}, nil)
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
			datastorer.On("CreateResource", ctx, models.CreateResourceRequest{
				Project:  projectSpec,
				Resource: resourceSpec2,
			}).Return(nil)

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("Save", ctx, resourceSpec1).Return(errors.New("cant save, too busy"))
			resourceRepo.On("Save", ctx, resourceSpec2).Return(nil)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, nil, dsRepo, nil, nil)
			err := service.CreateResource(ctx, namespaceSpec, []models.ResourceSpec{resourceSpec1, resourceSpec2}, nil)
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
			datastorer.On("UpdateResource", ctx, models.UpdateResourceRequest{
				Project:  projectSpec,
				Resource: resourceSpec1,
			}).Return(nil)
			datastorer.On("UpdateResource", ctx, models.UpdateResourceRequest{
				Project:  projectSpec,
				Resource: resourceSpec2,
			}).Return(nil)

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("Save", ctx, resourceSpec1).Return(nil)
			resourceRepo.On("Save", ctx, resourceSpec2).Return(nil)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, nil, dsRepo, nil, nil)
			err := service.UpdateResource(ctx, namespaceSpec, []models.ResourceSpec{resourceSpec1, resourceSpec2}, nil)
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
			datastorer.On("UpdateResource", ctx, models.UpdateResourceRequest{
				Project:  projectSpec,
				Resource: resourceSpec2,
			}).Return(nil)

			resourceRepo := new(mock.ResourceSpecRepository)
			resourceRepo.On("Save", ctx, resourceSpec1).Return(errors.New("cant save, too busy"))
			resourceRepo.On("Save", ctx, resourceSpec2).Return(nil)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			resourceRepoFac.On("New", namespaceSpec, datastorer).Return(resourceRepo)
			defer resourceRepoFac.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, nil, dsRepo, nil, nil)
			err := service.UpdateResource(ctx, namespaceSpec, []models.ResourceSpec{resourceSpec1, resourceSpec2}, nil)
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

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, nil, dsRepo, nil, nil)
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

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, nil, dsRepo, nil, nil)
			_, err := service.ReadResource(ctx, namespaceSpec, "bq", resourceSpec1.Name)
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

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, nil, dsRepo, nil, nil)
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

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepoFac.AssertExpectations(t)

			service := datastore.NewService(resourceRepoFac, nil, dsRepo, nil, nil)
			err := service.DeleteResource(ctx, namespaceSpec, "bq", resourceSpec1.Name)
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
		destination := &models.GenerateDestinationResponse{
			Destination: "project.dataset.table",
			Type:        models.DestinationTypeBigquery,
		}

		t.Run("should return list of resources without dependents to be backed up", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			depMod := new(mock.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			resourceRepo := new(mock.ResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			defer resourceRepoFac.AssertExpectations(t)

			projectResourceRepo := new(mock.ProjectResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepo.AssertExpectations(t)

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
			resourceSpec := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.table",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			backupReq := models.BackupRequest{
				ResourceName:      resourceSpec.Name,
				Project:           projectSpec,
				Namespace:         namespaceSpec,
				IgnoreDownstream:  false,
				DryRun:            true,
				AllowedDownstream: models.AllNamespace,
			}
			backupResourceReq := models.BackupResourceRequest{
				Resource:   resourceSpec,
				BackupSpec: backupReq,
			}

			depMod.On("GenerateDestination", ctx, unitData).Return(destination, nil)
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)
			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)
			projectResourceRepo.On("GetByURN", ctx, destination.URN()).Return(resourceSpec, namespaceSpec, nil)
			datastorer.On("BackupResource", ctx, backupResourceReq).Return(models.BackupResourceResponse{}, nil)

			service := datastore.NewService(resourceRepoFac, projectResourceRepoFac, dsRepo, nil, nil)
			resp, err := service.BackupResourceDryRun(ctx, backupReq, []models.JobSpec{jobSpec})
			assert.Nil(t, err)
			assert.Equal(t, []string{destination.Destination}, resp.Resources)
			assert.Nil(t, resp.IgnoredResources)
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

			resourceRepo := new(mock.ResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			defer resourceRepoFac.AssertExpectations(t)

			projectResourceRepo := new(mock.ProjectResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepo.AssertExpectations(t)

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
				Destination: "project:dataset.root",
				Type:        models.DestinationTypeBigquery,
			}
			resourceRoot := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.root",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			backupReq := models.BackupRequest{
				ResourceName:      resourceRoot.Name,
				Project:           projectSpec,
				Namespace:         namespaceSpec,
				IgnoreDownstream:  false,
				DryRun:            true,
				AllowedDownstream: models.AllNamespace,
			}
			backupResourceReqRoot := models.BackupResourceRequest{
				Resource:   resourceRoot,
				BackupSpec: backupReq,
			}

			unitDownstream := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobDownstream.Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobDownstream.Assets),
			}
			destinationDownstream := &models.GenerateDestinationResponse{
				Destination: "project:dataset.downstream",
				Type:        models.DestinationTypeBigquery,
			}
			resourceDownstream := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.downstream",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			backupResourceReqDownstream := models.BackupResourceRequest{
				Resource:   resourceDownstream,
				BackupSpec: backupReq,
			}

			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)

			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)

			depMod.On("GenerateDestination", ctx, unitRoot).Return(destinationRoot, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationRoot.URN()).Return(resourceRoot, namespaceSpec, nil).Once()
			datastorer.On("BackupResource", ctx, backupResourceReqRoot).Return(models.BackupResourceResponse{}, nil).Once()

			depMod.On("GenerateDestination", ctx, unitDownstream).Return(destinationDownstream, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationDownstream.URN()).Return(resourceDownstream, namespaceSpec, nil).Once()
			datastorer.On("BackupResource", ctx, backupResourceReqDownstream).Return(models.BackupResourceResponse{}, nil).Once()

			service := datastore.NewService(resourceRepoFac, projectResourceRepoFac, dsRepo, nil, nil)
			resp, err := service.BackupResourceDryRun(ctx, backupReq, []models.JobSpec{jobRoot, jobDownstream})

			assert.Nil(t, err)
			assert.Equal(t, []string{destinationRoot.Destination, destinationDownstream.Destination}, resp.Resources)
			assert.Nil(t, resp.IgnoredResources)
		})
		t.Run("should return error when unable to generate destination", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			depMod := new(mock.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

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
			backupReq := models.BackupRequest{
				Project:          projectSpec,
				Namespace:        namespaceSpec,
				IgnoreDownstream: false,
				DryRun:           true,
			}

			errorMsg := "unable to generate destination"
			depMod.On("GenerateDestination", ctx, unitData).Return(&models.GenerateDestinationResponse{}, errors.New(errorMsg))

			service := datastore.NewService(nil, nil, dsRepo, nil, nil)
			resp, err := service.BackupResourceDryRun(ctx, backupReq, []models.JobSpec{jobSpec})

			assert.Contains(t, err.Error(), errorMsg)
			assert.Equal(t, models.BackupPlan{}, resp)
		})
		t.Run("should return error when unable to get datastorer", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			depMod := new(mock.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

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
			backupReq := models.BackupRequest{
				Project:          projectSpec,
				Namespace:        namespaceSpec,
				IgnoreDownstream: false,
				DryRun:           true,
			}

			depMod.On("GenerateDestination", ctx, unitData).Return(destination, nil)

			errorMsg := "unable to get datastorer"
			dsRepo.On("GetByName", destination.Type.String()).Return(datastorer, errors.New(errorMsg))

			service := datastore.NewService(nil, nil, dsRepo, nil, nil)
			resp, err := service.BackupResourceDryRun(ctx, backupReq, []models.JobSpec{jobSpec})

			assert.Contains(t, err.Error(), errorMsg)
			assert.Equal(t, models.BackupPlan{}, resp)
		})
		t.Run("should return error when unable to do backup dry run", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			depMod := new(mock.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			resourceRepo := new(mock.ResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			defer resourceRepoFac.AssertExpectations(t)

			projectResourceRepo := new(mock.ProjectResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepo.AssertExpectations(t)

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
			resourceSpec := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.table",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			backupReq := models.BackupRequest{
				ResourceName:      resourceSpec.Name,
				Project:           projectSpec,
				Namespace:         namespaceSpec,
				IgnoreDownstream:  false,
				DryRun:            true,
				AllowedDownstream: models.AllNamespace,
			}
			backupResourceReq := models.BackupResourceRequest{
				Resource:   resourceSpec,
				BackupSpec: backupReq,
			}

			depMod.On("GenerateDestination", ctx, unitData).Return(destination, nil)
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)
			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)
			projectResourceRepo.On("GetByURN", ctx, destination.URN()).Return(resourceSpec, namespaceSpec, nil)

			errorMsg := "unable to do backup dry run"
			datastorer.On("BackupResource", ctx, backupResourceReq).Return(models.BackupResourceResponse{}, errors.New(errorMsg))

			service := datastore.NewService(resourceRepoFac, projectResourceRepoFac, dsRepo, nil, nil)
			resp, err := service.BackupResourceDryRun(ctx, backupReq, []models.JobSpec{jobSpec})

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.BackupPlan{}, resp)
		})
		t.Run("should return error when unable to get resource", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			depMod := new(mock.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			resourceRepo := new(mock.ResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			defer resourceRepoFac.AssertExpectations(t)

			projectResourceRepo := new(mock.ProjectResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepo.AssertExpectations(t)

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
			backupReq := models.BackupRequest{
				Project:           projectSpec,
				Namespace:         namespaceSpec,
				IgnoreDownstream:  false,
				AllowedDownstream: models.AllNamespace,
			}

			depMod.On("GenerateDestination", ctx, unitData).Return(destination, nil)
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)
			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)

			errorMsg := "unable to get resource"
			projectResourceRepo.On("GetByURN", ctx, destination.URN()).Return(models.ResourceSpec{}, models.NamespaceSpec{}, errors.New(errorMsg))

			service := datastore.NewService(resourceRepoFac, projectResourceRepoFac, dsRepo, nil, nil)
			resp, err := service.BackupResourceDryRun(ctx, backupReq, []models.JobSpec{jobSpec})

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.BackupPlan{}, resp)
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

			resourceRepo := new(mock.ResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			defer resourceRepoFac.AssertExpectations(t)

			projectResourceRepo := new(mock.ProjectResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepo.AssertExpectations(t)

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
			resourceRoot := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.root",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			backupReq := models.BackupRequest{
				ResourceName:      resourceRoot.Name,
				Project:           projectSpec,
				Namespace:         namespaceSpec,
				IgnoreDownstream:  false,
				DryRun:            true,
				AllowedDownstream: models.AllNamespace,
			}
			backupResourceReqRoot := models.BackupResourceRequest{
				Resource:   resourceRoot,
				BackupSpec: backupReq,
			}
			unitDownstream := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobDownstream.Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobDownstream.Assets),
			}

			depMod.On("GenerateDestination", ctx, unitRoot).Return(destinationRoot, nil).Once()
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)
			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)
			projectResourceRepo.On("GetByURN", ctx, destinationRoot.URN()).Return(resourceRoot, namespaceSpec, nil).Once()
			datastorer.On("BackupResource", ctx, backupResourceReqRoot).Return(models.BackupResourceResponse{}, nil).Once()

			errorMsg := "unable to generate destination"
			depMod.On("GenerateDestination", ctx, unitDownstream).Return(&models.GenerateDestinationResponse{}, errors.New(errorMsg)).Once()

			service := datastore.NewService(resourceRepoFac, projectResourceRepoFac, dsRepo, nil, nil)
			resp, err := service.BackupResourceDryRun(ctx, backupReq, []models.JobSpec{jobRoot, jobDownstream})

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.BackupPlan{}, resp)
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

			resourceRepo := new(mock.ResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			defer resourceRepoFac.AssertExpectations(t)

			projectResourceRepo := new(mock.ProjectResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepo.AssertExpectations(t)

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
			resourceRoot := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.root",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			backupReq := models.BackupRequest{
				ResourceName:      resourceRoot.Name,
				Project:           projectSpec,
				Namespace:         namespaceSpec,
				IgnoreDownstream:  false,
				DryRun:            true,
				AllowedDownstream: models.AllNamespace,
			}
			backupResourceReqRoot := models.BackupResourceRequest{
				Resource:   resourceRoot,
				BackupSpec: backupReq,
			}

			unitDownstream := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobDownstream.Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobDownstream.Assets),
			}
			destinationDownstream := &models.GenerateDestinationResponse{
				Destination: "project.dataset.downstream",
				Type:        models.DestinationTypeBigquery,
			}

			depMod.On("GenerateDestination", ctx, unitRoot).Return(destinationRoot, nil).Once()
			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)
			projectResourceRepo.On("GetByURN", ctx, destinationRoot.URN()).Return(resourceRoot, namespaceSpec, nil).Once()
			datastorer.On("BackupResource", ctx, backupResourceReqRoot).Return(models.BackupResourceResponse{}, nil).Once()

			depMod.On("GenerateDestination", ctx, unitDownstream).Return(destinationDownstream, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationDownstream.URN()).Return(models.ResourceSpec{}, models.NamespaceSpec{}, store.ErrResourceNotFound).Once()

			service := datastore.NewService(resourceRepoFac, projectResourceRepoFac, dsRepo, nil, nil)
			resp, err := service.BackupResourceDryRun(ctx, backupReq, []models.JobSpec{jobRoot, jobDownstream})

			assert.Nil(t, err)
			assert.Equal(t, []string{destinationRoot.Destination}, resp.Resources)
			assert.Nil(t, resp.IgnoredResources)
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

			resourceRepo := new(mock.ResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			defer resourceRepoFac.AssertExpectations(t)

			projectResourceRepo := new(mock.ProjectResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepo.AssertExpectations(t)

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
			resourceRoot := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.root",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			backupReq := models.BackupRequest{
				ResourceName:      resourceRoot.Name,
				Project:           projectSpec,
				Namespace:         namespaceSpec,
				IgnoreDownstream:  false,
				DryRun:            true,
				AllowedDownstream: models.AllNamespace,
			}
			backupResourceReqRoot := models.BackupResourceRequest{
				Resource:   resourceRoot,
				BackupSpec: backupReq,
			}

			unitDownstream := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobDownstream.Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobDownstream.Assets),
			}
			destinationDownstream := &models.GenerateDestinationResponse{
				Destination: "project.dataset.downstream",
				Type:        models.DestinationTypeBigquery,
			}
			resourceDownstream := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.downstream",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			backupResourceReqDownstream := models.BackupResourceRequest{
				Resource:   resourceDownstream,
				BackupSpec: backupReq,
			}

			depMod.On("GenerateDestination", ctx, unitRoot).Return(destinationRoot, nil).Once()
			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)
			projectResourceRepo.On("GetByURN", ctx, destinationRoot.URN()).Return(resourceRoot, namespaceSpec, nil).Once()
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)
			datastorer.On("BackupResource", ctx, backupResourceReqRoot).Return(models.BackupResourceResponse{}, nil).Once()

			depMod.On("GenerateDestination", ctx, unitDownstream).Return(destinationDownstream, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationDownstream.URN()).Return(resourceDownstream, namespaceSpec, nil).Once()
			datastorer.On("BackupResource", ctx, backupResourceReqDownstream).Return(models.BackupResourceResponse{}, models.ErrUnsupportedResource).Once()

			service := datastore.NewService(resourceRepoFac, projectResourceRepoFac, dsRepo, nil, nil)
			resp, err := service.BackupResourceDryRun(ctx, backupReq, []models.JobSpec{jobRoot, jobDownstream})

			assert.Nil(t, err)
			assert.Equal(t, []string{destinationRoot.Destination}, resp.Resources)
			assert.Nil(t, resp.IgnoredResources)
		})
		t.Run("should return list of resources with dependents of only same namespace to be backed up", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			depMod := new(mock.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			resourceRepo := new(mock.ResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			defer resourceRepoFac.AssertExpectations(t)

			projectResourceRepo := new(mock.ProjectResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepo.AssertExpectations(t)

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
				Destination: "project:dataset.root",
				Type:        models.DestinationTypeBigquery,
			}
			resourceRoot := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.root",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			backupReq := models.BackupRequest{
				ResourceName:      resourceRoot.Name,
				Project:           projectSpec,
				Namespace:         namespaceSpec,
				IgnoreDownstream:  false,
				DryRun:            true,
				AllowedDownstream: namespaceSpec.Name,
			}
			backupResourceReqRoot := models.BackupResourceRequest{
				Resource:   resourceRoot,
				BackupSpec: backupReq,
			}

			unitDownstream := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobDownstream.Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobDownstream.Assets),
			}
			destinationDownstream := &models.GenerateDestinationResponse{
				Destination: "project:dataset.downstream",
				Type:        models.DestinationTypeBigquery,
			}
			resourceDownstream := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.downstream",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}

			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)

			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)

			depMod.On("GenerateDestination", ctx, unitRoot).Return(destinationRoot, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationRoot.URN()).Return(resourceRoot, namespaceSpec, nil).Once()
			datastorer.On("BackupResource", ctx, backupResourceReqRoot).Return(models.BackupResourceResponse{}, nil).Once()

			otherNamespaceSpec := models.NamespaceSpec{
				ID:          uuid.Must(uuid.NewRandom()),
				Name:        "dev-team-2",
				ProjectSpec: projectSpec,
			}
			depMod.On("GenerateDestination", ctx, unitDownstream).Return(destinationDownstream, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationDownstream.URN()).Return(resourceDownstream, otherNamespaceSpec, nil).Once()

			service := datastore.NewService(resourceRepoFac, projectResourceRepoFac, dsRepo, nil, nil)
			resp, err := service.BackupResourceDryRun(ctx, backupReq, []models.JobSpec{jobRoot, jobDownstream})

			assert.Nil(t, err)
			assert.Equal(t, []string{destinationRoot.Destination}, resp.Resources)
			assert.Equal(t, []string{destinationDownstream.Destination}, resp.IgnoredResources)
		})
	})
	t.Run("BackupResource", func(t *testing.T) {
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
		destination := &models.GenerateDestinationResponse{
			Destination: "project.dataset.table",
			Type:        models.DestinationTypeBigquery,
		}
		backupUUID := uuid.Must(uuid.NewRandom())

		t.Run("should able to do backup without downstream", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			depMod := new(mock.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			resourceRepo := new(mock.ResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			defer resourceRepoFac.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			backupRepo := new(mock.BackupRepo)
			defer backupRepo.AssertExpectations(t)

			backupRepoFac := new(mock.BackupRepoFactory)
			defer backupRepoFac.AssertExpectations(t)

			projectResourceRepo := new(mock.ProjectResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepo.AssertExpectations(t)

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
			resourceSpec := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.table",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			backupReq := models.BackupRequest{
				ID:                backupUUID,
				ResourceName:      resourceSpec.Name,
				Project:           projectSpec,
				Namespace:         namespaceSpec,
				IgnoreDownstream:  false,
				DryRun:            true,
				AllowedDownstream: models.AllNamespace,
			}
			backupResourceReq := models.BackupResourceRequest{
				Resource:   resourceSpec,
				BackupSpec: backupReq,
			}
			resultURN := "store://backupURN"
			resultSpec := map[string]interface{}{"project": projectSpec.Name, "location": "optimus_backup"}
			backupResult := models.BackupResult{
				URN:  resultURN,
				Spec: resultSpec,
			}
			backupSpec := models.BackupSpec{
				ID:          backupUUID,
				Resource:    resourceSpec,
				Result:      map[string]interface{}{resourceSpec.Name: backupResult},
				Description: "",
			}

			depMod.On("GenerateDestination", ctx, unitData).Return(destination, nil)
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)
			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)
			projectResourceRepo.On("GetByURN", ctx, destination.URN()).Return(resourceSpec, namespaceSpec, nil)
			datastorer.On("BackupResource", ctx, backupResourceReq).
				Return(models.BackupResourceResponse{ResultURN: resultURN, ResultSpec: resultSpec}, nil)
			uuidProvider.On("NewUUID").Return(backupUUID, nil)
			backupRepoFac.On("New", projectSpec, datastorer).Return(backupRepo)
			backupRepo.On("Save", ctx, backupSpec).Return(nil)

			service := datastore.NewService(resourceRepoFac, projectResourceRepoFac, dsRepo, uuidProvider, backupRepoFac)
			resp, err := service.BackupResource(ctx, backupReq, []models.JobSpec{jobSpec})
			assert.Nil(t, err)
			assert.Equal(t, []string{resultURN}, resp)
		})
		t.Run("should able to do backup with downstream", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			depMod := new(mock.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			resourceRepo := new(mock.ResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			defer resourceRepoFac.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			backupRepo := new(mock.BackupRepo)
			defer backupRepo.AssertExpectations(t)

			backupRepoFac := new(mock.BackupRepoFactory)
			defer backupRepoFac.AssertExpectations(t)

			projectResourceRepo := new(mock.ProjectResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepo.AssertExpectations(t)

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

			//root
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
				Destination: "project:dataset.root",
				Type:        models.DestinationTypeBigquery,
			}
			resourceRoot := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.root",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			backupReq := models.BackupRequest{
				ID:                backupUUID,
				ResourceName:      resourceRoot.Name,
				Project:           projectSpec,
				Namespace:         namespaceSpec,
				IgnoreDownstream:  false,
				DryRun:            true,
				AllowedDownstream: models.AllNamespace,
			}
			backupResourceReqRoot := models.BackupResourceRequest{
				Resource:   resourceRoot,
				BackupSpec: backupReq,
			}
			resultURNRoot := "store://optimus_backup:backupURNRoot"
			resultSpecRoot := map[string]interface{}{
				"project": projectSpec.Name, "location": "optimus_backup", "name": "backup_resource_root",
			}
			backupResultRoot := models.BackupResult{
				URN:  resultURNRoot,
				Spec: resultSpecRoot,
			}

			//downstream
			unitDownstream := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobDownstream.Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobDownstream.Assets),
			}
			destinationDownstream := &models.GenerateDestinationResponse{
				Destination: "project:dataset.downstream",
				Type:        models.DestinationTypeBigquery,
			}
			resourceDownstream := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.downstream",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			backupResourceReqDownstream := models.BackupResourceRequest{
				Resource:   resourceDownstream,
				BackupSpec: backupReq,
			}
			resultURNDownstream := "store://optimus_backup:backupURNDownstream"
			resultSpecDownstream := map[string]interface{}{
				"project": projectSpec.Name, "location": "optimus_backup", "name": "backup_resource_downstream",
			}
			backupResultDownstream := models.BackupResult{
				URN:  resultURNDownstream,
				Spec: resultSpecDownstream,
			}

			backupResult := map[string]interface{}{
				destinationRoot.Destination:       backupResultRoot,
				destinationDownstream.Destination: backupResultDownstream,
			}
			backupSpec := models.BackupSpec{
				ID:          backupUUID,
				Resource:    resourceRoot,
				Result:      backupResult,
				Description: "",
			}

			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)

			depMod.On("GenerateDestination", ctx, unitRoot).Return(destinationRoot, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationRoot.URN()).Return(resourceRoot, namespaceSpec, nil).Once()
			datastorer.On("BackupResource", ctx, backupResourceReqRoot).
				Return(models.BackupResourceResponse{ResultURN: resultURNRoot, ResultSpec: resultSpecRoot}, nil).Once()

			depMod.On("GenerateDestination", ctx, unitDownstream).Return(destinationDownstream, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationDownstream.URN()).Return(resourceDownstream, namespaceSpec, nil).Once()
			datastorer.On("BackupResource", ctx, backupResourceReqDownstream).
				Return(models.BackupResourceResponse{ResultURN: resultURNDownstream, ResultSpec: resultSpecDownstream}, nil).Once()

			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)

			uuidProvider.On("NewUUID").Return(backupUUID, nil)
			backupRepoFac.On("New", projectSpec, datastorer).Return(backupRepo)
			backupRepo.On("Save", ctx, backupSpec).Return(nil)

			service := datastore.NewService(resourceRepoFac, projectResourceRepoFac, dsRepo, uuidProvider, backupRepoFac)
			resp, err := service.BackupResource(ctx, backupReq, []models.JobSpec{jobRoot, jobDownstream})

			assert.Nil(t, err)
			assert.Equal(t, []string{resultURNRoot, resultURNDownstream}, resp)
		})
		t.Run("should able to do backup with only same namespace downstream", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			depMod := new(mock.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			resourceRepo := new(mock.ResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			defer resourceRepoFac.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			backupRepo := new(mock.BackupRepo)
			defer backupRepo.AssertExpectations(t)

			backupRepoFac := new(mock.BackupRepoFactory)
			defer backupRepoFac.AssertExpectations(t)

			projectResourceRepo := new(mock.ProjectResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepo.AssertExpectations(t)

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

			//root
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
				Destination: "project:dataset.root",
				Type:        models.DestinationTypeBigquery,
			}
			resourceRoot := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.root",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			backupReq := models.BackupRequest{
				ID:                backupUUID,
				ResourceName:      resourceRoot.Name,
				Project:           projectSpec,
				Namespace:         namespaceSpec,
				IgnoreDownstream:  false,
				DryRun:            true,
				AllowedDownstream: namespaceSpec.Name,
			}
			backupResourceReqRoot := models.BackupResourceRequest{
				Resource:   resourceRoot,
				BackupSpec: backupReq,
			}
			resultURNRoot := "store://optimus_backup:backupURNRoot"
			resultSpecRoot := map[string]interface{}{
				"project": projectSpec.Name, "location": "optimus_backup", "name": "backup_resource_root",
			}
			backupResultRoot := models.BackupResult{
				URN:  resultURNRoot,
				Spec: resultSpecRoot,
			}

			//downstream
			unitDownstream := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobDownstream.Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobDownstream.Assets),
			}
			destinationDownstream := &models.GenerateDestinationResponse{
				Destination: "project:dataset.downstream",
				Type:        models.DestinationTypeBigquery,
			}
			resourceDownstream := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.downstream",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}

			backupResult := map[string]interface{}{
				destinationRoot.Destination: backupResultRoot,
			}
			backupSpec := models.BackupSpec{
				ID:          backupUUID,
				Resource:    resourceRoot,
				Result:      backupResult,
				Description: "",
			}

			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)
			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)

			depMod.On("GenerateDestination", ctx, unitRoot).Return(destinationRoot, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationRoot.URN()).Return(resourceRoot, namespaceSpec, nil).Once()
			datastorer.On("BackupResource", ctx, backupResourceReqRoot).
				Return(models.BackupResourceResponse{ResultURN: resultURNRoot, ResultSpec: resultSpecRoot}, nil).Once()

			otherNamespaceSpec := models.NamespaceSpec{
				ID:          uuid.Must(uuid.NewRandom()),
				Name:        "dev-team-2",
				ProjectSpec: projectSpec,
			}
			depMod.On("GenerateDestination", ctx, unitDownstream).Return(destinationDownstream, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationDownstream.URN()).Return(resourceDownstream, otherNamespaceSpec, nil).Once()

			uuidProvider.On("NewUUID").Return(backupUUID, nil)
			backupRepoFac.On("New", projectSpec, datastorer).Return(backupRepo)
			backupRepo.On("Save", ctx, backupSpec).Return(nil)

			service := datastore.NewService(resourceRepoFac, projectResourceRepoFac, dsRepo, uuidProvider, backupRepoFac)
			resp, err := service.BackupResource(ctx, backupReq, []models.JobSpec{jobRoot, jobDownstream})

			assert.Nil(t, err)
			assert.Equal(t, []string{resultURNRoot}, resp)
		})
		t.Run("should return error when unable to generate destination", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			depMod := new(mock.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

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
			backupReq := models.BackupRequest{
				Project:          projectSpec,
				Namespace:        namespaceSpec,
				IgnoreDownstream: false,
				DryRun:           true,
			}

			uuidProvider.On("NewUUID").Return(backupUUID, nil)

			errorMsg := "unable to generate destination"
			depMod.On("GenerateDestination", ctx, unitData).Return(&models.GenerateDestinationResponse{}, errors.New(errorMsg))

			service := datastore.NewService(nil, nil, dsRepo, uuidProvider, nil)
			resp, err := service.BackupResource(ctx, backupReq, []models.JobSpec{jobSpec})

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, resp)
		})
		t.Run("should return error when unable to get datastorer", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			depMod := new(mock.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

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
			backupReq := models.BackupRequest{
				Project:          projectSpec,
				Namespace:        namespaceSpec,
				IgnoreDownstream: false,
				DryRun:           true,
			}

			uuidProvider.On("NewUUID").Return(backupUUID, nil)
			depMod.On("GenerateDestination", ctx, unitData).Return(destination, nil)

			errorMsg := "unable to get datastorer"
			dsRepo.On("GetByName", destination.Type.String()).Return(datastorer, errors.New(errorMsg))

			service := datastore.NewService(nil, nil, dsRepo, uuidProvider, nil)
			resp, err := service.BackupResource(ctx, backupReq, []models.JobSpec{jobSpec})

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, resp)
		})
		t.Run("should return error when unable to get resource", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			depMod := new(mock.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			resourceRepo := new(mock.ResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			defer resourceRepoFac.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			projectResourceRepo := new(mock.ProjectResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepo.AssertExpectations(t)

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
			backupReq := models.BackupRequest{
				Project:          projectSpec,
				Namespace:        namespaceSpec,
				IgnoreDownstream: false,
			}

			uuidProvider.On("NewUUID").Return(backupUUID, nil)
			depMod.On("GenerateDestination", ctx, unitData).Return(destination, nil)
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)
			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)

			errorMsg := "unable to get resource"
			projectResourceRepo.On("GetByURN", ctx, destination.URN()).Return(models.ResourceSpec{}, models.NamespaceSpec{}, errors.New(errorMsg))

			service := datastore.NewService(resourceRepoFac, projectResourceRepoFac, dsRepo, uuidProvider, nil)
			resp, err := service.BackupResource(ctx, backupReq, []models.JobSpec{jobSpec})

			assert.Equal(t, errorMsg, err.Error())
			assert.Nil(t, resp)
		})
		t.Run("should return error when unable to do backup", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			depMod := new(mock.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			resourceRepo := new(mock.ResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			defer resourceRepoFac.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			projectResourceRepo := new(mock.ProjectResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepo.AssertExpectations(t)

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
			resourceSpec := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.table",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			backupReq := models.BackupRequest{
				ID:                backupUUID,
				ResourceName:      resourceSpec.Name,
				Project:           projectSpec,
				Namespace:         namespaceSpec,
				IgnoreDownstream:  false,
				DryRun:            false,
				AllowedDownstream: models.AllNamespace,
			}
			backupResourceReq := models.BackupResourceRequest{
				Resource:   resourceSpec,
				BackupSpec: backupReq,
			}

			uuidProvider.On("NewUUID").Return(backupUUID, nil)
			depMod.On("GenerateDestination", ctx, unitData).Return(destination, nil)
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)
			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)
			projectResourceRepo.On("GetByURN", ctx, destination.URN()).Return(resourceSpec, namespaceSpec, nil)

			errorMsg := "unable to do backup"
			datastorer.On("BackupResource", ctx, backupResourceReq).Return(models.BackupResourceResponse{}, errors.New(errorMsg))

			service := datastore.NewService(resourceRepoFac, projectResourceRepoFac, dsRepo, uuidProvider, nil)
			resp, err := service.BackupResource(ctx, backupReq, []models.JobSpec{jobSpec})

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

			resourceRepo := new(mock.ResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			defer resourceRepoFac.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			projectResourceRepo := new(mock.ProjectResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepo.AssertExpectations(t)

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
			resourceRoot := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.root",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			backupReq := models.BackupRequest{
				ID:                backupUUID,
				ResourceName:      resourceRoot.Name,
				Project:           projectSpec,
				Namespace:         namespaceSpec,
				IgnoreDownstream:  false,
				DryRun:            true,
				AllowedDownstream: models.AllNamespace,
			}
			backupResourceReqRoot := models.BackupResourceRequest{
				Resource:   resourceRoot,
				BackupSpec: backupReq,
			}
			unitDownstream := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobDownstream.Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobDownstream.Assets),
			}

			uuidProvider.On("NewUUID").Return(backupUUID, nil)
			depMod.On("GenerateDestination", ctx, unitRoot).Return(destinationRoot, nil).Once()
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)
			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)
			projectResourceRepo.On("GetByURN", ctx, destinationRoot.URN()).Return(resourceRoot, namespaceSpec, nil).Once()
			datastorer.On("BackupResource", ctx, backupResourceReqRoot).Return(models.BackupResourceResponse{}, nil).Once()

			errorMsg := "unable to generate destination"
			depMod.On("GenerateDestination", ctx, unitDownstream).Return(&models.GenerateDestinationResponse{}, errors.New(errorMsg)).Once()

			service := datastore.NewService(resourceRepoFac, projectResourceRepoFac, dsRepo, uuidProvider, nil)
			resp, err := service.BackupResource(ctx, backupReq, []models.JobSpec{jobRoot, jobDownstream})

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

			resourceRepo := new(mock.ResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			defer resourceRepoFac.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			backupRepo := new(mock.BackupRepo)
			defer backupRepo.AssertExpectations(t)

			backupRepoFac := new(mock.BackupRepoFactory)
			defer backupRepoFac.AssertExpectations(t)

			projectResourceRepo := new(mock.ProjectResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepo.AssertExpectations(t)

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
			resourceRoot := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.root",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			backupReq := models.BackupRequest{
				ID:                backupUUID,
				ResourceName:      resourceRoot.Name,
				Project:           projectSpec,
				Namespace:         namespaceSpec,
				IgnoreDownstream:  false,
				DryRun:            true,
				AllowedDownstream: models.AllNamespace,
			}
			backupResourceReqRoot := models.BackupResourceRequest{
				Resource:   resourceRoot,
				BackupSpec: backupReq,
			}
			resultURNRoot := "store://optimus_backup:backupURNRoot"
			resultSpecRoot := map[string]interface{}{
				"project": projectSpec.Name, "location": "optimus_backup", "name": "backup_resource_root",
			}
			backupResultRoot := models.BackupResult{
				URN:  resultURNRoot,
				Spec: resultSpecRoot,
			}

			unitDownstream := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobDownstream.Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobDownstream.Assets),
			}
			destinationDownstream := &models.GenerateDestinationResponse{
				Destination: "project.dataset.downstream",
				Type:        models.DestinationTypeBigquery,
			}

			backupResult := map[string]interface{}{
				destinationRoot.Destination: backupResultRoot,
			}
			backupSpec := models.BackupSpec{
				ID:          backupUUID,
				Resource:    resourceRoot,
				Result:      backupResult,
				Description: "",
			}

			uuidProvider.On("NewUUID").Return(backupUUID, nil)
			depMod.On("GenerateDestination", ctx, unitRoot).Return(destinationRoot, nil).Once()
			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)
			projectResourceRepo.On("GetByURN", ctx, destinationRoot.URN()).Return(resourceRoot, namespaceSpec, nil).Once()
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)
			datastorer.On("BackupResource", ctx, backupResourceReqRoot).
				Return(models.BackupResourceResponse{ResultURN: resultURNRoot, ResultSpec: resultSpecRoot}, nil).Once()

			depMod.On("GenerateDestination", ctx, unitDownstream).Return(destinationDownstream, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationDownstream.URN()).Return(models.ResourceSpec{}, models.NamespaceSpec{}, store.ErrResourceNotFound).Once()

			backupRepoFac.On("New", projectSpec, datastorer).Return(backupRepo)
			backupRepo.On("Save", ctx, backupSpec).Return(nil)

			service := datastore.NewService(resourceRepoFac, projectResourceRepoFac, dsRepo, uuidProvider, backupRepoFac)
			resp, err := service.BackupResource(ctx, backupReq, []models.JobSpec{jobRoot, jobDownstream})

			assert.Nil(t, err)
			assert.Equal(t, []string{resultURNRoot}, resp)
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

			resourceRepo := new(mock.ResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			defer resourceRepoFac.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			backupRepo := new(mock.BackupRepo)
			defer backupRepo.AssertExpectations(t)

			backupRepoFac := new(mock.BackupRepoFactory)
			defer backupRepoFac.AssertExpectations(t)

			projectResourceRepo := new(mock.ProjectResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepo.AssertExpectations(t)

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
			resourceRoot := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.root",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			backupReq := models.BackupRequest{
				ID:                backupUUID,
				ResourceName:      resourceRoot.Name,
				Project:           projectSpec,
				Namespace:         namespaceSpec,
				IgnoreDownstream:  false,
				DryRun:            true,
				AllowedDownstream: models.AllNamespace,
			}
			backupResourceReqRoot := models.BackupResourceRequest{
				Resource:   resourceRoot,
				BackupSpec: backupReq,
			}
			resultURNRoot := "store://optimus_backup:backupURNRoot"
			resultSpecRoot := map[string]interface{}{
				"project": projectSpec.Name, "location": "optimus_backup", "name": "backup_resource_root",
			}
			backupResultRoot := models.BackupResult{
				URN:  resultURNRoot,
				Spec: resultSpecRoot,
			}

			unitDownstream := models.GenerateDestinationRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobDownstream.Task.Config),
				Assets: models.PluginAssets{}.FromJobSpec(jobDownstream.Assets),
			}
			destinationDownstream := &models.GenerateDestinationResponse{
				Destination: "project.dataset.downstream",
				Type:        models.DestinationTypeBigquery,
			}
			resourceDownstream := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.downstream",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			backupResourceReqDownstream := models.BackupResourceRequest{
				Resource:   resourceDownstream,
				BackupSpec: backupReq,
			}

			backupResult := map[string]interface{}{
				destinationRoot.Destination: backupResultRoot,
			}
			backupSpec := models.BackupSpec{
				ID:          backupUUID,
				Resource:    resourceRoot,
				Result:      backupResult,
				Description: "",
			}

			uuidProvider.On("NewUUID").Return(backupUUID, nil)
			depMod.On("GenerateDestination", ctx, unitRoot).Return(destinationRoot, nil).Once()
			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)
			projectResourceRepo.On("GetByURN", ctx, destinationRoot.URN()).Return(resourceRoot, namespaceSpec, nil).Once()
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)
			datastorer.On("BackupResource", ctx, backupResourceReqRoot).
				Return(models.BackupResourceResponse{ResultURN: resultURNRoot, ResultSpec: resultSpecRoot}, nil).Once()

			depMod.On("GenerateDestination", ctx, unitDownstream).Return(destinationDownstream, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationDownstream.URN()).Return(resourceDownstream, namespaceSpec, nil).Once()
			datastorer.On("BackupResource", ctx, backupResourceReqDownstream).Return(models.BackupResourceResponse{}, models.ErrUnsupportedResource).Once()

			backupRepoFac.On("New", projectSpec, datastorer).Return(backupRepo)
			backupRepo.On("Save", ctx, backupSpec).Return(nil)

			service := datastore.NewService(resourceRepoFac, projectResourceRepoFac, dsRepo, uuidProvider, backupRepoFac)
			resp, err := service.BackupResource(ctx, backupReq, []models.JobSpec{jobRoot, jobDownstream})

			assert.Nil(t, err)
			assert.Equal(t, []string{resultURNRoot}, resp)
		})
	})
	t.Run("ListBackupResources", func(t *testing.T) {
		datastoreName := models.DestinationTypeBigquery.String()
		backupSpecs := []models.BackupSpec{
			{
				ID:        uuid.Must(uuid.NewRandom()),
				CreatedAt: time.Now().Add(time.Hour * 24 * -30),
			},
			{
				ID:        uuid.Must(uuid.NewRandom()),
				CreatedAt: time.Now().Add(time.Hour * 24 * -50),
			},
			{
				ID:        uuid.Must(uuid.NewRandom()),
				CreatedAt: time.Now().Add(time.Hour * 24 * -100),
			},
		}
		t.Run("should return list of recent backups", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			backupRepo := new(mock.BackupRepo)
			defer backupRepo.AssertExpectations(t)

			backupRepoFac := new(mock.BackupRepoFactory)
			defer backupRepoFac.AssertExpectations(t)

			dsRepo.On("GetByName", datastoreName).Return(datastorer, nil)
			backupRepoFac.On("New", projectSpec, datastorer).Return(backupRepo)
			backupRepo.On("GetAll", ctx).Return(backupSpecs, nil)

			service := datastore.NewService(nil, nil, dsRepo, nil, backupRepoFac)
			resp, err := service.ListBackupResources(ctx, projectSpec, datastoreName)

			assert.Nil(t, err)
			assert.Equal(t, []models.BackupSpec{backupSpecs[0], backupSpecs[1]}, resp)
		})
		t.Run("should fail when unable to get datastore", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			errorMsg := "unable to get datastore"
			dsRepo.On("GetByName", datastoreName).Return(datastorer, errors.New(errorMsg))

			service := datastore.NewService(nil, nil, dsRepo, nil, nil)
			resp, err := service.ListBackupResources(ctx, projectSpec, datastoreName)

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, []models.BackupSpec{}, resp)
		})
		t.Run("should fail when unable to get backups", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			backupRepo := new(mock.BackupRepo)
			defer backupRepo.AssertExpectations(t)

			backupRepoFac := new(mock.BackupRepoFactory)
			defer backupRepoFac.AssertExpectations(t)

			dsRepo.On("GetByName", datastoreName).Return(datastorer, nil)
			backupRepoFac.On("New", projectSpec, datastorer).Return(backupRepo)

			errorMsg := "unable to get backups"
			backupRepo.On("GetAll", ctx).Return([]models.BackupSpec{}, errors.New(errorMsg))

			service := datastore.NewService(nil, nil, dsRepo, nil, backupRepoFac)
			resp, err := service.ListBackupResources(ctx, projectSpec, datastoreName)

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, []models.BackupSpec{}, resp)
		})
		t.Run("should not return error when no backups are found", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			backupRepo := new(mock.BackupRepo)
			defer backupRepo.AssertExpectations(t)

			backupRepoFac := new(mock.BackupRepoFactory)
			defer backupRepoFac.AssertExpectations(t)

			dsRepo.On("GetByName", datastoreName).Return(datastorer, nil)
			backupRepoFac.On("New", projectSpec, datastorer).Return(backupRepo)
			backupRepo.On("GetAll", ctx).Return([]models.BackupSpec{}, store.ErrResourceNotFound)

			service := datastore.NewService(nil, nil, dsRepo, nil, backupRepoFac)
			resp, err := service.ListBackupResources(ctx, projectSpec, datastoreName)

			assert.Nil(t, err)
			assert.Equal(t, []models.BackupSpec{}, resp)
		})
		t.Run("should not return error when no recent backups are found", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			backupRepo := new(mock.BackupRepo)
			defer backupRepo.AssertExpectations(t)

			backupRepoFac := new(mock.BackupRepoFactory)
			defer backupRepoFac.AssertExpectations(t)

			dsRepo.On("GetByName", datastoreName).Return(datastorer, nil)
			backupRepoFac.On("New", projectSpec, datastorer).Return(backupRepo)
			backupRepo.On("GetAll", ctx).Return([]models.BackupSpec{backupSpecs[2]}, nil)

			service := datastore.NewService(nil, nil, dsRepo, nil, backupRepoFac)
			resp, err := service.ListBackupResources(ctx, projectSpec, datastoreName)

			assert.Nil(t, err)
			assert.Equal(t, 0, len(resp))
		})
	})
}
