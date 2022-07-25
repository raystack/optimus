package datastore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/datastore"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

func TestBackupService(t *testing.T) {
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

	t.Run("BackupResourceDryRun", func(t *testing.T) {
		jobTask := models.JobSpecTask{
			Config: models.JobSpecConfigs{
				{
					Name:  "do",
					Value: "this",
				},
			},
			Priority: 2000,
			Window: &&models.WindowV1{
				SizeAsDuration:   time.Hour,
				OffsetAsDuration: 0,
				TruncateTo:       "d",
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

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			resourceRepo := new(mock.ResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			defer resourceRepoFac.AssertExpectations(t)

			projectResourceRepo := new(mock.ProjectResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepo.AssertExpectations(t)

			jobTask.Unit = &models.Plugin{Base: execUnit}
			jobSpec := models.JobSpec{
				ID:     uuid.New(),
				Name:   "job-1",
				Task:   jobTask,
				Assets: jobAssets,
			}
			resourceSpec := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.table",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			backupReq := models.BackupRequest{
				ResourceName:                resourceSpec.Name,
				Project:                     projectSpec,
				Namespace:                   namespaceSpec,
				DryRun:                      true,
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			backupResourceReq := models.BackupResourceRequest{
				Resource:   resourceSpec,
				BackupSpec: backupReq,
			}

			pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)
			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)
			projectResourceRepo.On("GetByURN", ctx, destination.URN()).Return(resourceSpec, namespaceSpec, nil)
			datastorer.On("BackupResource", ctx, backupResourceReq).Return(models.BackupResourceResponse{}, nil)

			service := datastore.NewBackupService(projectResourceRepoFac, dsRepo, nil, nil, pluginService)
			resp, err := service.BackupResourceDryRun(ctx, backupReq, []models.JobSpec{jobSpec})
			assert.Nil(t, err)
			assert.Equal(t, []string{destination.Destination}, resp.Resources)
			assert.Nil(t, resp.IgnoredResources)
		})
		t.Run("should return list of resources with dependents to be backed up", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

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

			jobTask.Unit = &models.Plugin{Base: execUnit}
			jobDownstream := models.JobSpec{
				ID:     uuid.New(),
				Name:   "job-2",
				Task:   jobTask,
				Assets: jobAssets,
			}
			dependencies := make(map[string]models.JobSpecDependency)
			dependencies[jobDownstream.GetName()] = models.JobSpecDependency{
				Job: &jobDownstream,
			}

			jobRoot := models.JobSpec{
				ID:           uuid.New(),
				Name:         "job-1",
				Task:         jobTask,
				Assets:       jobAssets,
				Dependencies: dependencies,
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
				ResourceName:                resourceRoot.Name,
				Project:                     projectSpec,
				Namespace:                   namespaceSpec,
				DryRun:                      true,
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			backupResourceReqRoot := models.BackupResourceRequest{
				Resource:   resourceRoot,
				BackupSpec: backupReq,
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

			pluginService.On("GenerateDestination", ctx, jobRoot, namespaceSpec).Return(destinationRoot, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationRoot.URN()).Return(resourceRoot, namespaceSpec, nil).Once()
			datastorer.On("BackupResource", ctx, backupResourceReqRoot).Return(models.BackupResourceResponse{}, nil).Once()

			pluginService.On("GenerateDestination", ctx, jobDownstream, namespaceSpec).Return(destinationDownstream, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationDownstream.URN()).Return(resourceDownstream, namespaceSpec, nil).Once()
			datastorer.On("BackupResource", ctx, backupResourceReqDownstream).Return(models.BackupResourceResponse{}, nil).Once()

			service := datastore.NewBackupService(projectResourceRepoFac, dsRepo, nil, nil, pluginService)
			resp, err := service.BackupResourceDryRun(ctx, backupReq, []models.JobSpec{jobRoot, jobDownstream})

			assert.Nil(t, err)
			assert.Equal(t, []string{destinationRoot.Destination, destinationDownstream.Destination}, resp.Resources)
			assert.Nil(t, resp.IgnoredResources)
		})
		t.Run("should return error when unable to generate destination", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			jobTask.Unit = &models.Plugin{Base: execUnit}
			jobSpec := models.JobSpec{
				ID:     uuid.New(),
				Name:   "job-1",
				Task:   jobTask,
				Assets: jobAssets,
			}
			backupReq := models.BackupRequest{
				Project:   projectSpec,
				Namespace: namespaceSpec,
				DryRun:    true,
			}

			errorMsg := "unable to generate destination"
			pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(&models.GenerateDestinationResponse{}, errors.New(errorMsg))

			service := datastore.NewBackupService(nil, dsRepo, nil, nil, pluginService)
			resp, err := service.BackupResourceDryRun(ctx, backupReq, []models.JobSpec{jobSpec})

			assert.Contains(t, err.Error(), errorMsg)
			assert.Equal(t, models.BackupPlan{}, resp)
		})
		t.Run("should return error when unable to get datastorer", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			jobTask.Unit = &models.Plugin{Base: execUnit}
			jobSpec := models.JobSpec{
				ID:     uuid.New(),
				Name:   "job-1",
				Task:   jobTask,
				Assets: jobAssets,
			}
			backupReq := models.BackupRequest{
				Project:   projectSpec,
				Namespace: namespaceSpec,
				DryRun:    true,
			}

			pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)

			errorMsg := "unable to get datastorer"
			dsRepo.On("GetByName", destination.Type.String()).Return(datastorer, errors.New(errorMsg))

			service := datastore.NewBackupService(nil, dsRepo, nil, nil, pluginService)
			resp, err := service.BackupResourceDryRun(ctx, backupReq, []models.JobSpec{jobSpec})

			assert.Contains(t, err.Error(), errorMsg)
			assert.Equal(t, models.BackupPlan{}, resp)
		})
		t.Run("should return error when unable to do backup dry run", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

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

			jobTask.Unit = &models.Plugin{Base: execUnit}
			jobSpec := models.JobSpec{
				ID:     uuid.New(),
				Name:   "job-1",
				Task:   jobTask,
				Assets: jobAssets,
			}
			resourceSpec := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.table",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			backupReq := models.BackupRequest{
				ResourceName:                resourceSpec.Name,
				Project:                     projectSpec,
				Namespace:                   namespaceSpec,
				DryRun:                      true,
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			backupResourceReq := models.BackupResourceRequest{
				Resource:   resourceSpec,
				BackupSpec: backupReq,
			}

			pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)
			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)
			projectResourceRepo.On("GetByURN", ctx, destination.URN()).Return(resourceSpec, namespaceSpec, nil)

			errorMsg := "unable to do backup dry run"
			datastorer.On("BackupResource", ctx, backupResourceReq).Return(models.BackupResourceResponse{}, errors.New(errorMsg))

			service := datastore.NewBackupService(projectResourceRepoFac, dsRepo, nil, nil, pluginService)
			resp, err := service.BackupResourceDryRun(ctx, backupReq, []models.JobSpec{jobSpec})

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.BackupPlan{}, resp)
		})
		t.Run("should return error when unable to get resource", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

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

			jobTask.Unit = &models.Plugin{Base: execUnit}
			jobSpec := models.JobSpec{
				ID:     uuid.New(),
				Name:   "job-1",
				Task:   jobTask,
				Assets: jobAssets,
			}

			backupReq := models.BackupRequest{
				Project:                     projectSpec,
				Namespace:                   namespaceSpec,
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}

			pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)
			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)

			errorMsg := "unable to get resource"
			projectResourceRepo.On("GetByURN", ctx, destination.URN()).Return(models.ResourceSpec{}, models.NamespaceSpec{}, errors.New(errorMsg))

			service := datastore.NewBackupService(projectResourceRepoFac, dsRepo, nil, nil, pluginService)
			resp, err := service.BackupResourceDryRun(ctx, backupReq, []models.JobSpec{jobSpec})

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.BackupPlan{}, resp)
		})
		t.Run("should return error when unable to generate destination for downstream", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

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

			jobTask.Unit = &models.Plugin{Base: execUnit}
			jobDownstream := models.JobSpec{
				ID:     uuid.New(),
				Name:   "job-2",
				Task:   jobTask,
				Assets: jobAssets,
			}
			dependencies := make(map[string]models.JobSpecDependency)
			dependencies[jobDownstream.GetName()] = models.JobSpecDependency{
				Job: &jobDownstream,
			}
			jobRoot := models.JobSpec{
				ID:           uuid.New(),
				Name:         "job-1",
				Task:         jobTask,
				Assets:       jobAssets,
				Dependencies: dependencies,
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
				ResourceName:                resourceRoot.Name,
				Project:                     projectSpec,
				Namespace:                   namespaceSpec,
				DryRun:                      true,
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			backupResourceReqRoot := models.BackupResourceRequest{
				Resource:   resourceRoot,
				BackupSpec: backupReq,
			}

			pluginService.On("GenerateDestination", ctx, jobRoot, namespaceSpec).Return(destinationRoot, nil).Once()
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)
			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)
			projectResourceRepo.On("GetByURN", ctx, destinationRoot.URN()).Return(resourceRoot, namespaceSpec, nil).Once()
			datastorer.On("BackupResource", ctx, backupResourceReqRoot).Return(models.BackupResourceResponse{}, nil).Once()

			errorMsg := "unable to generate destination"
			pluginService.On("GenerateDestination", ctx, jobDownstream, namespaceSpec).Return(&models.GenerateDestinationResponse{}, errors.New(errorMsg)).Once()

			service := datastore.NewBackupService(projectResourceRepoFac, dsRepo, nil, nil, pluginService)
			resp, err := service.BackupResourceDryRun(ctx, backupReq, []models.JobSpec{jobRoot, jobDownstream})

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.BackupPlan{}, resp)
		})
		t.Run("should not return error when one of the resources is not found", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

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

			jobTask.Unit = &models.Plugin{Base: execUnit}
			jobDownstream := models.JobSpec{
				ID:     uuid.New(),
				Name:   "job-2",
				Task:   jobTask,
				Assets: jobAssets,
			}
			dependencies := make(map[string]models.JobSpecDependency)
			dependencies[jobDownstream.GetName()] = models.JobSpecDependency{
				Job: &jobDownstream,
			}
			jobRoot := models.JobSpec{
				ID:           uuid.New(),
				Name:         "job-1",
				Task:         jobTask,
				Assets:       jobAssets,
				Dependencies: dependencies,
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
				ResourceName:                resourceRoot.Name,
				Project:                     projectSpec,
				Namespace:                   namespaceSpec,
				DryRun:                      true,
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			backupResourceReqRoot := models.BackupResourceRequest{
				Resource:   resourceRoot,
				BackupSpec: backupReq,
			}

			destinationDownstream := &models.GenerateDestinationResponse{
				Destination: "project.dataset.downstream",
				Type:        models.DestinationTypeBigquery,
			}

			pluginService.On("GenerateDestination", ctx, jobRoot, namespaceSpec).Return(destinationRoot, nil).Once()
			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)
			projectResourceRepo.On("GetByURN", ctx, destinationRoot.URN()).Return(resourceRoot, namespaceSpec, nil).Once()
			datastorer.On("BackupResource", ctx, backupResourceReqRoot).Return(models.BackupResourceResponse{}, nil).Once()

			pluginService.On("GenerateDestination", ctx, jobDownstream, namespaceSpec).Return(destinationDownstream, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationDownstream.URN()).Return(models.ResourceSpec{}, models.NamespaceSpec{}, store.ErrResourceNotFound).Once()

			service := datastore.NewBackupService(projectResourceRepoFac, dsRepo, nil, nil, pluginService)
			resp, err := service.BackupResourceDryRun(ctx, backupReq, []models.JobSpec{jobRoot, jobDownstream})

			assert.Nil(t, err)
			assert.Equal(t, []string{destinationRoot.Destination}, resp.Resources)
			assert.Nil(t, resp.IgnoredResources)
		})
		t.Run("should not return error when one of the resources is not supported", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

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

			jobTask.Unit = &models.Plugin{Base: execUnit}
			jobDownstream := models.JobSpec{
				ID:     uuid.New(),
				Name:   "job-2",
				Task:   jobTask,
				Assets: jobAssets,
			}
			dependencies := make(map[string]models.JobSpecDependency)
			dependencies[jobDownstream.GetName()] = models.JobSpecDependency{
				Job: &jobDownstream,
			}
			jobRoot := models.JobSpec{
				ID:           uuid.New(),
				Name:         "job-1",
				Task:         jobTask,
				Assets:       jobAssets,
				Dependencies: dependencies,
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
				ResourceName:                resourceRoot.Name,
				Project:                     projectSpec,
				Namespace:                   namespaceSpec,
				DryRun:                      true,
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			backupResourceReqRoot := models.BackupResourceRequest{
				Resource:   resourceRoot,
				BackupSpec: backupReq,
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

			pluginService.On("GenerateDestination", ctx, jobRoot, namespaceSpec).Return(destinationRoot, nil).Once()
			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)
			projectResourceRepo.On("GetByURN", ctx, destinationRoot.URN()).Return(resourceRoot, namespaceSpec, nil).Once()
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)
			datastorer.On("BackupResource", ctx, backupResourceReqRoot).Return(models.BackupResourceResponse{}, nil).Once()

			pluginService.On("GenerateDestination", ctx, jobDownstream, namespaceSpec).Return(destinationDownstream, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationDownstream.URN()).Return(resourceDownstream, namespaceSpec, nil).Once()
			datastorer.On("BackupResource", ctx, backupResourceReqDownstream).Return(models.BackupResourceResponse{}, models.ErrUnsupportedResource).Once()

			service := datastore.NewBackupService(projectResourceRepoFac, dsRepo, nil, nil, pluginService)
			resp, err := service.BackupResourceDryRun(ctx, backupReq, []models.JobSpec{jobRoot, jobDownstream})

			assert.Nil(t, err)
			assert.Equal(t, []string{destinationRoot.Destination}, resp.Resources)
			assert.Nil(t, resp.IgnoredResources)
		})
		t.Run("should return list of resources with dependents of only same namespace to be backed up", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

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

			jobTask.Unit = &models.Plugin{Base: execUnit}
			jobDownstream := models.JobSpec{
				ID:     uuid.New(),
				Name:   "job-2",
				Task:   jobTask,
				Assets: jobAssets,
			}
			dependencies := make(map[string]models.JobSpecDependency)
			dependencies[jobDownstream.GetName()] = models.JobSpecDependency{
				Job: &jobDownstream,
			}

			jobRoot := models.JobSpec{
				ID:           uuid.New(),
				Name:         "job-1",
				Task:         jobTask,
				Assets:       jobAssets,
				Dependencies: dependencies,
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
				ResourceName:                resourceRoot.Name,
				Project:                     projectSpec,
				Namespace:                   namespaceSpec,
				DryRun:                      true,
				AllowedDownstreamNamespaces: []string{namespaceSpec.Name},
			}
			backupResourceReqRoot := models.BackupResourceRequest{
				Resource:   resourceRoot,
				BackupSpec: backupReq,
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

			pluginService.On("GenerateDestination", ctx, jobRoot, namespaceSpec).Return(destinationRoot, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationRoot.URN()).Return(resourceRoot, namespaceSpec, nil).Once()
			datastorer.On("BackupResource", ctx, backupResourceReqRoot).Return(models.BackupResourceResponse{}, nil).Once()

			otherNamespaceSpec := models.NamespaceSpec{
				ID:          uuid.New(),
				Name:        "dev-team-2",
				ProjectSpec: projectSpec,
			}
			pluginService.On("GenerateDestination", ctx, jobDownstream, namespaceSpec).Return(destinationDownstream, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationDownstream.URN()).Return(resourceDownstream, otherNamespaceSpec, nil).Once()

			service := datastore.NewBackupService(projectResourceRepoFac, dsRepo, nil, nil, pluginService)
			resp, err := service.BackupResourceDryRun(ctx, backupReq, []models.JobSpec{jobRoot, jobDownstream})

			assert.Nil(t, err)
			assert.Equal(t, []string{destinationRoot.Destination}, resp.Resources)
			assert.Equal(t, []string{destinationDownstream.Destination}, resp.IgnoredResources)
		})
		t.Run("should return list of resources without dependents to be backed up if downstream is ignored", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

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

			jobTask.Unit = &models.Plugin{Base: execUnit}
			jobDownstream := models.JobSpec{
				ID:     uuid.New(),
				Name:   "job-2",
				Task:   jobTask,
				Assets: jobAssets,
			}
			dependencies := make(map[string]models.JobSpecDependency)
			dependencies[jobDownstream.GetName()] = models.JobSpecDependency{
				Job: &jobDownstream,
			}

			jobRoot := models.JobSpec{
				ID:           uuid.New(),
				Name:         "job-1",
				Task:         jobTask,
				Assets:       jobAssets,
				Dependencies: dependencies,
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
				ResourceName: resourceRoot.Name,
				Project:      projectSpec,
				Namespace:    namespaceSpec,
				DryRun:       true,
			}
			backupResourceReqRoot := models.BackupResourceRequest{
				Resource:   resourceRoot,
				BackupSpec: backupReq,
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

			pluginService.On("GenerateDestination", ctx, jobRoot, namespaceSpec).Return(destinationRoot, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationRoot.URN()).Return(resourceRoot, namespaceSpec, nil).Once()
			datastorer.On("BackupResource", ctx, backupResourceReqRoot).Return(models.BackupResourceResponse{}, nil).Once()

			pluginService.On("GenerateDestination", ctx, jobDownstream, namespaceSpec).Return(destinationDownstream, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationDownstream.URN()).Return(resourceDownstream, namespaceSpec, nil).Once()

			service := datastore.NewBackupService(projectResourceRepoFac, dsRepo, nil, nil, pluginService)
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
			Window: &&models.WindowV1{
				SizeAsDuration:   time.Hour,
				OffsetAsDuration: 0,
				TruncateTo:       "d",
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
		backupUUID := uuid.New()

		t.Run("should able to do backup without downstream", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			resourceRepo := new(mock.ResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			resourceRepoFac := new(mock.ResourceSpecRepoFactory)
			defer resourceRepoFac.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			backupRepo := new(mock.BackupRepo)
			defer backupRepo.AssertExpectations(t)

			projectResourceRepo := new(mock.ProjectResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepo.AssertExpectations(t)

			jobTask.Unit = &models.Plugin{Base: execUnit}
			jobSpec := models.JobSpec{
				ID:     uuid.New(),
				Name:   "job-1",
				Task:   jobTask,
				Assets: jobAssets,
			}
			resourceSpec := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.table",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			backupReq := models.BackupRequest{
				ID:                          backupUUID,
				ResourceName:                resourceSpec.Name,
				Project:                     projectSpec,
				Namespace:                   namespaceSpec,
				DryRun:                      true,
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			backupResourceReq := models.BackupResourceRequest{
				Resource:   resourceSpec,
				BackupSpec: backupReq,
			}
			resultURN := "store://backupURN"
			resultSpec := map[string]interface{}{"project": projectSpec.Name, "location": "optimus_backup"}
			backupResult := models.BackupDetail{
				URN:  resultURN,
				Spec: resultSpec,
			}
			backupSpec := models.BackupSpec{
				ID:          backupUUID,
				Resource:    resourceSpec,
				Result:      map[string]interface{}{resourceSpec.Name: backupResult},
				Description: "",
				Config:      map[string]string{models.ConfigIgnoreDownstream: "false"},
			}

			pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)
			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)
			projectResourceRepo.On("GetByURN", ctx, destination.URN()).Return(resourceSpec, namespaceSpec, nil)
			datastorer.On("BackupResource", ctx, backupResourceReq).
				Return(models.BackupResourceResponse{ResultURN: resultURN, ResultSpec: resultSpec}, nil)
			uuidProvider.On("NewUUID").Return(backupUUID, nil)
			backupRepo.On("Save", ctx, backupSpec).Return(nil)

			service := datastore.NewBackupService(projectResourceRepoFac, dsRepo, uuidProvider, backupRepo, pluginService)
			resp, err := service.BackupResource(ctx, backupReq, []models.JobSpec{jobSpec})
			assert.Nil(t, err)
			assert.Equal(t, []string{resultURN}, resp.Resources)
			assert.Nil(t, resp.IgnoredResources)
		})
		t.Run("should able to do backup with downstream", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

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

			projectResourceRepo := new(mock.ProjectResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepo.AssertExpectations(t)

			jobTask.Unit = &models.Plugin{Base: execUnit}
			jobDownstream := models.JobSpec{
				ID:     uuid.New(),
				Name:   "job-2",
				Task:   jobTask,
				Assets: jobAssets,
			}
			dependencies := make(map[string]models.JobSpecDependency)
			dependencies[jobDownstream.GetName()] = models.JobSpecDependency{
				Job: &jobDownstream,
			}

			// root
			jobRoot := models.JobSpec{
				ID:           uuid.New(),
				Name:         "job-1",
				Task:         jobTask,
				Assets:       jobAssets,
				Dependencies: dependencies,
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
				ID:                          backupUUID,
				ResourceName:                resourceRoot.Name,
				Project:                     projectSpec,
				Namespace:                   namespaceSpec,
				DryRun:                      true,
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			backupResourceReqRoot := models.BackupResourceRequest{
				Resource:   resourceRoot,
				BackupSpec: backupReq,
			}
			resultURNRoot := "store://optimus_backup:backupURNRoot"
			resultSpecRoot := map[string]interface{}{
				"project": projectSpec.Name, "location": "optimus_backup", "name": "backup_resource_root",
			}
			backupResultRoot := models.BackupDetail{
				URN:  resultURNRoot,
				Spec: resultSpecRoot,
			}

			// downstream
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
			backupResultDownstream := models.BackupDetail{
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
				Config:      map[string]string{models.ConfigIgnoreDownstream: "false"},
			}

			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)

			pluginService.On("GenerateDestination", ctx, jobRoot, namespaceSpec).Return(destinationRoot, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationRoot.URN()).Return(resourceRoot, namespaceSpec, nil).Once()
			datastorer.On("BackupResource", ctx, backupResourceReqRoot).
				Return(models.BackupResourceResponse{ResultURN: resultURNRoot, ResultSpec: resultSpecRoot}, nil).Once()

			pluginService.On("GenerateDestination", ctx, jobDownstream, namespaceSpec).Return(destinationDownstream, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationDownstream.URN()).Return(resourceDownstream, namespaceSpec, nil).Once()
			datastorer.On("BackupResource", ctx, backupResourceReqDownstream).
				Return(models.BackupResourceResponse{ResultURN: resultURNDownstream, ResultSpec: resultSpecDownstream}, nil).Once()

			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)

			uuidProvider.On("NewUUID").Return(backupUUID, nil)
			backupRepo.On("Save", ctx, backupSpec).Return(nil)

			service := datastore.NewBackupService(projectResourceRepoFac, dsRepo, uuidProvider, backupRepo, pluginService)
			resp, err := service.BackupResource(ctx, backupReq, []models.JobSpec{jobRoot, jobDownstream})

			assert.Nil(t, err)
			assert.Equal(t, []string{resultURNRoot, resultURNDownstream}, resp.Resources)
			assert.Nil(t, resp.IgnoredResources)
		})
		t.Run("should able to do backup with only same namespace downstream", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

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

			projectResourceRepo := new(mock.ProjectResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepo.AssertExpectations(t)

			jobTask.Unit = &models.Plugin{Base: execUnit}
			jobDownstream := models.JobSpec{
				ID:     uuid.New(),
				Name:   "job-2",
				Task:   jobTask,
				Assets: jobAssets,
			}
			dependencies := make(map[string]models.JobSpecDependency)
			dependencies[jobDownstream.GetName()] = models.JobSpecDependency{
				Job: &jobDownstream,
			}

			// root
			jobRoot := models.JobSpec{
				ID:           uuid.New(),
				Name:         "job-1",
				Task:         jobTask,
				Assets:       jobAssets,
				Dependencies: dependencies,
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
				ID:                          backupUUID,
				ResourceName:                resourceRoot.Name,
				Project:                     projectSpec,
				Namespace:                   namespaceSpec,
				DryRun:                      true,
				AllowedDownstreamNamespaces: []string{namespaceSpec.Name},
			}
			backupResourceReqRoot := models.BackupResourceRequest{
				Resource:   resourceRoot,
				BackupSpec: backupReq,
			}
			resultURNRoot := "store://optimus_backup:backupURNRoot"
			resultSpecRoot := map[string]interface{}{
				"project": projectSpec.Name, "location": "optimus_backup", "name": "backup_resource_root",
			}
			backupResultRoot := models.BackupDetail{
				URN:  resultURNRoot,
				Spec: resultSpecRoot,
			}

			// downstream
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
				Config:      map[string]string{models.ConfigIgnoreDownstream: "false"},
			}

			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)
			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)

			pluginService.On("GenerateDestination", ctx, jobRoot, namespaceSpec).Return(destinationRoot, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationRoot.URN()).Return(resourceRoot, namespaceSpec, nil).Once()
			datastorer.On("BackupResource", ctx, backupResourceReqRoot).
				Return(models.BackupResourceResponse{ResultURN: resultURNRoot, ResultSpec: resultSpecRoot}, nil).Once()

			otherNamespaceSpec := models.NamespaceSpec{
				ID:          uuid.New(),
				Name:        "dev-team-2",
				ProjectSpec: projectSpec,
			}
			pluginService.On("GenerateDestination", ctx, jobDownstream, namespaceSpec).Return(destinationDownstream, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationDownstream.URN()).Return(resourceDownstream, otherNamespaceSpec, nil).Once()

			uuidProvider.On("NewUUID").Return(backupUUID, nil)
			backupRepo.On("Save", ctx, backupSpec).Return(nil)

			service := datastore.NewBackupService(projectResourceRepoFac, dsRepo, uuidProvider, backupRepo, pluginService)
			resp, err := service.BackupResource(ctx, backupReq, []models.JobSpec{jobRoot, jobDownstream})

			assert.Nil(t, err)
			assert.Equal(t, []string{resultURNRoot}, resp.Resources)
			assert.Equal(t, []string{destinationDownstream.Destination}, resp.IgnoredResources)
		})
		t.Run("should return error when unable to generate destination", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			jobTask.Unit = &models.Plugin{Base: execUnit}
			jobSpec := models.JobSpec{
				ID:     uuid.New(),
				Name:   "job-1",
				Task:   jobTask,
				Assets: jobAssets,
			}
			backupReq := models.BackupRequest{
				Project:   projectSpec,
				Namespace: namespaceSpec,
				DryRun:    true,
			}

			uuidProvider.On("NewUUID").Return(backupUUID, nil)

			errorMsg := "unable to generate destination"
			pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(&models.GenerateDestinationResponse{}, errors.New(errorMsg))

			service := datastore.NewBackupService(nil, dsRepo, uuidProvider, nil, pluginService)
			resp, err := service.BackupResource(ctx, backupReq, []models.JobSpec{jobSpec})

			assert.Contains(t, err.Error(), errorMsg)
			assert.Equal(t, models.BackupResult{}, resp)
		})
		t.Run("should return error when unable to get datastorer", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			jobTask.Unit = &models.Plugin{Base: execUnit}
			jobSpec := models.JobSpec{
				ID:     uuid.New(),
				Name:   "job-1",
				Task:   jobTask,
				Assets: jobAssets,
			}
			backupReq := models.BackupRequest{
				Project:   projectSpec,
				Namespace: namespaceSpec,
				DryRun:    true,
			}

			uuidProvider.On("NewUUID").Return(backupUUID, nil)
			pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)

			errorMsg := "unable to get datastorer"
			dsRepo.On("GetByName", destination.Type.String()).Return(datastorer, errors.New(errorMsg))

			service := datastore.NewBackupService(nil, dsRepo, uuidProvider, nil, pluginService)
			resp, err := service.BackupResource(ctx, backupReq, []models.JobSpec{jobSpec})

			assert.Contains(t, err.Error(), errorMsg)
			assert.Equal(t, models.BackupResult{}, resp)
		})
		t.Run("should return error when unable to get resource", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

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

			jobTask.Unit = &models.Plugin{Base: execUnit}
			jobSpec := models.JobSpec{
				ID:     uuid.New(),
				Name:   "job-1",
				Task:   jobTask,
				Assets: jobAssets,
			}

			backupReq := models.BackupRequest{
				Project:   projectSpec,
				Namespace: namespaceSpec,
			}

			uuidProvider.On("NewUUID").Return(backupUUID, nil)
			pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)
			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)

			errorMsg := "unable to get resource"
			projectResourceRepo.On("GetByURN", ctx, destination.URN()).Return(models.ResourceSpec{}, models.NamespaceSpec{}, errors.New(errorMsg))

			service := datastore.NewBackupService(projectResourceRepoFac, dsRepo, uuidProvider, nil, pluginService)
			resp, err := service.BackupResource(ctx, backupReq, []models.JobSpec{jobSpec})

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.BackupResult{}, resp)
		})
		t.Run("should return error when unable to do backup", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

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

			jobTask.Unit = &models.Plugin{Base: execUnit}
			jobSpec := models.JobSpec{
				ID:     uuid.New(),
				Name:   "job-1",
				Task:   jobTask,
				Assets: jobAssets,
			}
			resourceSpec := models.ResourceSpec{
				Version:   1,
				Name:      "project.dataset.table",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
			}
			backupReq := models.BackupRequest{
				ID:                          backupUUID,
				ResourceName:                resourceSpec.Name,
				Project:                     projectSpec,
				Namespace:                   namespaceSpec,
				DryRun:                      false,
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			backupResourceReq := models.BackupResourceRequest{
				Resource:   resourceSpec,
				BackupSpec: backupReq,
			}

			uuidProvider.On("NewUUID").Return(backupUUID, nil)
			pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)
			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)
			projectResourceRepo.On("GetByURN", ctx, destination.URN()).Return(resourceSpec, namespaceSpec, nil)

			errorMsg := "unable to do backup"
			datastorer.On("BackupResource", ctx, backupResourceReq).Return(models.BackupResourceResponse{}, errors.New(errorMsg))

			service := datastore.NewBackupService(projectResourceRepoFac, dsRepo, uuidProvider, nil, pluginService)
			resp, err := service.BackupResource(ctx, backupReq, []models.JobSpec{jobSpec})

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.BackupResult{}, resp)
		})
		t.Run("should return error when unable to generate destination for downstream", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

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

			jobTask.Unit = &models.Plugin{Base: execUnit}
			jobDownstream := models.JobSpec{
				ID:     uuid.New(),
				Name:   "job-2",
				Task:   jobTask,
				Assets: jobAssets,
			}
			dependencies := make(map[string]models.JobSpecDependency)
			dependencies[jobDownstream.GetName()] = models.JobSpecDependency{
				Job: &jobDownstream,
			}
			jobRoot := models.JobSpec{
				ID:           uuid.New(),
				Name:         "job-1",
				Task:         jobTask,
				Assets:       jobAssets,
				Dependencies: dependencies,
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
				ID:                          backupUUID,
				ResourceName:                resourceRoot.Name,
				Project:                     projectSpec,
				Namespace:                   namespaceSpec,
				DryRun:                      true,
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			backupResourceReqRoot := models.BackupResourceRequest{
				Resource:   resourceRoot,
				BackupSpec: backupReq,
			}

			uuidProvider.On("NewUUID").Return(backupUUID, nil)
			pluginService.On("GenerateDestination", ctx, jobRoot, namespaceSpec).Return(destinationRoot, nil).Once()
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)
			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)
			projectResourceRepo.On("GetByURN", ctx, destinationRoot.URN()).Return(resourceRoot, namespaceSpec, nil).Once()
			datastorer.On("BackupResource", ctx, backupResourceReqRoot).Return(models.BackupResourceResponse{}, nil).Once()

			errorMsg := "unable to generate destination"
			pluginService.On("GenerateDestination", ctx, jobDownstream, namespaceSpec).Return(&models.GenerateDestinationResponse{}, errors.New(errorMsg)).Once()

			service := datastore.NewBackupService(projectResourceRepoFac, dsRepo, uuidProvider, nil, pluginService)
			resp, err := service.BackupResource(ctx, backupReq, []models.JobSpec{jobRoot, jobDownstream})

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.BackupResult{}, resp)
		})
		t.Run("should not return error when one of the resources is not found", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

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

			projectResourceRepo := new(mock.ProjectResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepo.AssertExpectations(t)

			jobTask.Unit = &models.Plugin{Base: execUnit}
			jobDownstream := models.JobSpec{
				ID:     uuid.New(),
				Name:   "job-2",
				Task:   jobTask,
				Assets: jobAssets,
			}
			dependencies := make(map[string]models.JobSpecDependency)
			dependencies[jobDownstream.GetName()] = models.JobSpecDependency{
				Job: &jobDownstream,
			}
			jobRoot := models.JobSpec{
				ID:           uuid.New(),
				Name:         "job-1",
				Task:         jobTask,
				Assets:       jobAssets,
				Dependencies: dependencies,
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
				ID:                          backupUUID,
				ResourceName:                resourceRoot.Name,
				Project:                     projectSpec,
				Namespace:                   namespaceSpec,
				DryRun:                      true,
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			backupResourceReqRoot := models.BackupResourceRequest{
				Resource:   resourceRoot,
				BackupSpec: backupReq,
			}
			resultURNRoot := "store://optimus_backup:backupURNRoot"
			resultSpecRoot := map[string]interface{}{
				"project": projectSpec.Name, "location": "optimus_backup", "name": "backup_resource_root",
			}
			backupResultRoot := models.BackupDetail{
				URN:  resultURNRoot,
				Spec: resultSpecRoot,
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
				Config:      map[string]string{models.ConfigIgnoreDownstream: "false"},
			}

			uuidProvider.On("NewUUID").Return(backupUUID, nil)
			pluginService.On("GenerateDestination", ctx, jobRoot, namespaceSpec).Return(destinationRoot, nil).Once()
			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)
			projectResourceRepo.On("GetByURN", ctx, destinationRoot.URN()).Return(resourceRoot, namespaceSpec, nil).Once()
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)
			datastorer.On("BackupResource", ctx, backupResourceReqRoot).
				Return(models.BackupResourceResponse{ResultURN: resultURNRoot, ResultSpec: resultSpecRoot}, nil).Once()

			pluginService.On("GenerateDestination", ctx, jobDownstream, namespaceSpec).Return(destinationDownstream, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationDownstream.URN()).Return(models.ResourceSpec{}, models.NamespaceSpec{}, store.ErrResourceNotFound).Once()

			backupRepo.On("Save", ctx, backupSpec).Return(nil)

			service := datastore.NewBackupService(projectResourceRepoFac, dsRepo, uuidProvider, backupRepo, pluginService)
			resp, err := service.BackupResource(ctx, backupReq, []models.JobSpec{jobRoot, jobDownstream})

			assert.Nil(t, err)
			assert.Equal(t, []string{resultURNRoot}, resp.Resources)
			assert.Nil(t, resp.IgnoredResources)
		})
		t.Run("should not return error when one of the resources is not supported", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

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

			projectResourceRepo := new(mock.ProjectResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepo.AssertExpectations(t)

			jobTask.Unit = &models.Plugin{Base: execUnit}
			jobDownstream := models.JobSpec{
				ID:     uuid.New(),
				Name:   "job-2",
				Task:   jobTask,
				Assets: jobAssets,
			}
			dependencies := make(map[string]models.JobSpecDependency)
			dependencies[jobDownstream.GetName()] = models.JobSpecDependency{
				Job: &jobDownstream,
			}
			jobRoot := models.JobSpec{
				ID:           uuid.New(),
				Name:         "job-1",
				Task:         jobTask,
				Assets:       jobAssets,
				Dependencies: dependencies,
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
				ID:                          backupUUID,
				ResourceName:                resourceRoot.Name,
				Project:                     projectSpec,
				Namespace:                   namespaceSpec,
				DryRun:                      true,
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			backupResourceReqRoot := models.BackupResourceRequest{
				Resource:   resourceRoot,
				BackupSpec: backupReq,
			}
			resultURNRoot := "store://optimus_backup:backupURNRoot"
			resultSpecRoot := map[string]interface{}{
				"project": projectSpec.Name, "location": "optimus_backup", "name": "backup_resource_root",
			}
			backupResultRoot := models.BackupDetail{
				URN:  resultURNRoot,
				Spec: resultSpecRoot,
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
				Config:      map[string]string{models.ConfigIgnoreDownstream: "false"},
			}

			uuidProvider.On("NewUUID").Return(backupUUID, nil)
			pluginService.On("GenerateDestination", ctx, jobRoot, namespaceSpec).Return(destinationRoot, nil).Once()
			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)
			projectResourceRepo.On("GetByURN", ctx, destinationRoot.URN()).Return(resourceRoot, namespaceSpec, nil).Once()
			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)
			datastorer.On("BackupResource", ctx, backupResourceReqRoot).
				Return(models.BackupResourceResponse{ResultURN: resultURNRoot, ResultSpec: resultSpecRoot}, nil).Once()

			pluginService.On("GenerateDestination", ctx, jobDownstream, namespaceSpec).Return(destinationDownstream, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationDownstream.URN()).Return(resourceDownstream, namespaceSpec, nil).Once()
			datastorer.On("BackupResource", ctx, backupResourceReqDownstream).Return(models.BackupResourceResponse{}, models.ErrUnsupportedResource).Once()

			backupRepo.On("Save", ctx, backupSpec).Return(nil)

			service := datastore.NewBackupService(projectResourceRepoFac, dsRepo, uuidProvider, backupRepo, pluginService)
			resp, err := service.BackupResource(ctx, backupReq, []models.JobSpec{jobRoot, jobDownstream})

			assert.Nil(t, err)
			assert.Equal(t, []string{resultURNRoot}, resp.Resources)
			assert.Nil(t, resp.IgnoredResources)
		})
		t.Run("should able to do backup without downstream if the downstream is ignored", func(t *testing.T) {
			execUnit := new(mock.BasePlugin)
			defer execUnit.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

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

			projectResourceRepo := new(mock.ProjectResourceSpecRepository)
			defer resourceRepo.AssertExpectations(t)

			projectResourceRepoFac := new(mock.ProjectResourceSpecRepoFactory)
			defer projectResourceRepo.AssertExpectations(t)

			jobTask.Unit = &models.Plugin{Base: execUnit}
			jobDownstream := models.JobSpec{
				ID:     uuid.New(),
				Name:   "job-2",
				Task:   jobTask,
				Assets: jobAssets,
			}
			dependencies := make(map[string]models.JobSpecDependency)
			dependencies[jobDownstream.GetName()] = models.JobSpecDependency{
				Job: &jobDownstream,
			}

			// root
			jobRoot := models.JobSpec{
				ID:           uuid.New(),
				Name:         "job-1",
				Task:         jobTask,
				Assets:       jobAssets,
				Dependencies: dependencies,
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
				ID:           backupUUID,
				ResourceName: resourceRoot.Name,
				Project:      projectSpec,
				Namespace:    namespaceSpec,
				DryRun:       true,
			}
			backupResourceReqRoot := models.BackupResourceRequest{
				Resource:   resourceRoot,
				BackupSpec: backupReq,
			}
			resultURNRoot := "store://optimus_backup:backupURNRoot"
			resultSpecRoot := map[string]interface{}{
				"project": projectSpec.Name, "location": "optimus_backup", "name": "backup_resource_root",
			}
			backupResultRoot := models.BackupDetail{
				URN:  resultURNRoot,
				Spec: resultSpecRoot,
			}

			// downstream
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
				Config:      map[string]string{models.ConfigIgnoreDownstream: "true"},
			}

			dsRepo.On("GetByName", models.DestinationTypeBigquery.String()).Return(datastorer, nil)

			pluginService.On("GenerateDestination", ctx, jobRoot, namespaceSpec).Return(destinationRoot, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationRoot.URN()).Return(resourceRoot, namespaceSpec, nil).Once()
			datastorer.On("BackupResource", ctx, backupResourceReqRoot).
				Return(models.BackupResourceResponse{ResultURN: resultURNRoot, ResultSpec: resultSpecRoot}, nil).Once()

			pluginService.On("GenerateDestination", ctx, jobDownstream, namespaceSpec).Return(destinationDownstream, nil).Once()
			projectResourceRepo.On("GetByURN", ctx, destinationDownstream.URN()).Return(resourceDownstream, namespaceSpec, nil).Once()

			projectResourceRepoFac.On("New", projectSpec, datastorer).Return(projectResourceRepo)

			uuidProvider.On("NewUUID").Return(backupUUID, nil)
			backupRepo.On("Save", ctx, backupSpec).Return(nil)

			service := datastore.NewBackupService(projectResourceRepoFac, dsRepo, uuidProvider, backupRepo, pluginService)
			resp, err := service.BackupResource(ctx, backupReq, []models.JobSpec{jobRoot, jobDownstream})

			assert.Nil(t, err)
			assert.Equal(t, []string{resultURNRoot}, resp.Resources)
			assert.Equal(t, []string{destinationDownstream.Destination}, resp.IgnoredResources)
		})
	})
	t.Run("ListResourceBackups", func(t *testing.T) {
		datastoreName := models.DestinationTypeBigquery.String()
		backupSpecs := []models.BackupSpec{
			{
				ID:        uuid.New(),
				CreatedAt: time.Now().Add(time.Hour * 24 * -30),
			},
			{
				ID:        uuid.New(),
				CreatedAt: time.Now().Add(time.Hour * 24 * -50),
			},
			{
				ID:        uuid.New(),
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

			dsRepo.On("GetByName", datastoreName).Return(datastorer, nil)
			backupRepo.On("GetAll", ctx, projectSpec, datastorer).Return(backupSpecs, nil)

			service := datastore.NewBackupService(nil, dsRepo, nil, backupRepo, nil)
			resp, err := service.ListResourceBackups(ctx, projectSpec, datastoreName)

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

			service := datastore.NewBackupService(nil, dsRepo, nil, nil, nil)
			resp, err := service.ListResourceBackups(ctx, projectSpec, datastoreName)

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

			dsRepo.On("GetByName", datastoreName).Return(datastorer, nil)

			errorMsg := "unable to get backups"
			backupRepo.On("GetAll", ctx, projectSpec, datastorer).Return([]models.BackupSpec{}, errors.New(errorMsg))

			service := datastore.NewBackupService(nil, dsRepo, nil, backupRepo, nil)
			resp, err := service.ListResourceBackups(ctx, projectSpec, datastoreName)

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

			dsRepo.On("GetByName", datastoreName).Return(datastorer, nil)
			backupRepo.On("GetAll", ctx, projectSpec, datastorer).Return([]models.BackupSpec{}, store.ErrResourceNotFound)

			service := datastore.NewBackupService(nil, dsRepo, nil, backupRepo, nil)
			resp, err := service.ListResourceBackups(ctx, projectSpec, datastoreName)

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

			dsRepo.On("GetByName", datastoreName).Return(datastorer, nil)
			backupRepo.On("GetAll", ctx, projectSpec, datastorer).Return([]models.BackupSpec{backupSpecs[2]}, nil)

			service := datastore.NewBackupService(nil, dsRepo, nil, backupRepo, nil)
			resp, err := service.ListResourceBackups(ctx, projectSpec, datastoreName)

			assert.Nil(t, err)
			assert.Equal(t, 0, len(resp))
		})
	})

	t.Run("GetResourceBackup", func(t *testing.T) {
		datastoreName := models.DestinationTypeBigquery.String()
		backupID := uuid.New()
		backupSpec := models.BackupSpec{
			ID:        backupID,
			CreatedAt: time.Now().Add(time.Hour * 24 * -30),
		}
		t.Run("should return backup detail", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			backupRepo := new(mock.BackupRepo)
			defer backupRepo.AssertExpectations(t)

			dsRepo.On("GetByName", datastoreName).Return(datastorer, nil)
			backupRepo.On("GetByID", ctx, backupID, datastorer).Return(backupSpec, nil)

			service := datastore.NewBackupService(nil, dsRepo, nil, backupRepo, nil)
			resp, err := service.GetResourceBackup(ctx, projectSpec, datastoreName, backupID)

			assert.Nil(t, err)
			assert.Equal(t, backupSpec, resp)
		})
		t.Run("should fail when unable to get datastore", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			errorMsg := "unable to get datastore"
			dsRepo.On("GetByName", datastoreName).Return(datastorer, errors.New(errorMsg))

			service := datastore.NewBackupService(nil, dsRepo, nil, nil, nil)
			resp, err := service.GetResourceBackup(ctx, projectSpec, datastoreName, backupID)

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.BackupSpec{}, resp)
		})
		t.Run("should fail when unable to get backup", func(t *testing.T) {
			datastorer := new(mock.Datastorer)
			defer datastorer.AssertExpectations(t)

			dsRepo := new(mock.SupportedDatastoreRepo)
			defer dsRepo.AssertExpectations(t)

			backupRepo := new(mock.BackupRepo)
			defer backupRepo.AssertExpectations(t)

			dsRepo.On("GetByName", datastoreName).Return(datastorer, nil)

			errorMsg := "unable to get backup"
			backupRepo.On("GetByID", ctx, backupID, datastorer).Return(models.BackupSpec{}, errors.New(errorMsg))

			service := datastore.NewBackupService(nil, dsRepo, nil, backupRepo, nil)
			resp, err := service.GetResourceBackup(ctx, projectSpec, datastoreName, backupID)

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.BackupSpec{}, resp)
		})
	})
}
