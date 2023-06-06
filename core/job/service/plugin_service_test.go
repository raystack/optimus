package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/job/service"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/compiler"
	"github.com/goto/optimus/internal/models"
	"github.com/goto/optimus/sdk/plugin"
	mockOpt "github.com/goto/optimus/sdk/plugin/mock"
)

func TestPluginService(t *testing.T) {
	ctx := context.Background()
	project, _ := tenant.NewProject("test-proj",
		map[string]string{
			"bucket":                     "gs://some_folder-2",
			tenant.ProjectSchedulerHost:  "host",
			tenant.ProjectStoragePathKey: "gs://location",
		})
	namespace, _ := tenant.NewNamespace("test-ns", project.Name(),
		map[string]string{
			"bucket": "gs://ns_bucket",
		})

	secret1, err := tenant.NewPlainTextSecret("table_name", "secret_table")
	assert.Nil(t, err)

	secret2, err := tenant.NewPlainTextSecret("bucket", "gs://some_secret_bucket")
	assert.Nil(t, err)

	tenantDetails, _ := tenant.NewTenantDetails(project, namespace, tenant.PlainTextSecrets{secret1, secret2})
	startDate, err := job.ScheduleDateFrom("2022-10-01")
	assert.NoError(t, err)
	jobSchedule, err := job.NewScheduleBuilder(startDate).Build()
	assert.NoError(t, err)
	jobVersion := 1
	assert.NoError(t, err)
	jobWindow, err := models.NewWindow(jobVersion, "d", "24h", "24h")
	assert.NoError(t, err)
	jobTaskConfig, err := job.ConfigFrom(map[string]string{
		"SECRET_TABLE_NAME": "{{.secret.table_name}}",
	})
	assert.NoError(t, err)
	jobTask := job.NewTask("bq2bq", jobTaskConfig)

	logger := log.NewLogrus()

	t.Run("Info", func(t *testing.T) {
		t.Run("returns error when no plugin", func(t *testing.T) {
			pluginRepo := new(mockPluginRepo)
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(nil, errors.New("some error when fetch plugin"))
			defer pluginRepo.AssertExpectations(t)

			pluginService := service.NewJobPluginService(pluginRepo, nil, logger)
			result, err := pluginService.Info(ctx, jobTask.Name())
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Equal(t, "some error when fetch plugin", err.Error())
		})
		t.Run("returns error when yaml mod not supported", func(t *testing.T) {
			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			depMod := new(mockOpt.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			defer yamlMod.AssertExpectations(t)

			newPlugin := &plugin.Plugin{DependencyMod: depMod}
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(newPlugin, nil)

			pluginService := service.NewJobPluginService(pluginRepo, nil, logger)
			result, err := pluginService.Info(ctx, jobTask.Name())
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Equal(t, "yaml mod not found for plugin", err.Error())
		})
		t.Run("returns plugin info", func(t *testing.T) {
			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			depMod := new(mockOpt.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			defer yamlMod.AssertExpectations(t)

			taskPlugin := &plugin.Plugin{DependencyMod: depMod, YamlMod: yamlMod}

			pluginRepo.On("GetByName", jobTask.Name().String()).Return(taskPlugin, nil)
			yamlMod.On("PluginInfo").Return(&plugin.Info{
				Name:        jobTask.Name().String(),
				Description: "example",
				Image:       "http://to.repo",
			}, nil)
			defer yamlMod.AssertExpectations(t)

			pluginService := service.NewJobPluginService(pluginRepo, nil, logger)
			result, err := pluginService.Info(ctx, jobTask.Name())
			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.Equal(t, jobTask.Name().String(), result.Name)
			assert.Equal(t, "example", result.Description)
			assert.Equal(t, "http://to.repo", result.Image)
		})
	})

	t.Run("GenerateDestination", func(t *testing.T) {
		t.Run("returns destination", func(t *testing.T) {
			logger := log.NewLogrus()

			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			engine := compiler.NewEngine()
			defer pluginRepo.AssertExpectations(t)

			depMod := new(mockOpt.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			defer yamlMod.AssertExpectations(t)

			taskPlugin := &plugin.Plugin{DependencyMod: depMod, YamlMod: yamlMod}
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(taskPlugin, nil)

			destination := "project.dataset.table"
			destinationURN := job.ResourceURN("bigquery://project.dataset.table")
			depMod.On("GenerateDestination", ctx, mock.Anything).Return(&plugin.GenerateDestinationResponse{
				Destination: destination,
				Type:        "bigquery",
			}, nil)

			pluginService := service.NewJobPluginService(pluginRepo, engine, logger)
			result, err := pluginService.GenerateDestination(ctx, tenantDetails, jobTask)
			assert.Nil(t, err)
			assert.Equal(t, destinationURN, result)
		})
		t.Run("returns error if unable to find the plugin", func(t *testing.T) {
			logger := log.NewLogrus()

			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			engine := compiler.NewEngine()
			defer pluginRepo.AssertExpectations(t)

			pluginRepo.On("GetByName", jobTask.Name().String()).Return(nil, errors.New("not found"))

			pluginService := service.NewJobPluginService(pluginRepo, engine, logger)
			result, err := pluginService.GenerateDestination(ctx, tenantDetails, jobTask)
			assert.ErrorContains(t, err, "not found")
			assert.Equal(t, "", result.String())
		})
		t.Run("returns proper error if the upstream mod is not found", func(t *testing.T) {
			logger := log.NewLogrus()

			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			engine := compiler.NewEngine()
			defer pluginRepo.AssertExpectations(t)

			depMod := new(mockOpt.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			defer yamlMod.AssertExpectations(t)

			pluginWithoutDependencyMod := &plugin.Plugin{YamlMod: yamlMod}
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(pluginWithoutDependencyMod, nil)

			pluginService := service.NewJobPluginService(pluginRepo, engine, logger)
			result, err := pluginService.GenerateDestination(ctx, tenantDetails, jobTask)
			assert.ErrorIs(t, err, service.ErrUpstreamModNotFound)
			assert.Equal(t, "", result.String())
		})
		t.Run("returns error if generate destination failed", func(t *testing.T) {
			logger := log.NewLogrus()

			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			engine := compiler.NewEngine()

			depMod := new(mockOpt.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			defer yamlMod.AssertExpectations(t)

			taskPlugin := &plugin.Plugin{DependencyMod: depMod, YamlMod: yamlMod}
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(taskPlugin, nil)

			depMod.On("GenerateDestination", ctx, mock.Anything).Return(&plugin.GenerateDestinationResponse{}, errors.New("generate destination error"))

			pluginService := service.NewJobPluginService(pluginRepo, engine, logger)
			result, err := pluginService.GenerateDestination(ctx, tenantDetails, jobTask)
			assert.ErrorContains(t, err, "generate destination error")
			assert.Equal(t, "", result.String())
		})
	})

	t.Run("GenerateUpstreams", func(t *testing.T) {
		t.Run("returns upstreams", func(t *testing.T) {
			logger := log.NewLogrus()

			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			engine := compiler.NewEngine()
			defer pluginRepo.AssertExpectations(t)

			depMod := new(mockOpt.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			defer yamlMod.AssertExpectations(t)

			taskPlugin := &plugin.Plugin{DependencyMod: depMod, YamlMod: yamlMod}
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(taskPlugin, nil)

			destination := "project.dataset.table"
			depMod.On("GenerateDestination", ctx, mock.Anything).Return(&plugin.GenerateDestinationResponse{
				Destination: destination,
				Type:        "bigquery",
			}, nil)

			jobSource := job.ResourceURN("project.dataset.table_upstream")
			depMod.On("GenerateDependencies", ctx, mock.Anything).Return(&plugin.GenerateDependenciesResponse{
				Dependencies: []string{jobSource.String()},
			},
				nil)

			asset, err := job.AssetFrom(map[string]string{"sample-key": "sample-value"})
			assert.NoError(t, err)
			specA, err := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithAsset(asset).Build()
			assert.NoError(t, err)

			pluginService := service.NewJobPluginService(pluginRepo, engine, logger)
			result, err := pluginService.GenerateUpstreams(ctx, tenantDetails, specA, false)
			assert.Nil(t, err)
			assert.Equal(t, []job.ResourceURN{jobSource}, result)
		})
		t.Run("returns error if unable to find the plugin", func(t *testing.T) {
			logger := log.NewLogrus()

			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			engine := compiler.NewEngine()
			defer pluginRepo.AssertExpectations(t)

			pluginRepo.On("GetByName", jobTask.Name().String()).Return(nil, errors.New("not found"))

			specA, err := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).Build()
			assert.NoError(t, err)

			pluginService := service.NewJobPluginService(pluginRepo, engine, logger)
			result, err := pluginService.GenerateUpstreams(ctx, tenantDetails, specA, false)
			assert.ErrorContains(t, err, "not found")
			assert.Nil(t, result)
		})
		t.Run("returns proper error if the upstream mod is not found", func(t *testing.T) {
			logger := log.NewLogrus()

			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			engine := compiler.NewEngine()
			defer pluginRepo.AssertExpectations(t)

			depMod := new(mockOpt.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			defer yamlMod.AssertExpectations(t)

			pluginWithoutDependencyMod := &plugin.Plugin{YamlMod: yamlMod}
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(pluginWithoutDependencyMod, nil)

			specA, err := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).Build()
			assert.NoError(t, err)

			pluginService := service.NewJobPluginService(pluginRepo, engine, logger)
			result, err := pluginService.GenerateUpstreams(ctx, tenantDetails, specA, false)
			assert.ErrorContains(t, err, "not found")
			assert.Nil(t, result)
		})
		t.Run("returns error if unable to generate destination successfully", func(t *testing.T) {
			logger := log.NewLogrus()

			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			engine := compiler.NewEngine()
			defer pluginRepo.AssertExpectations(t)

			depMod := new(mockOpt.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			defer yamlMod.AssertExpectations(t)

			taskPlugin := &plugin.Plugin{DependencyMod: depMod, YamlMod: yamlMod}
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(taskPlugin, nil)

			depMod.On("GenerateDestination", ctx, mock.Anything).Return(&plugin.GenerateDestinationResponse{}, errors.New("generate destination error"))

			specA, err := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).Build()
			assert.NoError(t, err)

			pluginService := service.NewJobPluginService(pluginRepo, engine, logger)
			result, err := pluginService.GenerateUpstreams(ctx, tenantDetails, specA, false)
			assert.ErrorContains(t, err, "generate destination error")
			assert.Nil(t, result)
		})
		t.Run("returns error if unable to generate dependencies successfully", func(t *testing.T) {
			logger := log.NewLogrus()

			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			engine := compiler.NewEngine()
			defer pluginRepo.AssertExpectations(t)

			depMod := new(mockOpt.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			defer yamlMod.AssertExpectations(t)

			taskPlugin := &plugin.Plugin{DependencyMod: depMod, YamlMod: yamlMod}
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(taskPlugin, nil)

			destination := "project.dataset.table"
			depMod.On("GenerateDestination", ctx, mock.Anything).Return(&plugin.GenerateDestinationResponse{
				Destination: destination,
				Type:        "bigquery",
			}, nil)

			depMod.On("GenerateDependencies", ctx, mock.Anything).Return(&plugin.GenerateDependenciesResponse{},
				errors.New("generate dependencies error"))

			specA, err := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).Build()
			assert.NoError(t, err)

			pluginService := service.NewJobPluginService(pluginRepo, engine, logger)
			result, err := pluginService.GenerateUpstreams(ctx, tenantDetails, specA, false)
			assert.ErrorContains(t, err, "generate dependencies error")
			assert.Nil(t, result)
		})
	})
}

type mockPluginRepo struct {
	mock.Mock
}

func (m *mockPluginRepo) GetByName(name string) (*plugin.Plugin, error) {
	args := m.Called(name)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*plugin.Plugin), args.Error(1)
}
