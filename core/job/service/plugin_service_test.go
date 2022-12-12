package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/service"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/compiler"
	mockOpt "github.com/odpf/optimus/internal/mock"
	"github.com/odpf/optimus/internal/models"
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
	tenantDetails, _ := tenant.NewTenantDetails(project, namespace)
	startDate, err := job.ScheduleDateFrom("2022-10-01")
	assert.NoError(t, err)
	jobSchedule, err := job.NewScheduleBuilder(startDate).Build()
	assert.NoError(t, err)
	jobVersion, err := job.VersionFrom(1)
	assert.NoError(t, err)
	jobWindow, err := models.NewWindow(jobVersion.Int(), "d", "24h", "24h")
	assert.NoError(t, err)
	jobTaskConfig, err := job.NewConfig(map[string]string{
		"SECRET_TABLE_NAME": "{{.secret.table_name}}",
	})
	assert.NoError(t, err)
	jobTask := job.NewTaskBuilder("bq2bq", jobTaskConfig).Build()

	t.Run("Info", func(t *testing.T) {
		t.Run("returns error when no plugin", func(t *testing.T) {
			pluginRepo := new(mockPluginRepo)
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(nil, errors.New("some error when fetch plugin"))
			defer pluginRepo.AssertExpectations(t)

			pluginService := service.NewJobPluginService(nil, pluginRepo, nil, nil)
			result, err := pluginService.Info(ctx, jobTask)
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

			newPlugin := &models.Plugin{DependencyMod: depMod}
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(newPlugin, nil)

			pluginService := service.NewJobPluginService(nil, pluginRepo, nil, nil)
			result, err := pluginService.Info(ctx, jobTask)
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

			plugin := &models.Plugin{DependencyMod: depMod, YamlMod: yamlMod}

			pluginRepo.On("GetByName", jobTask.Name().String()).Return(plugin, nil)
			yamlMod.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:        jobTask.Name().String(),
				Description: "example",
				Image:       "http://to.repo",
			}, nil)
			defer yamlMod.AssertExpectations(t)

			pluginService := service.NewJobPluginService(nil, pluginRepo, nil, nil)
			result, err := pluginService.Info(ctx, jobTask)
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

			secretsGetter := new(SecretsGetter)
			defer secretsGetter.AssertExpectations(t)

			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			engine := compiler.NewEngine()
			defer pluginRepo.AssertExpectations(t)

			depMod := new(mockOpt.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			defer yamlMod.AssertExpectations(t)

			plugin := &models.Plugin{DependencyMod: depMod, YamlMod: yamlMod}
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(plugin, nil)

			secret1, err := tenant.NewPlainTextSecret("table_name", "secret_table")
			assert.Nil(t, err)

			secret2, err := tenant.NewPlainTextSecret("bucket", "gs://some_secret_bucket")
			assert.Nil(t, err)

			secretsGetter.On("GetAll", ctx, project.Name(), namespace.Name().String()).Return([]*tenant.PlainTextSecret{secret1, secret2}, nil)

			destination := "project.dataset.table"
			destinationURN := job.ResourceURN("bigquery://project.dataset.table")
			depMod.On("GenerateDestination", ctx, mock.Anything).Return(&models.GenerateDestinationResponse{
				Destination: destination,
				Type:        models.DestinationTypeBigquery,
			}, nil)

			pluginService := service.NewJobPluginService(secretsGetter, pluginRepo, engine, logger)
			result, err := pluginService.GenerateDestination(ctx, tenantDetails, jobTask)
			assert.Nil(t, err)
			assert.Equal(t, destinationURN, result)
		})
		t.Run("returns error if unable to find the plugin", func(t *testing.T) {
			logger := log.NewLogrus()

			secretsGetter := new(SecretsGetter)
			defer secretsGetter.AssertExpectations(t)

			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			engine := compiler.NewEngine()
			defer pluginRepo.AssertExpectations(t)

			pluginRepo.On("GetByName", jobTask.Name().String()).Return(nil, errors.New("not found"))

			pluginService := service.NewJobPluginService(secretsGetter, pluginRepo, engine, logger)
			result, err := pluginService.GenerateDestination(ctx, tenantDetails, jobTask)
			assert.ErrorContains(t, err, "not found")
			assert.Equal(t, "", result.String())
		})
		t.Run("returns proper error if the upstream mod is not found", func(t *testing.T) {
			logger := log.NewLogrus()

			secretsGetter := new(SecretsGetter)
			defer secretsGetter.AssertExpectations(t)

			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			engine := compiler.NewEngine()
			defer pluginRepo.AssertExpectations(t)

			depMod := new(mockOpt.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			defer yamlMod.AssertExpectations(t)

			pluginWithoutDependencyMod := &models.Plugin{YamlMod: yamlMod}
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(pluginWithoutDependencyMod, nil)

			pluginService := service.NewJobPluginService(secretsGetter, pluginRepo, engine, logger)
			result, err := pluginService.GenerateDestination(ctx, tenantDetails, jobTask)
			assert.ErrorIs(t, err, service.ErrUpstreamModNotFound)
			assert.Equal(t, "", result.String())
		})
		t.Run("returns error if generate destination failed", func(t *testing.T) {
			logger := log.NewLogrus()

			secretsGetter := new(SecretsGetter)
			defer secretsGetter.AssertExpectations(t)

			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			engine := compiler.NewEngine()

			depMod := new(mockOpt.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			defer yamlMod.AssertExpectations(t)

			plugin := &models.Plugin{DependencyMod: depMod, YamlMod: yamlMod}
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(plugin, nil)

			secret1, err := tenant.NewPlainTextSecret("table_name", "secret_table")
			assert.Nil(t, err)

			secret2, err := tenant.NewPlainTextSecret("bucket", "gs://some_secret_bucket")
			assert.Nil(t, err)

			secretsGetter.On("GetAll", ctx, project.Name(), namespace.Name().String()).Return([]*tenant.PlainTextSecret{secret1, secret2}, nil)

			depMod.On("GenerateDestination", ctx, mock.Anything).Return(&models.GenerateDestinationResponse{}, errors.New("generate destination error"))

			pluginService := service.NewJobPluginService(secretsGetter, pluginRepo, engine, logger)
			result, err := pluginService.GenerateDestination(ctx, tenantDetails, jobTask)
			assert.ErrorContains(t, err, "generate destination error")
			assert.Equal(t, "", result.String())
		})
		t.Run("returns error if unable to get secrets", func(t *testing.T) {
			logger := log.NewLogrus()

			secretsGetter := new(SecretsGetter)
			defer secretsGetter.AssertExpectations(t)

			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			engine := compiler.NewEngine()
			defer pluginRepo.AssertExpectations(t)

			depMod := new(mockOpt.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			defer yamlMod.AssertExpectations(t)

			plugin := &models.Plugin{DependencyMod: depMod, YamlMod: yamlMod}
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(plugin, nil)

			secretsGetter.On("GetAll", ctx, project.Name(), namespace.Name().String()).Return([]*tenant.PlainTextSecret{}, errors.New("getting secret error"))

			pluginService := service.NewJobPluginService(secretsGetter, pluginRepo, engine, logger)
			result, err := pluginService.GenerateDestination(ctx, tenantDetails, jobTask)
			assert.ErrorContains(t, err, "getting secret error")
			assert.Equal(t, "", result.String())
		})
	})

	t.Run("GenerateDependencies", func(t *testing.T) {
		t.Run("returns upstreams", func(t *testing.T) {
			logger := log.NewLogrus()

			secretsGetter := new(SecretsGetter)
			defer secretsGetter.AssertExpectations(t)

			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			engine := compiler.NewEngine()
			defer pluginRepo.AssertExpectations(t)

			depMod := new(mockOpt.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			defer yamlMod.AssertExpectations(t)

			plugin := &models.Plugin{DependencyMod: depMod, YamlMod: yamlMod}
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(plugin, nil)

			secretsGetter.On("GetAll", ctx, project.Name(), namespace.Name().String()).Return(nil, nil)

			destination := "project.dataset.table"
			depMod.On("GenerateDestination", ctx, mock.Anything).Return(&models.GenerateDestinationResponse{
				Destination: destination,
				Type:        models.DestinationTypeBigquery,
			}, nil)

			jobSource := job.ResourceURN("project.dataset.table_upstream")
			depMod.On("GenerateDependencies", ctx, mock.Anything).Return(&models.GenerateDependenciesResponse{
				Dependencies: []string{jobSource.String()}},
				nil)

			asset, err := job.NewAsset(map[string]string{"sample-key": "sample-value"})
			assert.NoError(t, err)
			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).WithAsset(asset).Build()

			pluginService := service.NewJobPluginService(secretsGetter, pluginRepo, engine, logger)
			result, err := pluginService.GenerateUpstreams(ctx, tenantDetails, specA, false)
			assert.Nil(t, err)
			assert.Equal(t, []job.ResourceURN{jobSource}, result)
		})
		t.Run("returns error if unable to find the plugin", func(t *testing.T) {
			logger := log.NewLogrus()

			secretsGetter := new(SecretsGetter)
			defer secretsGetter.AssertExpectations(t)

			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			engine := compiler.NewEngine()
			defer pluginRepo.AssertExpectations(t)

			pluginRepo.On("GetByName", jobTask.Name().String()).Return(nil, errors.New("not found"))

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()

			pluginService := service.NewJobPluginService(secretsGetter, pluginRepo, engine, logger)
			result, err := pluginService.GenerateUpstreams(ctx, tenantDetails, specA, false)
			assert.ErrorContains(t, err, "not found")
			assert.Nil(t, result)
		})
		t.Run("returns proper error if the upstream mod is not found", func(t *testing.T) {
			logger := log.NewLogrus()

			secretsGetter := new(SecretsGetter)
			defer secretsGetter.AssertExpectations(t)

			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			engine := compiler.NewEngine()
			defer pluginRepo.AssertExpectations(t)

			depMod := new(mockOpt.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			defer yamlMod.AssertExpectations(t)

			pluginWithoutDependencyMod := &models.Plugin{YamlMod: yamlMod}
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(pluginWithoutDependencyMod, nil)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()

			pluginService := service.NewJobPluginService(secretsGetter, pluginRepo, engine, logger)
			result, err := pluginService.GenerateUpstreams(ctx, tenantDetails, specA, false)
			assert.ErrorContains(t, err, "not found")
			assert.Nil(t, result)
		})
		t.Run("returns error if unable to get secrets", func(t *testing.T) {
			logger := log.NewLogrus()

			secretsGetter := new(SecretsGetter)
			defer secretsGetter.AssertExpectations(t)

			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			engine := compiler.NewEngine()
			defer pluginRepo.AssertExpectations(t)

			depMod := new(mockOpt.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			defer yamlMod.AssertExpectations(t)

			plugin := &models.Plugin{DependencyMod: depMod, YamlMod: yamlMod}
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(plugin, nil)

			destination := "project.dataset.table"
			depMod.On("GenerateDestination", ctx, mock.Anything).Return(&models.GenerateDestinationResponse{
				Destination: destination,
				Type:        models.DestinationTypeBigquery,
			}, nil)

			secretsGetter.On("GetAll", ctx, project.Name(), namespace.Name().String()).Return([]*tenant.PlainTextSecret{}, errors.New("getting secret error"))

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()

			pluginService := service.NewJobPluginService(secretsGetter, pluginRepo, engine, logger)
			result, err := pluginService.GenerateUpstreams(ctx, tenantDetails, specA, false)
			assert.ErrorContains(t, err, "getting secret error")
			assert.Nil(t, result)
		})
		t.Run("returns error if unable to generate dependencies successfully", func(t *testing.T) {
			logger := log.NewLogrus()

			secretsGetter := new(SecretsGetter)
			defer secretsGetter.AssertExpectations(t)

			pluginRepo := new(mockPluginRepo)
			defer pluginRepo.AssertExpectations(t)

			engine := compiler.NewEngine()
			defer pluginRepo.AssertExpectations(t)

			depMod := new(mockOpt.DependencyResolverMod)
			defer depMod.AssertExpectations(t)

			yamlMod := new(mockOpt.YamlMod)
			defer yamlMod.AssertExpectations(t)

			plugin := &models.Plugin{DependencyMod: depMod, YamlMod: yamlMod}
			pluginRepo.On("GetByName", jobTask.Name().String()).Return(plugin, nil)

			secretsGetter.On("GetAll", ctx, project.Name(), namespace.Name().String()).Return(nil, nil)

			destination := "project.dataset.table"
			depMod.On("GenerateDestination", ctx, mock.Anything).Return(&models.GenerateDestinationResponse{
				Destination: destination,
				Type:        models.DestinationTypeBigquery,
			}, nil)

			depMod.On("GenerateDependencies", ctx, mock.Anything).Return(&models.GenerateDependenciesResponse{},
				errors.New("generate dependencies error"))

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()

			pluginService := service.NewJobPluginService(secretsGetter, pluginRepo, engine, logger)
			result, err := pluginService.GenerateUpstreams(ctx, tenantDetails, specA, false)
			assert.ErrorContains(t, err, "generate dependencies error")
			assert.Nil(t, result)
		})
	})
}

// SecretsGetter is an autogenerated mock type for the SecretsGetter type
type SecretsGetter struct {
	mock.Mock
}

// Get provides a mock function with given fields: ctx, projName, namespaceName, name
func (_m *SecretsGetter) Get(ctx context.Context, projName tenant.ProjectName, namespaceName string, name string) (*tenant.PlainTextSecret, error) {
	ret := _m.Called(ctx, projName, namespaceName, name)

	var r0 *tenant.PlainTextSecret
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, string, string) *tenant.PlainTextSecret); ok {
		r0 = rf(ctx, projName, namespaceName, name)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*tenant.PlainTextSecret)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, string, string) error); ok {
		r1 = rf(ctx, projName, namespaceName, name)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetAll provides a mock function with given fields: ctx, projName, namespaceName
func (_m *SecretsGetter) GetAll(ctx context.Context, projName tenant.ProjectName, namespaceName string) ([]*tenant.PlainTextSecret, error) {
	ret := _m.Called(ctx, projName, namespaceName)

	var r0 []*tenant.PlainTextSecret
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, string) []*tenant.PlainTextSecret); ok {
		r0 = rf(ctx, projName, namespaceName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*tenant.PlainTextSecret)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, string) error); ok {
		r1 = rf(ctx, projName, namespaceName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type mockPluginRepo struct {
	mock.Mock
}

func (m *mockPluginRepo) GetByName(name string) (*models.Plugin, error) {
	args := m.Called(name)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.Plugin), args.Error(1)
}
