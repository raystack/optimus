package service_test

import (
	"context"
	"testing"

	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/compiler"
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/service"
	"github.com/odpf/optimus/core/tenant"
	mockOpt "github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
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
	sampleTenant, _ := tenant.NewTenant(project.Name().String(), namespace.Name().String())
	tenantDetails, _ := tenant.NewTenantDetails(project, namespace)
	jobSchedule := job.NewSchedule("2022-10-01", "", "", false, false, nil)
	jobVersion := 1
	jobWindow, _ := models.NewWindow(jobVersion, "d", "24h", "24h")
	jobTaskConfig := job.NewConfig(map[string]string{
		"SECRET_TABLE_NAME": "{{.secret.table_name}}",
	})
	jobTask := job.NewTask("bq2bq", jobTaskConfig)
	depMod := new(mockOpt.DependencyResolverMod)
	baseUnit := new(mockOpt.BasePlugin)
	plugin := &models.Plugin{Base: baseUnit, DependencyMod: depMod}

	t.Run("GenerateDestination", func(t *testing.T) {
		t.Run("returns destination", func(t *testing.T) {
			logger := log.NewLogrus()

			secretsGetter := new(SecretsGetter)
			defer secretsGetter.AssertExpectations(t)

			pluginRepo := mockOpt.NewPluginRepository(t)
			defer pluginRepo.AssertExpectations(t)

			engine := compiler.NewGoEngine()
			defer pluginRepo.AssertExpectations(t)

			pluginRepo.On("GetByName", jobTask.Name()).Return(plugin, nil)

			secret1, err := tenant.NewPlainTextSecret("table_name", "secret_table")
			assert.Nil(t, err)

			secret2, err := tenant.NewPlainTextSecret("bucket", "gs://some_secret_bucket")
			assert.Nil(t, err)

			secretsGetter.On("GetAll", ctx, tenantDetails.ToTenant()).Return([]*tenant.PlainTextSecret{secret1, secret2}, nil)

			destination := "project.dataset.table"
			destinationURN := "bigquery://project.dataset.table"
			depMod.On("GenerateDestination", ctx, mock.Anything).Return(&models.GenerateDestinationResponse{
				Destination: destination,
				Type:        models.DestinationTypeBigquery,
			}, nil)

			pluginService := service.NewJobPluginService(secretsGetter, pluginRepo, engine, logger)
			result, err := pluginService.GenerateDestination(ctx, tenantDetails, jobTask)
			assert.Nil(t, err)
			assert.Equal(t, destinationURN, result)
		})
	})

	t.Run("GenerateDependencies", func(t *testing.T) {
		t.Run("returns dependencies", func(t *testing.T) {
			logger := log.NewLogrus()

			secretsGetter := new(SecretsGetter)
			defer secretsGetter.AssertExpectations(t)

			pluginRepo := mockOpt.NewPluginRepository(t)
			defer pluginRepo.AssertExpectations(t)

			engine := compiler.NewGoEngine()
			defer pluginRepo.AssertExpectations(t)

			pluginRepo.On("GetByName", jobTask.Name()).Return(plugin, nil)

			secretsGetter.On("GetAll", ctx, tenantDetails.ToTenant()).Return(nil, nil)

			destination := "project.dataset.table"
			depMod.On("GenerateDestination", ctx, mock.Anything).Return(&models.GenerateDestinationResponse{
				Destination: destination,
				Type:        models.DestinationTypeBigquery,
			}, nil)

			jobSource := "project.dataset.table_upstream"
			depMod.On("GenerateDependencies", ctx, mock.Anything).Return(&models.GenerateDependenciesResponse{
				Dependencies: []string{jobSource}},
				nil)

			jobSpecA, err := job.NewJobSpec(sampleTenant, jobVersion, "job-A", "", "", nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			assert.Nil(t, err)

			pluginService := service.NewJobPluginService(secretsGetter, pluginRepo, engine, logger)
			result, err := pluginService.GenerateDependencies(ctx, tenantDetails, jobSpecA, false)
			assert.Nil(t, err)
			assert.Equal(t, []string{jobSource}, result)
		})
	})
}

// SecretsGetter is an autogenerated mock type for the SecretsGetter type
type SecretsGetter struct {
	mock.Mock
}

// Get provides a mock function with given fields: ctx, ten, name
func (_m *SecretsGetter) Get(ctx context.Context, ten tenant.Tenant, name string) (*tenant.PlainTextSecret, error) {
	ret := _m.Called(ctx, ten, name)

	var r0 *tenant.PlainTextSecret
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, string) *tenant.PlainTextSecret); ok {
		r0 = rf(ctx, ten, name)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*tenant.PlainTextSecret)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.Tenant, string) error); ok {
		r1 = rf(ctx, ten, name)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetAll provides a mock function with given fields: ctx, ten
func (_m *SecretsGetter) GetAll(ctx context.Context, ten tenant.Tenant) ([]*tenant.PlainTextSecret, error) {
	ret := _m.Called(ctx, ten)

	var r0 []*tenant.PlainTextSecret
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant) []*tenant.PlainTextSecret); ok {
		r0 = rf(ctx, ten)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*tenant.PlainTextSecret)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.Tenant) error); ok {
		r1 = rf(ctx, ten)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
