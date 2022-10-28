package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/dto"
	"github.com/odpf/optimus/core/job/service"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/models"
)

func TestJobService(t *testing.T) {
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
	detailedTenant, _ := tenant.NewTenantDetails(project, namespace)

	jobVersion := 1
	jobSchedule := job.NewSchedule("2022-10-01", "", "", false, false, nil)
	jobWindow, _ := models.NewWindow(jobVersion, "d", "24h", "24h")
	jobTaskConfig := job.NewConfig(map[string]string{"sample_task_key": "sample_value"})
	jobTask := job.NewTask("bq2bq", jobTaskConfig)

	t.Run("Add", func(t *testing.T) {
		t.Run("add jobs and return deployment ID", func(t *testing.T) {
			jobRepo := NewJobRepository(t)
			defer jobRepo.AssertExpectations(t)

			jobDependencyRepo := NewJobDependencyRepository(t)
			defer jobDependencyRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)
			defer pluginService.AssertExpectations(t)

			dependencyResolver := NewDependencyResolver(t)
			defer dependencyResolver.AssertExpectations(t)

			deployManager := NewDeploymentManager(t)
			defer deployManager.AssertExpectations(t)

			tenantDetailsGetter := NewTenantDetailsGetter(t)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobSpecA, err := job.NewJobSpec(sampleTenant, jobVersion, "job-A", "", "", nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			assert.Nil(t, err)

			jobSpecs := []*job.JobSpec{jobSpecA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := "resource-A"
			pluginService.On("GenerateDestination", ctx, detailedTenant, jobSpecA.Task()).Return(jobADestination, nil)

			jobADependencies := []string{"job-B"}
			pluginService.On("GenerateDependencies", ctx, detailedTenant, jobSpecA, true).Return(jobADependencies, nil)

			jobA := job.NewJob(jobSpecA, jobADestination, jobADependencies)
			jobs := []*job.Job{jobA}
			jobRepo.On("Save", ctx, jobs).Return(nil)

			dependency := dto.NewDependency("job-B", sampleTenant, "", "resource-B")
			jobWithDependency := job.NewWithDependency("job-A", project.Name(), []*dto.Dependency{dependency}, nil)
			dependencyResolver.On("Resolve", ctx, jobs).Return([]*job.WithDependency{jobWithDependency}, nil)

			jobDependencyRepo.On("Save", ctx, []*job.WithDependency{jobWithDependency}).Return(nil)

			deployID := uuid.New()
			deployManager.On("Create", ctx, project.Name()).Return(deployID, nil)

			jobService := service.NewJobService(jobRepo, jobDependencyRepo, pluginService, dependencyResolver, tenantDetailsGetter, deployManager)
			result, jobErr, sysErr := jobService.Add(ctx, sampleTenant, jobSpecs)
			assert.Nil(t, jobErr)
			assert.Nil(t, sysErr)
			assert.Equal(t, deployID, result)
		})
		t.Run("skip invalid job, add the rest and return deployment ID", func(t *testing.T) {
			jobRepo := NewJobRepository(t)
			defer jobRepo.AssertExpectations(t)

			jobDependencyRepo := NewJobDependencyRepository(t)
			defer jobDependencyRepo.AssertExpectations(t)

			pluginService := NewPluginService(t)
			defer pluginService.AssertExpectations(t)

			dependencyResolver := NewDependencyResolver(t)
			defer dependencyResolver.AssertExpectations(t)

			deployManager := NewDeploymentManager(t)
			defer deployManager.AssertExpectations(t)

			tenantDetailsGetter := NewTenantDetailsGetter(t)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobSpecA, err := job.NewJobSpec(sampleTenant, jobVersion, "job-A", "", "", nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			assert.Nil(t, err)

			jobSpecB, err := job.NewJobSpec(sampleTenant, jobVersion, "job-B", "", "", nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			assert.Nil(t, err)

			jobSpecC, err := job.NewJobSpec(sampleTenant, jobVersion, "job-C", "", "", nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			assert.Nil(t, err)

			jobSpecs := []*job.JobSpec{jobSpecB, jobSpecA, jobSpecC}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := "resource-A"
			jobBDestination := "resource-B"
			pluginService.On("GenerateDestination", ctx, detailedTenant, jobSpecB.Task()).Return(jobBDestination, nil).Once()
			pluginService.On("GenerateDestination", ctx, detailedTenant, jobSpecA.Task()).Return(jobADestination, nil).Once()
			pluginService.On("GenerateDestination", ctx, detailedTenant, jobSpecC.Task()).Return("", errors.New("generate destination error")).Once()

			jobADependencies := []string{"job-B"}
			pluginService.On("GenerateDependencies", ctx, detailedTenant, jobSpecB, true).Return(nil, errors.New("generate dependencies error"))
			pluginService.On("GenerateDependencies", ctx, detailedTenant, jobSpecA, true).Return(jobADependencies, nil)

			jobA := job.NewJob(jobSpecA, jobADestination, jobADependencies)
			jobs := []*job.Job{jobA}
			jobRepo.On("Save", ctx, jobs).Return(nil)

			dependency := dto.NewDependency("job-B", sampleTenant, "", "resource-B")
			jobWithDependency := job.NewWithDependency("job-A", project.Name(), []*dto.Dependency{dependency}, nil)
			dependencyResolver.On("Resolve", ctx, jobs).Return([]*job.WithDependency{jobWithDependency}, nil)

			jobDependencyRepo.On("Save", ctx, []*job.WithDependency{jobWithDependency}).Return(nil)

			deployID := uuid.New()
			deployManager.On("Create", ctx, project.Name()).Return(deployID, nil)

			jobService := service.NewJobService(jobRepo, jobDependencyRepo, pluginService, dependencyResolver, tenantDetailsGetter, deployManager)
			result, jobErr, sysErr := jobService.Add(ctx, sampleTenant, jobSpecs)
			assert.NotNil(t, jobErr)
			assert.Nil(t, sysErr)
			assert.Equal(t, deployID, result)
		})
	})
}

// JobDependencyRepository is an autogenerated mock type for the JobDependencyRepository type
type JobDependencyRepository struct {
	mock.Mock
}

// Save provides a mock function with given fields: ctx, jobsWithDependencies
func (_m *JobDependencyRepository) Save(ctx context.Context, jobsWithDependencies []*job.WithDependency) error {
	ret := _m.Called(ctx, jobsWithDependencies)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, []*job.WithDependency) error); ok {
		r0 = rf(ctx, jobsWithDependencies)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

type mockConstructorTestingTNewJobDependencyRepository interface {
	mock.TestingT
	Cleanup(func())
}

// NewJobDependencyRepository creates a new instance of JobDependencyRepository. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewJobDependencyRepository(t mockConstructorTestingTNewJobDependencyRepository) *JobDependencyRepository {
	mock := &JobDependencyRepository{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// PluginService is an autogenerated mock type for the PluginService type
type PluginService struct {
	mock.Mock
}

// GenerateDependencies provides a mock function with given fields: ctx, jobTenant, jobSpec, dryRun
func (_m *PluginService) GenerateDependencies(ctx context.Context, jobTenant *tenant.WithDetails, jobSpec *job.JobSpec, dryRun bool) ([]string, error) {
	ret := _m.Called(ctx, jobTenant, jobSpec, dryRun)

	var r0 []string
	if rf, ok := ret.Get(0).(func(context.Context, *tenant.WithDetails, *job.JobSpec, bool) []string); ok {
		r0 = rf(ctx, jobTenant, jobSpec, dryRun)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *tenant.WithDetails, *job.JobSpec, bool) error); ok {
		r1 = rf(ctx, jobTenant, jobSpec, dryRun)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GenerateDestination provides a mock function with given fields: _a0, _a1, _a2
func (_m *PluginService) GenerateDestination(_a0 context.Context, _a1 *tenant.WithDetails, _a2 *job.Task) (string, error) {
	ret := _m.Called(_a0, _a1, _a2)

	var r0 string
	if rf, ok := ret.Get(0).(func(context.Context, *tenant.WithDetails, *job.Task) string); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		r0 = ret.Get(0).(string)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *tenant.WithDetails, *job.Task) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type mockConstructorTestingTNewPluginService interface {
	mock.TestingT
	Cleanup(func())
}

// NewPluginService creates a new instance of PluginService. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewPluginService(t mockConstructorTestingTNewPluginService) *PluginService {
	mock := &PluginService{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// DependencyResolver is an autogenerated mock type for the DependencyResolver type
type DependencyResolver struct {
	mock.Mock
}

// Resolve provides a mock function with given fields: ctx, jobs
func (_m *DependencyResolver) Resolve(ctx context.Context, jobs []*job.Job) ([]*job.WithDependency, error) {
	ret := _m.Called(ctx, jobs)

	var r0 []*job.WithDependency
	if rf, ok := ret.Get(0).(func(context.Context, []*job.Job) []*job.WithDependency); ok {
		r0 = rf(ctx, jobs)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.WithDependency)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, []*job.Job) error); ok {
		r1 = rf(ctx, jobs)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type mockConstructorTestingTNewDependencyResolver interface {
	mock.TestingT
	Cleanup(func())
}

// NewDependencyResolver creates a new instance of DependencyResolver. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewDependencyResolver(t mockConstructorTestingTNewDependencyResolver) *DependencyResolver {
	mock := &DependencyResolver{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// DeploymentManager is an autogenerated mock type for the DeploymentManager type
type DeploymentManager struct {
	mock.Mock
}

// Create provides a mock function with given fields: ctx, projectName
func (_m *DeploymentManager) Create(ctx context.Context, projectName tenant.ProjectName) (uuid.UUID, error) {
	ret := _m.Called(ctx, projectName)

	var r0 uuid.UUID
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName) uuid.UUID); ok {
		r0 = rf(ctx, projectName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(uuid.UUID)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName) error); ok {
		r1 = rf(ctx, projectName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type mockConstructorTestingTNewDeploymentManager interface {
	mock.TestingT
	Cleanup(func())
}

// NewDeploymentManager creates a new instance of DeploymentManager. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewDeploymentManager(t mockConstructorTestingTNewDeploymentManager) *DeploymentManager {
	mock := &DeploymentManager{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// TenantDetailsGetter is an autogenerated mock type for the TenantDetailsGetter type
type TenantDetailsGetter struct {
	mock.Mock
}

// GetDetails provides a mock function with given fields: ctx, jobTenant
func (_m *TenantDetailsGetter) GetDetails(ctx context.Context, jobTenant tenant.Tenant) (*tenant.WithDetails, error) {
	ret := _m.Called(ctx, jobTenant)

	var r0 *tenant.WithDetails
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant) *tenant.WithDetails); ok {
		r0 = rf(ctx, jobTenant)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*tenant.WithDetails)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.Tenant) error); ok {
		r1 = rf(ctx, jobTenant)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type mockConstructorTestingTNewTenantDetailsGetter interface {
	mock.TestingT
	Cleanup(func())
}

// NewTenantDetailsGetter creates a new instance of TenantDetailsGetter. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewTenantDetailsGetter(t mockConstructorTestingTNewTenantDetailsGetter) *TenantDetailsGetter {
	mock := &TenantDetailsGetter{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// JobRepository is an autogenerated mock type for the JobRepository type
type JobRepository struct {
	mock.Mock
}

// GetJobWithDependencies provides a mock function with given fields: ctx, projectName, jobNames
func (_m *JobRepository) GetJobWithDependencies(ctx context.Context, projectName tenant.ProjectName, jobNames []job.Name) ([]*job.WithDependency, error) {
	ret := _m.Called(ctx, projectName, jobNames)

	var r0 []*job.WithDependency
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, []job.Name) []*job.WithDependency); ok {
		r0 = rf(ctx, projectName, jobNames)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.WithDependency)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, []job.Name) error); ok {
		r1 = rf(ctx, projectName, jobNames)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Save provides a mock function with given fields: ctx, jobs
func (_m *JobRepository) Save(ctx context.Context, jobs []*job.Job) error {
	ret := _m.Called(ctx, jobs)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, []*job.Job) error); ok {
		r0 = rf(ctx, jobs)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

type mockConstructorTestingTNewJobRepository interface {
	mock.TestingT
	Cleanup(func())
}

// NewJobRepository creates a new instance of JobRepository. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewJobRepository(t mockConstructorTestingTNewJobRepository) *JobRepository {
	mock := &JobRepository{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
