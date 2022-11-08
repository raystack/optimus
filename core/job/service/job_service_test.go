package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/core/job"
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
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA := job.NewSpecBuilder(sampleTenant, jobVersion, "job-A", "", nil, jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := "resource-A"
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, nil)

			jobAUpstreamName := []string{"job-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobAUpstreamName, nil)

			jobA := job.NewJob(specA, jobADestination, jobAUpstreamName)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, jobs).Return(jobs, nil, nil)

			upstream, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static")
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("Resolve", ctx, project.Name(), jobs).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
			err := jobService.Add(ctx, sampleTenant, specs)
			assert.NoError(t, err)
		})
		t.Run("skip invalid job and add the rest", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			invalidJobScheduleB := job.NewSchedule("invalid", "", "", false, false, nil)
			specA := job.NewSpecBuilder(sampleTenant, jobVersion, "job-A", "", nil, jobSchedule, jobWindow, jobTask).Build()
			specB := job.NewSpecBuilder(sampleTenant, jobVersion, "job-B", "", nil, invalidJobScheduleB, jobWindow, jobTask).Build()
			specs := []*job.Spec{specB, specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := "resource-A"
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, nil).Once()

			jobAUpstreamName := []string{"job-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobAUpstreamName, nil)

			jobA := job.NewJob(specA, jobADestination, jobAUpstreamName)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, jobs).Return(jobs, nil, nil)

			upstream, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static")
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("Resolve", ctx, project.Name(), jobs).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
			err := jobService.Add(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "start date format should be")
		})
		t.Run("return error if all jobs not pass validation", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			invalidJobScheduleA := job.NewSchedule("invalid", "", "", false, false, nil)
			invalidJobScheduleB := job.NewSchedule("2022-11-01", "invalid", "", false, false, nil)
			specA := job.NewSpecBuilder(sampleTenant, jobVersion, "job-A", "", nil, invalidJobScheduleA, jobWindow, jobTask).Build()
			specB := job.NewSpecBuilder(sampleTenant, jobVersion, "job-B", "", nil, invalidJobScheduleB, jobWindow, jobTask).Build()
			specs := []*job.Spec{specB, specA}

			tenantDetailsGetter.On("GetDetails", ctx, mock.Anything).Return(nil, nil)

			jobRepo.On("Add", ctx, mock.Anything).Return(nil, nil)

			upstreamResolver.On("Resolve", ctx, mock.Anything, mock.Anything).Return(nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
			err := jobService.Add(ctx, sampleTenant, specs)

			assert.Error(t, err)
		})
		t.Run("return error if unable to get detailed tenant", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA := job.NewSpecBuilder(sampleTenant, jobVersion, "job-A", "", nil, jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(&tenant.WithDetails{}, errors.New("internal error"))

			jobRepo.On("Add", ctx, mock.Anything).Return(nil, nil)

			upstreamResolver.On("Resolve", ctx, mock.Anything, mock.Anything).Return(nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
			err := jobService.Add(ctx, sampleTenant, specs)

			assert.ErrorContains(t, err, "internal error")
		})
		t.Run("skip job that has issue when generating destination and upstream and return error", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA := job.NewSpecBuilder(sampleTenant, jobVersion, "job-A", "", nil, jobSchedule, jobWindow, jobTask).Build()
			specB := job.NewSpecBuilder(sampleTenant, jobVersion, "job-B", "", nil, jobSchedule, jobWindow, jobTask).Build()
			specC := job.NewSpecBuilder(sampleTenant, jobVersion, "job-C", "", nil, jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specB, specA, specC}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := "resource-A"
			jobBDestination := "resource-B"
			pluginService.On("GenerateDestination", ctx, detailedTenant, specB.Task()).Return(jobBDestination, nil).Once()
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, nil).Once()
			pluginService.On("GenerateDestination", ctx, detailedTenant, specC.Task()).Return("", errors.New("generate destination error")).Once()

			jobAUpstreamName := []string{"job-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specB, true).Return(nil, errors.New("generate upstream error"))
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobAUpstreamName, nil)

			jobA := job.NewJob(specA, jobADestination, jobAUpstreamName)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, jobs).Return(jobs, nil, nil)

			upstream, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static")
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("Resolve", ctx, project.Name(), jobs).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
			err := jobService.Add(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "generate upstream error")
		})
		t.Run("return error when all jobs failed to have destination and upstream generated", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA := job.NewSpecBuilder(sampleTenant, jobVersion, "job-A", "", nil, jobSchedule, jobWindow, jobTask).Build()
			specB := job.NewSpecBuilder(sampleTenant, jobVersion, "job-B", "", nil, jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specB, specA}

			jobRepo.On("Add", ctx, mock.Anything).Return(nil, nil)

			upstreamResolver.On("Resolve", ctx, mock.Anything, mock.Anything).Return(nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobBDestination := "resource-B"
			pluginService.On("GenerateDestination", ctx, detailedTenant, specB.Task()).Return(jobBDestination, nil).Once()
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return("", errors.New("generate destination error")).Once()

			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specB, true).Return(nil, errors.New("generate upstream error"))

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
			err := jobService.Add(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "generate upstream error")
		})
		t.Run("should not skip nor return error if jobs does not have upstream mod and encounter issue on generate destination/upstream", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA := job.NewSpecBuilder(sampleTenant, jobVersion, "job-A", "", nil, jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return("", service.ErrUpstreamModNotFound).Once()
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(nil, service.ErrUpstreamModNotFound)

			jobA := job.NewJob(specA, "", nil)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, jobs).Return(jobs, nil, nil)

			jobWithUpstream := job.NewWithUpstream(jobA, nil)
			upstreamResolver.On("Resolve", ctx, project.Name(), jobs).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
			err := jobService.Add(ctx, sampleTenant, specs)
			assert.NoError(t, err)
		})
		t.Run("should skip and not return error if one of the job is failed to be inserted to db", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA := job.NewSpecBuilder(sampleTenant, jobVersion, "job-A", "", nil, jobSchedule, jobWindow, jobTask).Build()
			specB := job.NewSpecBuilder(sampleTenant, jobVersion, "job-B", "", nil, jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specA, specB}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			resourceA := "resource-A"
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(resourceA, nil).Once()
			pluginService.On("GenerateDestination", ctx, detailedTenant, specB.Task()).Return("", service.ErrUpstreamModNotFound).Once()

			jobSourcesA := []string{"resource-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobSourcesA, nil)
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specB, true).Return(nil, service.ErrUpstreamModNotFound)

			jobA := job.NewJob(specA, resourceA, jobSourcesA)
			jobB := job.NewJob(specB, "", nil)
			jobs := []*job.Job{jobA, jobB}
			savedJobs := []*job.Job{jobB}
			jobRepo.On("Add", ctx, jobs).Return(savedJobs, errors.New("unable to save job A"), nil)

			jobWithUpstreamB := job.NewWithUpstream(jobB, nil)
			upstreamResolver.On("Resolve", ctx, project.Name(), savedJobs).Return([]*job.WithUpstream{jobWithUpstreamB}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
			err := jobService.Add(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "unable to save job A")
		})
		t.Run("return error when all jobs failed to be inserted to db", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA := job.NewSpecBuilder(sampleTenant, jobVersion, "job-A", "", nil, jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			upstreamResolver.On("Resolve", ctx, mock.Anything, mock.Anything).Return(nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			resourceA := "resource-A"
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(resourceA, nil).Once()

			jobSourcesA := []string{"resource-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobSourcesA, nil)

			jobA := job.NewJob(specA, resourceA, jobSourcesA)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, jobs).Return([]*job.Job{}, errors.New("unable to save job A"), errors.New("all jobs failed"))

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
			err := jobService.Add(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "unable to save job A")
		})
		t.Run("should return error if failed to save upstream", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA := job.NewSpecBuilder(sampleTenant, jobVersion, "job-A", "", nil, jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			resourceA := "resource-A"
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(resourceA, nil).Once()

			jobSourcesA := []string{"resource-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobSourcesA, nil)

			jobA := job.NewJob(specA, resourceA, jobSourcesA)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, jobs).Return(jobs, nil, nil)

			jobWithUpstreamA := job.NewWithUpstream(jobA, nil)
			upstreamResolver.On("Resolve", ctx, project.Name(), jobs).Return([]*job.WithUpstream{jobWithUpstreamA}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(errors.New("internal error"))

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
			err := jobService.Add(ctx, sampleTenant, specs)
			assert.Error(t, err)
		})
	})
}

// JobRepository is an autogenerated mock type for the JobRepository type
type JobRepository struct {
	mock.Mock
}

// Add provides a mock function with given fields: ctx, jobs
func (_m *JobRepository) Add(ctx context.Context, jobs []*job.Job) ([]*job.Job, error) {
	ret := _m.Called(ctx, jobs)

	var r0 []*job.Job
	if rf, ok := ret.Get(0).(func(context.Context, []*job.Job) []*job.Job); ok {
		r0 = rf(ctx, jobs)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Job)
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

// GetJobNameWithInternalUpstreams provides a mock function with given fields: ctx, projectName, jobNames
func (_m *JobRepository) GetJobNameWithInternalUpstreams(ctx context.Context, projectName tenant.ProjectName, jobNames []job.Name) (map[job.Name][]*job.Upstream, error) {
	ret := _m.Called(ctx, projectName, jobNames)

	var r0 map[job.Name][]*job.Upstream
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, []job.Name) map[job.Name][]*job.Upstream); ok {
		r0 = rf(ctx, projectName, jobNames)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[job.Name][]*job.Upstream)
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

// ReplaceUpstreams provides a mock function with given fields: ctx, jobsWithUpstreams
func (_m *JobRepository) ReplaceUpstreams(ctx context.Context, jobsWithUpstreams []*job.WithUpstream) error {
	ret := _m.Called(ctx, jobsWithUpstreams)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, []*job.WithUpstream) error); ok {
		r0 = rf(ctx, jobsWithUpstreams)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PluginService is an autogenerated mock type for the PluginService type
type PluginService struct {
	mock.Mock
}

// GenerateUpstreams provides a mock function with given fields: ctx, jobTenant, spec, dryRun
func (_m *PluginService) GenerateUpstreams(ctx context.Context, jobTenant *tenant.WithDetails, spec *job.Spec, dryRun bool) ([]string, error) {
	ret := _m.Called(ctx, jobTenant, spec, dryRun)

	var r0 []string
	if rf, ok := ret.Get(0).(func(context.Context, *tenant.WithDetails, *job.Spec, bool) []string); ok {
		r0 = rf(ctx, jobTenant, spec, dryRun)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *tenant.WithDetails, *job.Spec, bool) error); ok {
		r1 = rf(ctx, jobTenant, spec, dryRun)
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

// UpstreamResolver is an autogenerated mock type for the UpstreamResolver type
type UpstreamResolver struct {
	mock.Mock
}

// Resolve provides a mock function with given fields: ctx, projectName, jobs
func (_m *UpstreamResolver) Resolve(ctx context.Context, projectName tenant.ProjectName, jobs []*job.Job) ([]*job.WithUpstream, error) {
	ret := _m.Called(ctx, projectName, jobs)

	var r0 []*job.WithUpstream
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, []*job.Job) []*job.WithUpstream); ok {
		r0 = rf(ctx, projectName, jobs)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.WithUpstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, []*job.Job) error); ok {
		r1 = rf(ctx, projectName, jobs)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
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
