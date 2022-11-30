package resolver_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/dto"
	"github.com/odpf/optimus/core/job/resolver"
	"github.com/odpf/optimus/core/tenant"
	optMock "github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

func TestUpstreamResolver(t *testing.T) {
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
	externalTenant, _ := tenant.NewTenant("external-proj", "external-namespace")
	jobVersion, err := job.VersionFrom(1)
	assert.NoError(t, err)
	startDate, err := job.ScheduleDateFrom("2022-10-01")
	assert.NoError(t, err)
	jobSchedule, err := job.NewScheduleBuilder(startDate).Build()
	assert.NoError(t, err)
	jobWindow, err := models.NewWindow(jobVersion.Int(), "d", "24h", "24h")
	assert.NoError(t, err)
	jobTaskConfig, err := job.NewConfig(map[string]string{"sample_task_key": "sample_value"})
	assert.NoError(t, err)
	taskName, _ := job.TaskNameFrom("sample-task")
	jobTask := job.NewTaskBuilder(taskName, jobTaskConfig).Build()

	t.Run("BulkResolve", func(t *testing.T) {
		t.Run("resolve upstream internally", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalUpstreamResolver := new(ExternalUpstreamResolver)

			logWriter := new(optMock.LogWriter)
			defer logWriter.AssertExpectations(t)

			upstreamName := job.SpecUpstreamName("test-proj/job-c")
			upstreamSpec := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName}).Build()
			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).WithSpecUpstream(upstreamSpec).Build()
			jobADestination := job.ResourceURN("resource-A")
			jobAUpstreams := []job.ResourceURN{"resource-B"}

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreams)
			jobs := []*job.Job{jobA}

			upstreamB := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "inferred", taskName, false)
			upstreamC := job.NewUpstreamResolved("job-C", "", "resource-C", sampleTenant, "static", taskName, false)
			upstreams := []*job.Upstream{upstreamB, upstreamC}
			jobNameWithUpstreams := map[job.Name][]*job.Upstream{
				jobA.Spec().Name(): upstreams,
			}

			jobRepo.On("GetJobNameWithInternalUpstreams", ctx, project.Name(), []job.Name{specA.Name()}).Return(jobNameWithUpstreams, nil)

			externalUpstreamResolver.On("Resolve", ctx, mock.Anything).Return([]*job.Upstream{}, nil, nil)

			expectedJobWitUpstreams := []*job.WithUpstream{job.NewWithUpstream(jobA, []*job.Upstream{upstreamB, upstreamC})}

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver)
			result, err := upstreamResolver.BulkResolve(ctx, project.Name(), jobs, logWriter)
			assert.NoError(t, err)
			assert.EqualValues(t, expectedJobWitUpstreams, result)
		})
		t.Run("resolve upstream internally and externally", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalUpstreamResolver := new(ExternalUpstreamResolver)

			logWriter := new(optMock.LogWriter)
			defer logWriter.AssertExpectations(t)

			upstreamName := job.SpecUpstreamNameFrom("job-c")
			upstreamSpec := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName}).Build()
			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).WithSpecUpstream(upstreamSpec).Build()
			jobADestination := job.ResourceURN("resource-A")
			jobAUpstreams := []job.ResourceURN{"resource-B", "resource-D"}

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreams)
			jobs := []*job.Job{jobA}

			internalUpstream := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static", taskName, false)
			jobNameWithUpstreams := map[job.Name][]*job.Upstream{
				jobA.Spec().Name(): {internalUpstream},
			}

			jobRepo.On("GetJobNameWithInternalUpstreams", ctx, project.Name(), []job.Name{specA.Name()}).Return(jobNameWithUpstreams, nil)

			externalUpstreamC := job.NewUpstreamResolved("job-C", "external-host", "resource-C", externalTenant, "static", taskName, true)
			externalUpstreamD := job.NewUpstreamResolved("job-D", "external-host", "resource-D", externalTenant, "inferred", taskName, true)
			externalUpstreamResolver.On("Resolve", ctx, mock.Anything).Return([]*job.Upstream{externalUpstreamC, externalUpstreamD}, nil, nil)

			expectedJobWitUpstreams := []*job.WithUpstream{
				job.NewWithUpstream(jobA, []*job.Upstream{internalUpstream, externalUpstreamC, externalUpstreamD}),
			}

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver)
			result, err := upstreamResolver.BulkResolve(ctx, project.Name(), jobs, logWriter)
			assert.NoError(t, err)
			assert.EqualValues(t, expectedJobWitUpstreams, result)
		})
		t.Run("returns error when unable to get internal upstreams", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalUpstreamResolver := new(ExternalUpstreamResolver)

			logWriter := new(optMock.LogWriter)
			defer logWriter.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			jobADestination := job.ResourceURN("resource-A")
			jobAUpstreams := []job.ResourceURN{"resource-B"}

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreams)
			jobs := []*job.Job{jobA}

			jobRepo.On("GetJobNameWithInternalUpstreams", ctx, project.Name(), mock.Anything).Return(map[job.Name][]*job.Upstream{}, errors.New("internal error"))

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver)
			result, err := upstreamResolver.BulkResolve(ctx, project.Name(), jobs, logWriter)
			assert.Error(t, err)
			assert.Nil(t, result)
		})
		t.Run("returns upstream error when there is unresolved static upstream", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalUpstreamResolver := new(ExternalUpstreamResolver)

			logWriter := new(optMock.LogWriter)
			defer logWriter.AssertExpectations(t)

			upstreamName := job.SpecUpstreamNameFrom("job-c")
			upstreamSpec := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName}).Build()
			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).WithSpecUpstream(upstreamSpec).Build()
			jobADestination := job.ResourceURN("resource-A")
			jobAUpstreams := []job.ResourceURN{"resource-B", "resource-D"}

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreams)
			jobs := []*job.Job{jobA}

			internalUpstream := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static", taskName, false)
			jobNameWithUpstreams := map[job.Name][]*job.Upstream{
				jobA.Spec().Name(): {internalUpstream},
			}

			jobRepo.On("GetJobNameWithInternalUpstreams", ctx, project.Name(), []job.Name{specA.Name()}).Return(jobNameWithUpstreams, nil)

			unresolvedUpstreamC := job.NewUpstreamUnresolved("job-C", "", externalTenant.ProjectName())
			unresolvedUpstreamD := job.NewUpstreamUnresolved("", "resource-D", "")
			externalUpstreamResolver.On("Resolve", ctx, mock.Anything).Return([]*job.Upstream{}, []*job.Upstream{unresolvedUpstreamC, unresolvedUpstreamD}, nil)

			externalUpstreamC := job.NewUpstreamUnresolved("job-C", "", "external-proj")
			externalUpstreamD := job.NewUpstreamUnresolved("", "resource-D", "")
			expectedJobWitUpstreams := []*job.WithUpstream{
				job.NewWithUpstream(jobA, []*job.Upstream{internalUpstream, externalUpstreamC, externalUpstreamD}),
			}

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver)
			result, err := upstreamResolver.BulkResolve(ctx, project.Name(), jobs, logWriter)
			assert.Error(t, err)
			assert.EqualValues(t, expectedJobWitUpstreams, result)
		})
		t.Run("returns upstream error when encounter error on fetching external upstreams", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalUpstreamResolver := new(ExternalUpstreamResolver)

			logWriter := new(optMock.LogWriter)
			defer logWriter.AssertExpectations(t)

			upstreamName := job.SpecUpstreamNameFrom("job-c")
			upstreamSpec := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName}).Build()
			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).WithSpecUpstream(upstreamSpec).Build()
			jobADestination := job.ResourceURN("resource-A")
			jobAUpstreams := []job.ResourceURN{"resource-B", "resource-D"}

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreams)
			jobs := []*job.Job{jobA}

			internalUpstream := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static", taskName, false)
			jobNameWithUpstreams := map[job.Name][]*job.Upstream{
				jobA.Spec().Name(): {internalUpstream},
			}

			externalUpstreamC := job.NewUpstreamResolved("job-C", "external-host", "resource-C", externalTenant, "static", taskName, true)
			externalUpstreamD := job.NewUpstreamResolved("job-D", "external-host", "resource-D", externalTenant, "inferred", taskName, true)
			jobRepo.On("GetJobNameWithInternalUpstreams", ctx, project.Name(), []job.Name{specA.Name()}).Return(jobNameWithUpstreams, nil)

			externalUpstreamResolver.On("Resolve", ctx, mock.Anything).Return([]*job.Upstream{externalUpstreamC, externalUpstreamD}, nil, errors.New("internal error"))

			expectedJobWitUpstreams := []*job.WithUpstream{
				job.NewWithUpstream(jobA, []*job.Upstream{internalUpstream, externalUpstreamC, externalUpstreamD}),
			}

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver)
			result, err := upstreamResolver.BulkResolve(ctx, project.Name(), jobs, logWriter)
			assert.Error(t, err)
			assert.EqualValues(t, expectedJobWitUpstreams, result)
		})
		t.Run("returns upstream error when encounter error on initializing unresolved upstream", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalUpstreamResolver := new(ExternalUpstreamResolver)

			logWriter := new(optMock.LogWriter)
			defer logWriter.AssertExpectations(t)

			upstreamName := job.SpecUpstreamName("job-c")
			upstreamSpec := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName}).Build()
			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).WithSpecUpstream(upstreamSpec).Build()
			jobADestination := job.ResourceURN("resource-A")
			jobAUpstreams := []job.ResourceURN{"resource-B", "resource-D"}

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreams)
			jobs := []*job.Job{jobA}

			internalUpstream := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static", taskName, false)
			jobNameWithUpstreams := map[job.Name][]*job.Upstream{
				jobA.Spec().Name(): {internalUpstream},
			}

			jobRepo.On("GetJobNameWithInternalUpstreams", ctx, project.Name(), []job.Name{specA.Name()}).Return(jobNameWithUpstreams, nil)

			unresolvedUpstreamC := job.NewUpstreamUnresolved("job-C", "", externalTenant.ProjectName())
			unresolvedUpstreamD := job.NewUpstreamUnresolved("", "resource-D", "")
			externalUpstreamResolver.On("Resolve", ctx, mock.Anything).Return([]*job.Upstream{}, []*job.Upstream{unresolvedUpstreamC, unresolvedUpstreamD}, nil)

			externalUpstreamC := job.NewUpstreamUnresolved("job-C", "", "external-proj")
			externalUpstreamD := job.NewUpstreamUnresolved("", "resource-D", "")
			expectedJobWitUpstreams := []*job.WithUpstream{
				job.NewWithUpstream(jobA, []*job.Upstream{internalUpstream, externalUpstreamC, externalUpstreamD}),
			}

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver)
			result, err := upstreamResolver.BulkResolve(ctx, project.Name(), jobs, logWriter)
			assert.ErrorContains(t, err, "resolve jobs errors")
			assert.EqualValues(t, expectedJobWitUpstreams, result)
		})
	})
	t.Run("Resolve", func(t *testing.T) {
		t.Run("resolve upstream internally and externally", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalUpstreamResolver := new(ExternalUpstreamResolver)

			logWriter := new(optMock.LogWriter)
			defer logWriter.AssertExpectations(t)

			jobAUpstreamCName := job.SpecUpstreamNameFrom("job-C")
			jobAUpstreamSpec := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{jobAUpstreamCName}).Build()
			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).WithSpecUpstream(jobAUpstreamSpec).Build()
			jobADestination := job.ResourceURN("resource-A")
			jobASources := []job.ResourceURN{"resource-B", "resource-D"}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobASources)

			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
			jobBDestination := job.ResourceURN("resource-B")
			jobB := job.NewJob(sampleTenant, specB, jobBDestination, nil)

			specC := job.NewSpecBuilder(jobVersion, "job-C", "", jobSchedule, jobWindow, jobTask).Build()
			jobCDestination := job.ResourceURN("resource-C")
			jobC := job.NewJob(sampleTenant, specC, jobCDestination, nil)

			internalUpstreamB := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "inferred", taskName, false)
			jobRepo.On("GetAllByResourceDestination", ctx, jobASources[0]).Return([]*job.Job{jobB}, nil)
			jobRepo.On("GetAllByResourceDestination", ctx, jobASources[1]).Return([]*job.Job{}, nil)

			internalUpstreamC := job.NewUpstreamResolved("job-C", "", "resource-C", sampleTenant, "static", taskName, false)
			jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specC.Name()).Return(jobC, nil)

			externalUpstreamD := job.NewUpstreamResolved("job-D", "external-host", "resource-D", externalTenant, "inferred", taskName, true)
			externalUpstreamResolver.On("Resolve", ctx, mock.Anything).Return([]*job.Upstream{externalUpstreamD}, nil, nil)

			expectedUpstream := []*job.Upstream{internalUpstreamB, internalUpstreamC, externalUpstreamD}

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver)
			result, err := upstreamResolver.Resolve(ctx, jobA)
			assert.NoError(t, err)
			assert.EqualValues(t, expectedUpstream, result)
		})
		t.Run("should skip resolving upstream if the static upstream name is invalid", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalUpstreamResolver := new(ExternalUpstreamResolver)

			logWriter := new(optMock.LogWriter)
			defer logWriter.AssertExpectations(t)

			jobAUpstreamCName := job.SpecUpstreamNameFrom("")
			jobAUpstreamSpec := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{jobAUpstreamCName}).Build()
			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).WithSpecUpstream(jobAUpstreamSpec).Build()
			jobADestination := job.ResourceURN("resource-A")
			jobASources := []job.ResourceURN{"resource-B", "resource-D"}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobASources)

			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
			jobBDestination := job.ResourceURN("resource-B")
			jobB := job.NewJob(sampleTenant, specB, jobBDestination, nil)

			internalUpstreamB := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "inferred", taskName, false)
			jobRepo.On("GetAllByResourceDestination", ctx, jobASources[0]).Return([]*job.Job{jobB}, nil)
			jobRepo.On("GetAllByResourceDestination", ctx, jobASources[1]).Return([]*job.Job{}, nil)

			externalUpstreamD := job.NewUpstreamResolved("job-D", "external-host", "resource-D", externalTenant, "inferred", taskName, true)
			externalUpstreamResolver.On("Resolve", ctx, mock.Anything).Return([]*job.Upstream{externalUpstreamD}, nil, nil)

			expectedUpstream := []*job.Upstream{internalUpstreamB, externalUpstreamD}

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver)
			result, err := upstreamResolver.Resolve(ctx, jobA)
			assert.ErrorContains(t, err, "name is empty")
			assert.EqualValues(t, expectedUpstream, result)
		})
		t.Run("should not break process but still return error if failed to resolve static upstream internally", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalUpstreamResolver := new(ExternalUpstreamResolver)

			logWriter := new(optMock.LogWriter)
			defer logWriter.AssertExpectations(t)

			jobAUpstreamCName := job.SpecUpstreamNameFrom("job-C")
			jobAUpstreamSpec := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{jobAUpstreamCName}).Build()
			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).WithSpecUpstream(jobAUpstreamSpec).Build()
			jobADestination := job.ResourceURN("resource-A")
			jobASources := []job.ResourceURN{"resource-B", "resource-D"}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobASources)

			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
			jobBDestination := job.ResourceURN("resource-B")
			jobB := job.NewJob(sampleTenant, specB, jobBDestination, nil)

			specC := job.NewSpecBuilder(jobVersion, "job-C", "", jobSchedule, jobWindow, jobTask).Build()

			internalUpstreamB := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "inferred", taskName, false)
			jobRepo.On("GetAllByResourceDestination", ctx, jobASources[0]).Return([]*job.Job{jobB}, nil)
			jobRepo.On("GetAllByResourceDestination", ctx, jobASources[1]).Return([]*job.Job{}, nil)

			errorMsg := "resolve static upstream internally failed"
			jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specC.Name()).Return(nil, errors.New(errorMsg))

			externalUpstreamD := job.NewUpstreamResolved("job-D", "external-host", "resource-D", externalTenant, "inferred", taskName, true)
			externalUpstreamResolver.On("Resolve", ctx, mock.Anything).Return([]*job.Upstream{externalUpstreamD}, nil, nil)

			expectedUpstream := []*job.Upstream{internalUpstreamB, externalUpstreamD}

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver)
			result, err := upstreamResolver.Resolve(ctx, jobA)
			assert.ErrorContains(t, err, errorMsg)
			assert.EqualValues(t, expectedUpstream, result)
		})
	})
}

// ExternalUpstreamResolver is an autogenerated mock type for the ExternalUpstreamResolver type
type ExternalUpstreamResolver struct {
	mock.Mock
}

// Resolve provides a mock function with given fields: ctx, unresolvedUpstreams
func (_m *ExternalUpstreamResolver) Resolve(ctx context.Context, unresolvedUpstreams []*dto.RawUpstream) ([]*job.Upstream, []*job.Upstream, error) {
	ret := _m.Called(ctx, unresolvedUpstreams)

	var r0 []*job.Upstream
	if rf, ok := ret.Get(0).(func(context.Context, []*dto.RawUpstream) []*job.Upstream); ok {
		r0 = rf(ctx, unresolvedUpstreams)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Upstream)
		}
	}

	var r1 []*job.Upstream
	if rf, ok := ret.Get(1).(func(context.Context, []*dto.RawUpstream) []*job.Upstream); ok {
		r1 = rf(ctx, unresolvedUpstreams)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).([]*job.Upstream)
		}
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(context.Context, []*dto.RawUpstream) error); ok {
		r2 = rf(ctx, unresolvedUpstreams)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// JobRepository is an autogenerated mock type for the JobRepository type
type JobRepository struct {
	mock.Mock
}

// GetAllByResourceDestination provides a mock function with given fields: ctx, resourceDestination
func (_m *JobRepository) GetAllByResourceDestination(ctx context.Context, resourceDestination job.ResourceURN) ([]*job.Job, error) {
	ret := _m.Called(ctx, resourceDestination)

	var r0 []*job.Job
	if rf, ok := ret.Get(0).(func(context.Context, job.ResourceURN) []*job.Job); ok {
		r0 = rf(ctx, resourceDestination)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Job)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, job.ResourceURN) error); ok {
		r1 = rf(ctx, resourceDestination)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetByJobName provides a mock function with given fields: ctx, projectName, jobName
func (_m *JobRepository) GetByJobName(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) (*job.Job, error) {
	ret := _m.Called(ctx, projectName, jobName)

	var r0 *job.Job
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, job.Name) *job.Job); ok {
		r0 = rf(ctx, projectName, jobName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*job.Job)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, job.Name) error); ok {
		r1 = rf(ctx, projectName, jobName)
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
