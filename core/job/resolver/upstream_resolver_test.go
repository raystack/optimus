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
	jobTask := job.NewTaskBuilder("bq2bq", jobTaskConfig).Build()

	t.Run("Resolve", func(t *testing.T) {
		t.Run("resolve upstream internally", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalUpstreamResolver := new(ExternalUpstreamResolver)

			logWriter := new(optMock.LogWriter)
			defer logWriter.AssertExpectations(t)

			upstreamName := job.SpecUpstreamName("test-proj/job-c")
			upstreamSpec := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName}).Build()
			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).WithSpecUpstream(upstreamSpec).Build()
			jobADestination, _ := job.ResourceURNFrom("resource-A")
			jobAUpstreams := []job.ResourceURN{"resource-B"}

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreams)
			jobs := []*job.Job{jobA}

			upstreamB, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "inferred")
			upstreamC, _ := job.NewUpstreamResolved("job-C", "", "resource-C", sampleTenant, "static")
			upstreams := []*job.Upstream{upstreamB, upstreamC}
			jobNameWithUpstreams := map[job.Name][]*job.Upstream{
				jobA.Spec().Name(): upstreams,
			}

			jobRepo.On("GetJobNameWithInternalUpstreams", ctx, project.Name(), []job.Name{specA.Name()}).Return(jobNameWithUpstreams, nil)

			externalUpstreamResolver.On("FetchExternalUpstreams", ctx, mock.Anything).Return([]*job.Upstream{}, nil, nil)

			expectedJobWitUpstreams := []*job.WithUpstream{job.NewWithUpstream(jobA, []*job.Upstream{upstreamB, upstreamC})}

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver)
			result, err := upstreamResolver.Resolve(ctx, project.Name(), jobs, logWriter)
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
			jobADestination, _ := job.ResourceURNFrom("resource-A")
			jobAUpstreams := []job.ResourceURN{"resource-B", "resource-D"}

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreams)
			jobs := []*job.Job{jobA}

			internalUpstream, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static")
			jobNameWithUpstreams := map[job.Name][]*job.Upstream{
				jobA.Spec().Name(): {internalUpstream},
			}

			jobRepo.On("GetJobNameWithInternalUpstreams", ctx, project.Name(), []job.Name{specA.Name()}).Return(jobNameWithUpstreams, nil)

			externalUpstreamC, _ := job.NewUpstreamResolved("job-C", "external-host", "resource-C", externalTenant, "static")
			externalUpstreamD, _ := job.NewUpstreamResolved("job-D", "external-host", "resource-D", externalTenant, "inferred")
			externalUpstreamResolver.On("FetchExternalUpstreams", ctx, mock.Anything).Return([]*job.Upstream{externalUpstreamC, externalUpstreamD}, nil, nil)

			expectedJobWitUpstreams := []*job.WithUpstream{
				job.NewWithUpstream(jobA, []*job.Upstream{internalUpstream, externalUpstreamC, externalUpstreamD}),
			}

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver)
			result, err := upstreamResolver.Resolve(ctx, project.Name(), jobs, logWriter)
			assert.NoError(t, err)
			assert.EqualValues(t, expectedJobWitUpstreams, result)
		})
		t.Run("returns error when unable to get internal upstreams", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalUpstreamResolver := new(ExternalUpstreamResolver)

			logWriter := new(optMock.LogWriter)
			defer logWriter.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			jobADestination, _ := job.ResourceURNFrom("resource-A")
			jobAUpstreams := []job.ResourceURN{"resource-B"}

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreams)
			jobs := []*job.Job{jobA}

			jobRepo.On("GetJobNameWithInternalUpstreams", ctx, project.Name(), mock.Anything).Return(map[job.Name][]*job.Upstream{}, errors.New("internal error"))

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver)
			result, err := upstreamResolver.Resolve(ctx, project.Name(), jobs, logWriter)
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
			jobADestination, _ := job.ResourceURNFrom("resource-A")
			jobAUpstreams := []job.ResourceURN{"resource-B", "resource-D"}

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreams)
			jobs := []*job.Job{jobA}

			internalUpstream, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static")
			jobNameWithUpstreams := map[job.Name][]*job.Upstream{
				jobA.Spec().Name(): {internalUpstream},
			}

			jobRepo.On("GetJobNameWithInternalUpstreams", ctx, project.Name(), []job.Name{specA.Name()}).Return(jobNameWithUpstreams, nil)

			unresolvedUpstreamC := &dto.RawUpstream{
				ProjectName: externalTenant.ProjectName().String(),
				JobName:     "job-C",
			}
			unresolvedUpstreamD := &dto.RawUpstream{
				ResourceURN: "resource-D",
			}
			externalUpstreamResolver.On("FetchExternalUpstreams", ctx, mock.Anything).Return([]*job.Upstream{}, []*dto.RawUpstream{unresolvedUpstreamC, unresolvedUpstreamD}, nil)

			externalUpstreamC := job.NewUpstreamUnresolved("job-C", "", "external-proj")
			externalUpstreamD := job.NewUpstreamUnresolved("", "resource-D", "")
			expectedJobWitUpstreams := []*job.WithUpstream{
				job.NewWithUpstream(jobA, []*job.Upstream{internalUpstream, externalUpstreamC, externalUpstreamD}),
			}

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver)
			result, err := upstreamResolver.Resolve(ctx, project.Name(), jobs, logWriter)
			assert.Error(t, err)
			assert.EqualValues(t, expectedJobWitUpstreams, result)
		})
		t.Run("returns upstream error when encounter error on fetching fetch external upstreams", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalUpstreamResolver := new(ExternalUpstreamResolver)

			logWriter := new(optMock.LogWriter)
			defer logWriter.AssertExpectations(t)

			upstreamName := job.SpecUpstreamNameFrom("job-c")
			upstreamSpec := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName}).Build()
			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).WithSpecUpstream(upstreamSpec).Build()
			jobADestination, _ := job.ResourceURNFrom("resource-A")
			jobAUpstreams := []job.ResourceURN{"resource-B", "resource-D"}

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreams)
			jobs := []*job.Job{jobA}

			internalUpstream, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static")
			jobNameWithUpstreams := map[job.Name][]*job.Upstream{
				jobA.Spec().Name(): {internalUpstream},
			}

			externalUpstreamC, _ := job.NewUpstreamResolved("job-C", "external-host", "resource-C", externalTenant, "static")
			externalUpstreamD, _ := job.NewUpstreamResolved("job-D", "external-host", "resource-D", externalTenant, "inferred")
			jobRepo.On("GetJobNameWithInternalUpstreams", ctx, project.Name(), []job.Name{specA.Name()}).Return(jobNameWithUpstreams, nil)

			externalUpstreamResolver.On("FetchExternalUpstreams", ctx, mock.Anything).Return([]*job.Upstream{externalUpstreamC, externalUpstreamD}, nil, errors.New("internal error"))

			expectedJobWitUpstreams := []*job.WithUpstream{
				job.NewWithUpstream(jobA, []*job.Upstream{internalUpstream, externalUpstreamC, externalUpstreamD}),
			}

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver)
			result, err := upstreamResolver.Resolve(ctx, project.Name(), jobs, logWriter)
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
			jobADestination, _ := job.ResourceURNFrom("resource-A")
			jobAUpstreams := []job.ResourceURN{"resource-B", "resource-D"}

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreams)
			jobs := []*job.Job{jobA}

			internalUpstream, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static")
			jobNameWithUpstreams := map[job.Name][]*job.Upstream{
				jobA.Spec().Name(): {internalUpstream},
			}

			jobRepo.On("GetJobNameWithInternalUpstreams", ctx, project.Name(), []job.Name{specA.Name()}).Return(jobNameWithUpstreams, nil)

			unresolvedUpstreamC := &dto.RawUpstream{
				ProjectName: externalTenant.ProjectName().String(),
				JobName:     "job-C",
			}
			unresolvedUpstreamD := &dto.RawUpstream{
				ResourceURN: "resource-D",
			}
			externalUpstreamResolver.On("FetchExternalUpstreams", ctx, mock.Anything).Return([]*job.Upstream{}, []*dto.RawUpstream{unresolvedUpstreamC, unresolvedUpstreamD}, nil)

			externalUpstreamC := job.NewUpstreamUnresolved("job-C", "", "external-proj")
			externalUpstreamD := job.NewUpstreamUnresolved("", "resource-D", "")
			expectedJobWitUpstreams := []*job.WithUpstream{
				job.NewWithUpstream(jobA, []*job.Upstream{internalUpstream, externalUpstreamC, externalUpstreamD}),
			}

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver)
			result, err := upstreamResolver.Resolve(ctx, project.Name(), jobs, logWriter)
			assert.ErrorContains(t, err, "resolve jobs errors")
			assert.EqualValues(t, expectedJobWitUpstreams, result)
		})
	})
}

// ExternalUpstreamResolver is an autogenerated mock type for the ExternalUpstreamResolver type
type ExternalUpstreamResolver struct {
	mock.Mock
}

// FetchExternalUpstreams provides a mock function with given fields: ctx, unresolvedUpstreams
func (_m *ExternalUpstreamResolver) FetchExternalUpstreams(ctx context.Context, unresolvedUpstreams []*dto.RawUpstream) ([]*job.Upstream, []*dto.RawUpstream, error) {
	ret := _m.Called(ctx, unresolvedUpstreams)

	var r0 []*job.Upstream
	if rf, ok := ret.Get(0).(func(context.Context, []*dto.RawUpstream) []*job.Upstream); ok {
		r0 = rf(ctx, unresolvedUpstreams)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Upstream)
		}
	}

	var r1 []*dto.RawUpstream
	if rf, ok := ret.Get(1).(func(context.Context, []*dto.RawUpstream) []*dto.RawUpstream); ok {
		r1 = rf(ctx, unresolvedUpstreams)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).([]*dto.RawUpstream)
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

// Add provides a mock function with given fields: ctx, jobs
func (_m *JobRepository) Add(ctx context.Context, jobs []*job.Job) ([]*job.Job, error, error) {
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

	var r2 error
	if rf, ok := ret.Get(2).(func(context.Context, []*job.Job) error); ok {
		r2 = rf(ctx, jobs)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

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
