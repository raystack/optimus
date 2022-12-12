package resolver_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/resolver"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/models"
	"github.com/odpf/optimus/internal/writer"
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
			internalUpstreamResolver := new(InternalUpstreamResolver)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			upstreamName := job.SpecUpstreamName("test-proj/job-c")
			upstreamSpec, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName}).Build()
			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).WithSpecUpstream(upstreamSpec).Build()
			jobADestination := job.ResourceURN("resource-A")
			jobAUpstreams := []job.ResourceURN{"resource-B"}

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreams)
			jobs := []*job.Job{jobA}

			upstreamB := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "inferred", taskName, false)
			upstreamC := job.NewUpstreamResolved("job-C", "", "resource-C", sampleTenant, "static", taskName, false)
			upstreams := []*job.Upstream{upstreamB, upstreamC}
			jobAWithUpstream := job.NewWithUpstream(jobA, upstreams)

			internalUpstreamResolver.On("BulkResolve", ctx, project.Name(), mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream}, nil)

			externalUpstreamResolver.On("BulkResolve", ctx, mock.Anything, mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream}, nil, nil)

			expectedJobWitUpstreams := []*job.WithUpstream{job.NewWithUpstream(jobA, []*job.Upstream{upstreamB, upstreamC})}

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver, internalUpstreamResolver)
			result, err := upstreamResolver.BulkResolve(ctx, project.Name(), jobs, logWriter)
			assert.NoError(t, err)
			assert.EqualValues(t, expectedJobWitUpstreams, result)
		})
		t.Run("resolve upstream internally and externally", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalUpstreamResolver := new(ExternalUpstreamResolver)
			internalUpstreamResolver := new(InternalUpstreamResolver)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			upstreamName := job.SpecUpstreamNameFrom("job-c")
			upstreamSpec, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName}).Build()
			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).WithSpecUpstream(upstreamSpec).Build()
			jobADestination := job.ResourceURN("resource-A")
			jobAUpstreams := []job.ResourceURN{"resource-B", "resource-D"}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreams)
			jobs := []*job.Job{jobA}

			unresolvedUpstreams := []*job.Upstream{
				job.NewUpstreamUnresolvedInferred("resource-B"),
				job.NewUpstreamUnresolvedStatic("job-c", project.Name()),
				job.NewUpstreamUnresolvedInferred("resource-D"),
			}

			internalUpstream := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static", taskName, false)
			jobWithInternalUpstreams := job.NewWithUpstream(jobA, []*job.Upstream{internalUpstream, unresolvedUpstreams[1], unresolvedUpstreams[2]})
			internalUpstreamResolver.On("BulkResolve", ctx, project.Name(), mock.Anything).Return([]*job.WithUpstream{jobWithInternalUpstreams}, nil)

			externalUpstreamC := job.NewUpstreamResolved("job-C", "external-host", "resource-C", externalTenant, "static", taskName, true)
			externalUpstreamD := job.NewUpstreamResolved("job-D", "external-host", "resource-D", externalTenant, "inferred", taskName, true)
			jobWithExternalUpstreams := job.NewWithUpstream(jobA, []*job.Upstream{internalUpstream, externalUpstreamC, externalUpstreamD})
			externalUpstreamResolver.On("BulkResolve", ctx, mock.Anything, mock.Anything).Return([]*job.WithUpstream{jobWithExternalUpstreams}, nil)

			expectedJobWitUpstreams := []*job.WithUpstream{
				job.NewWithUpstream(jobA, []*job.Upstream{internalUpstream, externalUpstreamC, externalUpstreamD}),
			}

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver, internalUpstreamResolver)
			result, err := upstreamResolver.BulkResolve(ctx, project.Name(), jobs, logWriter)
			assert.NoError(t, err)
			assert.EqualValues(t, expectedJobWitUpstreams, result)
		})
		t.Run("returns error when unable to get internal upstreams", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalUpstreamResolver := new(ExternalUpstreamResolver)
			internalUpstreamResolver := new(InternalUpstreamResolver)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			jobADestination := job.ResourceURN("resource-A")
			jobAUpstreams := []job.ResourceURN{"resource-B"}

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreams)
			jobs := []*job.Job{jobA}

			internalUpstreamResolver.On("BulkResolve", ctx, project.Name(), mock.Anything).Return(nil, errors.New("internal error"))

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver, internalUpstreamResolver)
			result, err := upstreamResolver.BulkResolve(ctx, project.Name(), jobs, logWriter)
			assert.ErrorContains(t, err, "internal error")
			assert.Nil(t, result)
		})
		t.Run("returns upstream error when there is unresolved static upstream", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalUpstreamResolver := new(ExternalUpstreamResolver)
			internalUpstreamResolver := new(InternalUpstreamResolver)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			upstreamName := job.SpecUpstreamNameFrom("job-c")
			upstreamSpec, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName}).Build()
			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).WithSpecUpstream(upstreamSpec).Build()
			jobADestination := job.ResourceURN("resource-A")
			jobAUpstreams := []job.ResourceURN{"resource-B", "resource-D"}

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreams)
			jobs := []*job.Job{jobA}

			unresolvedUpstreams := []*job.Upstream{
				job.NewUpstreamUnresolvedInferred("resource-B"),
				job.NewUpstreamUnresolvedStatic("job-c", project.Name()),
				job.NewUpstreamUnresolvedInferred("resource-D"),
			}

			internalUpstream := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static", taskName, false)
			jobWithInternalUpstreams := job.NewWithUpstream(jobA, []*job.Upstream{internalUpstream, unresolvedUpstreams[1], unresolvedUpstreams[2]})
			internalUpstreamResolver.On("BulkResolve", ctx, project.Name(), mock.Anything).Return([]*job.WithUpstream{jobWithInternalUpstreams}, nil)

			externalUpstreamD := job.NewUpstreamResolved("job-D", "external-host", "resource-D", externalTenant, "inferred", taskName, true)
			jobWithExternalUpstreams := job.NewWithUpstream(jobA, []*job.Upstream{internalUpstream, unresolvedUpstreams[1], externalUpstreamD})
			externalUpstreamResolver.On("BulkResolve", ctx, mock.Anything, mock.Anything).Return([]*job.WithUpstream{jobWithExternalUpstreams}, nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver, internalUpstreamResolver)
			result, err := upstreamResolver.BulkResolve(ctx, project.Name(), jobs, logWriter)
			assert.Error(t, err)
			assert.EqualValues(t, []*job.WithUpstream{jobWithExternalUpstreams}, result)
		})
		t.Run("returns upstream error when encounter error on fetching external upstreams", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalUpstreamResolver := new(ExternalUpstreamResolver)
			internalUpstreamResolver := new(InternalUpstreamResolver)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			upstreamName := job.SpecUpstreamNameFrom("job-c")
			upstreamSpec, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName}).Build()
			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).WithSpecUpstream(upstreamSpec).Build()
			jobADestination := job.ResourceURN("resource-A")
			jobAUpstreams := []job.ResourceURN{"resource-B", "resource-D"}

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreams)
			jobs := []*job.Job{jobA}

			unresolvedUpstreams := []*job.Upstream{
				job.NewUpstreamUnresolvedInferred("resource-B"),
				job.NewUpstreamUnresolvedStatic("job-c", project.Name()),
				job.NewUpstreamUnresolvedInferred("resource-D"),
			}

			internalUpstream := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static", taskName, false)
			jobWithInternalUpstreams := job.NewWithUpstream(jobA, []*job.Upstream{internalUpstream, unresolvedUpstreams[1], unresolvedUpstreams[2]})
			internalUpstreamResolver.On("BulkResolve", ctx, project.Name(), mock.Anything).Return([]*job.WithUpstream{jobWithInternalUpstreams}, nil)

			externalUpstreamD := job.NewUpstreamResolved("job-D", "external-host", "resource-D", externalTenant, "inferred", taskName, true)
			jobWithExternalUpstreams := job.NewWithUpstream(jobA, []*job.Upstream{internalUpstream, unresolvedUpstreams[1], externalUpstreamD})
			externalUpstreamResolver.On("BulkResolve", ctx, mock.Anything, mock.Anything).Return([]*job.WithUpstream{jobWithExternalUpstreams}, errors.New("internal error"))

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver, internalUpstreamResolver)
			result, err := upstreamResolver.BulkResolve(ctx, project.Name(), jobs, logWriter)
			assert.ErrorContains(t, err, "internal error")
			assert.EqualValues(t, []*job.WithUpstream{jobWithExternalUpstreams}, result)
		})
	})
	t.Run("Resolve", func(t *testing.T) {
		t.Run("resolve upstream internally and externally", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalUpstreamResolver := new(ExternalUpstreamResolver)
			internalUpstreamResolver := new(InternalUpstreamResolver)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobAUpstreamCName := job.SpecUpstreamNameFrom("job-C")
			jobAUpstreamSpec, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{jobAUpstreamCName}).Build()
			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).WithSpecUpstream(jobAUpstreamSpec).Build()
			jobADestination := job.ResourceURN("resource-A")
			jobASources := []job.ResourceURN{"resource-B", "resource-D"}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobASources)

			unresolvedUpstreams := []*job.Upstream{
				job.NewUpstreamUnresolvedInferred("resource-B"),
				job.NewUpstreamUnresolvedStatic("job-C", project.Name()),
				job.NewUpstreamUnresolvedInferred("resource-D"),
			}

			internalUpstreamB := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "inferred", taskName, false)
			internalUpstreamC := job.NewUpstreamResolved("job-C", "", "resource-C", sampleTenant, "static", taskName, false)
			externalUpstreamD := job.NewUpstreamResolved("job-D", "external-host", "resource-D", externalTenant, "inferred", taskName, true)

			jobWithInternalUpstream := job.NewWithUpstream(jobA, []*job.Upstream{internalUpstreamB, internalUpstreamC, unresolvedUpstreams[2]})
			jobWithUnresolvedUpstream := job.NewWithUpstream(jobA, []*job.Upstream{unresolvedUpstreams[0], unresolvedUpstreams[2], unresolvedUpstreams[1]})
			internalUpstreamResolver.On("Resolve", ctx, jobWithUnresolvedUpstream).Return(jobWithInternalUpstream, nil)

			jobWithExternalUpstream := job.NewWithUpstream(jobA, []*job.Upstream{internalUpstreamB, internalUpstreamC, externalUpstreamD})
			externalUpstreamResolver.On("Resolve", ctx, jobWithInternalUpstream, logWriter).Return(jobWithExternalUpstream, nil)

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver, internalUpstreamResolver)
			result, err := upstreamResolver.Resolve(ctx, jobA, logWriter)
			assert.NoError(t, err)
			assert.EqualValues(t, jobWithExternalUpstream.Upstreams(), result)
		})
		t.Run("should skip resolving upstream if the static upstream name is invalid", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalUpstreamResolver := new(ExternalUpstreamResolver)
			internalUpstreamResolver := new(InternalUpstreamResolver)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobAUpstreamCName := job.SpecUpstreamNameFrom("")
			jobAUpstreamSpec, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{jobAUpstreamCName}).Build()
			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).WithSpecUpstream(jobAUpstreamSpec).Build()
			jobADestination := job.ResourceURN("resource-A")
			jobASources := []job.ResourceURN{"resource-B", "resource-D"}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobASources)

			unresolvedUpstreams := []*job.Upstream{
				job.NewUpstreamUnresolvedInferred("resource-B"),
				job.NewUpstreamUnresolvedInferred("resource-D"),
			}

			internalUpstreamB := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "inferred", taskName, false)
			externalUpstreamD := job.NewUpstreamResolved("job-D", "external-host", "resource-D", externalTenant, "inferred", taskName, true)

			jobWithInternalUpstream := job.NewWithUpstream(jobA, []*job.Upstream{internalUpstreamB, unresolvedUpstreams[1]})
			jobWithUnresolvedUpstream := job.NewWithUpstream(jobA, []*job.Upstream{unresolvedUpstreams[0], unresolvedUpstreams[1]})
			internalUpstreamResolver.On("Resolve", ctx, jobWithUnresolvedUpstream).Return(jobWithInternalUpstream, nil)

			jobWithExternalUpstream := job.NewWithUpstream(jobA, []*job.Upstream{internalUpstreamB, externalUpstreamD})
			externalUpstreamResolver.On("Resolve", ctx, jobWithInternalUpstream, logWriter).Return(jobWithExternalUpstream, nil)

			expectedUpstream := []*job.Upstream{internalUpstreamB, externalUpstreamD}

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver, internalUpstreamResolver)
			result, err := upstreamResolver.Resolve(ctx, jobA, logWriter)
			assert.ErrorContains(t, err, "name is empty")
			assert.EqualValues(t, expectedUpstream, result)
		})
		t.Run("should not break process but still return error if failed to resolve some static upstream internally", func(t *testing.T) {
			jobRepo := new(JobRepository)
			externalUpstreamResolver := new(ExternalUpstreamResolver)
			internalUpstreamResolver := new(InternalUpstreamResolver)

			logWriter := new(mockWriter)
			defer logWriter.AssertExpectations(t)

			jobAUpstreamCName := job.SpecUpstreamNameFrom("job-C")
			jobAUpstreamSpec, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{jobAUpstreamCName}).Build()
			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).WithSpecUpstream(jobAUpstreamSpec).Build()
			jobADestination := job.ResourceURN("resource-A")
			jobASources := []job.ResourceURN{"resource-B", "resource-D"}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobASources)

			unresolvedUpstreams := []*job.Upstream{
				job.NewUpstreamUnresolvedInferred("resource-B"),
				job.NewUpstreamUnresolvedStatic("job-C", project.Name()),
				job.NewUpstreamUnresolvedInferred("resource-D"),
			}

			internalUpstreamC := job.NewUpstreamResolved("job-C", "", "resource-C", sampleTenant, "static", taskName, false)
			externalUpstreamD := job.NewUpstreamResolved("job-D", "external-host", "resource-D", externalTenant, "inferred", taskName, true)

			jobWithUnresolvedUpstream := job.NewWithUpstream(jobA, []*job.Upstream{unresolvedUpstreams[0], unresolvedUpstreams[2], unresolvedUpstreams[1]})
			jobWithInternalUpstream := job.NewWithUpstream(jobA, []*job.Upstream{unresolvedUpstreams[0], internalUpstreamC, unresolvedUpstreams[2]})
			errorMsg := "resolve upstream failed partially"
			internalUpstreamResolver.On("Resolve", ctx, jobWithUnresolvedUpstream).Return(jobWithInternalUpstream, errors.New(errorMsg))

			jobWithExternalUpstream := job.NewWithUpstream(jobA, []*job.Upstream{unresolvedUpstreams[0], internalUpstreamC, externalUpstreamD})
			externalUpstreamResolver.On("Resolve", ctx, jobWithInternalUpstream, logWriter).Return(jobWithExternalUpstream, nil)

			expectedUpstream := []*job.Upstream{unresolvedUpstreams[0], internalUpstreamC, externalUpstreamD}

			upstreamResolver := resolver.NewUpstreamResolver(jobRepo, externalUpstreamResolver, internalUpstreamResolver)
			result, err := upstreamResolver.Resolve(ctx, jobA, logWriter)
			assert.ErrorContains(t, err, errorMsg)
			assert.EqualValues(t, expectedUpstream, result)
		})
	})
}

// ExternalUpstreamResolver is an autogenerated mock type for the ExternalUpstreamResolver type
type ExternalUpstreamResolver struct {
	mock.Mock
}

// BulkResolve provides a mock function with given fields: _a0, _a1, _a2
func (_m *ExternalUpstreamResolver) BulkResolve(_a0 context.Context, _a1 []*job.WithUpstream, _a2 writer.LogWriter) ([]*job.WithUpstream, error) {
	ret := _m.Called(_a0, _a1, _a2)

	var r0 []*job.WithUpstream
	if rf, ok := ret.Get(0).(func(context.Context, []*job.WithUpstream, writer.LogWriter) []*job.WithUpstream); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.WithUpstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, []*job.WithUpstream, writer.LogWriter) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Resolve provides a mock function with given fields: ctx, jobWithUpstream, lw
func (_m *ExternalUpstreamResolver) Resolve(ctx context.Context, jobWithUpstream *job.WithUpstream, lw writer.LogWriter) (*job.WithUpstream, error) {
	ret := _m.Called(ctx, jobWithUpstream, lw)

	var r0 *job.WithUpstream
	if rf, ok := ret.Get(0).(func(context.Context, *job.WithUpstream, writer.LogWriter) *job.WithUpstream); ok {
		r0 = rf(ctx, jobWithUpstream, lw)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*job.WithUpstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *job.WithUpstream, writer.LogWriter) error); ok {
		r1 = rf(ctx, jobWithUpstream, lw)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// InternalUpstreamResolver is an autogenerated mock type for the InternalUpstreamResolver type
type InternalUpstreamResolver struct {
	mock.Mock
}

// BulkResolve provides a mock function with given fields: _a0, _a1, _a2
func (_m *InternalUpstreamResolver) BulkResolve(_a0 context.Context, _a1 tenant.ProjectName, _a2 []*job.WithUpstream) ([]*job.WithUpstream, error) {
	ret := _m.Called(_a0, _a1, _a2)

	var r0 []*job.WithUpstream
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, []*job.WithUpstream) []*job.WithUpstream); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.WithUpstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, []*job.WithUpstream) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Resolve provides a mock function with given fields: _a0, _a1
func (_m *InternalUpstreamResolver) Resolve(_a0 context.Context, _a1 *job.WithUpstream) (*job.WithUpstream, error) {
	ret := _m.Called(_a0, _a1)

	var r0 *job.WithUpstream
	if rf, ok := ret.Get(0).(func(context.Context, *job.WithUpstream) *job.WithUpstream); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*job.WithUpstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *job.WithUpstream) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
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
func (_m *JobRepository) ResolveUpstreams(ctx context.Context, projectName tenant.ProjectName, jobNames []job.Name) (map[job.Name][]*job.Upstream, error) {
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

type mockWriter struct {
	mock.Mock
}

func (m *mockWriter) Write(level writer.LogLevel, s string) error {
	return m.Called(level, s).Error(0)
}
