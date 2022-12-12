package resolver_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/resolver"
	"github.com/odpf/optimus/core/tenant"
	optMock "github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

func TestInternalUpstreamResolver(t *testing.T) {
	ctx := context.Background()
	sampleTenant, _ := tenant.NewTenant("project", "namespace")

	jobVersion, _ := job.VersionFrom(1)
	startDate, _ := job.ScheduleDateFrom("2022-10-01")
	jobSchedule, _ := job.NewScheduleBuilder(startDate).Build()
	jobWindow, _ := models.NewWindow(jobVersion.Int(), "d", "24h", "24h")
	taskName, _ := job.TaskNameFrom("sample-task")
	jobTaskConfig, _ := job.NewConfig(map[string]string{"sample_task_key": "sample_value"})
	jobTask := job.NewTaskBuilder(taskName, jobTaskConfig).Build()
	upstreamSpec, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"job-C"}).Build()
	specA := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).WithSpecUpstream(upstreamSpec).Build()
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
	internalUpstreamC := job.NewUpstreamResolved("job-C", "", "resource-C", sampleTenant, "static", taskName, false)

	unresolvedUpstreamB := job.NewUpstreamUnresolvedInferred("resource-B")
	unresolvedUpstreamC := job.NewUpstreamUnresolvedStatic("job-C", "project")
	unresolvedUpstreamD := job.NewUpstreamUnresolvedInferred("resource-D")

	t.Run("Resolve", func(t *testing.T) {
		t.Run("resolves upstream internally", func(t *testing.T) {
			jobRepo := new(JobRepository)

			logWriter := new(optMock.LogWriter)
			defer logWriter.AssertExpectations(t)

			jobRepo.On("GetAllByResourceDestination", ctx, jobASources[0]).Return([]*job.Job{jobB}, nil)
			jobRepo.On("GetAllByResourceDestination", ctx, jobASources[1]).Return([]*job.Job{}, nil)

			jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specC.Name()).Return(jobC, nil)

			jobWithUnresolvedUpstream := job.NewWithUpstream(jobA, []*job.Upstream{unresolvedUpstreamB, unresolvedUpstreamC, unresolvedUpstreamD})

			expectedJobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{internalUpstreamB, internalUpstreamC, unresolvedUpstreamD})

			internalUpstreamResolver := resolver.NewInternalUpstreamResolver(jobRepo)
			result, err := internalUpstreamResolver.Resolve(ctx, jobWithUnresolvedUpstream)
			assert.NoError(t, err)
			assert.EqualValues(t, expectedJobWithUpstream.Upstreams(), result.Upstreams())
		})
		t.Run("should not stop the process but keep appending error when unable to resolve inferred upstream", func(t *testing.T) {
			jobRepo := new(JobRepository)

			logWriter := new(optMock.LogWriter)
			defer logWriter.AssertExpectations(t)

			jobRepo.On("GetAllByResourceDestination", ctx, jobASources[0]).Return([]*job.Job{}, errors.New("internal error"))
			jobRepo.On("GetAllByResourceDestination", ctx, jobASources[1]).Return([]*job.Job{}, nil)

			jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specC.Name()).Return(jobC, nil)

			jobWithUnresolvedUpstream := job.NewWithUpstream(jobA, []*job.Upstream{unresolvedUpstreamB, unresolvedUpstreamC, unresolvedUpstreamD})

			expectedJobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{unresolvedUpstreamB, internalUpstreamC, unresolvedUpstreamD})

			internalUpstreamResolver := resolver.NewInternalUpstreamResolver(jobRepo)
			result, err := internalUpstreamResolver.Resolve(ctx, jobWithUnresolvedUpstream)
			assert.ErrorContains(t, err, "internal error")
			assert.EqualValues(t, expectedJobWithUpstream.Upstreams(), result.Upstreams())
		})
		t.Run("should not stop the process but keep appending error when unable to resolve static upstream", func(t *testing.T) {
			jobRepo := new(JobRepository)

			logWriter := new(optMock.LogWriter)
			defer logWriter.AssertExpectations(t)

			specEUpstreamSpec, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"job-unknown", "job-C"}).Build()
			specE := job.NewSpecBuilder(jobVersion, "job-E", "sample-owner", jobSchedule, jobWindow, jobTask).WithSpecUpstream(specEUpstreamSpec).Build()
			jobEDestination := job.ResourceURN("resource-E")
			jobE := job.NewJob(sampleTenant, specE, jobEDestination, nil)

			jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), job.Name("job-unknown")).Return(nil, errors.New("not found"))
			jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specC.Name()).Return(jobC, nil)

			unresolvedUpstreamUnknown := job.NewUpstreamUnresolvedStatic("job-unknown", sampleTenant.ProjectName())
			jobWithUnresolvedUpstream := job.NewWithUpstream(jobE, []*job.Upstream{unresolvedUpstreamUnknown, unresolvedUpstreamC})

			expectedJobWithUpstream := job.NewWithUpstream(jobE, []*job.Upstream{unresolvedUpstreamUnknown, internalUpstreamC})

			internalUpstreamResolver := resolver.NewInternalUpstreamResolver(jobRepo)
			result, err := internalUpstreamResolver.Resolve(ctx, jobWithUnresolvedUpstream)
			assert.ErrorContains(t, err, "not found")
			assert.EqualValues(t, expectedJobWithUpstream.Upstreams(), result.Upstreams())
		})
		t.Run("should not stop the process but keep appending error when static upstream name is invalid", func(t *testing.T) {
			jobRepo := new(JobRepository)

			logWriter := new(optMock.LogWriter)
			defer logWriter.AssertExpectations(t)

			specEUpstreamSpec, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"/", "job-C"}).Build()
			specE := job.NewSpecBuilder(jobVersion, "job-E", "sample-owner", jobSchedule, jobWindow, jobTask).WithSpecUpstream(specEUpstreamSpec).Build()
			jobEDestination := job.ResourceURN("resource-E")
			jobE := job.NewJob(sampleTenant, specE, jobEDestination, nil)

			jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specC.Name()).Return(jobC, nil)

			unresolvedUpstreamUnknown := job.NewUpstreamUnresolvedStatic("job-unknown", sampleTenant.ProjectName())
			jobWithUnresolvedUpstream := job.NewWithUpstream(jobE, []*job.Upstream{unresolvedUpstreamUnknown, unresolvedUpstreamC})

			expectedJobWithUpstream := job.NewWithUpstream(jobE, []*job.Upstream{unresolvedUpstreamUnknown, internalUpstreamC})

			internalUpstreamResolver := resolver.NewInternalUpstreamResolver(jobRepo)
			result, err := internalUpstreamResolver.Resolve(ctx, jobWithUnresolvedUpstream)
			assert.ErrorContains(t, err, "name is empty")
			assert.EqualValues(t, expectedJobWithUpstream.Upstreams(), result.Upstreams())
		})
	})
	t.Run("BulkResolve", func(t *testing.T) {
		specX := job.NewSpecBuilder(jobVersion, "job-X", "", jobSchedule, jobWindow, jobTask).WithSpecUpstream(upstreamSpec).Build()
		jobXDestination := job.ResourceURN("resource-X")
		jobX := job.NewJob(sampleTenant, specX, jobXDestination, []job.ResourceURN{"resource-B"})

		t.Run("resolves upstream internally in bulk", func(t *testing.T) {
			jobRepo := new(JobRepository)

			logWriter := new(optMock.LogWriter)
			defer logWriter.AssertExpectations(t)

			internalUpstreamMap := map[job.Name][]*job.Upstream{
				"job-A": {internalUpstreamB, internalUpstreamC},
				"job-X": {internalUpstreamB, internalUpstreamC},
			}
			jobRepo.On("ResolveUpstreams", ctx, sampleTenant.ProjectName(), []job.Name{"job-A", "job-X"}).Return(internalUpstreamMap, nil)

			jobsWithUnresolvedUpstream := []*job.WithUpstream{
				job.NewWithUpstream(jobA, []*job.Upstream{unresolvedUpstreamB, unresolvedUpstreamC, unresolvedUpstreamD}),
				job.NewWithUpstream(jobX, []*job.Upstream{unresolvedUpstreamB, unresolvedUpstreamC}),
			}

			expectedJobsWithUpstream := []*job.WithUpstream{
				job.NewWithUpstream(jobA, []*job.Upstream{internalUpstreamB, internalUpstreamC, unresolvedUpstreamD}),
				job.NewWithUpstream(jobX, []*job.Upstream{internalUpstreamB, internalUpstreamC}),
			}

			internalUpstreamResolver := resolver.NewInternalUpstreamResolver(jobRepo)
			result, err := internalUpstreamResolver.BulkResolve(ctx, sampleTenant.ProjectName(), jobsWithUnresolvedUpstream)
			assert.NoError(t, err)
			assert.EqualValues(t, expectedJobsWithUpstream, result)
		})
		t.Run("returns error if unable to resolve upstream internally", func(t *testing.T) {
			jobRepo := new(JobRepository)

			logWriter := new(optMock.LogWriter)
			defer logWriter.AssertExpectations(t)

			jobRepo.On("ResolveUpstreams", ctx, sampleTenant.ProjectName(), []job.Name{"job-A", "job-X"}).Return(nil, errors.New("internal error"))

			jobsWithUnresolvedUpstream := []*job.WithUpstream{
				job.NewWithUpstream(jobA, []*job.Upstream{unresolvedUpstreamB, unresolvedUpstreamC, unresolvedUpstreamD}),
				job.NewWithUpstream(jobX, []*job.Upstream{unresolvedUpstreamB, unresolvedUpstreamC}),
			}

			internalUpstreamResolver := resolver.NewInternalUpstreamResolver(jobRepo)
			result, err := internalUpstreamResolver.BulkResolve(ctx, sampleTenant.ProjectName(), jobsWithUnresolvedUpstream)
			assert.ErrorContains(t, err, "internal error")
			assert.Nil(t, result)
		})
	})
}
