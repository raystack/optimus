package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/odpf/optimus/core/job/dto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/api/writer"
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/service"
	"github.com/odpf/optimus/core/job/service/filter"
	"github.com/odpf/optimus/core/tenant"
	optMock "github.com/odpf/optimus/mock"
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

	jobVersion, err := job.VersionFrom(1)
	assert.NoError(t, err)
	startDate, err := job.ScheduleDateFrom("2022-10-01")
	assert.NoError(t, err)
	jobSchedule, err := job.NewScheduleBuilder(startDate).Build()
	assert.NoError(t, err)
	jobWindow, _ := models.NewWindow(jobVersion.Int(), "d", "24h", "24h")
	jobTaskConfig, err := job.NewConfig(map[string]string{"sample_task_key": "sample_value"})
	assert.NoError(t, err)
	taskName, _ := job.TaskNameFrom("bq2bq")
	jobTask := job.NewTaskBuilder(taskName, jobTaskConfig).Build()

	projectFilter := filter.WithString(filter.ProjectName, project.Name().String())
	namespacesFilter := filter.WithStringArray(filter.NamespaceNames, []string{namespace.Name().String()})

	t.Run("Add", func(t *testing.T) {
		t.Run("add jobs", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := job.ResourceURN("resource-A")
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, nil)

			jobAUpstreamName := []job.ResourceURN{"job-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobAUpstreamName, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, jobs).Return(jobs, nil, nil)

			upstream, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static", taskName, false)
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
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

			invalidAsset := job.NewAsset(map[string]string{"key": ""})
			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).
				WithAsset(invalidAsset).
				Build()
			specs := []*job.Spec{specA, specB}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := job.ResourceURN("resource-A")
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, nil).Once()

			jobAUpstreamName := []job.ResourceURN{"job-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobAUpstreamName, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, jobs).Return(jobs, nil, nil)

			upstream, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static", taskName, false)
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			err := jobService.Add(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "keys [key] are empty")
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

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(&tenant.WithDetails{}, errors.New("internal error"))

			jobRepo.On("Add", ctx, mock.Anything).Return(nil, nil)

			upstreamResolver.On("BulkResolve", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			err := jobService.Add(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "internal error")
		})
		t.Run("skip job that has issue when generating destination and return error", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
			specC := job.NewSpecBuilder(jobVersion, "job-C", "", jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specB, specA, specC}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := job.ResourceURN("resource-A")
			jobBDestination := job.ResourceURN("resource-B")
			var jobDestination job.ResourceURN
			pluginService.On("GenerateDestination", ctx, detailedTenant, specB.Task()).Return(jobBDestination, nil).Once()
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, nil).Once()
			pluginService.On("GenerateDestination", ctx, detailedTenant, specC.Task()).Return(jobDestination, errors.New("generate destination error")).Once()

			jobAUpstreamName := []job.ResourceURN{"job-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specB, true).Return([]job.ResourceURN{}, errors.New("generate upstream error"))
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobAUpstreamName, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, jobs).Return(jobs, nil, nil)

			upstream, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static", taskName, false)
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
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

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specB, specA}

			jobRepo.On("Add", ctx, mock.Anything).Return(nil, nil)

			upstreamResolver.On("BulkResolve", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			var jobADestination job.ResourceURN
			jobBDestination := job.ResourceURN("resource-B")
			pluginService.On("GenerateDestination", ctx, detailedTenant, specB.Task()).Return(jobBDestination, nil).Once()
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, errors.New("generate destination error")).Once()

			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specB, true).Return(nil, errors.New("generate upstream error"))

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
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

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			var jobADestination job.ResourceURN
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, service.ErrUpstreamModNotFound).Once()
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(nil, service.ErrUpstreamModNotFound)

			jobA := job.NewJob(sampleTenant, specA, "", nil)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, jobs).Return(jobs, nil, nil)

			jobWithUpstream := job.NewWithUpstream(jobA, nil)
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
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

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specA, specB}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			resourceA := job.ResourceURN("resource-A")
			var resourceB job.ResourceURN
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(resourceA, nil).Once()
			pluginService.On("GenerateDestination", ctx, detailedTenant, specB.Task()).Return(resourceB, service.ErrUpstreamModNotFound).Once()

			jobSourcesA := []job.ResourceURN{"resource-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobSourcesA, nil)
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specB, true).Return(nil, service.ErrUpstreamModNotFound)

			jobA := job.NewJob(sampleTenant, specA, resourceA, jobSourcesA)
			jobB := job.NewJob(sampleTenant, specB, "", nil)
			jobs := []*job.Job{jobA, jobB}
			savedJobs := []*job.Job{jobB}
			jobRepo.On("Add", ctx, jobs).Return(savedJobs, errors.New("unable to save job A"), nil)

			jobWithUpstreamB := job.NewWithUpstream(jobB, nil)
			upstreamResolver.On("BulkResolve", ctx, project.Name(), savedJobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstreamB}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
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

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			upstreamResolver.On("BulkResolve", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			resourceA := job.ResourceURN("resource-A")
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(resourceA, nil).Once()

			jobSourcesA := []job.ResourceURN{"resource-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobSourcesA, nil)

			jobA := job.NewJob(sampleTenant, specA, resourceA, jobSourcesA)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, jobs).Return([]*job.Job{}, errors.New("unable to save job A"), errors.New("all jobs failed"))

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
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

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			resourceA := job.ResourceURN("resource-A")
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(resourceA, nil).Once()

			jobSourcesA := []job.ResourceURN{"resource-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobSourcesA, nil)

			jobA := job.NewJob(sampleTenant, specA, resourceA, jobSourcesA)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, jobs).Return(jobs, nil, nil)

			jobWithUpstreamA := job.NewWithUpstream(jobA, nil)
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstreamA}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(errors.New("internal error"))

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			err := jobService.Add(ctx, sampleTenant, specs)
			assert.Error(t, err)
		})
	})
	t.Run("Update", func(t *testing.T) {
		t.Run("update jobs", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := job.ResourceURN("resource-A")
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, nil)

			jobAUpstreamName := []job.ResourceURN{"job-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobAUpstreamName, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName)
			jobs := []*job.Job{jobA}
			jobRepo.On("Update", ctx, jobs).Return(jobs, nil, nil)

			upstream, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static", taskName, false)
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			err := jobService.Update(ctx, sampleTenant, specs)
			assert.NoError(t, err)
		})
		t.Run("skip invalid job and update the rest", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			invalidAsset := job.NewAsset(map[string]string{"key": ""})
			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).
				WithAsset(invalidAsset).
				Build()
			specs := []*job.Spec{specA, specB}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := job.ResourceURN("resource-A")
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, nil).Once()

			jobAUpstreamName := []job.ResourceURN{"job-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobAUpstreamName, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName)
			jobs := []*job.Job{jobA}
			jobRepo.On("Update", ctx, jobs).Return(jobs, nil, nil)

			upstream, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static", taskName, false)
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			err := jobService.Update(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "keys [key] are empty")
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

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(&tenant.WithDetails{}, errors.New("internal error"))

			jobRepo.On("Update", ctx, mock.Anything).Return(nil, nil)

			upstreamResolver.On("BulkResolve", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			err := jobService.Update(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "internal error")
		})
		t.Run("skip job that has issue when generating destination and return error", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
			specC := job.NewSpecBuilder(jobVersion, "job-C", "", jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specB, specA, specC}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := job.ResourceURN("resource-A")
			jobBDestination := job.ResourceURN("resource-B")
			var jobDestination job.ResourceURN
			pluginService.On("GenerateDestination", ctx, detailedTenant, specB.Task()).Return(jobBDestination, nil).Once()
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, nil).Once()
			pluginService.On("GenerateDestination", ctx, detailedTenant, specC.Task()).Return(jobDestination, errors.New("generate destination error")).Once()

			jobAUpstreamName := []job.ResourceURN{"job-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specB, true).Return([]job.ResourceURN{}, errors.New("generate upstream error"))
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobAUpstreamName, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName)
			jobs := []*job.Job{jobA}
			jobRepo.On("Update", ctx, jobs).Return(jobs, nil, nil)

			upstream, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static", taskName, false)
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			err := jobService.Update(ctx, sampleTenant, specs)
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

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specB, specA}

			jobRepo.On("Update", ctx, mock.Anything).Return(nil, nil)

			upstreamResolver.On("BulkResolve", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			var jobADestination job.ResourceURN
			jobBDestination := job.ResourceURN("resource-B")
			pluginService.On("GenerateDestination", ctx, detailedTenant, specB.Task()).Return(jobBDestination, nil).Once()
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, errors.New("generate destination error")).Once()

			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specB, true).Return(nil, errors.New("generate upstream error"))

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			err := jobService.Update(ctx, sampleTenant, specs)
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

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			var jobADestination job.ResourceURN
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, service.ErrUpstreamModNotFound).Once()
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(nil, service.ErrUpstreamModNotFound)

			jobA := job.NewJob(sampleTenant, specA, "", nil)
			jobs := []*job.Job{jobA}
			jobRepo.On("Update", ctx, jobs).Return(jobs, nil, nil)

			jobWithUpstream := job.NewWithUpstream(jobA, nil)
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			err := jobService.Update(ctx, sampleTenant, specs)
			assert.NoError(t, err)
		})
		t.Run("should skip and not return error if one of the job is failed to be updated to db", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specA, specB}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			resourceA := job.ResourceURN("resource-A")
			var resourceB job.ResourceURN
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(resourceA, nil).Once()
			pluginService.On("GenerateDestination", ctx, detailedTenant, specB.Task()).Return(resourceB, service.ErrUpstreamModNotFound).Once()

			jobSourcesA := []job.ResourceURN{"resource-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobSourcesA, nil)
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specB, true).Return(nil, service.ErrUpstreamModNotFound)

			jobA := job.NewJob(sampleTenant, specA, resourceA, jobSourcesA)
			jobB := job.NewJob(sampleTenant, specB, "", nil)
			jobs := []*job.Job{jobA, jobB}
			savedJobs := []*job.Job{jobB}
			jobRepo.On("Update", ctx, jobs).Return(savedJobs, errors.New("unable to save job A"), nil)

			jobWithUpstreamB := job.NewWithUpstream(jobB, nil)
			upstreamResolver.On("BulkResolve", ctx, project.Name(), savedJobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstreamB}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			err := jobService.Update(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "unable to save job A")
		})
		t.Run("return error when all jobs failed to be updated to db", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			upstreamResolver.On("BulkResolve", ctx, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			resourceA := job.ResourceURN("resource-A")
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(resourceA, nil).Once()

			jobSourcesA := []job.ResourceURN{"resource-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobSourcesA, nil)

			jobA := job.NewJob(sampleTenant, specA, resourceA, jobSourcesA)
			jobs := []*job.Job{jobA}
			jobRepo.On("Update", ctx, jobs).Return([]*job.Job{}, errors.New("unable to update job A"), errors.New("all jobs failed"))

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			err := jobService.Update(ctx, sampleTenant, specs)
			assert.ErrorContains(t, err, "unable to update job A")
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

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			resourceA := job.ResourceURN("resource-A")
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(resourceA, nil).Once()

			jobSourcesA := []job.ResourceURN{"resource-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobSourcesA, nil)

			jobA := job.NewJob(sampleTenant, specA, resourceA, jobSourcesA)
			jobs := []*job.Job{jobA}
			jobRepo.On("Update", ctx, jobs).Return(jobs, nil, nil)

			jobWithUpstreamA := job.NewWithUpstream(jobA, nil)
			upstreamResolver.On("BulkResolve", ctx, project.Name(), jobs, mock.Anything).Return([]*job.WithUpstream{jobWithUpstreamA}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(errors.New("internal error"))

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			err := jobService.Update(ctx, sampleTenant, specs)
			assert.Error(t, err)
		})
	})
	t.Run("Delete", func(t *testing.T) {
		t.Run("deletes job without downstream", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()

			jobRepo.On("GetDownstreamFullNames", ctx, project.Name(), specA.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), specA.Name(), false).Return(nil)

			jobService := service.NewJobService(jobRepo, nil, nil, nil, nil)
			affectedDownstream, err := jobService.Delete(ctx, sampleTenant, specA.Name(), false, false)
			assert.NoError(t, err)
			assert.Empty(t, affectedDownstream)
		})
		t.Run("deletes job with downstream if it is a force delete", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()

			downstreamFullNames := []job.FullName{"test-proj/job-B", "test-proj/job-C"}
			jobRepo.On("GetDownstreamFullNames", ctx, project.Name(), specA.Name()).Return(downstreamFullNames, nil)
			jobRepo.On("Delete", ctx, project.Name(), specA.Name(), false).Return(nil)

			jobService := service.NewJobService(jobRepo, nil, nil, nil, nil)
			affectedDownstream, err := jobService.Delete(ctx, sampleTenant, specA.Name(), false, true)
			assert.NoError(t, err)
			assert.EqualValues(t, downstreamFullNames, affectedDownstream)
		})
		t.Run("not delete the job if it has downstream and not a force delete", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()

			downstreamFullNames := []job.FullName{"test-proj/job-B", "test-proj/job-C"}
			jobRepo.On("GetDownstreamFullNames", ctx, project.Name(), specA.Name()).Return(downstreamFullNames, nil)

			jobService := service.NewJobService(jobRepo, nil, nil, nil, nil)
			affectedDownstream, err := jobService.Delete(ctx, sampleTenant, specA.Name(), false, false)
			assert.Error(t, err)
			assert.Empty(t, affectedDownstream)
		})
		t.Run("returns error if unable to get downstream", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()

			jobRepo.On("GetDownstreamFullNames", ctx, project.Name(), specA.Name()).Return(nil, errors.New("internal error"))

			jobService := service.NewJobService(jobRepo, nil, nil, nil, nil)
			affectedDownstream, err := jobService.Delete(ctx, sampleTenant, specA.Name(), false, false)
			assert.Error(t, err)
			assert.Empty(t, affectedDownstream)
		})
		t.Run("returns error if unable to delete job", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()

			jobRepo.On("GetDownstreamFullNames", ctx, project.Name(), specA.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), specA.Name(), false).Return(errors.New("internal error"))

			jobService := service.NewJobService(jobRepo, nil, nil, nil, nil)
			affectedDownstream, err := jobService.Delete(ctx, sampleTenant, specA.Name(), false, false)
			assert.Error(t, err)
			assert.Empty(t, affectedDownstream)
		})
	})
	t.Run("ReplaceAll", func(t *testing.T) {
		t.Run("adds new jobs that does not exist yet", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(optMock.LogWriter)
			defer logWriter.AssertExpectations(t)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			jobADestination := job.ResourceURN("resource-A")
			jobAUpstreamName := []job.ResourceURN{"job-B"}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName)

			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
			jobB := job.NewJob(sampleTenant, specB, "", nil)

			incomingSpecs := []*job.Spec{specA, specB}

			existingJobs := []*job.Job{jobB}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingJobs, nil)

			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, nil).Once()
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobAUpstreamName, nil)

			jobRepo.On("Add", ctx, []*job.Job{jobA}).Return([]*job.Job{jobA}, nil)

			upstream, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static", taskName, false)

			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), []*job.Job{jobA}, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, logWriter)
			assert.NoError(t, err)
		})
		t.Run("updates modified existing jobs", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(optMock.LogWriter)
			defer logWriter.AssertExpectations(t)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			jobADestination := job.ResourceURN("resource-A")
			jobAUpstreamName := []job.ResourceURN{"job-B"}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName)

			incomingSpecs := []*job.Spec{specA}

			existingJobWindow, _ := models.NewWindow(jobVersion.Int(), "d", "0h", "24h")
			existingSpecA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, existingJobWindow, jobTask).Build()
			existingJobA := job.NewJob(sampleTenant, existingSpecA, jobADestination, jobAUpstreamName)
			existingSpecs := []*job.Job{existingJobA}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, nil).Once()
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobAUpstreamName, nil)

			jobRepo.On("Update", ctx, []*job.Job{jobA}).Return([]*job.Job{jobA}, nil)

			upstream, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static", taskName, false)

			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), []*job.Job{jobA}, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, logWriter)
			assert.NoError(t, err)
		})
		t.Run("deletes the removed jobs", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(optMock.LogWriter)
			defer logWriter.AssertExpectations(t)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			jobA := job.NewJob(sampleTenant, specA, "", nil)

			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
			jobB := job.NewJob(sampleTenant, specB, "", nil)

			incomingSpecs := []*job.Spec{specA}

			existingSpecs := []*job.Job{jobA, jobB}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			jobRepo.On("GetDownstreamFullNames", ctx, project.Name(), specB.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), specB.Name(), false).Return(nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, logWriter)
			assert.NoError(t, err)
		})
		t.Run("adds, updates, and deletes jobs in a request", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(optMock.LogWriter)
			defer logWriter.AssertExpectations(t)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
			incomingSpecs := []*job.Spec{specA, specB}

			existingJobWindow, _ := models.NewWindow(jobVersion.Int(), "d", "0h", "24h")
			existingSpecB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, existingJobWindow, jobTask).Build()
			existingJobB := job.NewJob(sampleTenant, existingSpecB, "", nil)
			existingSpecC := job.NewSpecBuilder(jobVersion, "job-C", "", jobSchedule, jobWindow, jobTask).Build()
			existingJobC := job.NewJob(sampleTenant, existingSpecC, "", nil)

			existingSpecs := []*job.Job{existingJobB, existingJobC}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			jobADestination := job.ResourceURN("resource-A")
			var jobBDestination job.ResourceURN
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, nil).Once()
			pluginService.On("GenerateDestination", ctx, detailedTenant, specB.Task()).Return(jobBDestination, service.ErrUpstreamModNotFound).Once()

			jobAUpstreamNames := []job.ResourceURN{"job-B"}
			var jobBUpstreamNames []job.ResourceURN
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobAUpstreamNames, nil)
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specB, true).Return(jobBUpstreamNames, service.ErrUpstreamModNotFound)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamNames)
			jobRepo.On("Add", ctx, []*job.Job{jobA}).Return([]*job.Job{jobA}, nil)

			jobB := job.NewJob(sampleTenant, specB, "", nil)
			jobRepo.On("Update", ctx, []*job.Job{jobB}).Return([]*job.Job{jobB}, nil)

			jobRepo.On("GetDownstreamFullNames", ctx, project.Name(), existingSpecC.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), existingSpecC.Name(), false).Return(nil)

			upstream, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static", taskName, false)
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), []*job.Job{jobA, jobB}, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, logWriter)
			assert.NoError(t, err)
		})
		t.Run("skips adding new invalid jobs", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(optMock.LogWriter)
			defer logWriter.AssertExpectations(t)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			incomingSpecs := []*job.Spec{specA}

			existingSpecC := job.NewSpecBuilder(jobVersion, "job-C", "", jobSchedule, jobWindow, jobTask).Build()
			existingJobC := job.NewJob(sampleTenant, existingSpecC, "", nil)
			existingSpecs := []*job.Job{existingJobC}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			var specADestination job.ResourceURN
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(specADestination, errors.New("internal error")).Once()

			jobRepo.On("GetDownstreamFullNames", ctx, project.Name(), existingSpecC.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), existingSpecC.Name(), false).Return(nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, logWriter)
			assert.ErrorContains(t, err, "internal error")
		})
		t.Run("skips invalid modified jobs", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(optMock.LogWriter)
			defer logWriter.AssertExpectations(t)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
			incomingSpecs := []*job.Spec{specB}

			existingJobWindow, _ := models.NewWindow(jobVersion.Int(), "d", "0h", "24h")
			existingSpecB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, existingJobWindow, jobTask).Build()
			existingJobB := job.NewJob(sampleTenant, existingSpecB, "", nil)
			existingSpecC := job.NewSpecBuilder(jobVersion, "job-C", "", jobSchedule, jobWindow, jobTask).Build()
			existingJobC := job.NewJob(sampleTenant, existingSpecC, "", nil)
			existingSpecs := []*job.Job{existingJobB, existingJobC}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			var jobBDestination job.ResourceURN
			pluginService.On("GenerateDestination", ctx, detailedTenant, specB.Task()).Return(jobBDestination, errors.New("internal error")).Once()

			jobRepo.On("GetDownstreamFullNames", ctx, project.Name(), existingSpecC.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), existingSpecC.Name(), false).Return(nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, logWriter)
			assert.ErrorContains(t, err, "internal error")
		})
		t.Run("skips to delete jobs with downstream", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			logWriter := new(optMock.LogWriter)
			defer logWriter.AssertExpectations(t)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			incomingSpecs := []*job.Spec{specA}

			existingSpecC := job.NewSpecBuilder(jobVersion, "job-C", "", jobSchedule, jobWindow, jobTask).Build()
			existingJobC := job.NewJob(sampleTenant, existingSpecC, "", nil)
			existingSpecD := job.NewSpecBuilder(jobVersion, "job-D", "", jobSchedule, jobWindow, jobTask).Build()
			existingJobD := job.NewJob(sampleTenant, existingSpecD, "", nil)
			existingSpecs := []*job.Job{existingJobC, existingJobD}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			jobADestination := job.ResourceURN("resource-A")
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, nil).Once()

			jobAUpstreamNames := []job.ResourceURN{"job-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobAUpstreamNames, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamNames)
			jobRepo.On("Add", ctx, []*job.Job{jobA}).Return([]*job.Job{jobA}, nil)

			jobRepo.On("GetDownstreamFullNames", ctx, project.Name(), existingSpecC.Name()).Return([]job.FullName{"sample-job-E"}, nil)
			jobRepo.On("GetDownstreamFullNames", ctx, project.Name(), existingSpecD.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), existingSpecD.Name(), false).Return(nil)

			upstream, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static", taskName, false)
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), []*job.Job{jobA}, mock.Anything).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			logWriter.On("Write", mock.Anything, mock.Anything).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs, logWriter)
			assert.ErrorContains(t, err, "job is being used by [sample-job-E]")
		})
	})

	t.Run("Refresh", func(t *testing.T) {
		t.Run("resolves and saves upstream for all existing jobs in the given tenant", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			jobA := job.NewJob(sampleTenant, specA, "resource-A", []job.ResourceURN{"resource-B"})
			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
			jobB := job.NewJob(sampleTenant, specB, "", nil)
			jobs := []*job.Job{jobA, jobB}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(jobs, nil)

			upstreamB, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static", taskName, false)
			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamB})
			upstreamC, _ := job.NewUpstreamResolved("job-C", "", "resource-C", sampleTenant, "static", taskName, false)
			jobBWithUpstream := job.NewWithUpstream(jobB, []*job.Upstream{upstreamC})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), []*job.Job{jobA, jobB}, mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			err := jobService.Refresh(ctx, project.Name(), nil, projectFilter, namespacesFilter)
			assert.NoError(t, err)
		})
		t.Run("resolves and saves upstream for all existing jobs in the given tenant", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			jobA := job.NewJob(sampleTenant, specA, "resource-A", []job.ResourceURN{"resource-B"})
			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
			jobB := job.NewJob(sampleTenant, specB, "", nil)
			jobs := []*job.Job{jobA, jobB}

			jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(jobs, nil)

			upstreamB, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static", taskName, false)
			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamB})
			upstreamC, _ := job.NewUpstreamResolved("job-C", "", "resource-C", sampleTenant, "static", taskName, false)
			jobBWithUpstream := job.NewWithUpstream(jobB, []*job.Upstream{upstreamC})
			upstreamResolver.On("BulkResolve", ctx, project.Name(), []*job.Job{jobA, jobB}, mock.Anything).Return([]*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			err := jobService.Refresh(ctx, project.Name(), nil, projectFilter, namespacesFilter)
			assert.NoError(t, err)
		})
	})

	t.Run("Get", func(t *testing.T) {
		t.Run("return error when repo get by job name error", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			jobName, _ := job.NameFrom("job-A")
			jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), jobName).Return(nil, errors.New("error when fetch job"))

			jobService := service.NewJobService(jobRepo, nil, nil, nil, nil)
			actual, err := jobService.Get(ctx, sampleTenant, jobName)
			assert.Error(t, err, "error when fetch job")
			assert.Nil(t, actual)
		})
		t.Run("return job when success fetch the job", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			jobA := job.NewJob(sampleTenant, specA, "table-A", []job.ResourceURN{"table-B"})
			jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specA.Name()).Return(jobA, nil)

			jobService := service.NewJobService(jobRepo, nil, nil, nil, nil)
			actual, err := jobService.Get(ctx, sampleTenant, specA.Name())
			assert.NoError(t, err, "error when fetch job")
			assert.NotNil(t, actual)
			assert.Equal(t, jobA, actual)
		})
	})

	t.Run("GetByFilter", func(t *testing.T) {
		t.Run("filter by resource destination", func(t *testing.T) {
			t.Run("return error when repo error", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				jobRepo.On("GetAllByResourceDestination", ctx, job.ResourceURN("example")).Return(nil, errors.New("error encountered"))

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil)
				actual, err := jobService.GetByFilter(ctx, filter.WithString(filter.ResourceDestination, "example"))
				assert.Error(t, err, "error encountered")
				assert.Nil(t, actual)
			})
			t.Run("return success", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
				jobA := job.NewJob(sampleTenant, specA, "table-A", []job.ResourceURN{"table-B"})
				jobRepo.On("GetAllByResourceDestination", ctx, job.ResourceURN("table-A")).Return([]*job.Job{jobA}, nil)

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil)
				actual, err := jobService.GetByFilter(ctx, filter.WithString(filter.ResourceDestination, "table-A"))
				assert.NoError(t, err)
				assert.NotNil(t, actual)
				assert.NotEmpty(t, actual)
				assert.Len(t, actual, 1)
			})
		})
		t.Run("filter by project name and job names", func(t *testing.T) {
			t.Run("return error when repo error", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				jobName, _ := job.NameFrom("job-A")
				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), jobName).Return(nil, errors.New("error encountered"))

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil)
				actual, err := jobService.GetByFilter(ctx,
					filter.WithString(filter.ProjectName, sampleTenant.ProjectName().String()),
					filter.WithStringArray(filter.JobNames, []string{jobName.String()}),
				)
				assert.Error(t, err, "error encountered")
				assert.Nil(t, actual)
			})
			t.Run("return success and some error when some of job is failed to retrieved", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
				jobA := job.NewJob(sampleTenant, specA, "table-A", []job.ResourceURN{"table-B"})
				specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specA.Name()).Return(jobA, nil)
				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specB.Name()).Return(nil, errors.New("error encountered"))

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil)
				actual, err := jobService.GetByFilter(ctx,
					filter.WithString(filter.ProjectName, sampleTenant.ProjectName().String()),
					filter.WithStringArray(filter.JobNames, []string{specA.Name().String(), specB.Name().String()}),
				)
				assert.Error(t, err)
				assert.NotNil(t, actual)
				assert.NotEmpty(t, actual)
				assert.Len(t, actual, 1)
			})
			t.Run("return success", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
				jobA := job.NewJob(sampleTenant, specA, "table-A", []job.ResourceURN{"table-B"})
				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specA.Name()).Return(jobA, nil)

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil)
				actual, err := jobService.GetByFilter(ctx,
					filter.WithString(filter.ProjectName, sampleTenant.ProjectName().String()),
					filter.WithStringArray(filter.JobNames, []string{specA.Name().String()}),
				)
				assert.NoError(t, err)
				assert.NotNil(t, actual)
				assert.NotEmpty(t, actual)
				assert.Len(t, actual, 1)
			})
		})
		t.Run("filter by project name and job name", func(t *testing.T) {
			t.Run("return error when repo error", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				jobName, _ := job.NameFrom("job-A")
				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), jobName).Return(nil, errors.New("error encountered"))

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil)
				actual, err := jobService.GetByFilter(ctx,
					filter.WithString(filter.ProjectName, sampleTenant.ProjectName().String()),
					filter.WithString(filter.JobName, jobName.String()),
				)
				assert.Error(t, err, "error encountered")
				assert.Nil(t, actual)
			})
			t.Run("return success", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
				jobA := job.NewJob(sampleTenant, specA, "table-A", []job.ResourceURN{"table-B"})
				jobRepo.On("GetByJobName", ctx, sampleTenant.ProjectName(), specA.Name()).Return(jobA, nil)

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil)
				actual, err := jobService.GetByFilter(ctx,
					filter.WithString(filter.ProjectName, sampleTenant.ProjectName().String()),
					filter.WithString(filter.JobName, specA.Name().String()),
				)
				assert.NoError(t, err)
				assert.NotNil(t, actual)
				assert.NotEmpty(t, actual)
				assert.Equal(t, []*job.Job{jobA}, actual)
			})
		})
		t.Run("filter by project name and namespace names", func(t *testing.T) {
			t.Run("return error when repo error", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return(nil, errors.New("error encountered"))

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil)
				actual, err := jobService.GetByFilter(ctx,
					filter.WithString(filter.ProjectName, sampleTenant.ProjectName().String()),
					filter.WithStringArray(filter.NamespaceNames, []string{sampleTenant.NamespaceName().String()}),
				)
				assert.Error(t, err, "error encountered")
				assert.Nil(t, actual)
			})
			t.Run("return error when namespace empty", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil)
				actual, err := jobService.GetByFilter(ctx,
					filter.WithString(filter.ProjectName, sampleTenant.ProjectName().String()),
					filter.WithStringArray(filter.NamespaceNames, []string{""}),
				)
				assert.Error(t, err)
				assert.Nil(t, actual)
			})
			t.Run("return success", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
				jobA := job.NewJob(sampleTenant, specA, "table-A", []job.ResourceURN{"table-B"})
				jobRepo.On("GetAllByTenant", ctx, sampleTenant).Return([]*job.Job{jobA}, nil)

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil)
				actual, err := jobService.GetByFilter(ctx,
					filter.WithString(filter.ProjectName, sampleTenant.ProjectName().String()),
					filter.WithStringArray(filter.NamespaceNames, []string{sampleTenant.NamespaceName().String()}),
				)
				assert.NoError(t, err)
				assert.NotNil(t, actual)
				assert.NotEmpty(t, actual)
				assert.Len(t, actual, 1)
			})
		})
		t.Run("filter by project name", func(t *testing.T) {
			t.Run("return error when repo error", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				jobRepo.On("GetAllByProjectName", ctx, sampleTenant.ProjectName()).Return(nil, errors.New("error encountered"))

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil)
				actual, err := jobService.GetByFilter(ctx,
					filter.WithString(filter.ProjectName, sampleTenant.ProjectName().String()),
				)
				assert.Error(t, err, "error encountered")
				assert.Nil(t, actual)
			})
			t.Run("return success", func(t *testing.T) {
				jobRepo := new(JobRepository)
				defer jobRepo.AssertExpectations(t)

				specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
				jobA := job.NewJob(sampleTenant, specA, "table-A", []job.ResourceURN{"table-B"})
				jobRepo.On("GetAllByProjectName", ctx, sampleTenant.ProjectName()).Return([]*job.Job{jobA}, nil)

				jobService := service.NewJobService(jobRepo, nil, nil, nil, nil)
				actual, err := jobService.GetByFilter(ctx,
					filter.WithString(filter.ProjectName, sampleTenant.ProjectName().String()),
				)
				assert.NoError(t, err)
				assert.NotNil(t, actual)
				assert.NotEmpty(t, actual)
				assert.Len(t, actual, 1)
			})
		})
		t.Run("return error when there's no filter", func(t *testing.T) {
			jobService := service.NewJobService(nil, nil, nil, nil, nil)
			actual, err := jobService.GetByFilter(ctx)
			assert.Error(t, err, "no filter matched")
			assert.Nil(t, actual)
		})
	})

	t.Run("GetTaskInfo", func(t *testing.T) {
		t.Run("return error when plugin could not retrieve info", func(t *testing.T) {
			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			pluginService.On("Info", ctx, jobTask).Return(nil, errors.New("error encountered"))

			jobService := service.NewJobService(nil, pluginService, nil, nil, nil)
			actual, err := jobService.GetTaskInfo(ctx, jobTask)
			assert.Error(t, err, "error encountered")
			assert.Nil(t, actual)
		})
		t.Run("return task with information included when success", func(t *testing.T) {
			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			pluginInfoResp := &models.PluginInfoResponse{
				Name:        "bq2bq",
				Description: "plugin desc",
				Image:       "odpf/bq2bq:latest",
			}
			pluginService.On("Info", ctx, jobTask).Return(pluginInfoResp, nil)

			expected := job.NewTaskBuilder(jobTask.Name(), jobTask.Config()).WithInfo(pluginInfoResp).Build()
			jobService := service.NewJobService(nil, pluginService, nil, nil, nil)
			actual, err := jobService.GetTaskInfo(ctx, jobTask)
			assert.NoError(t, err)
			assert.NotNil(t, actual)
			assert.Equal(t, expected, actual)
		})
	})

	t.Run("Validate", func(t *testing.T) {
		t.Run("returns error when get tenant details if failed", func(t *testing.T) {
			tenantDetailsGetter := new(TenantDetailsGetter)
			tenantDetailsGetter.On("GetDetails", ctx, mock.Anything).Return(nil, errors.New("get tenant details fail"))
			defer tenantDetailsGetter.AssertExpectations(t)

			jobService := service.NewJobService(nil, nil, nil, tenantDetailsGetter, nil)
			err := jobService.Validate(ctx, sampleTenant, []*job.Spec{}, nil)
			assert.Error(t, err)
			assert.Equal(t, "validate specs errors:\n get tenant details fail", err.Error())
		})
		t.Run("returns error when one of the specs is not valid", func(t *testing.T) {
			tenantDetailsGetter := new(TenantDetailsGetter)
			tenantDetailsGetter.On("GetDetails", ctx, mock.Anything).Return(detailedTenant, nil)
			defer tenantDetailsGetter.AssertExpectations(t)

			invalidJobSpec := job.NewSpecBuilder(jobVersion, "job-invalid", "", jobSchedule, jobWindow, jobTask).
				WithLabels(map[string]string{"a": ""}).
				Build()
			jobService := service.NewJobService(nil, nil, nil, tenantDetailsGetter, nil)
			err := jobService.Validate(ctx, sampleTenant, []*job.Spec{invalidJobSpec}, nil)
			assert.Error(t, err)
			assert.Equal(t, "validate specs errors:\n invalid argument for entity job: keys [a] are empty", err.Error())
		})
		t.Run("returns error when generate jobs failed", func(t *testing.T) {
			tenantDetailsGetter := new(TenantDetailsGetter)
			tenantDetailsGetter.On("GetDetails", ctx, mock.Anything).Return(detailedTenant, nil)
			defer tenantDetailsGetter.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specA}

			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(job.ResourceURN(""), errors.New("some error on generate destination"))
			jobService := service.NewJobService(nil, pluginService, nil, tenantDetailsGetter, nil)

			err := jobService.Validate(ctx, sampleTenant, specs, nil)
			assert.Error(t, err)
			assert.Equal(t, "validate specs errors:\n internal error for entity job: unable to add job-A: some error on generate destination", err.Error())
		})
		t.Run("returns error when get all by project name failed", func(t *testing.T) {
			tenantDetailsGetter := new(TenantDetailsGetter)
			tenantDetailsGetter.On("GetDetails", ctx, mock.Anything).Return(detailedTenant, nil)
			defer tenantDetailsGetter.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			repo := new(JobRepository)
			repo.On("GetAllByProjectName", ctx, sampleTenant.ProjectName()).Return(nil, errors.New("error on get all by project name"))
			defer repo.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specA}

			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(job.ResourceURN("example_destination"), nil)
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return([]job.ResourceURN{"example_upstream"}, nil)
			jobService := service.NewJobService(repo, pluginService, nil, tenantDetailsGetter, nil)

			err := jobService.Validate(ctx, sampleTenant, specs, nil)
			assert.Error(t, err)
			assert.Equal(t, "error on get all by project name", err.Error())
		})
		t.Run("returns error when fail build cyclic multi tree", func(t *testing.T) {
			tenantDetailsGetter := new(TenantDetailsGetter)
			tenantDetailsGetter.On("GetDetails", ctx, mock.Anything).Return(detailedTenant, nil)
			defer tenantDetailsGetter.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			repo := new(JobRepository)
			defer repo.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
			specC := job.NewSpecBuilder(jobVersion, "job-C", "", jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specA, specB, specC}

			repo.On("GetAllByProjectName", ctx, sampleTenant.ProjectName()).Return([]*job.Job{}, nil)

			pluginService.On("GenerateDestination", ctx, detailedTenant, jobTask).Return(job.ResourceURN("example_destination"), nil)
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, mock.Anything, true).Return([]job.ResourceURN{"example_upstream"}, nil)
			jobService := service.NewJobService(repo, pluginService, nil, tenantDetailsGetter, nil)

			err := jobService.Validate(ctx, sampleTenant, specs, nil)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "validate specs errors")
			assert.ErrorContains(t, err, "couldn't find any job with name job-A")
			assert.ErrorContains(t, err, "couldn't find any job with name job-B")
			assert.ErrorContains(t, err, "couldn't find any job with name job-C")
		})
		t.Run("returns error when there's a cyclic", func(t *testing.T) {
			tenantDetailsGetter := new(TenantDetailsGetter)
			tenantDetailsGetter.On("GetDetails", ctx, mock.Anything).Return(detailedTenant, nil)
			defer tenantDetailsGetter.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			repo := new(JobRepository)
			defer repo.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
			specC := job.NewSpecBuilder(jobVersion, "job-C", "", jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specA, specB, specC}

			jobA := job.NewJob(sampleTenant, specA, "table-A", []job.ResourceURN{"table-C"})
			jobB := job.NewJob(sampleTenant, specB, "table-B", []job.ResourceURN{"table-A"})
			jobC := job.NewJob(sampleTenant, specC, "table-C", []job.ResourceURN{"table-B"})
			repo.On("GetAllByProjectName", ctx, sampleTenant.ProjectName()).Return([]*job.Job{jobA, jobB, jobC}, nil)

			pluginService.On("GenerateDestination", ctx, detailedTenant, jobTask).Return(job.ResourceURN("example_destination"), nil)
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, mock.Anything, true).Return([]job.ResourceURN{"example_upstream"}, nil)
			jobService := service.NewJobService(repo, pluginService, nil, tenantDetailsGetter, nil)

			err := jobService.Validate(ctx, sampleTenant, specs, nil)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "a cycle dependency encountered in the tree:")
		})
		t.Run("returns no error when success", func(t *testing.T) {
			tenantDetailsGetter := new(TenantDetailsGetter)
			tenantDetailsGetter.On("GetDetails", ctx, mock.Anything).Return(detailedTenant, nil)
			defer tenantDetailsGetter.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			repo := new(JobRepository)
			defer repo.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
			specC := job.NewSpecBuilder(jobVersion, "job-C", "", jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specA, specB, specC}

			jobA := job.NewJob(sampleTenant, specA, "table-A", []job.ResourceURN{})
			jobB := job.NewJob(sampleTenant, specB, "table-B", []job.ResourceURN{"table-A"})
			jobC := job.NewJob(sampleTenant, specC, "table-C", []job.ResourceURN{"table-B"})
			repo.On("GetAllByProjectName", ctx, sampleTenant.ProjectName()).Return([]*job.Job{jobA, jobB, jobC}, nil)

			pluginService.On("GenerateDestination", ctx, detailedTenant, jobTask).Return(job.ResourceURN("example_destination"), nil)
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, mock.Anything, true).Return([]job.ResourceURN{"example_upstream"}, nil)
			jobService := service.NewJobService(repo, pluginService, nil, tenantDetailsGetter, nil)

			err := jobService.Validate(ctx, sampleTenant, specs, nil)
			assert.NoError(t, err)
		})
	})

	t.Run("GetUpstreamsToInspect", func(t *testing.T) {
		t.Run("should return upstream for an existing job", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			jobA := job.NewJob(sampleTenant, specA, "resource-A", []job.ResourceURN{"resource-B"})

			upstreamB, err := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "inferred", taskName, false)
			assert.NoError(t, err)

			jobRepo.On("GetUpstreams", ctx, project.Name(), jobA.Spec().Name()).Return([]*job.Upstream{upstreamB}, nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			result, err := jobService.GetUpstreamsToInspect(ctx, jobA, false)
			assert.NoError(t, err)
			assert.EqualValues(t, []*job.Upstream{upstreamB}, result)
		})
		t.Run("should return upstream for client's local job", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			jobA := job.NewJob(sampleTenant, specA, "resource-A", []job.ResourceURN{"resource-B"})

			upstreamB, err := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "inferred", taskName, false)
			assert.NoError(t, err)

			upstreamResolver.On("Resolve", ctx, jobA).Return([]*job.Upstream{upstreamB}, nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			result, err := jobService.GetUpstreamsToInspect(ctx, jobA, true)
			assert.NoError(t, err)
			assert.EqualValues(t, []*job.Upstream{upstreamB}, result)
		})
	})

	t.Run("GetJobBasicInfo", func(t *testing.T) {
		t.Run("should return job basic info and its logger for user given job spec", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := job.ResourceURN("resource-A")
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, nil)

			jobASources := []job.ResourceURN{"job-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobASources, nil)

			jobRepo.On("GetAllByResourceDestination", ctx, jobADestination).Return([]*job.Job{}, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobASources)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			result, logger := jobService.GetJobBasicInfo(ctx, sampleTenant, "", specA)
			assert.Nil(t, logger.Messages)
			assert.Equal(t, jobA, result)
		})
		t.Run("should return job basic info and its logger for existing job spec", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			jobADestination := job.ResourceURN("resource-A")
			jobASources := []job.ResourceURN{"job-B"}
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobASources)

			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(jobA, nil)
			jobRepo.On("GetAllByResourceDestination", ctx, jobADestination).Return([]*job.Job{}, nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			result, logger := jobService.GetJobBasicInfo(ctx, sampleTenant, specA.Name(), nil)
			assert.Nil(t, logger.Messages)
			assert.Equal(t, jobA, result)
		})
		t.Run("should return error if unable to get tenant details", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, errors.New("sample error"))

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			result, logger := jobService.GetJobBasicInfo(ctx, sampleTenant, "", specA)
			assert.Contains(t, logger.Messages[0].Message, "sample error")
			assert.Nil(t, result)
		})
		t.Run("should return error if unable to generate job based on spec", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := job.ResourceURN("resource-A")
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, nil).Once()

			jobAUpstreamName := []job.ResourceURN{"job-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobAUpstreamName, errors.New("sample error"))

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			result, logger := jobService.GetJobBasicInfo(ctx, sampleTenant, "", specA)
			assert.Contains(t, logger.Messages[0].Message, "sample error")
			assert.Nil(t, result)
		})
		t.Run("should return job information with warning and errors", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specASchedule, err := job.NewScheduleBuilder(startDate).WithCatchUp(true).Build()
			assert.NoError(t, err)

			invalidLabels := map[string]string{"": ""}
			specA := job.NewSpecBuilder(jobVersion, "job-A", "", specASchedule, jobWindow, jobTask).WithLabels(invalidLabels).Build()
			jobADestination := job.ResourceURN("resource-A")
			jobA := job.NewJob(sampleTenant, specA, jobADestination, nil)

			jobRepo.On("GetByJobName", ctx, project.Name(), specA.Name()).Return(jobA, nil)
			jobRepo.On("GetAllByResourceDestination", ctx, jobADestination).Return([]*job.Job{}, errors.New("sample-error"))

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			result, logger := jobService.GetJobBasicInfo(ctx, sampleTenant, specA.Name(), nil)
			assert.Contains(t, logger.Messages[0].Message, "no job sources detected")
			assert.Contains(t, logger.Messages[1].Message, "job validation failed")
			assert.Contains(t, logger.Messages[2].Message, "catchup is enabled")
			assert.Contains(t, logger.Messages[3].Message, "could not perform duplicate job destination check")
			assert.Equal(t, jobA, result)
		})
	})

	t.Run("GetDownstream", func(t *testing.T) {
		t.Run("should return downstream for client's local job", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			jobA := job.NewJob(sampleTenant, specA, "resource-A", nil)

			jobADownstream := []*dto.Downstream{
				{
					Name:          "job-B",
					ProjectName:   project.Name().String(),
					NamespaceName: namespace.Name().String(),
					TaskName:      taskName.String(),
				},
			}
			jobRepo.On("GetDownstreamByDestination", ctx, project.Name(), jobA.Destination()).Return(jobADownstream, nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			result, err := jobService.GetDownstream(ctx, jobA, true)
			assert.NoError(t, err)
			assert.Equal(t, jobADownstream, result)
		})
		t.Run("should return downstream of an existing job", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			upstreamResolver := new(UpstreamResolver)
			defer upstreamResolver.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			jobA := job.NewJob(sampleTenant, specA, "resource-A", nil)

			jobADownstream := []*dto.Downstream{
				{
					Name:          "job-B",
					ProjectName:   project.Name().String(),
					NamespaceName: namespace.Name().String(),
					TaskName:      taskName.String(),
				},
			}
			jobRepo.On("GetDownstreamByJobName", ctx, project.Name(), specA.Name()).Return(jobADownstream, nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter, nil)
			result, err := jobService.GetDownstream(ctx, jobA, false)
			assert.NoError(t, err)
			assert.Equal(t, jobADownstream, result)
		})
	})
}

// JobRepository is an autogenerated mock type for the JobRepository type
type JobRepository struct {
	mock.Mock
}

// Add provides a mock function with given fields: _a0, _a1
func (_m *JobRepository) Add(_a0 context.Context, _a1 []*job.Job) ([]*job.Job, error) {
	ret := _m.Called(_a0, _a1)

	var r0 []*job.Job
	if rf, ok := ret.Get(0).(func(context.Context, []*job.Job) []*job.Job); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Job)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, []*job.Job) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Delete provides a mock function with given fields: ctx, projectName, jobName, cleanHistory
func (_m *JobRepository) Delete(ctx context.Context, projectName tenant.ProjectName, jobName job.Name, cleanHistory bool) error {
	ret := _m.Called(ctx, projectName, jobName, cleanHistory)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, job.Name, bool) error); ok {
		r0 = rf(ctx, projectName, jobName, cleanHistory)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetAllByProjectName provides a mock function with given fields: ctx, projectName
func (_m *JobRepository) GetAllByProjectName(ctx context.Context, projectName tenant.ProjectName) ([]*job.Job, error) {
	ret := _m.Called(ctx, projectName)

	var r0 []*job.Job
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName) []*job.Job); ok {
		r0 = rf(ctx, projectName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Job)
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

// GetAllByTenant provides a mock function with given fields: ctx, jobTenant
func (_m *JobRepository) GetAllByTenant(ctx context.Context, jobTenant tenant.Tenant) ([]*job.Job, error) {
	ret := _m.Called(ctx, jobTenant)

	var r0 []*job.Job
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant) []*job.Job); ok {
		r0 = rf(ctx, jobTenant)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Job)
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

// GetDownstreamByDestination provides a mock function with given fields: ctx, projectName, destination
func (_m *JobRepository) GetDownstreamByDestination(ctx context.Context, projectName tenant.ProjectName, destination job.ResourceURN) ([]*dto.Downstream, error) {
	ret := _m.Called(ctx, projectName, destination)

	var r0 []*dto.Downstream
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, job.ResourceURN) []*dto.Downstream); ok {
		r0 = rf(ctx, projectName, destination)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*dto.Downstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, job.ResourceURN) error); ok {
		r1 = rf(ctx, projectName, destination)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetDownstreamByJobName provides a mock function with given fields: ctx, projectName, jobName
func (_m *JobRepository) GetDownstreamByJobName(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) ([]*dto.Downstream, error) {
	ret := _m.Called(ctx, projectName, jobName)

	var r0 []*dto.Downstream
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, job.Name) []*dto.Downstream); ok {
		r0 = rf(ctx, projectName, jobName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*dto.Downstream)
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

// GetDownstreamFullNames provides a mock function with given fields: _a0, _a1, _a2
func (_m *JobRepository) GetDownstreamFullNames(_a0 context.Context, _a1 tenant.ProjectName, _a2 job.Name) ([]job.FullName, error) {
	ret := _m.Called(_a0, _a1, _a2)

	var r0 []job.FullName
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, job.Name) []job.FullName); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]job.FullName)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, job.Name) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetJobNameWithInternalUpstreams provides a mock function with given fields: _a0, _a1, _a2
func (_m *JobRepository) GetJobNameWithInternalUpstreams(_a0 context.Context, _a1 tenant.ProjectName, _a2 []job.Name) (map[job.Name][]*job.Upstream, error) {
	ret := _m.Called(_a0, _a1, _a2)

	var r0 map[job.Name][]*job.Upstream
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, []job.Name) map[job.Name][]*job.Upstream); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[job.Name][]*job.Upstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, []job.Name) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetUpstreams provides a mock function with given fields: ctx, projectName, jobName
func (_m *JobRepository) GetUpstreams(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) ([]*job.Upstream, error) {
	ret := _m.Called(ctx, projectName, jobName)

	var r0 []*job.Upstream
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, job.Name) []*job.Upstream); ok {
		r0 = rf(ctx, projectName, jobName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Upstream)
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

// ReplaceUpstreams provides a mock function with given fields: _a0, _a1
func (_m *JobRepository) ReplaceUpstreams(_a0 context.Context, _a1 []*job.WithUpstream) error {
	ret := _m.Called(_a0, _a1)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, []*job.WithUpstream) error); ok {
		r0 = rf(_a0, _a1)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Update provides a mock function with given fields: _a0, _a1
func (_m *JobRepository) Update(_a0 context.Context, _a1 []*job.Job) ([]*job.Job, error) {
	ret := _m.Called(_a0, _a1)

	var r0 []*job.Job
	if rf, ok := ret.Get(0).(func(context.Context, []*job.Job) []*job.Job); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Job)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, []*job.Job) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// PluginService is an autogenerated mock type for the PluginService type
type PluginService struct {
	mock.Mock
}

// GenerateDestination provides a mock function with given fields: _a0, _a1, _a2
func (_m *PluginService) GenerateDestination(_a0 context.Context, _a1 *tenant.WithDetails, _a2 *job.Task) (job.ResourceURN, error) {
	ret := _m.Called(_a0, _a1, _a2)

	var r0 job.ResourceURN
	if rf, ok := ret.Get(0).(func(context.Context, *tenant.WithDetails, *job.Task) job.ResourceURN); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		r0 = ret.Get(0).(job.ResourceURN)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *tenant.WithDetails, *job.Task) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GenerateUpstreams provides a mock function with given fields: ctx, jobTenant, spec, dryRun
func (_m *PluginService) GenerateUpstreams(ctx context.Context, jobTenant *tenant.WithDetails, spec *job.Spec, dryRun bool) ([]job.ResourceURN, error) {
	ret := _m.Called(ctx, jobTenant, spec, dryRun)

	var r0 []job.ResourceURN
	if rf, ok := ret.Get(0).(func(context.Context, *tenant.WithDetails, *job.Spec, bool) []job.ResourceURN); ok {
		r0 = rf(ctx, jobTenant, spec, dryRun)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]job.ResourceURN)
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

// Info provides a mock function with given fields: _a0, _a1
func (_m *PluginService) Info(_a0 context.Context, _a1 *job.Task) (*models.PluginInfoResponse, error) {
	ret := _m.Called(_a0, _a1)

	var r0 *models.PluginInfoResponse
	if rf, ok := ret.Get(0).(func(context.Context, *job.Task) *models.PluginInfoResponse); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*models.PluginInfoResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *job.Task) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// UpstreamResolver is an autogenerated mock type for the UpstreamResolver type
type UpstreamResolver struct {
	mock.Mock
}

// BulkResolve provides a mock function with given fields: ctx, projectName, jobs, logWriter
func (_m *UpstreamResolver) BulkResolve(ctx context.Context, projectName tenant.ProjectName, jobs []*job.Job, logWriter writer.LogWriter) ([]*job.WithUpstream, error) {
	ret := _m.Called(ctx, projectName, jobs, logWriter)

	var r0 []*job.WithUpstream
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, []*job.Job, writer.LogWriter) []*job.WithUpstream); ok {
		r0 = rf(ctx, projectName, jobs, logWriter)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.WithUpstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, []*job.Job, writer.LogWriter) error); ok {
		r1 = rf(ctx, projectName, jobs, logWriter)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Resolve provides a mock function with given fields: ctx, subjectJob
func (_m *UpstreamResolver) Resolve(ctx context.Context, subjectJob *job.Job) ([]*job.Upstream, error) {
	ret := _m.Called(ctx, subjectJob)

	var r0 []*job.Upstream
	if rf, ok := ret.Get(0).(func(context.Context, *job.Job) []*job.Upstream); ok {
		r0 = rf(ctx, subjectJob)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Upstream)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *job.Job) error); ok {
		r1 = rf(ctx, subjectJob)
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
