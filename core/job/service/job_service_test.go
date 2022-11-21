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

	jobVersion, err := job.VersionFrom(1)
	assert.NoError(t, err)
	startDate, err := job.ScheduleDateFrom("2022-10-01")
	assert.NoError(t, err)
	jobSchedule, err := job.NewScheduleBuilder(startDate).Build()
	assert.NoError(t, err)
	jobWindow, _ := models.NewWindow(jobVersion.Int(), "d", "24h", "24h")
	jobTaskConfig, err := job.NewConfig(map[string]string{"sample_task_key": "sample_value"})
	assert.NoError(t, err)
	jobTask := job.NewTask("bq2bq", jobTaskConfig)

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

			jobADestination, _ := job.ResourceURNFrom("resource-A")
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, nil)

			jobAUpstreamName := []job.ResourceURN{"job-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobAUpstreamName, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName)
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

			invalidAsset := job.NewAsset(map[string]string{"key": ""})
			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).
				WithAsset(invalidAsset).
				Build()
			specs := []*job.Spec{specA, specB}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination, _ := job.ResourceURNFrom("resource-A")
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, nil).Once()

			jobAUpstreamName := []job.ResourceURN{"job-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobAUpstreamName, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, jobs).Return(jobs, nil, nil)

			upstream, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static")
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("Resolve", ctx, project.Name(), jobs).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
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

			upstreamResolver.On("Resolve", ctx, mock.Anything, mock.Anything).Return(nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
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

			jobADestination, _ := job.ResourceURNFrom("resource-A")
			jobBDestination, _ := job.ResourceURNFrom("resource-B")
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

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specB, specA}

			jobRepo.On("Add", ctx, mock.Anything).Return(nil, nil)

			upstreamResolver.On("Resolve", ctx, mock.Anything, mock.Anything).Return(nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			var jobADestination job.ResourceURN
			jobBDestination, _ := job.ResourceURNFrom("resource-B")
			pluginService.On("GenerateDestination", ctx, detailedTenant, specB.Task()).Return(jobBDestination, nil).Once()
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, errors.New("generate destination error")).Once()

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

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specA, specB}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			resourceA, _ := job.ResourceURNFrom("resource-A")
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

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			upstreamResolver.On("Resolve", ctx, mock.Anything, mock.Anything).Return(nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			resourceA, _ := job.ResourceURNFrom("resource-A")
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(resourceA, nil).Once()

			jobSourcesA := []job.ResourceURN{"resource-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobSourcesA, nil)

			jobA := job.NewJob(sampleTenant, specA, resourceA, jobSourcesA)
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

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specs := []*job.Spec{specA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			resourceA, _ := job.ResourceURNFrom("resource-A")
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(resourceA, nil).Once()

			jobSourcesA := []job.ResourceURN{"resource-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobSourcesA, nil)

			jobA := job.NewJob(sampleTenant, specA, resourceA, jobSourcesA)
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

			jobADestination, _ := job.ResourceURNFrom("resource-A")
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, nil)

			jobAUpstreamName := []job.ResourceURN{"job-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobAUpstreamName, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName)
			jobs := []*job.Job{jobA}
			jobRepo.On("Update", ctx, jobs).Return(jobs, nil, nil)

			upstream, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static")
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("Resolve", ctx, project.Name(), jobs).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
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

			jobADestination, _ := job.ResourceURNFrom("resource-A")
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, nil).Once()

			jobAUpstreamName := []job.ResourceURN{"job-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobAUpstreamName, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName)
			jobs := []*job.Job{jobA}
			jobRepo.On("Update", ctx, jobs).Return(jobs, nil, nil)

			upstream, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static")
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("Resolve", ctx, project.Name(), jobs).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
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

			upstreamResolver.On("Resolve", ctx, mock.Anything, mock.Anything).Return(nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
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

			jobADestination, _ := job.ResourceURNFrom("resource-A")
			jobBDestination, _ := job.ResourceURNFrom("resource-B")
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

			upstream, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static")
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("Resolve", ctx, project.Name(), jobs).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
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

			upstreamResolver.On("Resolve", ctx, mock.Anything, mock.Anything).Return(nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			var jobADestination job.ResourceURN
			jobBDestination, _ := job.ResourceURNFrom("resource-B")
			pluginService.On("GenerateDestination", ctx, detailedTenant, specB.Task()).Return(jobBDestination, nil).Once()
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, errors.New("generate destination error")).Once()

			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specB, true).Return(nil, errors.New("generate upstream error"))

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
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
			upstreamResolver.On("Resolve", ctx, project.Name(), jobs).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
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

			resourceA, _ := job.ResourceURNFrom("resource-A")
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
			upstreamResolver.On("Resolve", ctx, project.Name(), savedJobs).Return([]*job.WithUpstream{jobWithUpstreamB}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
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

			upstreamResolver.On("Resolve", ctx, mock.Anything, mock.Anything).Return(nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(nil)

			resourceA, _ := job.ResourceURNFrom("resource-A")
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(resourceA, nil).Once()

			jobSourcesA := []job.ResourceURN{"resource-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobSourcesA, nil)

			jobA := job.NewJob(sampleTenant, specA, resourceA, jobSourcesA)
			jobs := []*job.Job{jobA}
			jobRepo.On("Update", ctx, jobs).Return([]*job.Job{}, errors.New("unable to update job A"), errors.New("all jobs failed"))

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
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

			resourceA, _ := job.ResourceURNFrom("resource-A")
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(resourceA, nil).Once()

			jobSourcesA := []job.ResourceURN{"resource-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobSourcesA, nil)

			jobA := job.NewJob(sampleTenant, specA, resourceA, jobSourcesA)
			jobs := []*job.Job{jobA}
			jobRepo.On("Update", ctx, jobs).Return(jobs, nil, nil)

			jobWithUpstreamA := job.NewWithUpstream(jobA, nil)
			upstreamResolver.On("Resolve", ctx, project.Name(), jobs).Return([]*job.WithUpstream{jobWithUpstreamA}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, mock.Anything).Return(errors.New("internal error"))

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
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

			jobService := service.NewJobService(jobRepo, nil, nil, nil)
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

			jobService := service.NewJobService(jobRepo, nil, nil, nil)
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

			jobService := service.NewJobService(jobRepo, nil, nil, nil)
			affectedDownstream, err := jobService.Delete(ctx, sampleTenant, specA.Name(), false, false)
			assert.Error(t, err)
			assert.Empty(t, affectedDownstream)
		})
		t.Run("returns error if unable to get downstream", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()

			jobRepo.On("GetDownstreamFullNames", ctx, project.Name(), specA.Name()).Return(nil, errors.New("internal error"))

			jobService := service.NewJobService(jobRepo, nil, nil, nil)
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

			jobService := service.NewJobService(jobRepo, nil, nil, nil)
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

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
			incomingSpecs := []*job.Spec{specA, specB}

			existingSpecs := []*job.Spec{specB}

			jobRepo.On("GetAllSpecsByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			jobADestination, _ := job.ResourceURNFrom("resource-A")
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, nil).Once()

			jobAUpstreamName := []job.ResourceURN{"job-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobAUpstreamName, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName)

			jobRepo.On("Add", ctx, []*job.Job{jobA}).Return([]*job.Job{jobA}, nil)

			upstream, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static")

			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("Resolve", ctx, project.Name(), []*job.Job{jobA}).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs)
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

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			incomingSpecs := []*job.Spec{specA}

			existingJobWindow, _ := models.NewWindow(jobVersion.Int(), "d", "0h", "24h")
			existingSpecA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, existingJobWindow, jobTask).Build()
			existingSpecs := []*job.Spec{existingSpecA}

			jobRepo.On("GetAllSpecsByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			jobADestination, _ := job.ResourceURNFrom("resource-A")
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, nil).Once()

			jobAUpstreamName := []job.ResourceURN{"job-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobAUpstreamName, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamName)

			jobRepo.On("Update", ctx, []*job.Job{jobA}).Return([]*job.Job{jobA}, nil)

			upstream, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static")

			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("Resolve", ctx, project.Name(), []*job.Job{jobA}).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs)
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

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
			incomingSpecs := []*job.Spec{specA}

			existingSpecs := []*job.Spec{specA, specB}

			jobRepo.On("GetAllSpecsByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			jobRepo.On("GetDownstreamFullNames", ctx, project.Name(), specB.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), specB.Name(), false).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs)
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

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
			incomingSpecs := []*job.Spec{specA, specB}

			existingJobWindow, _ := models.NewWindow(jobVersion.Int(), "d", "0h", "24h")
			existingSpecB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, existingJobWindow, jobTask).Build()
			existingSpecC := job.NewSpecBuilder(jobVersion, "job-C", "", jobSchedule, jobWindow, jobTask).Build()
			existingSpecs := []*job.Spec{existingSpecB, existingSpecC}

			jobRepo.On("GetAllSpecsByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			jobADestination, _ := job.ResourceURNFrom("resource-A")
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

			upstream, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static")
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("Resolve", ctx, project.Name(), []*job.Job{jobA, jobB}).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs)
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

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			incomingSpecs := []*job.Spec{specA}

			existingSpecC := job.NewSpecBuilder(jobVersion, "job-C", "", jobSchedule, jobWindow, jobTask).Build()
			existingSpecs := []*job.Spec{existingSpecC}

			jobRepo.On("GetAllSpecsByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			var specADestination job.ResourceURN
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(specADestination, errors.New("internal error")).Once()

			jobRepo.On("GetDownstreamFullNames", ctx, project.Name(), existingSpecC.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), existingSpecC.Name(), false).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs)
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

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, jobWindow, jobTask).Build()
			incomingSpecs := []*job.Spec{specB}

			existingJobWindow, _ := models.NewWindow(jobVersion.Int(), "d", "0h", "24h")
			existingSpecB := job.NewSpecBuilder(jobVersion, "job-B", "", jobSchedule, existingJobWindow, jobTask).Build()
			existingSpecC := job.NewSpecBuilder(jobVersion, "job-C", "", jobSchedule, jobWindow, jobTask).Build()
			existingSpecs := []*job.Spec{existingSpecB, existingSpecC}

			jobRepo.On("GetAllSpecsByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			var jobBDestination job.ResourceURN
			pluginService.On("GenerateDestination", ctx, detailedTenant, specB.Task()).Return(jobBDestination, errors.New("internal error")).Once()

			jobRepo.On("GetDownstreamFullNames", ctx, project.Name(), existingSpecC.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), existingSpecC.Name(), false).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs)
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

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			specA := job.NewSpecBuilder(jobVersion, "job-A", "", jobSchedule, jobWindow, jobTask).Build()
			incomingSpecs := []*job.Spec{specA}

			existingSpecC := job.NewSpecBuilder(jobVersion, "job-C", "", jobSchedule, jobWindow, jobTask).Build()
			existingSpecD := job.NewSpecBuilder(jobVersion, "job-D", "", jobSchedule, jobWindow, jobTask).Build()
			existingSpecs := []*job.Spec{existingSpecC, existingSpecD}

			jobRepo.On("GetAllSpecsByTenant", ctx, sampleTenant).Return(existingSpecs, nil)

			jobADestination, _ := job.ResourceURNFrom("resource-A")
			pluginService.On("GenerateDestination", ctx, detailedTenant, specA.Task()).Return(jobADestination, nil).Once()

			jobAUpstreamNames := []job.ResourceURN{"job-B"}
			pluginService.On("GenerateUpstreams", ctx, detailedTenant, specA, true).Return(jobAUpstreamNames, nil)

			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobAUpstreamNames)
			jobRepo.On("Add", ctx, []*job.Job{jobA}).Return([]*job.Job{jobA}, nil)

			jobRepo.On("GetDownstreamFullNames", ctx, project.Name(), existingSpecC.Name()).Return([]job.FullName{"sample-job-E"}, nil)
			jobRepo.On("GetDownstreamFullNames", ctx, project.Name(), existingSpecD.Name()).Return(nil, nil)
			jobRepo.On("Delete", ctx, project.Name(), existingSpecD.Name(), false).Return(nil)

			upstream, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static")
			jobWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstream})
			upstreamResolver.On("Resolve", ctx, project.Name(), []*job.Job{jobA}).Return([]*job.WithUpstream{jobWithUpstream}, nil, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobWithUpstream}).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
			err := jobService.ReplaceAll(ctx, sampleTenant, incomingSpecs)
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

			upstreamB, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static")
			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamB})
			upstreamC, _ := job.NewUpstreamResolved("job-C", "", "resource-C", sampleTenant, "static")
			jobBWithUpstream := job.NewWithUpstream(jobB, []*job.Upstream{upstreamC})
			upstreamResolver.On("Resolve", ctx, project.Name(), []*job.Job{jobA, jobB}).Return([]*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
			err := jobService.Refresh(ctx, sampleTenant)
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

			upstreamB, _ := job.NewUpstreamResolved("job-B", "", "resource-B", sampleTenant, "static")
			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamB})
			upstreamC, _ := job.NewUpstreamResolved("job-C", "", "resource-C", sampleTenant, "static")
			jobBWithUpstream := job.NewWithUpstream(jobB, []*job.Upstream{upstreamC})
			upstreamResolver.On("Resolve", ctx, project.Name(), []*job.Job{jobA, jobB}).Return([]*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}, nil)

			jobRepo.On("ReplaceUpstreams", ctx, []*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}).Return(nil)

			jobService := service.NewJobService(jobRepo, pluginService, upstreamResolver, tenantDetailsGetter)
			err := jobService.Refresh(ctx, sampleTenant)
			assert.NoError(t, err)
		})
	})

	t.Run("Get", func(t *testing.T) {
		// TODO: implement test cases
	})

	t.Run("GetAll", func(t *testing.T) {
		// TODO: implement test cases
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

// GetAllByProjectName provides a mock function with given fields: ctx, projectName
func (_m *JobRepository) GetAllByProjectName(ctx context.Context, projectName string) ([]*job.Spec, error) {
	ret := _m.Called(ctx, projectName)

	var r0 []*job.Spec
	if rf, ok := ret.Get(0).(func(context.Context, string) []*job.Spec); ok {
		r0 = rf(ctx, projectName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Spec)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, projectName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetByJobName provides a mock function with given fields: ctx, projectName, jobName
func (_m *JobRepository) GetByJobName(ctx context.Context, projectName string, jobName string) (*job.Spec, error) {
	ret := _m.Called(ctx, projectName, jobName)

	var r0 *job.Spec
	if rf, ok := ret.Get(0).(func(context.Context, string, string) *job.Spec); ok {
		r0 = rf(ctx, projectName, jobName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*job.Spec)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, string) error); ok {
		r1 = rf(ctx, projectName, jobName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetAllByResourceDestination provides a mock function with given fields: ctx, resourceDestination
func (_m *JobRepository) GetAllByResourceDestination(ctx context.Context, resourceDestination string) ([]*job.Spec, error) {
	ret := _m.Called(ctx, resourceDestination)

	var r0 []*job.Spec
	if rf, ok := ret.Get(0).(func(context.Context, string) []*job.Spec); ok {
		r0 = rf(ctx, resourceDestination)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Spec)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, resourceDestination)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetAllSpecsByTenant provides a mock function with given fields: ctx, jobTenant
func (_m *JobRepository) GetAllSpecsByTenant(ctx context.Context, jobTenant tenant.Tenant) ([]*job.Spec, error) {
	ret := _m.Called(ctx, jobTenant)

	var r0 []*job.Spec
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant) []*job.Spec); ok {
		r0 = rf(ctx, jobTenant)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Spec)
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
