package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
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

	t.Run("AddAndDeploy", func(t *testing.T) {
		t.Run("add jobs and return deployment ID", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			dependencyResolver := new(DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			deployManager := new(DeploymentManager)
			defer deployManager.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobSpecA, _ := job.NewJobSpec(sampleTenant, jobVersion, "job-A", "", "", nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			jobSpecs := []*job.JobSpec{jobSpecA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := "resource-A"
			pluginService.On("GenerateDestination", ctx, detailedTenant, jobSpecA.Task()).Return(jobADestination, nil)

			jobADependencies := []string{"job-B"}
			pluginService.On("GenerateDependencies", ctx, detailedTenant, jobSpecA, true).Return(jobADependencies, nil)

			jobA := job.NewJob(jobSpecA, jobADestination, jobADependencies)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, jobs).Return(jobs, nil, nil)

			dependency, _ := job.NewDependencyResolved("job-B", "", "resource-B", sampleTenant, "static")
			jobWithDependency := job.NewWithDependency(jobA, []*job.Dependency{dependency})
			dependencyResolver.On("Resolve", ctx, project.Name(), jobs).Return([]*job.WithDependency{jobWithDependency}, nil, nil)

			jobRepo.On("SaveDependency", ctx, []*job.WithDependency{jobWithDependency}).Return(nil)

			deployID := uuid.New()
			deployManager.On("Create", ctx, project.Name()).Return(deployID, nil)

			jobService := service.NewJobService(jobRepo, pluginService, dependencyResolver, tenantDetailsGetter, deployManager)
			result, jobErr, sysErr := jobService.AddAndDeploy(ctx, sampleTenant, jobSpecs)
			assert.Nil(t, jobErr)
			assert.Nil(t, sysErr)
			assert.Equal(t, deployID, result)
		})
		t.Run("skip invalid job, add the rest and return deployment ID", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			dependencyResolver := new(DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			deployManager := new(DeploymentManager)
			defer deployManager.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			invalidJobScheduleB := job.NewSchedule("invalid", "", "", false, false, nil)
			jobSpecA, _ := job.NewJobSpec(sampleTenant, jobVersion, "job-A", "", "", nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			jobSpecB, _ := job.NewJobSpec(sampleTenant, jobVersion, "job-B", "", "", nil, invalidJobScheduleB,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			jobSpecs := []*job.JobSpec{jobSpecB, jobSpecA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobADestination := "resource-A"
			pluginService.On("GenerateDestination", ctx, detailedTenant, jobSpecA.Task()).Return(jobADestination, nil).Once()

			jobADependencies := []string{"job-B"}
			pluginService.On("GenerateDependencies", ctx, detailedTenant, jobSpecA, true).Return(jobADependencies, nil)

			jobA := job.NewJob(jobSpecA, jobADestination, jobADependencies)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, jobs).Return(jobs, nil, nil)

			dependency, _ := job.NewDependencyResolved("job-B", "", "resource-B", sampleTenant, "static")
			jobWithDependency := job.NewWithDependency(jobA, []*job.Dependency{dependency})
			dependencyResolver.On("Resolve", ctx, project.Name(), jobs).Return([]*job.WithDependency{jobWithDependency}, nil, nil)

			jobRepo.On("SaveDependency", ctx, []*job.WithDependency{jobWithDependency}).Return(nil)

			deployID := uuid.New()
			deployManager.On("Create", ctx, project.Name()).Return(deployID, nil)

			jobService := service.NewJobService(jobRepo, pluginService, dependencyResolver, tenantDetailsGetter, deployManager)
			result, jobErr, sysErr := jobService.AddAndDeploy(ctx, sampleTenant, jobSpecs)
			assert.NotNil(t, jobErr)
			assert.Nil(t, sysErr)
			assert.Equal(t, deployID, result)
		})
		t.Run("return error if all jobs not pass validation", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			dependencyResolver := new(DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			deployManager := new(DeploymentManager)
			defer deployManager.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			invalidJobScheduleA := job.NewSchedule("invalid", "", "", false, false, nil)
			invalidJobScheduleB := job.NewSchedule("2022-11-01", "invalid", "", false, false, nil)
			jobSpecA, _ := job.NewJobSpec(sampleTenant, jobVersion, "job-A", "", "", nil, invalidJobScheduleA,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			jobSpecB, _ := job.NewJobSpec(sampleTenant, jobVersion, "job-B", "", "", nil, invalidJobScheduleB,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			jobSpecs := []*job.JobSpec{jobSpecB, jobSpecA}

			jobService := service.NewJobService(jobRepo, pluginService, dependencyResolver, tenantDetailsGetter, deployManager)
			deployID, jobErrors, err := jobService.AddAndDeploy(ctx, sampleTenant, jobSpecs)

			assert.NotNil(t, jobErrors)
			assert.NotNil(t, err)
			assert.Equal(t, uuid.Nil, deployID)
		})
		t.Run("return error if unable to get detailed tenant", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			dependencyResolver := new(DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			deployManager := new(DeploymentManager)
			defer deployManager.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobSpecA, _ := job.NewJobSpec(sampleTenant, jobVersion, "job-A", "", "", nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			jobSpecs := []*job.JobSpec{jobSpecA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(&tenant.WithDetails{}, errors.New("internal error"))

			jobService := service.NewJobService(jobRepo, pluginService, dependencyResolver, tenantDetailsGetter, deployManager)
			deployID, jobErrors, err := jobService.AddAndDeploy(ctx, sampleTenant, jobSpecs)

			assert.Nil(t, jobErrors)
			assert.NotNil(t, err)
			assert.Equal(t, uuid.Nil, deployID)
		})
		t.Run("skip job that has issue when generating destination and dependencies and return deployment ID", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			dependencyResolver := new(DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			deployManager := new(DeploymentManager)
			defer deployManager.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobSpecA, _ := job.NewJobSpec(sampleTenant, jobVersion, "job-A", "", "", nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			jobSpecB, _ := job.NewJobSpec(sampleTenant, jobVersion, "job-B", "", "", nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			jobSpecC, _ := job.NewJobSpec(sampleTenant, jobVersion, "job-C", "", "", nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
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
			jobRepo.On("Add", ctx, jobs).Return(jobs, nil, nil)

			dependency, _ := job.NewDependencyResolved("job-B", "", "resource-B", sampleTenant, "static")
			jobWithDependency := job.NewWithDependency(jobA, []*job.Dependency{dependency})
			dependencyResolver.On("Resolve", ctx, project.Name(), jobs).Return([]*job.WithDependency{jobWithDependency}, nil, nil)

			jobRepo.On("SaveDependency", ctx, []*job.WithDependency{jobWithDependency}).Return(nil)

			deployID := uuid.New()
			deployManager.On("Create", ctx, project.Name()).Return(deployID, nil)

			jobService := service.NewJobService(jobRepo, pluginService, dependencyResolver, tenantDetailsGetter, deployManager)
			result, jobErr, sysErr := jobService.AddAndDeploy(ctx, sampleTenant, jobSpecs)
			assert.NotNil(t, jobErr)
			assert.Nil(t, sysErr)
			assert.Equal(t, deployID, result)
		})
		t.Run("return error when all jobs failed to have destination and dependencies generated", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			dependencyResolver := new(DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			deployManager := new(DeploymentManager)
			defer deployManager.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobSpecA, _ := job.NewJobSpec(sampleTenant, jobVersion, "job-A", "", "", nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			jobSpecB, _ := job.NewJobSpec(sampleTenant, jobVersion, "job-B", "", "", nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			jobSpecs := []*job.JobSpec{jobSpecB, jobSpecA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			jobBDestination := "resource-B"
			pluginService.On("GenerateDestination", ctx, detailedTenant, jobSpecB.Task()).Return(jobBDestination, nil).Once()
			pluginService.On("GenerateDestination", ctx, detailedTenant, jobSpecA.Task()).Return("", errors.New("generate destination error")).Once()

			pluginService.On("GenerateDependencies", ctx, detailedTenant, jobSpecB, true).Return(nil, errors.New("generate dependencies error"))

			jobService := service.NewJobService(jobRepo, pluginService, dependencyResolver, tenantDetailsGetter, deployManager)
			result, jobErr, sysErr := jobService.AddAndDeploy(ctx, sampleTenant, jobSpecs)
			assert.NotNil(t, jobErr)
			assert.NotNil(t, sysErr)
			assert.Equal(t, uuid.Nil, result)
		})
		t.Run("should not skip nor return error if jobs does not have dependency mod and encounter issue on generate destination/dependency", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			dependencyResolver := new(DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			deployManager := new(DeploymentManager)
			defer deployManager.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobSpecA, _ := job.NewJobSpec(sampleTenant, jobVersion, "job-A", "", "", nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			jobSpecs := []*job.JobSpec{jobSpecA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			pluginService.On("GenerateDestination", ctx, detailedTenant, jobSpecA.Task()).Return("", service.ErrDependencyModNotFound).Once()
			pluginService.On("GenerateDependencies", ctx, detailedTenant, jobSpecA, true).Return(nil, service.ErrDependencyModNotFound)

			jobA := job.NewJob(jobSpecA, "", nil)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, jobs).Return(jobs, nil, nil)

			jobWithDependency := job.NewWithDependency(jobA, nil)
			dependencyResolver.On("Resolve", ctx, project.Name(), jobs).Return([]*job.WithDependency{jobWithDependency}, nil, nil)

			jobRepo.On("SaveDependency", ctx, []*job.WithDependency{jobWithDependency}).Return(nil)

			deployID := uuid.New()
			deployManager.On("Create", ctx, project.Name()).Return(deployID, nil)

			jobService := service.NewJobService(jobRepo, pluginService, dependencyResolver, tenantDetailsGetter, deployManager)
			result, jobErr, sysErr := jobService.AddAndDeploy(ctx, sampleTenant, jobSpecs)
			assert.Nil(t, jobErr)
			assert.Nil(t, sysErr)
			assert.Equal(t, deployID, result)
		})
		t.Run("should skip and not return error if one of the job is failed to be inserted to db", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			dependencyResolver := new(DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			deployManager := new(DeploymentManager)
			defer deployManager.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobSpecA, _ := job.NewJobSpec(sampleTenant, jobVersion, "job-A", "", "", nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			jobSpecB, _ := job.NewJobSpec(sampleTenant, jobVersion, "job-B", "", "", nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			jobSpecs := []*job.JobSpec{jobSpecA, jobSpecB}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			resourceA := "resource-A"
			pluginService.On("GenerateDestination", ctx, detailedTenant, jobSpecA.Task()).Return(resourceA, nil).Once()
			pluginService.On("GenerateDestination", ctx, detailedTenant, jobSpecB.Task()).Return("", service.ErrDependencyModNotFound).Once()

			jobSourcesA := []string{"resource-B"}
			pluginService.On("GenerateDependencies", ctx, detailedTenant, jobSpecA, true).Return(jobSourcesA, nil)
			pluginService.On("GenerateDependencies", ctx, detailedTenant, jobSpecB, true).Return(nil, service.ErrDependencyModNotFound)

			jobA := job.NewJob(jobSpecA, resourceA, jobSourcesA)
			jobB := job.NewJob(jobSpecB, "", nil)
			jobs := []*job.Job{jobA, jobB}
			savedJobs := []*job.Job{jobB}
			jobRepo.On("Add", ctx, jobs).Return(savedJobs, errors.New("unable to save job A"), nil)

			jobWithDependencyB := job.NewWithDependency(jobB, nil)
			dependencyResolver.On("Resolve", ctx, project.Name(), savedJobs).Return([]*job.WithDependency{jobWithDependencyB}, nil, nil)

			jobRepo.On("SaveDependency", ctx, mock.Anything).Return(nil)

			deployID := uuid.New()
			deployManager.On("Create", ctx, project.Name()).Return(deployID, nil)

			jobService := service.NewJobService(jobRepo, pluginService, dependencyResolver, tenantDetailsGetter, deployManager)
			result, jobErr, sysErr := jobService.AddAndDeploy(ctx, sampleTenant, jobSpecs)
			assert.NotNil(t, jobErr)
			assert.Nil(t, sysErr)
			assert.Equal(t, deployID, result)
		})
		t.Run("return error when all jobs failed to be inserted to db", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			dependencyResolver := new(DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			deployManager := new(DeploymentManager)
			defer deployManager.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobSpecA, _ := job.NewJobSpec(sampleTenant, jobVersion, "job-A", "", "", nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			jobSpecs := []*job.JobSpec{jobSpecA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			resourceA := "resource-A"
			pluginService.On("GenerateDestination", ctx, detailedTenant, jobSpecA.Task()).Return(resourceA, nil).Once()

			jobSourcesA := []string{"resource-B"}
			pluginService.On("GenerateDependencies", ctx, detailedTenant, jobSpecA, true).Return(jobSourcesA, nil)

			jobA := job.NewJob(jobSpecA, resourceA, jobSourcesA)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, jobs).Return([]*job.Job{}, errors.New("unable to save job A"), errors.New("all jobs failed"))

			jobService := service.NewJobService(jobRepo, pluginService, dependencyResolver, tenantDetailsGetter, deployManager)
			result, jobErr, sysErr := jobService.AddAndDeploy(ctx, sampleTenant, jobSpecs)
			assert.NotNil(t, jobErr)
			assert.NotNil(t, sysErr)
			assert.Equal(t, uuid.Nil, result)
		})
		t.Run("should not return error if there is dependency errors when resolving, without critical error", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			dependencyResolver := new(DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			deployManager := new(DeploymentManager)
			defer deployManager.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobSpecA, _ := job.NewJobSpec(sampleTenant, jobVersion, "job-A", "", "", nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			jobSpecs := []*job.JobSpec{jobSpecA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			resourceA := "resource-A"
			pluginService.On("GenerateDestination", ctx, detailedTenant, jobSpecA.Task()).Return(resourceA, nil).Once()

			jobSourcesA := []string{"resource-B"}
			pluginService.On("GenerateDependencies", ctx, detailedTenant, jobSpecA, true).Return(jobSourcesA, nil)

			jobA := job.NewJob(jobSpecA, resourceA, jobSourcesA)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, jobs).Return(jobs, nil, nil)

			jobWithDependencyA := job.NewWithDependency(jobA, nil)
			dependencyResolver.On("Resolve", ctx, project.Name(), jobs).Return([]*job.WithDependency{jobWithDependencyA}, errors.New("dependency error"), nil)

			jobRepo.On("SaveDependency", ctx, mock.Anything).Return(nil)

			deployID := uuid.New()
			deployManager.On("Create", ctx, project.Name()).Return(deployID, nil)

			jobService := service.NewJobService(jobRepo, pluginService, dependencyResolver, tenantDetailsGetter, deployManager)
			result, jobErr, sysErr := jobService.AddAndDeploy(ctx, sampleTenant, jobSpecs)
			assert.NotNil(t, jobErr)
			assert.Nil(t, sysErr)
			assert.Equal(t, deployID, result)
		})
		t.Run("should return error if there is dependency errors when resolving, with critical error", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			dependencyResolver := new(DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			deployManager := new(DeploymentManager)
			defer deployManager.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobSpecA, _ := job.NewJobSpec(sampleTenant, jobVersion, "job-A", "", "", nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			jobSpecs := []*job.JobSpec{jobSpecA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			resourceA := "resource-A"
			pluginService.On("GenerateDestination", ctx, detailedTenant, jobSpecA.Task()).Return(resourceA, nil).Once()

			jobSourcesA := []string{"resource-B"}
			pluginService.On("GenerateDependencies", ctx, detailedTenant, jobSpecA, true).Return(jobSourcesA, nil)

			jobA := job.NewJob(jobSpecA, resourceA, jobSourcesA)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, jobs).Return(jobs, nil, nil)

			jobWithDependencyA := job.NewWithDependency(jobA, nil)
			dependencyResolver.On("Resolve", ctx, project.Name(), jobs).Return([]*job.WithDependency{jobWithDependencyA}, errors.New("dependency error"), errors.New("internal error"))

			jobService := service.NewJobService(jobRepo, pluginService, dependencyResolver, tenantDetailsGetter, deployManager)
			result, jobErr, sysErr := jobService.AddAndDeploy(ctx, sampleTenant, jobSpecs)
			assert.NotNil(t, jobErr)
			assert.NotNil(t, sysErr)
			assert.Equal(t, uuid.Nil, result)
		})
		t.Run("should return error if failed to save dependency", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			dependencyResolver := new(DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			deployManager := new(DeploymentManager)
			defer deployManager.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobSpecA, _ := job.NewJobSpec(sampleTenant, jobVersion, "job-A", "", "", nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			jobSpecs := []*job.JobSpec{jobSpecA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			resourceA := "resource-A"
			pluginService.On("GenerateDestination", ctx, detailedTenant, jobSpecA.Task()).Return(resourceA, nil).Once()

			jobSourcesA := []string{"resource-B"}
			pluginService.On("GenerateDependencies", ctx, detailedTenant, jobSpecA, true).Return(jobSourcesA, nil)

			jobA := job.NewJob(jobSpecA, resourceA, jobSourcesA)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, jobs).Return(jobs, nil, nil)

			jobWithDependencyA := job.NewWithDependency(jobA, nil)
			dependencyResolver.On("Resolve", ctx, project.Name(), jobs).Return([]*job.WithDependency{jobWithDependencyA}, nil, nil)

			jobRepo.On("SaveDependency", ctx, mock.Anything).Return(errors.New("internal error"))

			jobService := service.NewJobService(jobRepo, pluginService, dependencyResolver, tenantDetailsGetter, deployManager)
			result, jobErr, sysErr := jobService.AddAndDeploy(ctx, sampleTenant, jobSpecs)
			assert.Nil(t, jobErr)
			assert.NotNil(t, sysErr)
			assert.Equal(t, uuid.Nil, result)
		})
		t.Run("should not return error if there is dependency errors when resolving, without critical error", func(t *testing.T) {
			jobRepo := new(JobRepository)
			defer jobRepo.AssertExpectations(t)

			pluginService := new(PluginService)
			defer pluginService.AssertExpectations(t)

			dependencyResolver := new(DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			deployManager := new(DeploymentManager)
			defer deployManager.AssertExpectations(t)

			tenantDetailsGetter := new(TenantDetailsGetter)
			defer tenantDetailsGetter.AssertExpectations(t)

			jobSpecA, _ := job.NewJobSpec(sampleTenant, jobVersion, "job-A", "", "", nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			jobSpecs := []*job.JobSpec{jobSpecA}

			tenantDetailsGetter.On("GetDetails", ctx, sampleTenant).Return(detailedTenant, nil)

			resourceA := "resource-A"
			pluginService.On("GenerateDestination", ctx, detailedTenant, jobSpecA.Task()).Return(resourceA, nil).Once()

			jobSourcesA := []string{"resource-B"}
			pluginService.On("GenerateDependencies", ctx, detailedTenant, jobSpecA, true).Return(jobSourcesA, nil)

			jobA := job.NewJob(jobSpecA, resourceA, jobSourcesA)
			jobs := []*job.Job{jobA}
			jobRepo.On("Add", ctx, jobs).Return(jobs, nil, nil)

			jobWithDependencyA := job.NewWithDependency(jobA, nil)
			dependencyResolver.On("Resolve", ctx, project.Name(), jobs).Return([]*job.WithDependency{jobWithDependencyA}, nil, nil)

			jobRepo.On("SaveDependency", ctx, mock.Anything).Return(nil)

			deployManager.On("Create", ctx, project.Name()).Return(uuid.Nil, errors.New("unable to trigger deployment"))

			jobService := service.NewJobService(jobRepo, pluginService, dependencyResolver, tenantDetailsGetter, deployManager)
			result, jobErr, sysErr := jobService.AddAndDeploy(ctx, sampleTenant, jobSpecs)
			assert.Nil(t, jobErr)
			assert.NotNil(t, sysErr)
			assert.Equal(t, uuid.Nil, result)
		})
	})
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

// GetJobNameWithDependencies provides a mock function with given fields: ctx, projectName, jobNames
func (_m *JobRepository) GetJobNameWithInternalDependencies(ctx context.Context, projectName tenant.ProjectName, jobNames []job.Name) (map[job.Name][]*job.Dependency, error) {
	ret := _m.Called(ctx, projectName, jobNames)

	var r0 map[job.Name][]*job.Dependency
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, []job.Name) map[job.Name][]*job.Dependency); ok {
		r0 = rf(ctx, projectName, jobNames)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[job.Name][]*job.Dependency)
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

// SaveDependency provides a mock function with given fields: ctx, jobsWithDependencies
func (_m *JobRepository) SaveDependency(ctx context.Context, jobsWithDependencies []*job.WithDependency) error {
	ret := _m.Called(ctx, jobsWithDependencies)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, []*job.WithDependency) error); ok {
		r0 = rf(ctx, jobsWithDependencies)
	} else {
		r0 = ret.Error(0)
	}

	return r0
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

// DependencyResolver is an autogenerated mock type for the DependencyResolver type
type DependencyResolver struct {
	mock.Mock
}

// Resolve provides a mock function with given fields: ctx, projectName, jobs
func (_m *DependencyResolver) Resolve(ctx context.Context, projectName tenant.ProjectName, jobs []*job.Job) ([]*job.WithDependency, error, error) {
	ret := _m.Called(ctx, projectName, jobs)

	var r0 []*job.WithDependency
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, []*job.Job) []*job.WithDependency); ok {
		r0 = rf(ctx, projectName, jobs)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.WithDependency)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, []*job.Job) error); ok {
		r1 = rf(ctx, projectName, jobs)
	} else {
		r1 = ret.Error(1)
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(context.Context, tenant.ProjectName, []*job.Job) error); ok {
		r2 = rf(ctx, projectName, jobs)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
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
