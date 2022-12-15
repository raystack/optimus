//go:build !unit_test

package job_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/models"
	postgres "github.com/odpf/optimus/internal/store/postgres/job"
	tenantPostgres "github.com/odpf/optimus/internal/store/postgres/tenant"
	"github.com/odpf/optimus/tests/setup"
)

func TestPostgresJobRepository(t *testing.T) {
	ctx := context.Background()

	proj, err := tenant.NewProject("test-proj",
		map[string]string{
			"bucket":                     "gs://some_folder-2",
			tenant.ProjectSchedulerHost:  "host",
			tenant.ProjectStoragePathKey: "gs://location",
		})
	assert.NoError(t, err)
	namespace, err := tenant.NewNamespace("test-ns", proj.Name(),
		map[string]string{
			"bucket": "gs://ns_bucket",
		})
	assert.NoError(t, err)
	otherNamespace, err := tenant.NewNamespace("other-ns", proj.Name(),
		map[string]string{
			"bucket": "gs://ns_bucket",
		})
	assert.NoError(t, err)
	sampleTenant, err := tenant.NewTenant(proj.Name().String(), namespace.Name().String())
	assert.NoError(t, err)

	otherProj, err := tenant.NewProject("test-other-proj",
		map[string]string{
			"bucket":                     "gs://some_folder-3",
			tenant.ProjectSchedulerHost:  "host",
			tenant.ProjectStoragePathKey: "gs://location",
		})
	assert.NoError(t, err)

	dbSetup := func() *gorm.DB {
		dbConn := setup.TestDB()
		setup.TruncateTables(dbConn)

		projRepo := tenantPostgres.NewProjectRepository(dbConn)
		assert.NoError(t, projRepo.Save(ctx, proj))

		namespaceRepo := tenantPostgres.NewNamespaceRepository(dbConn)
		assert.NoError(t, namespaceRepo.Save(ctx, namespace))
		assert.NoError(t, namespaceRepo.Save(ctx, otherNamespace))

		return dbConn
	}

	jobVersion, err := job.VersionFrom(1)
	assert.NoError(t, err)
	jobOwner, err := job.OwnerFrom("dev_test")
	assert.NoError(t, err)
	jobDescription := "sample job"
	jobRetry := job.NewRetry(5, 0, false)
	startDate, err := job.ScheduleDateFrom("2022-10-01")
	assert.NoError(t, err)
	jobSchedule, err := job.NewScheduleBuilder(startDate).WithRetry(jobRetry).Build()
	assert.NoError(t, err)
	jobWindow, err := models.NewWindow(jobVersion.Int(), "d", "24h", "24h")
	assert.NoError(t, err)
	jobTaskConfig, err := job.NewConfig(map[string]string{"sample_task_key": "sample_value"})
	assert.NoError(t, err)
	taskName, err := job.TaskNameFrom("bq2bq")
	assert.NoError(t, err)
	jobTask := job.NewTaskBuilder(taskName, jobTaskConfig).Build()

	host := "sample-host"
	upstreamType := job.UpstreamTypeInferred

	t.Run("Add", func(t *testing.T) {
		t.Run("inserts job spec", func(t *testing.T) {
			db := dbSetup()

			jobLabels := map[string]string{
				"environment": "integration",
			}
			jobHookConfig, err := job.NewConfig(map[string]string{"sample_hook_key": "sample_value"})
			assert.NoError(t, err)
			jobHooks := []*job.Hook{job.NewHook("sample_hook", jobHookConfig)}
			jobAlertConfig, err := job.NewConfig(map[string]string{"sample_alert_key": "sample_value"})
			assert.NoError(t, err)
			alert, _ := job.NewAlertBuilder(job.SLAMissEvent, []string{"sample-channel"}).WithConfig(jobAlertConfig).Build()
			jobAlerts := []*job.AlertSpec{alert}
			upstreamName1 := job.SpecUpstreamNameFrom("job-upstream-1")
			upstreamName2 := job.SpecUpstreamNameFrom("job-upstream-2")
			jobUpstream, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName1, upstreamName2}).Build()
			jobAsset, err := job.NewAsset(map[string]string{"sample-asset": "value-asset"})
			assert.NoError(t, err)
			resourceRequestConfig := job.NewMetadataResourceConfig("250m", "128Mi")
			resourceLimitConfig := job.NewMetadataResourceConfig("250m", "128Mi")
			resourceMetadata := job.NewResourceMetadata(resourceRequestConfig, resourceLimitConfig)
			jobMetadata, _ := job.NewMetadataBuilder().
				WithResource(resourceMetadata).
				WithScheduler(map[string]string{"scheduler_config_key": "value"}).
				Build()

			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).
				WithDescription(jobDescription).
				WithLabels(jobLabels).
				WithHooks(jobHooks).
				WithAlerts(jobAlerts).
				WithSpecUpstream(jobUpstream).
				WithAsset(jobAsset).
				WithMetadata(jobMetadata).
				Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_a", []job.ResourceURN{"resource-3"})

			jobSpecB := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, jobWindow, jobTask).
				WithDescription(jobDescription).
				WithLabels(jobLabels).
				WithHooks(jobHooks).
				WithAlerts(jobAlerts).
				WithAsset(jobAsset).
				WithMetadata(jobMetadata).
				Build()
			jobB := job.NewJob(sampleTenant, jobSpecB, "dev.resource.sample_b", nil)

			jobs := []*job.Job{jobA, jobB}

			jobRepo := postgres.NewJobRepository(db)
			addedJobs, err := jobRepo.Add(ctx, jobs)
			assert.NoError(t, err)
			assert.EqualValues(t, jobs, addedJobs)
		})
		t.Run("inserts job spec with optional fields empty", func(t *testing.T) {
			db := dbSetup()

			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_a", []job.ResourceURN{"resource-3"})

			jobs := []*job.Job{jobA}

			jobRepo := postgres.NewJobRepository(db)
			addedJobs, err := jobRepo.Add(ctx, jobs)
			assert.NoError(t, err)
			assert.EqualValues(t, jobs, addedJobs)
		})
		t.Run("skip job and return job error if job already exist", func(t *testing.T) {
			db := dbSetup()

			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_a", []job.ResourceURN{"resource-3"})

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA})
			assert.NoError(t, err)

			jobSpecB := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobB := job.NewJob(sampleTenant, jobSpecB, "dev.resource.sample_b", []job.ResourceURN{"resource-3"})

			addedJobs, err := jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.ErrorContains(t, err, "already exists")
			assert.EqualValues(t, []*job.Job{jobB}, addedJobs)
		})
		t.Run("return error if all jobs are failed to be saved", func(t *testing.T) {
			db := dbSetup()

			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_a", []job.ResourceURN{"resource-3"})

			jobSpecB := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobB := job.NewJob(sampleTenant, jobSpecB, "dev.resource.sample_b", []job.ResourceURN{"resource-3"})

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.NoError(t, err)

			addedJobs, err := jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.ErrorContains(t, err, "already exists")
			assert.Nil(t, addedJobs)
		})
		t.Run("update job spec if the job is already exist but soft deleted", func(t *testing.T) {
			db := dbSetup()

			jobLabels := map[string]string{
				"environment": "integration",
			}
			jobHookConfig, err := job.NewConfig(map[string]string{"sample_hook_key": "sample_value"})
			assert.NoError(t, err)
			jobHooks := []*job.Hook{job.NewHook("sample_hook", jobHookConfig)}
			jobAlertConfig, err := job.NewConfig(map[string]string{"sample_alert_key": "sample_value"})
			assert.NoError(t, err)
			alert, _ := job.NewAlertBuilder(job.SLAMissEvent, []string{"sample-channel"}).WithConfig(jobAlertConfig).Build()
			jobAlerts := []*job.AlertSpec{alert}
			upstreamName1 := job.SpecUpstreamNameFrom("job-upstream-1")
			upstreamName2 := job.SpecUpstreamNameFrom("job-upstream-2")
			jobUpstream, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName1, upstreamName2}).Build()
			jobAsset, err := job.NewAsset(map[string]string{"sample-asset": "value-asset"})
			assert.NoError(t, err)
			resourceRequestConfig := job.NewMetadataResourceConfig("250m", "128Mi")
			resourceLimitConfig := job.NewMetadataResourceConfig("250m", "128Mi")
			resourceMetadata := job.NewResourceMetadata(resourceRequestConfig, resourceLimitConfig)
			jobMetadata, _ := job.NewMetadataBuilder().
				WithResource(resourceMetadata).
				WithScheduler(map[string]string{"scheduler_config_key": "value"}).
				Build()

			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).
				WithDescription(jobDescription).
				WithLabels(jobLabels).
				WithHooks(jobHooks).
				WithAlerts(jobAlerts).
				WithSpecUpstream(jobUpstream).
				WithAsset(jobAsset).
				WithMetadata(jobMetadata).
				Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_a", []job.ResourceURN{"resource-3"})

			jobSpecB := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, jobWindow, jobTask).
				WithDescription(jobDescription).
				WithLabels(jobLabels).
				WithHooks(jobHooks).
				WithAlerts(jobAlerts).
				WithAsset(jobAsset).
				WithMetadata(jobMetadata).
				Build()
			jobB := job.NewJob(sampleTenant, jobSpecB, "dev.resource.sample_b", nil)

			jobs := []*job.Job{jobA, jobB}

			jobRepo := postgres.NewJobRepository(db)
			addedJobs, err := jobRepo.Add(ctx, jobs)
			assert.NoError(t, err)
			assert.EqualValues(t, jobs, addedJobs)

			err = jobRepo.Delete(ctx, proj.Name(), jobA.Spec().Name(), false)
			assert.NoError(t, err)

			addedJobs, err = jobRepo.Add(ctx, []*job.Job{jobA})
			assert.NoError(t, err)
			assert.EqualValues(t, []*job.Job{jobA}, addedJobs)
		})
		t.Run("avoid re-inserting job if it is soft deleted in other namespace", func(t *testing.T) {
			db := dbSetup()

			jobLabels := map[string]string{
				"environment": "integration",
			}
			jobHookConfig, err := job.NewConfig(map[string]string{"sample_hook_key": "sample_value"})
			assert.NoError(t, err)
			jobHooks := []*job.Hook{job.NewHook("sample_hook", jobHookConfig)}
			jobAlertConfig, err := job.NewConfig(map[string]string{"sample_alert_key": "sample_value"})
			assert.NoError(t, err)
			alert, _ := job.NewAlertBuilder(job.SLAMissEvent, []string{"sample-channel"}).WithConfig(jobAlertConfig).Build()
			jobAlerts := []*job.AlertSpec{alert}
			upstreamName1 := job.SpecUpstreamNameFrom("job-upstream-1")
			upstreamName2 := job.SpecUpstreamNameFrom("job-upstream-2")
			jobUpstream, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName1, upstreamName2}).Build()
			jobAsset, err := job.NewAsset(map[string]string{"sample-asset": "value-asset"})
			assert.NoError(t, err)
			resourceRequestConfig := job.NewMetadataResourceConfig("250m", "128Mi")
			resourceLimitConfig := job.NewMetadataResourceConfig("250m", "128Mi")
			resourceMetadata := job.NewResourceMetadata(resourceRequestConfig, resourceLimitConfig)
			jobMetadata, _ := job.NewMetadataBuilder().
				WithResource(resourceMetadata).
				WithScheduler(map[string]string{"scheduler_config_key": "value"}).
				Build()

			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).
				WithDescription(jobDescription).
				WithLabels(jobLabels).
				WithHooks(jobHooks).
				WithAlerts(jobAlerts).
				WithSpecUpstream(jobUpstream).
				WithAsset(jobAsset).
				WithMetadata(jobMetadata).
				Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_a", []job.ResourceURN{"resource-3"})

			jobs := []*job.Job{jobA}

			jobRepo := postgres.NewJobRepository(db)
			addedJobs, err := jobRepo.Add(ctx, jobs)
			assert.NoError(t, err)
			assert.EqualValues(t, jobs, addedJobs)

			err = jobRepo.Delete(ctx, proj.Name(), jobA.Spec().Name(), false)
			assert.NoError(t, err)

			otherTenant, err := tenant.NewTenant(proj.Name().String(), otherNamespace.Name().String())
			assert.NoError(t, err)

			jobAToReAdd := job.NewJob(otherTenant, jobSpecA, "dev.resource.sample_a", []job.ResourceURN{"resource-3"})
			addedJobs, err = jobRepo.Add(ctx, []*job.Job{jobAToReAdd})
			assert.ErrorContains(t, err, "already exists and soft deleted in namespace test-ns")
			assert.Nil(t, addedJobs)
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Run("updates job spec", func(t *testing.T) {
			db := dbSetup()

			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_a", []job.ResourceURN{"resource-3"})

			jobSpecB := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, jobWindow, jobTask).Build()
			jobB := job.NewJob(sampleTenant, jobSpecB, "dev.resource.sample_b", nil)

			jobs := []*job.Job{jobA, jobB}

			jobRepo := postgres.NewJobRepository(db)
			addedJobs, err := jobRepo.Add(ctx, jobs)
			assert.NoError(t, err)
			assert.EqualValues(t, jobs, addedJobs)

			jobSpecAToUpdate := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).
				WithDescription(jobDescription).
				Build()
			jobAToUpdate := job.NewJob(sampleTenant, jobSpecAToUpdate, "dev.resource.sample_a", []job.ResourceURN{"resource-3"})
			jobBToUpdate := job.NewJob(sampleTenant, jobSpecB, "dev.resource.sample_b", []job.ResourceURN{"resource-4"})
			jobsToUpdate := []*job.Job{jobAToUpdate, jobBToUpdate}

			updatedJobs, err := jobRepo.Update(ctx, jobsToUpdate)
			assert.NoError(t, err)
			assert.EqualValues(t, jobsToUpdate, updatedJobs)
		})
		t.Run("skip job and return job error if job not exist yet", func(t *testing.T) {
			db := dbSetup()

			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_a", []job.ResourceURN{"resource-3"})

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA})
			assert.NoError(t, err)

			jobSpecAToUpdate := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).
				WithDescription(jobDescription).
				Build()
			jobAToUpdate := job.NewJob(sampleTenant, jobSpecAToUpdate, "dev.resource.sample_a", []job.ResourceURN{"resource-3"})

			jobSpecB := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, jobWindow, jobTask).Build()
			jobBToUpdate := job.NewJob(sampleTenant, jobSpecB, "dev.resource.sample_b", []job.ResourceURN{"resource-4"})
			jobsToUpdate := []*job.Job{jobAToUpdate, jobBToUpdate}

			updatedJobs, err := jobRepo.Update(ctx, jobsToUpdate)
			assert.ErrorContains(t, err, "not exists yet")
			assert.EqualValues(t, []*job.Job{jobAToUpdate}, updatedJobs)
		})
		t.Run("return error if all jobs are failed to be updated", func(t *testing.T) {
			db := dbSetup()

			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_a", []job.ResourceURN{"resource-3"})

			jobSpecB := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobB := job.NewJob(sampleTenant, jobSpecB, "dev.resource.sample_b", []job.ResourceURN{"resource-3"})

			jobRepo := postgres.NewJobRepository(db)
			addedJobs, err := jobRepo.Update(ctx, []*job.Job{jobA, jobB})
			assert.Error(t, err)
			assert.Nil(t, addedJobs)
		})
		t.Run("should not update job if it has been soft deleted", func(t *testing.T) {
			db := dbSetup()

			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_a", []job.ResourceURN{"resource-3"})

			jobs := []*job.Job{jobA}

			jobRepo := postgres.NewJobRepository(db)
			addedJobs, err := jobRepo.Add(ctx, jobs)
			assert.NoError(t, err)
			assert.EqualValues(t, jobs, addedJobs)

			err = jobRepo.Delete(ctx, proj.Name(), jobSpecA.Name(), false)
			assert.NoError(t, err)

			updatedJobs, err := jobRepo.Update(ctx, []*job.Job{jobA})
			assert.ErrorContains(t, err, "update is not allowed as job sample-job-A has been soft deleted")
			assert.Nil(t, updatedJobs)

			otherTenant, err := tenant.NewTenant(proj.Name().String(), otherNamespace.Name().String())
			assert.NoError(t, err)
			jobToUpdate := job.NewJob(otherTenant, jobSpecA, "", nil)
			updatedJobs, err = jobRepo.Update(ctx, []*job.Job{jobToUpdate})
			assert.ErrorContains(t, err, "already exists and soft deleted in namespace test-ns")
			assert.Nil(t, updatedJobs)
		})
		t.Run("should not update job if it is owned by different namespace", func(t *testing.T) {
			db := dbSetup()

			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_a", []job.ResourceURN{"resource-3"})

			jobs := []*job.Job{jobA}

			jobRepo := postgres.NewJobRepository(db)
			addedJobs, err := jobRepo.Add(ctx, jobs)
			assert.NoError(t, err)
			assert.EqualValues(t, jobs, addedJobs)

			otherTenant, err := tenant.NewTenant(proj.Name().String(), otherNamespace.Name().String())
			assert.NoError(t, err)
			jobAToUpdate := job.NewJob(otherTenant, jobSpecA, "dev.resource.sample_a", []job.ResourceURN{"resource-3"})

			updatedJobs, err := jobRepo.Update(ctx, []*job.Job{jobAToUpdate})
			assert.ErrorContains(t, err, "job sample-job-A already exists in namespace test-ns")
			assert.Nil(t, updatedJobs)
		})
	})

	t.Run("ResolveUpstreams", func(t *testing.T) {
		t.Run("returns job with inferred upstreams", func(t *testing.T) {
			db := dbSetup()

			tenantDetails, err := tenant.NewTenantDetails(proj, namespace)
			assert.NoError(t, err)

			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_a", []job.ResourceURN{"dev.resource.sample_b"})

			jobSpecB := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobB := job.NewJob(sampleTenant, jobSpecB, "dev.resource.sample_b", nil)

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.NoError(t, err)

			expectedUpstream := job.NewUpstreamResolved(jobSpecB.Name(), "", jobB.Destination(), tenantDetails.ToTenant(), "inferred", taskName, false)

			upstreams, err := jobRepo.ResolveUpstreams(ctx, proj.Name(), []job.Name{jobSpecA.Name()})
			assert.NoError(t, err)
			assert.Equal(t, expectedUpstream, upstreams[jobSpecA.Name()][0])
		})
		t.Run("returns job with static upstreams", func(t *testing.T) {
			db := dbSetup()

			tenantDetails, err := tenant.NewTenantDetails(proj, namespace)
			assert.NoError(t, err)

			upstreamName := job.SpecUpstreamNameFrom("sample-job-B")
			jobAUpstream, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName}).Build()
			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).
				WithDescription(jobDescription).
				WithSpecUpstream(jobAUpstream).
				Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_a", nil)

			jobSpecB := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobB := job.NewJob(sampleTenant, jobSpecB, "dev.resource.sample_b", nil)

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.NoError(t, err)

			expectedUpstream := job.NewUpstreamResolved(jobSpecB.Name(), "", jobB.Destination(), tenantDetails.ToTenant(), "static", taskName, false)

			upstreams, err := jobRepo.ResolveUpstreams(ctx, proj.Name(), []job.Name{jobSpecA.Name()})
			assert.NoError(t, err)
			assert.Equal(t, expectedUpstream, upstreams[jobSpecA.Name()][0])
		})
		t.Run("returns job with static and inferred upstreams", func(t *testing.T) {
			db := dbSetup()

			tenantDetails, err := tenant.NewTenantDetails(proj, namespace)
			assert.NoError(t, err)

			upstreamName := job.SpecUpstreamNameFrom("test-proj/sample-job-B")
			jobAUpstream, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName}).Build()
			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).
				WithDescription(jobDescription).
				WithSpecUpstream(jobAUpstream).
				Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_a", []job.ResourceURN{"dev.resource.sample_c"})

			jobSpecB := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobB := job.NewJob(sampleTenant, jobSpecB, "dev.resource.sample_b", nil)

			jobSpecC := job.NewSpecBuilder(jobVersion, "sample-job-C", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobC := job.NewJob(sampleTenant, jobSpecC, "dev.resource.sample_c", nil)

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB, jobC})
			assert.NoError(t, err)

			upstreamB := job.NewUpstreamResolved(jobSpecB.Name(), "", jobB.Destination(), tenantDetails.ToTenant(), "static", taskName, false)
			upstreamC := job.NewUpstreamResolved(jobSpecC.Name(), "", jobC.Destination(), tenantDetails.ToTenant(), "inferred", taskName, false)

			expectedUpstreams := []*job.Upstream{
				upstreamB,
				upstreamC,
			}

			upstreams, err := jobRepo.ResolveUpstreams(ctx, proj.Name(), []job.Name{jobSpecA.Name()})
			assert.NoError(t, err)
			assert.EqualValues(t, expectedUpstreams, upstreams[jobSpecA.Name()])
		})
		t.Run("returns job with external project static and inferred upstreams", func(t *testing.T) {
			db := dbSetup()

			otherTenant, err := tenant.NewTenant(otherProj.Name().String(), otherNamespace.Name().String())
			assert.NoError(t, err)

			upstreamDName := job.SpecUpstreamNameFrom("test-other-proj/sample-job-D")

			upstreamBName := job.SpecUpstreamNameFrom("test-proj/sample-job-B")
			jobAUpstream, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamBName, upstreamDName}).Build()
			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).
				WithDescription(jobDescription).
				WithSpecUpstream(jobAUpstream).
				Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_a", []job.ResourceURN{"dev.resource.sample_c", "dev.resource.sample_e"})

			// internal project, same server
			jobSpecB := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobB := job.NewJob(sampleTenant, jobSpecB, "dev.resource.sample_b", nil)
			jobSpecC := job.NewSpecBuilder(jobVersion, "sample-job-C", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobC := job.NewJob(sampleTenant, jobSpecC, "dev.resource.sample_c", nil)

			// external project, same server
			jobSpecD := job.NewSpecBuilder(jobVersion, "sample-job-D", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobD := job.NewJob(otherTenant, jobSpecD, "dev.resource.sample_d", nil)
			jobSpecE := job.NewSpecBuilder(jobVersion, "sample-job-E", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobE := job.NewJob(otherTenant, jobSpecE, "dev.resource.sample_e", nil)

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB, jobC, jobD, jobE})
			assert.NoError(t, err)

			upstreamB := job.NewUpstreamResolved(jobSpecB.Name(), "", jobB.Destination(), sampleTenant, "static", taskName, false)
			upstreamC := job.NewUpstreamResolved(jobSpecC.Name(), "", jobC.Destination(), sampleTenant, "inferred", taskName, false)
			upstreamD := job.NewUpstreamResolved(jobSpecD.Name(), "", jobD.Destination(), otherTenant, "static", taskName, false)
			upstreamE := job.NewUpstreamResolved(jobSpecE.Name(), "", jobE.Destination(), otherTenant, "inferred", taskName, false)

			expectedUpstreams := []*job.Upstream{
				upstreamB,
				upstreamD,
				upstreamC,
				upstreamE,
			}

			upstreams, err := jobRepo.ResolveUpstreams(ctx, proj.Name(), []job.Name{jobSpecA.Name()})
			assert.NoError(t, err)
			assert.EqualValues(t, expectedUpstreams, upstreams[jobSpecA.Name()])
		})
	})

	t.Run("ReplaceUpstreams", func(t *testing.T) {
		jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
		jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_a", []job.ResourceURN{"dev.resource.sample_c"})

		jobSpecB := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
		jobB := job.NewJob(sampleTenant, jobSpecB, "dev.resource.sample_b", nil)

		jobSpecC := job.NewSpecBuilder(jobVersion, "sample-job-C", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
		jobC := job.NewJob(sampleTenant, jobSpecC, "dev.resource.sample_c", nil)

		t.Run("inserts job upstreams", func(t *testing.T) {
			db := dbSetup()

			upstreamB := job.NewUpstreamResolved("jobB", host, "resource-B", sampleTenant, upstreamType, taskName, false)
			upstreamC := job.NewUpstreamResolved("jobC", host, "resource-C", sampleTenant, upstreamType, taskName, false)
			upstreams := []*job.Upstream{upstreamB, upstreamC}
			jobWithUpstream := job.NewWithUpstream(jobA, upstreams)

			jobUpstreamRepo := postgres.NewJobRepository(db)
			_, err := jobUpstreamRepo.Add(ctx, []*job.Job{jobA, jobB, jobC})
			assert.NoError(t, err)

			assert.Nil(t, jobUpstreamRepo.ReplaceUpstreams(ctx, []*job.WithUpstream{jobWithUpstream}))
		})
		t.Run("inserts job upstreams including unresolved upstreams", func(t *testing.T) {
			db := dbSetup()

			upstreamB := job.NewUpstreamResolved("jobB", host, "resource-B", sampleTenant, upstreamType, taskName, false)
			upstreamC := job.NewUpstreamResolved("jobC", host, "resource-C", sampleTenant, upstreamType, taskName, false)
			upstreamD := job.NewUpstreamUnresolvedInferred("resource-D")
			upstreams := []*job.Upstream{upstreamB, upstreamC, upstreamD}
			jobWithUpstream := job.NewWithUpstream(jobA, upstreams)

			jobUpstreamRepo := postgres.NewJobRepository(db)
			_, err := jobUpstreamRepo.Add(ctx, []*job.Job{jobA, jobB, jobC})
			assert.NoError(t, err)
			assert.Nil(t, jobUpstreamRepo.ReplaceUpstreams(ctx, []*job.WithUpstream{jobWithUpstream}))
		})
		t.Run("deletes existing job upstream and inserts", func(t *testing.T) {
			db := dbSetup()

			upstreamB := job.NewUpstreamResolved("jobB", host, "resource-B", sampleTenant, upstreamType, taskName, false)
			upstreamC := job.NewUpstreamResolved("jobC", host, "resource-C", sampleTenant, upstreamType, taskName, false)

			jobUpstreamRepo := postgres.NewJobRepository(db)
			_, err = jobUpstreamRepo.Add(ctx, []*job.Job{jobA, jobB, jobC})
			assert.NoError(t, err)

			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamB})
			assert.NoError(t, jobUpstreamRepo.ReplaceUpstreams(ctx, []*job.WithUpstream{jobAWithUpstream}))

			upstreamsOfJobA, err := jobUpstreamRepo.GetUpstreams(ctx, proj.Name(), jobA.Spec().Name())
			assert.NoError(t, err)
			assert.EqualValues(t, []*job.Upstream{upstreamB}, upstreamsOfJobA)

			jobAWithUpstream = job.NewWithUpstream(jobA, []*job.Upstream{upstreamC})
			assert.NoError(t, jobUpstreamRepo.ReplaceUpstreams(ctx, []*job.WithUpstream{jobAWithUpstream}))

			upstreamsOfJobA, err = jobUpstreamRepo.GetUpstreams(ctx, proj.Name(), jobA.Spec().Name())
			assert.NoError(t, err)
			assert.EqualValues(t, []*job.Upstream{upstreamC}, upstreamsOfJobA)
		})
	})

	t.Run("Delete", func(t *testing.T) {
		t.Run("soft delete a job if not asked to do clean delete", func(t *testing.T) {
			db := dbSetup()

			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_a", nil)

			jobRepo := postgres.NewJobRepository(db)

			addedJob, err := jobRepo.Add(ctx, []*job.Job{jobA})
			assert.NoError(t, err)
			assert.NotNil(t, addedJob)

			err = jobRepo.Delete(ctx, proj.Name(), jobSpecA.Name(), false)
			assert.NoError(t, err)

			// update failure with proper log message shows job has been soft deleted
			_, err = jobRepo.Update(ctx, []*job.Job{jobA})
			assert.ErrorContains(t, err, "update is not allowed as job sample-job-A has been soft deleted")
		})
		t.Run("should return error if the soft delete failed", func(t *testing.T) {
			db := dbSetup()

			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()

			jobRepo := postgres.NewJobRepository(db)

			err = jobRepo.Delete(ctx, proj.Name(), jobSpecA.Name(), false)
			assert.ErrorContains(t, err, "failed to be deleted")
		})
		t.Run("hard delete a job if asked to do clean delete", func(t *testing.T) {
			db := dbSetup()

			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_a", nil)

			jobRepo := postgres.NewJobRepository(db)

			addedJob, err := jobRepo.Add(ctx, []*job.Job{jobA})
			assert.NoError(t, err)
			assert.NotNil(t, addedJob)

			err = jobRepo.Delete(ctx, proj.Name(), jobSpecA.Name(), true)
			assert.NoError(t, err)

			// update failure with proper log message shows job has been hard deleted
			_, err = jobRepo.Update(ctx, []*job.Job{jobA})
			assert.ErrorContains(t, err, "job sample-job-A not exists yet")
		})
		t.Run("should return error if the hard delete failed", func(t *testing.T) {
			db := dbSetup()

			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()

			jobRepo := postgres.NewJobRepository(db)

			err = jobRepo.Delete(ctx, proj.Name(), jobSpecA.Name(), true)
			assert.ErrorContains(t, err, "failed to be deleted")
		})
		t.Run("do delete job and delete upstream relationship", func(t *testing.T) {
			db := dbSetup()

			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_a", []job.ResourceURN{"dev.resource.sample_b"})

			jobRepo := postgres.NewJobRepository(db)

			addedJob, err := jobRepo.Add(ctx, []*job.Job{jobA})
			assert.NoError(t, err)
			assert.NotNil(t, addedJob)

			upstreamCInferred := job.NewUpstreamResolved("sample-job-B", "host-1", "dev.resource.sample_b", sampleTenant, "inferred", taskName, false)
			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamCInferred})
			err = jobRepo.ReplaceUpstreams(ctx, []*job.WithUpstream{jobAWithUpstream})
			assert.NoError(t, err)

			err = jobRepo.Delete(ctx, proj.Name(), jobSpecA.Name(), true)
			assert.NoError(t, err)

			// should succeed adding as job already cleaned earlier
			addedJob, err = jobRepo.Add(ctx, []*job.Job{jobA})
			assert.NoError(t, err)
			assert.NotNil(t, addedJob)
		})
	})

	t.Run("GetByJobName", func(t *testing.T) {
		t.Run("returns job success", func(t *testing.T) {
			db := dbSetup()

			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_a", []job.ResourceURN{"dev.resource.sample_b", "dev.resource.sample_c"})

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA})
			assert.NoError(t, err)

			actual, err := jobRepo.GetByJobName(ctx, sampleTenant.ProjectName(), "sample-job-A")
			assert.NoError(t, err)
			assert.NotNil(t, actual)
			assert.Equal(t, jobA, actual)
		})
		t.Run("should not return job if it is soft deleted", func(t *testing.T) {
			db := dbSetup()

			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_a", []job.ResourceURN{"dev.resource.sample_b", "dev.resource.sample_c"})

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA})
			assert.NoError(t, err)

			actual, err := jobRepo.GetByJobName(ctx, sampleTenant.ProjectName(), "sample-job-A")
			assert.NoError(t, err)
			assert.NotNil(t, actual)
			assert.Equal(t, jobA, actual)

			err = jobRepo.Delete(ctx, sampleTenant.ProjectName(), jobSpecA.Name(), false)
			assert.NoError(t, err)

			actual, err = jobRepo.GetByJobName(ctx, sampleTenant.ProjectName(), "sample-job-A")
			assert.Error(t, err)
			assert.Nil(t, actual)
		})
	})

	t.Run("GetAllByProjectName", func(t *testing.T) {
		t.Run("returns no error when get all jobs success", func(t *testing.T) {
			db := dbSetup()

			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_a", []job.ResourceURN{"dev.resource.sample_b", "dev.resource.sample_c"})
			jobSpecB := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobB := job.NewJob(sampleTenant, jobSpecB, "dev.resource.sample_b", []job.ResourceURN{"dev.resource.sample_c"})

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.NoError(t, err)

			actual, err := jobRepo.GetAllByProjectName(ctx, sampleTenant.ProjectName())
			assert.NoError(t, err)
			assert.NotNil(t, actual)
			assert.Len(t, actual, 2)
			assert.Equal(t, []*job.Job{jobA, jobB}, actual)
		})
		t.Run("returns only active jobs excluding the soft deleted jobs", func(t *testing.T) {
			db := dbSetup()

			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_a", []job.ResourceURN{"dev.resource.sample_b", "dev.resource.sample_c"})
			jobSpecB := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobB := job.NewJob(sampleTenant, jobSpecB, "dev.resource.sample_b", []job.ResourceURN{"dev.resource.sample_c"})

			jobRepo := postgres.NewJobRepository(db)
			_, err := jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.NoError(t, err)

			err = jobRepo.Delete(ctx, sampleTenant.ProjectName(), jobSpecB.Name(), false)
			assert.NoError(t, err)

			actual, err := jobRepo.GetAllByProjectName(ctx, sampleTenant.ProjectName())
			assert.NoError(t, err)
			assert.Equal(t, []*job.Job{jobA}, actual)
		})
	})

	t.Run("GetAllByResourceDestination", func(t *testing.T) {
		t.Run("returns no error when get all jobs success", func(t *testing.T) {
			db := dbSetup()

			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_general", []job.ResourceURN{"dev.resource.sample_b", "dev.resource.sample_c"})
			jobSpecB := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobB := job.NewJob(sampleTenant, jobSpecB, "dev.resource.sample_general", []job.ResourceURN{"dev.resource.sample_c"})

			jobRepo := postgres.NewJobRepository(db)
			_, err := jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.NoError(t, err)

			actual, err := jobRepo.GetAllByResourceDestination(ctx, "dev.resource.sample_general")
			assert.NoError(t, err)
			assert.NotNil(t, actual)
			assert.Len(t, actual, 2)
			assert.Equal(t, []*job.Job{jobA, jobB}, actual)
		})
		t.Run("returns only active jobs excluding the soft deleted jobs", func(t *testing.T) {
			db := dbSetup()

			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_general", []job.ResourceURN{"dev.resource.sample_b", "dev.resource.sample_c"})
			jobSpecB := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobB := job.NewJob(sampleTenant, jobSpecB, "dev.resource.sample_general", []job.ResourceURN{"dev.resource.sample_c"})

			jobRepo := postgres.NewJobRepository(db)
			_, err := jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.NoError(t, err)

			err = jobRepo.Delete(ctx, sampleTenant.ProjectName(), jobSpecB.Name(), false)
			assert.NoError(t, err)

			actual, err := jobRepo.GetAllByResourceDestination(ctx, "dev.resource.sample_general")
			assert.NoError(t, err)
			assert.Equal(t, []*job.Job{jobA}, actual)
		})
	})

	t.Run("GetUpstreams", func(t *testing.T) {
		t.Run("returns upstream given project and job name", func(t *testing.T) {
			db := dbSetup()

			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_general", []job.ResourceURN{"dev.resource.sample_b", "dev.resource.sample_c"})
			jobAUpstreamResolved := job.NewUpstreamResolved("sample-job-B", "", "", sampleTenant, "inferred", taskName, false)
			jobAUpstreamUnresolved := job.NewUpstreamUnresolvedInferred("dev.resource.sample_c")

			jobSpecB := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobB := job.NewJob(sampleTenant, jobSpecB, "dev.resource.sample_b", nil)

			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{jobAUpstreamResolved, jobAUpstreamUnresolved})

			jobRepo := postgres.NewJobRepository(db)

			_, err := jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.NoError(t, err)

			err = jobRepo.ReplaceUpstreams(ctx, []*job.WithUpstream{jobAWithUpstream})
			assert.NoError(t, err)

			result, err := jobRepo.GetUpstreams(ctx, proj.Name(), jobSpecA.Name())
			assert.NoError(t, err)
			assert.EqualValues(t, []*job.Upstream{jobAUpstreamResolved, jobAUpstreamUnresolved}, result)
		})
	})

	t.Run("GetDownstreamByDestination", func(t *testing.T) {
		t.Run("returns downstream given a job destination", func(t *testing.T) {
			db := dbSetup()

			jobAUpstreamSpec, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"sample-job-B"}).Build()
			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).WithSpecUpstream(jobAUpstreamSpec).Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_a", []job.ResourceURN{"dev.resource.sample_c"})

			jobSpecB := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobB := job.NewJob(sampleTenant, jobSpecB, "dev.resource.sample_b", nil)

			jobSpecC := job.NewSpecBuilder(jobVersion, "sample-job-C", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobC := job.NewJob(sampleTenant, jobSpecC, "dev.resource.sample_c", nil)

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB, jobC})
			assert.NoError(t, err)

			expectedDownstream := []*job.Downstream{
				job.NewDownstream(jobSpecA.Name(), proj.Name(), namespace.Name(), jobSpecA.Task().Name()),
			}
			result, err := jobRepo.GetDownstreamByDestination(ctx, proj.Name(), "dev.resource.sample_c")
			assert.NoError(t, err)
			assert.EqualValues(t, expectedDownstream, result)
		})
	})

	t.Run("GetDownstreamByJobName", func(t *testing.T) {
		t.Run("returns downstream given a job name", func(t *testing.T) {
			db := dbSetup()

			jobSpecA := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_a", []job.ResourceURN{"dev.resource.sample_b", "dev.resource.sample_c"})
			jobAUpstreamResolved := job.NewUpstreamResolved("sample-job-B", "", "", sampleTenant, "inferred", taskName, false)
			jobAUpstreamUnresolved := job.NewUpstreamUnresolvedInferred("dev.resource.sample_c")

			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{jobAUpstreamResolved, jobAUpstreamUnresolved})

			jobSpecB := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobB := job.NewJob(sampleTenant, jobSpecB, "dev.resource.sample_b", nil)

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.NoError(t, err)
			err = jobRepo.ReplaceUpstreams(ctx, []*job.WithUpstream{jobAWithUpstream})
			assert.NoError(t, err)

			expectedDownstream := []*job.Downstream{
				job.NewDownstream(jobSpecA.Name(), proj.Name(), namespace.Name(), jobSpecA.Task().Name()),
			}
			result, err := jobRepo.GetDownstreamByJobName(ctx, proj.Name(), jobSpecB.Name())
			assert.NoError(t, err)
			assert.EqualValues(t, expectedDownstream, result)
		})
	})
}
