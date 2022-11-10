//go:build !unit_test

package job_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/tenant"
	postgres "github.com/odpf/optimus/internal/store/postgres/job"
	tenantPostgres "github.com/odpf/optimus/internal/store/postgres/tenant"
	"github.com/odpf/optimus/models"
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
	jobSchedule, err := job.NewScheduleBuilder(startDate, "").WithRetry(jobRetry).Build()
	assert.NoError(t, err)
	jobWindow, err := models.NewWindow(jobVersion.Int(), "d", "24h", "24h")
	assert.NoError(t, err)
	jobTaskConfig, err := job.NewConfig(map[string]string{"sample_task_key": "sample_value"})
	assert.NoError(t, err)
	jobTask := job.NewTask("bq2bq", jobTaskConfig)

	host := "sample-host"
	upstreamType := "inferred"

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
			alert := job.NewAlertBuilder(job.SLAMissEvent, []string{"sample-channel"}).WithConfig(jobAlertConfig).Build()
			jobAlerts := []*job.Alert{alert}
			upstreamName1, err := job.NameFrom("job-upstream-1")
			assert.NoError(t, err)
			upstreamName2, err := job.NameFrom("job-upstream-2")
			assert.NoError(t, err)
			jobUpstream := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.Name{upstreamName1, upstreamName2}).Build()
			jobAsset := job.NewAsset(map[string]string{"sample-asset": "value-asset"})
			resourceRequestConfig := job.NewMetadataResourceConfig("250m", "128Mi")
			resourceLimitConfig := job.NewMetadataResourceConfig("250m", "128Mi")
			resourceMetadata := job.NewResourceMetadata(resourceRequestConfig, resourceLimitConfig)
			jobMetadata := job.NewMetadataBuilder().
				WithResource(resourceMetadata).
				WithScheduler(map[string]string{"scheduler_config_key": "value"}).
				Build()

			jobSpecA := job.NewSpecBuilder(sampleTenant, jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).
				WithDescription(jobDescription).
				WithLabels(jobLabels).
				WithHooks(jobHooks).
				WithAlerts(jobAlerts).
				WithSpecUpstream(jobUpstream).
				WithAsset(jobAsset).
				WithMetadata(jobMetadata).
				Build()
			jobA := job.NewJob(jobSpecA, "dev.resource.sample_a", []string{"resource-3"})

			jobSpecB := job.NewSpecBuilder(sampleTenant, jobVersion, "sample-job-B", jobOwner, jobSchedule, jobWindow, jobTask).
				WithDescription(jobDescription).
				WithLabels(jobLabels).
				WithHooks(jobHooks).
				WithAlerts(jobAlerts).
				WithAsset(jobAsset).
				WithMetadata(jobMetadata).
				Build()
			assert.NoError(t, err)
			jobB := job.NewJob(jobSpecB, "dev.resource.sample_b", nil)

			jobs := []*job.Job{jobA, jobB}

			jobRepo := postgres.NewJobRepository(db)
			addedJobs, err := jobRepo.Add(ctx, jobs)
			assert.NoError(t, err)
			assert.EqualValues(t, jobs, addedJobs)
		})
		t.Run("inserts job spec with optional fields empty", func(t *testing.T) {
			db := dbSetup()

			jobSpecA := job.NewSpecBuilder(sampleTenant, jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobA := job.NewJob(jobSpecA, "dev.resource.sample_a", []string{"resource-3"})

			jobs := []*job.Job{jobA}

			jobRepo := postgres.NewJobRepository(db)
			addedJobs, err := jobRepo.Add(ctx, jobs)
			assert.NoError(t, err)
			assert.EqualValues(t, jobs, addedJobs)
		})
		t.Run("skip job and return job error if job already exist", func(t *testing.T) {
			db := dbSetup()

			jobSpecA := job.NewSpecBuilder(sampleTenant, jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobA := job.NewJob(jobSpecA, "dev.resource.sample_a", []string{"resource-3"})

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA})
			assert.NoError(t, err)

			jobSpecB := job.NewSpecBuilder(sampleTenant, jobVersion, "sample-job-B", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobB := job.NewJob(jobSpecB, "dev.resource.sample_b", []string{"resource-3"})

			addedJobs, err := jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.ErrorContains(t, err, "job already exists")
			assert.EqualValues(t, []*job.Job{jobB}, addedJobs)
		})
		t.Run("return error if all jobs are failed to be saved", func(t *testing.T) {
			db := dbSetup()

			jobSpecA := job.NewSpecBuilder(sampleTenant, jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobA := job.NewJob(jobSpecA, "dev.resource.sample_a", []string{"resource-3"})

			jobSpecB := job.NewSpecBuilder(sampleTenant, jobVersion, "sample-job-B", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobB := job.NewJob(jobSpecB, "dev.resource.sample_b", []string{"resource-3"})

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.NoError(t, err)

			addedJobs, err := jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.ErrorContains(t, err, "job already exists")
			assert.Nil(t, addedJobs)
		})
	})

	t.Run("GetJobWithUpstreams", func(t *testing.T) {
		t.Run("returns job with inferred upstreams", func(t *testing.T) {
			db := dbSetup()

			tenantDetails, err := tenant.NewTenantDetails(proj, namespace)
			assert.NoError(t, err)

			jobSpecA := job.NewSpecBuilder(sampleTenant, jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobA := job.NewJob(jobSpecA, "dev.resource.sample_a", []string{"dev.resource.sample_b"})

			jobSpecB := job.NewSpecBuilder(sampleTenant, jobVersion, "sample-job-B", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobB := job.NewJob(jobSpecB, "dev.resource.sample_b", nil)

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.NoError(t, err)

			expectedUpstream, _ := job.NewUpstreamResolved(jobSpecB.Name().String(), "", jobB.Destination(), tenantDetails.ToTenant(), "inferred")

			// TODO: consider using this error
			upstreams, err := jobRepo.GetJobNameWithInternalUpstreams(ctx, proj.Name(), []job.Name{jobSpecA.Name()})
			assert.Equal(t, expectedUpstream, upstreams[jobSpecA.Name()][0])
		})
		t.Run("returns job with static upstreams", func(t *testing.T) {
			db := dbSetup()

			tenantDetails, err := tenant.NewTenantDetails(proj, namespace)
			assert.NoError(t, err)

			upstreamName, err := job.NameFrom("sample-job-B")
			assert.NoError(t, err)
			jobAUpstream := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.Name{upstreamName}).Build()
			jobSpecA := job.NewSpecBuilder(sampleTenant, jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).
				WithDescription(jobDescription).
				WithSpecUpstream(jobAUpstream).
				Build()
			jobA := job.NewJob(jobSpecA, "dev.resource.sample_a", nil)

			jobSpecB := job.NewSpecBuilder(sampleTenant, jobVersion, "sample-job-B", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobB := job.NewJob(jobSpecB, "dev.resource.sample_b", nil)

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.NoError(t, err)

			expectedUpstream, _ := job.NewUpstreamResolved(jobSpecB.Name().String(), "", jobB.Destination(), tenantDetails.ToTenant(), "static")

			// TODO: consider using this error
			upstreams, err := jobRepo.GetJobNameWithInternalUpstreams(ctx, proj.Name(), []job.Name{jobSpecA.Name()})
			assert.Equal(t, expectedUpstream, upstreams[jobSpecA.Name()][0])
		})
		t.Run("returns job with static and inferred upstreams", func(t *testing.T) {
			db := dbSetup()

			tenantDetails, err := tenant.NewTenantDetails(proj, namespace)
			assert.NoError(t, err)

			upstreamName, err := job.NameFrom("test-proj/sample-job-B")
			assert.NoError(t, err)
			jobAUpstream := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.Name{upstreamName}).Build()
			jobSpecA := job.NewSpecBuilder(sampleTenant, jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).
				WithDescription(jobDescription).
				WithSpecUpstream(jobAUpstream).
				Build()
			jobA := job.NewJob(jobSpecA, "dev.resource.sample_a", []string{"dev.resource.sample_c"})

			jobSpecB := job.NewSpecBuilder(sampleTenant, jobVersion, "sample-job-B", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobB := job.NewJob(jobSpecB, "dev.resource.sample_b", nil)

			jobSpecC := job.NewSpecBuilder(sampleTenant, jobVersion, "sample-job-C", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
			jobC := job.NewJob(jobSpecC, "dev.resource.sample_c", nil)

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB, jobC})
			assert.NoError(t, err)

			upstreamB, _ := job.NewUpstreamResolved(jobSpecB.Name().String(), "", jobB.Destination(), tenantDetails.ToTenant(), "static")
			upstreamC, _ := job.NewUpstreamResolved(jobSpecC.Name().String(), "", jobC.Destination(), tenantDetails.ToTenant(), "inferred")

			expectedUpstreams := []*job.Upstream{
				upstreamB,
				upstreamC,
			}

			upstreams, err := jobRepo.GetJobNameWithInternalUpstreams(ctx, proj.Name(), []job.Name{jobSpecA.Name()})
			assert.NoError(t, err)
			assert.EqualValues(t, expectedUpstreams, upstreams[jobSpecA.Name()])
		})
	})

	t.Run("ReplaceUpstreams", func(t *testing.T) {
		jobSpecA := job.NewSpecBuilder(sampleTenant, jobVersion, "sample-job-A", jobOwner, jobSchedule, jobWindow, jobTask).WithDescription(jobDescription).Build()
		jobA := job.NewJob(jobSpecA, "dev.resource.sample_a", []string{"dev.resource.sample_c"})
		t.Run("inserts job upstreams", func(t *testing.T) {
			db := dbSetup()

			upstreamB, err := job.NewUpstreamResolved("jobB", host, "resource-B", sampleTenant, upstreamType)
			assert.NoError(t, err)

			upstreamC, err := job.NewUpstreamResolved("jobC", host, "resource-C", sampleTenant, upstreamType)
			assert.NoError(t, err)

			upstreams := []*job.Upstream{upstreamB, upstreamC}
			jobWithUpstream := job.NewWithUpstream(jobA, upstreams)

			jobUpstreamRepo := postgres.NewJobRepository(db)
			assert.Nil(t, jobUpstreamRepo.ReplaceUpstreams(ctx, []*job.WithUpstream{jobWithUpstream}))
		})
		t.Run("inserts job upstreams including unresolved upstreams", func(t *testing.T) {
			db := dbSetup()

			upstreamB, err := job.NewUpstreamResolved("jobB", host, "resource-B", sampleTenant, upstreamType)
			assert.NoError(t, err)

			upstreamC, err := job.NewUpstreamResolved("jobC", host, "resource-C", sampleTenant, upstreamType)
			assert.NoError(t, err)

			upstreamD := job.NewUpstreamUnresolved("", "resource-D", "")

			upstreams := []*job.Upstream{upstreamB, upstreamC, upstreamD}
			jobWithUpstream := job.NewWithUpstream(jobA, upstreams)

			jobUpstreamRepo := postgres.NewJobRepository(db)
			assert.Nil(t, jobUpstreamRepo.ReplaceUpstreams(ctx, []*job.WithUpstream{jobWithUpstream}))
		})
		t.Run("deletes existing job upstream and inserts", func(t *testing.T) {
			db := dbSetup()

			sampleTenant, err := tenant.NewTenant(proj.Name().String(), namespace.Name().String())
			assert.NoError(t, err)

			upstreamB, err := job.NewUpstreamResolved("jobB", host, "resource-B", sampleTenant, upstreamType)
			assert.NoError(t, err)

			upstreams := []*job.Upstream{upstreamB}
			jobWithUpstream := job.NewWithUpstream(jobA, upstreams)

			jobUpstreamRepo := postgres.NewJobRepository(db)
			assert.Nil(t, jobUpstreamRepo.ReplaceUpstreams(ctx, []*job.WithUpstream{jobWithUpstream}))

			// TODO: consider using this error
			upstreamC, err := job.NewUpstreamResolved("jobC", host, "resource-C", sampleTenant, upstreamType)
			upstreams = []*job.Upstream{upstreamC}
			jobWithUpstream = job.NewWithUpstream(jobA, upstreams)

			assert.Nil(t, jobUpstreamRepo.ReplaceUpstreams(ctx, []*job.WithUpstream{jobWithUpstream}))
		})
	})
}
