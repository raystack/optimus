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

	proj, _ := tenant.NewProject("test-proj",
		map[string]string{
			"bucket":                     "gs://some_folder-2",
			tenant.ProjectSchedulerHost:  "host",
			tenant.ProjectStoragePathKey: "gs://location",
		})
	namespace, _ := tenant.NewNamespace("test-ns", proj.Name(),
		map[string]string{
			"bucket": "gs://ns_bucket",
		})
	otherNamespace, _ := tenant.NewNamespace("other-ns", proj.Name(),
		map[string]string{
			"bucket": "gs://ns_bucket",
		})
	sampleTenant, _ := tenant.NewTenant(proj.Name().String(), namespace.Name().String())

	dbSetup := func() *gorm.DB {
		dbConn := setup.TestDB()
		setup.TruncateTables(dbConn)

		projRepo := tenantPostgres.NewProjectRepository(dbConn)
		assert.Nil(t, projRepo.Save(ctx, proj))

		namespaceRepo := tenantPostgres.NewNamespaceRepository(dbConn)
		assert.Nil(t, namespaceRepo.Save(ctx, namespace))
		assert.Nil(t, namespaceRepo.Save(ctx, otherNamespace))

		return dbConn
	}

	jobVersion := 1
	jobOwner := "dev_test"
	jobDescription := "sample job"
	jobRetry := job.NewRetry(5, 0, false)
	jobSchedule := job.NewSchedule("2022-10-01", "", "", false, false, jobRetry)
	jobWindow, _ := models.NewWindow(jobVersion, "d", "24h", "24h")
	jobTaskConfig := job.NewConfig(map[string]string{"sample_task_key": "sample_value"})
	jobTask := job.NewTask("bq2bq", jobTaskConfig)

	host := "sample-host"
	upstreamType := "inferred"

	t.Run("Add", func(t *testing.T) {
		t.Run("inserts job spec", func(t *testing.T) {
			db := dbSetup()

			jobLabels := map[string]string{
				"environment": "integration",
			}
			jobHookConfig := job.NewConfig(map[string]string{"sample_hook_key": "sample_value"})
			jobHooks := []*job.Hook{job.NewHook("sample_hook", jobHookConfig)}
			jobAlertConfig := job.NewConfig(map[string]string{"sample_alert_key": "sample_value"})
			jobAlerts := []*job.Alert{job.NewAlert(job.SLAMissEvent, []string{"sample-channel"}, jobAlertConfig.Config())}
			jobUpstreams := job.NewSpecUpstream([]string{"job-upstream-1", "job-upstream-2"}, nil)
			jobAssets := map[string]string{"sample-asset": "value-asset"}
			resourceRequestConfig := job.NewResourceConfig("250m", "128Mi")
			resourceLimitConfig := job.NewResourceConfig("500m", "1024Mi")
			resourceMetadata := job.NewResourceMetadata(resourceRequestConfig, resourceLimitConfig)
			jobMetadata := job.NewMetadata(resourceMetadata, map[string]string{"scheduler_config_key": "value"})

			jobSpecA, err := job.NewSpec(sampleTenant, jobVersion, "sample-job-A", jobOwner, jobDescription, jobLabels, jobSchedule,
				jobWindow, jobTask, jobHooks, jobAlerts, jobUpstreams, jobAssets, jobMetadata)
			assert.Nil(t, err)
			jobA := job.NewJob(jobSpecA, "dev.resource.sample_a", []string{"resource-3"})

			jobSpecB, err := job.NewSpec(sampleTenant, jobVersion, "sample-job-B", jobOwner, jobDescription, jobLabels, jobSchedule,
				jobWindow, jobTask, jobHooks, jobAlerts, nil, jobAssets, jobMetadata)
			assert.Nil(t, err)
			jobB := job.NewJob(jobSpecB, "dev.resource.sample_b", nil)

			jobs := []*job.Job{jobA, jobB}

			jobRepo := postgres.NewJobRepository(db)
			addedJobs, jobErrors, err := jobRepo.Add(ctx, jobs)
			assert.Nil(t, jobErrors)
			assert.Nil(t, err)
			assert.EqualValues(t, jobs, addedJobs)
		})
		t.Run("inserts job spec with optional fields empty", func(t *testing.T) {
			db := dbSetup()

			jobSpecA, err := job.NewSpec(sampleTenant, jobVersion, "sample-job-A", jobOwner, jobDescription, nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			assert.Nil(t, err)
			jobA := job.NewJob(jobSpecA, "dev.resource.sample_a", []string{"resource-3"})

			jobs := []*job.Job{jobA}

			jobRepo := postgres.NewJobRepository(db)
			addedJobs, jobErrors, err := jobRepo.Add(ctx, jobs)
			assert.Nil(t, jobErrors)
			assert.Nil(t, err)
			assert.EqualValues(t, jobs, addedJobs)
		})
		t.Run("skip job and return job error if job already exist", func(t *testing.T) {
			db := dbSetup()

			jobSpecA, err := job.NewSpec(sampleTenant, jobVersion, "sample-job-A", jobOwner, jobDescription, nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			assert.Nil(t, err)
			jobA := job.NewJob(jobSpecA, "dev.resource.sample_a", []string{"resource-3"})

			jobRepo := postgres.NewJobRepository(db)
			// TODO: consider using this error
			addedJobs, jobErrors, err := jobRepo.Add(ctx, []*job.Job{jobA})
			assert.Nil(t, err)

			jobSpecB, err := job.NewSpec(sampleTenant, jobVersion, "sample-job-B", jobOwner, jobDescription, nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			assert.Nil(t, err)
			jobB := job.NewJob(jobSpecB, "dev.resource.sample_b", []string{"resource-3"})

			addedJobs, jobErrors, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.ErrorContains(t, jobErrors, "job already exists")
			assert.Nil(t, err)
			assert.EqualValues(t, []*job.Job{jobB}, addedJobs)
		})
		t.Run("return error if all jobs are failed to be saved", func(t *testing.T) {
			db := dbSetup()

			jobSpecA, err := job.NewSpec(sampleTenant, jobVersion, "sample-job-A", jobOwner, jobDescription, nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			assert.Nil(t, err)
			jobA := job.NewJob(jobSpecA, "dev.resource.sample_a", []string{"resource-3"})

			jobSpecB, err := job.NewSpec(sampleTenant, jobVersion, "sample-job-B", jobOwner, jobDescription, nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			assert.Nil(t, err)
			jobB := job.NewJob(jobSpecB, "dev.resource.sample_b", []string{"resource-3"})

			jobRepo := postgres.NewJobRepository(db)
			// TODO: consider using this error
			addedJobs, jobErrors, err := jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.Nil(t, err)

			addedJobs, jobErrors, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.ErrorContains(t, jobErrors, "job already exists")
			assert.ErrorContains(t, err, "no jobs to create")
			assert.Nil(t, addedJobs)
		})
	})

	t.Run("GetJobWithUpstreams", func(t *testing.T) {
		t.Run("returns job with inferred upstreams", func(t *testing.T) {
			db := dbSetup()

			tenantDetails, err := tenant.NewTenantDetails(proj, namespace)
			assert.Nil(t, err)

			jobSpecA, err := job.NewSpec(sampleTenant, jobVersion, "sample-job-A", jobOwner, jobDescription, nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			assert.Nil(t, err)
			jobA := job.NewJob(jobSpecA, "dev.resource.sample_a", []string{"dev.resource.sample_b"})

			jobSpecB, err := job.NewSpec(sampleTenant, jobVersion, "sample-job-B", jobOwner, jobDescription, nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			assert.Nil(t, err)
			jobB := job.NewJob(jobSpecB, "dev.resource.sample_b", nil)

			jobRepo := postgres.NewJobRepository(db)
			_, jobErrors, err := jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.Nil(t, jobErrors)
			assert.Nil(t, err)

			expectedUpstream, _ := job.NewUpstreamResolved(jobSpecB.Name().String(), "", jobB.Destination(), tenantDetails.ToTenant(), "inferred")

			// TODO: consider using this error
			upstreams, err := jobRepo.GetJobNameWithInternalUpstreams(ctx, proj.Name(), []job.Name{jobSpecA.Name()})
			assert.Equal(t, expectedUpstream, upstreams[jobSpecA.Name()][0])
		})
		t.Run("returns job with static upstreams", func(t *testing.T) {
			db := dbSetup()

			tenantDetails, err := tenant.NewTenantDetails(proj, namespace)
			assert.Nil(t, err)

			jobAUpstreams := job.NewSpecUpstream([]string{"sample-job-B"}, nil)
			// TODO: consider using this error
			jobSpecA, err := job.NewSpec(sampleTenant, jobVersion, "sample-job-A", jobOwner, jobDescription, nil, jobSchedule,
				jobWindow, jobTask, nil, nil, jobAUpstreams, nil, nil)
			jobA := job.NewJob(jobSpecA, "dev.resource.sample_a", nil)

			// TODO: consider using this error
			jobSpecB, err := job.NewSpec(sampleTenant, jobVersion, "sample-job-B", jobOwner, jobDescription, nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			jobB := job.NewJob(jobSpecB, "dev.resource.sample_b", nil)

			jobRepo := postgres.NewJobRepository(db)
			_, jobErrors, err := jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.Nil(t, jobErrors)
			assert.Nil(t, err)

			expectedUpstream, _ := job.NewUpstreamResolved(jobSpecB.Name().String(), "", jobB.Destination(), tenantDetails.ToTenant(), "static")

			// TODO: consider using this error
			upstreams, err := jobRepo.GetJobNameWithInternalUpstreams(ctx, proj.Name(), []job.Name{jobSpecA.Name()})
			assert.Equal(t, expectedUpstream, upstreams[jobSpecA.Name()][0])
		})
		t.Run("returns job with static and inferred upstreams", func(t *testing.T) {
			db := dbSetup()

			tenantDetails, err := tenant.NewTenantDetails(proj, namespace)
			assert.Nil(t, err)

			jobAUpstreams := job.NewSpecUpstream([]string{"test-proj/sample-job-B"}, nil)
			// TODO: consider using this error
			jobSpecA, err := job.NewSpec(sampleTenant, jobVersion, "sample-job-A", jobOwner, jobDescription, nil, jobSchedule,
				jobWindow, jobTask, nil, nil, jobAUpstreams, nil, nil)
			jobA := job.NewJob(jobSpecA, "dev.resource.sample_a", []string{"dev.resource.sample_c"})

			// TODO: consider using this error
			jobSpecB, err := job.NewSpec(sampleTenant, jobVersion, "sample-job-B", jobOwner, jobDescription, nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			jobB := job.NewJob(jobSpecB, "dev.resource.sample_b", nil)

			// TODO: consider using this error
			jobSpecC, err := job.NewSpec(sampleTenant, jobVersion, "sample-job-C", jobOwner, jobDescription, nil, jobSchedule,
				jobWindow, jobTask, nil, nil, nil, nil, nil)
			jobC := job.NewJob(jobSpecC, "dev.resource.sample_c", nil)

			jobRepo := postgres.NewJobRepository(db)
			_, jobErrors, err := jobRepo.Add(ctx, []*job.Job{jobA, jobB, jobC})
			assert.Nil(t, jobErrors)
			assert.Nil(t, err)

			upstreamB, _ := job.NewUpstreamResolved(jobSpecB.Name().String(), "", jobB.Destination(), tenantDetails.ToTenant(), "static")
			upstreamC, _ := job.NewUpstreamResolved(jobSpecC.Name().String(), "", jobC.Destination(), tenantDetails.ToTenant(), "inferred")

			expectedUpstreams := []*job.Upstream{
				upstreamB,
				upstreamC,
			}

			upstreams, err := jobRepo.GetJobNameWithInternalUpstreams(ctx, proj.Name(), []job.Name{jobSpecA.Name()})
			assert.Nil(t, err)
			assert.EqualValues(t, expectedUpstreams, upstreams[jobSpecA.Name()])
		})
	})

	t.Run("SaveUpstream", func(t *testing.T) {
		jobSpecA, _ := job.NewSpec(sampleTenant, jobVersion, "sample-job-A", jobOwner, jobDescription, nil, jobSchedule,
			jobWindow, jobTask, nil, nil, nil, nil, nil)
		jobA := job.NewJob(jobSpecA, "dev.resource.sample_a", []string{"dev.resource.sample_c"})
		t.Run("inserts job upstreams", func(t *testing.T) {
			db := dbSetup()

			upstreamB, err := job.NewUpstreamResolved("jobB", host, "resource-B", sampleTenant, upstreamType)
			assert.Nil(t, err)

			upstreamC, err := job.NewUpstreamResolved("jobC", host, "resource-C", sampleTenant, upstreamType)
			assert.Nil(t, err)

			upstreams := []*job.Upstream{upstreamB, upstreamC}
			jobWithUpstream := job.NewWithUpstream(jobA, upstreams)

			jobUpstreamRepo := postgres.NewJobRepository(db)
			assert.Nil(t, jobUpstreamRepo.SaveUpstream(ctx, []*job.WithUpstream{jobWithUpstream}))
		})
		t.Run("inserts job upstreams including unresolved upstreams", func(t *testing.T) {
			db := dbSetup()

			upstreamB, err := job.NewUpstreamResolved("jobB", host, "resource-B", sampleTenant, upstreamType)
			assert.Nil(t, err)

			upstreamC, err := job.NewUpstreamResolved("jobC", host, "resource-C", sampleTenant, upstreamType)
			assert.Nil(t, err)

			upstreamD := job.NewUpstreamUnresolved("", "resource-D", "")

			upstreams := []*job.Upstream{upstreamB, upstreamC, upstreamD}
			jobWithUpstream := job.NewWithUpstream(jobA, upstreams)

			jobUpstreamRepo := postgres.NewJobRepository(db)
			assert.Nil(t, jobUpstreamRepo.SaveUpstream(ctx, []*job.WithUpstream{jobWithUpstream}))
		})
		t.Run("deletes existing job upstream and inserts", func(t *testing.T) {
			db := dbSetup()

			sampleTenant, err := tenant.NewTenant(proj.Name().String(), namespace.Name().String())
			assert.Nil(t, err)

			upstreamB, err := job.NewUpstreamResolved("jobB", host, "resource-B", sampleTenant, upstreamType)
			assert.Nil(t, err)

			upstreams := []*job.Upstream{upstreamB}
			jobWithUpstream := job.NewWithUpstream(jobA, upstreams)

			jobUpstreamRepo := postgres.NewJobRepository(db)
			assert.Nil(t, jobUpstreamRepo.SaveUpstream(ctx, []*job.WithUpstream{jobWithUpstream}))

			// TODO: consider using this error
			upstreamC, err := job.NewUpstreamResolved("jobC", host, "resource-C", sampleTenant, upstreamType)
			upstreams = []*job.Upstream{upstreamC}
			jobWithUpstream = job.NewWithUpstream(jobA, upstreams)

			assert.Nil(t, jobUpstreamRepo.SaveUpstream(ctx, []*job.WithUpstream{jobWithUpstream}))
		})
	})
}
