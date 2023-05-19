//go:build !unit_test

package scheduler_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/models"
	jobRepo "github.com/goto/optimus/internal/store/postgres/job"
	postgres "github.com/goto/optimus/internal/store/postgres/scheduler"
	tenantPostgres "github.com/goto/optimus/internal/store/postgres/tenant"
	"github.com/goto/optimus/tests/setup"
)

const (
	jobAName = "sample-job-A"
	jobBName = "sample-job-B"
)

func TestPostgresJobRepository(t *testing.T) {
	ctx := context.Background()
	tnnt, _ := tenant.NewTenant("test-proj", "test-ns")

	t.Run("GetAll", func(t *testing.T) {
		t.Run("get all jobs for a given project name", func(t *testing.T) {
			db := dbSetup()
			jobs := addJobs(ctx, t, db)

			jobProviderRepo := postgres.NewJobProviderRepository(db)

			allJobs, err := jobProviderRepo.GetAll(ctx, tnnt.ProjectName())
			assert.Nil(t, err)
			assert.Equal(t, len(jobs), len(allJobs))
			for _, expectedJob := range jobs {
				found := false
				for _, job := range allJobs {
					if compareEqualJobWithDetails(expectedJob, job) {
						found = true
						break
					}
				}
				assert.True(t, found)
			}
			for _, job := range allJobs {
				if job.Name.String() == jobAName {
					assert.Len(t, job.Upstreams.UpstreamJobs, 1)
					assert.Equal(t, jobBName, job.Upstreams.UpstreamJobs[0].JobName)
					assert.Equal(t, "internal", job.Upstreams.UpstreamJobs[0].Host)
					assert.Equal(t, "resolved", job.Upstreams.UpstreamJobs[0].State)
					assert.Equal(t, false, job.Upstreams.UpstreamJobs[0].External)
					assert.Equal(t, "bq2bq", job.Upstreams.UpstreamJobs[0].TaskName)
					assert.Equal(t, "dev.resource.sample_b", job.Upstreams.UpstreamJobs[0].DestinationURN)
				}
			}
		})

		t.Run("return not found error when jobs not found", func(t *testing.T) {
			db := dbSetup()
			jobProviderRepo := postgres.NewJobProviderRepository(db)
			allJobs, err := jobProviderRepo.GetAll(ctx, "some-other-project-1")
			assert.True(t, errors.IsErrorType(err, errors.ErrNotFound))
			assert.Nil(t, allJobs)
		})
	})

	t.Run("GetJobDetails", func(t *testing.T) {
		t.Run("gets one job with details", func(t *testing.T) {
			db := dbSetup()
			jobs := addJobs(ctx, t, db)

			jobProviderRepo := postgres.NewJobProviderRepository(db)

			jobWithDetails, err := jobProviderRepo.GetJobDetails(ctx, tnnt.ProjectName(), jobAName)
			assert.Nil(t, err)
			assert.True(t, compareEqualJobWithDetails(jobs[jobAName], jobWithDetails))
		})
		t.Run("returns not found error when job not found", func(t *testing.T) {
			db := dbSetup()
			jobProviderRepo := postgres.NewJobProviderRepository(db)
			jobObject, err := jobProviderRepo.GetJobDetails(ctx, tnnt.ProjectName(), "some-other-job")
			assert.True(t, errors.IsErrorType(err, errors.ErrNotFound))
			assert.Nil(t, jobObject)
		})
	})
	t.Run("GetJob", func(t *testing.T) {
		t.Run("returns one job", func(t *testing.T) {
			db := dbSetup()
			jobs := addJobs(ctx, t, db)
			jobProviderRepo := postgres.NewJobProviderRepository(db)

			jobObject, err := jobProviderRepo.GetJob(ctx, tnnt.ProjectName(), jobAName)
			assert.Nil(t, err)
			assert.True(t, compareEqualJob(jobs[jobAName], jobObject))
		})
		t.Run("returns not found error when job not found", func(t *testing.T) {
			db := dbSetup()
			jobProviderRepo := postgres.NewJobProviderRepository(db)
			jobObject, err := jobProviderRepo.GetJob(ctx, tnnt.ProjectName(), "some-other-job")
			assert.True(t, errors.IsErrorType(err, errors.ErrNotFound))
			assert.Nil(t, jobObject)
		})
	})
	t.Run("GetJobs", func(t *testing.T) {
		t.Run("returns multiple job", func(t *testing.T) {
			db := dbSetup()
			jobs := addJobs(ctx, t, db)
			jobProviderRepo := postgres.NewJobProviderRepository(db)

			jobObjects, err := jobProviderRepo.GetJobs(ctx, tnnt.ProjectName(), []string{jobAName, jobBName})
			assert.Nil(t, err)
			assert.Equal(t, 2, len(jobObjects))
			for _, jobObject := range jobObjects {
				compareEqualJobWithDetails(jobs[jobObject.Name.String()], jobObject)
			}
		})
		t.Run("returns not found error when jobs are not found", func(t *testing.T) {
			db := dbSetup()
			jobProviderRepo := postgres.NewJobProviderRepository(db)
			jobObject, err := jobProviderRepo.GetJobs(ctx, tnnt.ProjectName(), []string{"some-other-job"})
			assert.ErrorContains(t, err, "unable to find job")
			assert.Nil(t, jobObject)
		})
		t.Run("returns the found job when some other job is not found", func(t *testing.T) {
			db := dbSetup()
			jobs := addJobs(ctx, t, db)
			jobProviderRepo := postgres.NewJobProviderRepository(db)
			jobObjects, err := jobProviderRepo.GetJobs(ctx, tnnt.ProjectName(), []string{jobAName, "some-other-job"})
			assert.ErrorContains(t, err, "unable to find job")
			assert.Equal(t, 1, len(jobObjects))
			for _, jobObject := range jobObjects {
				compareEqualJobWithDetails(jobs[jobObject.Name.String()], jobObject)
			}
		})
	})
}

func dbSetup() *pgxpool.Pool {
	pool := setup.TestPool()
	setup.TruncateTablesWith(pool)
	return pool
}

func addJobs(ctx context.Context, t *testing.T, pool *pgxpool.Pool) map[string]*job.Job {
	t.Helper()
	jobVersion := 1
	jobOwner := "dev_test"
	jobDescription := "sample job"
	jobRetry := job.NewRetry(5, 0, true)
	startDate, err := job.ScheduleDateFrom("2022-10-01")
	assert.NoError(t, err)
	jobSchedule, err := job.NewScheduleBuilder(startDate).WithRetry(jobRetry).WithDependsOnPast(true).Build()
	assert.NoError(t, err)
	jobWindow, err := models.NewWindow(jobVersion, "d", "24h", "24h")
	assert.NoError(t, err)
	jobTaskConfig, err := job.ConfigFrom(map[string]string{"sample_task_key": "sample_value"})
	assert.NoError(t, err)
	taskName, err := job.TaskNameFrom("bq2bq")
	assert.NoError(t, err)
	jobTask := job.NewTask(taskName, jobTaskConfig)

	jobLabels := map[string]string{
		"environment": "integration",
	}

	proj, err := tenant.NewProject("test-proj",
		map[string]string{
			"bucket":                     "gs://some_folder-2",
			tenant.ProjectSchedulerHost:  "host",
			tenant.ProjectStoragePathKey: "gs://location",
		})
	assert.NoError(t, err)

	projRepo := tenantPostgres.NewProjectRepository(pool)
	assert.NoError(t, projRepo.Save(ctx, proj))

	namespace, err := tenant.NewNamespace("test-ns", proj.Name(),
		map[string]string{
			"bucket": "gs://ns_bucket",
		})
	assert.NoError(t, err)

	namespaceRepo := tenantPostgres.NewNamespaceRepository(pool)
	assert.NoError(t, namespaceRepo.Save(ctx, namespace))

	jobHookConfig, err := job.ConfigFrom(map[string]string{"sample_hook_key": "sample_value"})
	assert.NoError(t, err)
	hookSpec, err := job.NewHook("sample_hook", jobHookConfig)
	assert.NoError(t, err)
	jobHooks := []*job.Hook{hookSpec}
	jobAlertConfig, err := job.ConfigFrom(map[string]string{"sample_alert_key": "sample_value"})
	assert.NoError(t, err)
	alert, _ := job.NewAlertSpec("sla_miss", []string{"sample-channel"}, jobAlertConfig)
	jobAlerts := []*job.AlertSpec{alert}
	upstreamName1 := job.SpecUpstreamNameFrom("job-upstream-1")
	upstreamName2 := job.SpecUpstreamNameFrom("job-upstream-2")
	jobUpstream, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName1, upstreamName2}).Build()
	jobAsset, err := job.AssetFrom(map[string]string{"sample-asset": "value-asset"})
	assert.NoError(t, err)
	resourceRequestConfig := job.NewMetadataResourceConfig("250m", "128Mi")
	resourceLimitConfig := job.NewMetadataResourceConfig("250m", "128Mi")
	resourceMetadata := job.NewResourceMetadata(resourceRequestConfig, resourceLimitConfig)
	jobMetadata, _ := job.NewMetadataBuilder().
		WithResource(resourceMetadata).
		WithScheduler(map[string]string{"scheduler_config_key": "value"}).
		Build()
	jobSpecA, err := job.NewSpecBuilder(jobVersion, jobAName, jobOwner, jobSchedule, jobWindow, jobTask).
		WithDescription(jobDescription).
		WithLabels(jobLabels).
		WithHooks(jobHooks).
		WithAlerts(jobAlerts).
		WithSpecUpstream(jobUpstream).
		WithAsset(jobAsset).
		WithMetadata(jobMetadata).
		Build()
	assert.NoError(t, err)
	sampleTenant, err := tenant.NewTenant(proj.Name().String(), namespace.Name().String())
	assert.NoError(t, err)
	jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_a", []job.ResourceURN{"resource-3"})

	jobSpecB, err := job.NewSpecBuilder(jobVersion, jobBName, jobOwner, jobSchedule, jobWindow, jobTask).
		WithDescription(jobDescription).
		WithLabels(jobLabels).
		WithHooks(jobHooks).
		WithAlerts(jobAlerts).
		WithAsset(jobAsset).
		WithMetadata(jobMetadata).
		Build()
	assert.NoError(t, err)
	jobB := job.NewJob(sampleTenant, jobSpecB, "dev.resource.sample_b", nil)

	jobs := []*job.Job{jobA, jobB}

	jobRepository := jobRepo.NewJobRepository(pool)
	addedJobs, err := jobRepository.Add(ctx, jobs)
	assert.NoError(t, err)
	assert.Nil(t, err)
	assert.EqualValues(t, jobs, addedJobs)

	upstreamB := job.NewUpstreamResolved(jobBName, "internal", "dev.resource.sample_b", sampleTenant, job.UpstreamTypeStatic, taskName, false)
	upstreams := []*job.Upstream{upstreamB}
	jobWithUpstream := job.NewWithUpstream(jobA, upstreams)
	err = jobRepository.ReplaceUpstreams(ctx, []*job.WithUpstream{jobWithUpstream})
	assert.Nil(t, err)
	jobMap := make(map[string]*job.Job)
	for _, jobSpec := range jobs {
		jobMap[jobSpec.GetName()] = jobSpec
	}
	return jobMap
}

func compareEqualJob(j *job.Job, s *scheduler.Job) bool {
	return j.GetName() == s.Name.String() &&
		j.Tenant() == s.Tenant &&
		j.Destination().String() == s.Destination &&
		j.Spec().Task().Name().String() == s.Task.Name
}

func compareEqualJobWithDetails(j *job.Job, s *scheduler.JobWithDetails) bool {
	return compareEqualJob(j, s.Job) &&
		j.GetName() == s.Name.String() &&
		j.Spec().Version() == s.JobMetadata.Version &&
		j.Spec().Owner() == s.JobMetadata.Owner &&
		j.Spec().Schedule().Interval() == s.Schedule.Interval &&
		j.Spec().Schedule().DependsOnPast() == s.Schedule.DependsOnPast &&
		j.Spec().Schedule().Retry().ExponentialBackoff() == s.Retry.ExponentialBackoff
}
