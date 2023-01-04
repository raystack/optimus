//go:build !unit_test

package scheduler_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/internal/models"
	jobRepo "github.com/odpf/optimus/internal/store/postgres/job"
	postgres "github.com/odpf/optimus/internal/store/postgres/scheduler"
	"github.com/odpf/optimus/tests/setup"
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
}

func dbSetup() *gorm.DB {
	dbConn := setup.TestDB()
	setup.TruncateTables(dbConn)
	return dbConn
}

func addJobs(ctx context.Context, t *testing.T, db *gorm.DB) map[string]*job.Job {
	t.Helper()
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

	//host := "sample-host"
	//upstreamType := job.UpstreamTypeInferred
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
	namespace, err := tenant.NewNamespace("test-ns", proj.Name(),
		map[string]string{
			"bucket": "gs://ns_bucket",
		})
	assert.NoError(t, err)

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
	jobSpecA := job.NewSpecBuilder(jobVersion, jobAName, jobOwner, jobSchedule, jobWindow, jobTask).
		WithDescription(jobDescription).
		WithLabels(jobLabels).
		WithHooks(jobHooks).
		WithAlerts(jobAlerts).
		WithSpecUpstream(jobUpstream).
		WithAsset(jobAsset).
		WithMetadata(jobMetadata).
		Build()
	sampleTenant, err := tenant.NewTenant(proj.Name().String(), namespace.Name().String())
	assert.NoError(t, err)
	jobA := job.NewJob(sampleTenant, jobSpecA, "dev.resource.sample_a", []job.ResourceURN{"resource-3"})

	jobSpecB := job.NewSpecBuilder(jobVersion, jobBName, jobOwner, jobSchedule, jobWindow, jobTask).
		WithDescription(jobDescription).
		WithLabels(jobLabels).
		WithHooks(jobHooks).
		WithAlerts(jobAlerts).
		WithAsset(jobAsset).
		WithMetadata(jobMetadata).
		Build()
	jobB := job.NewJob(sampleTenant, jobSpecB, "dev.resource.sample_b", nil)

	jobs := []*job.Job{jobA, jobB}

	jobRepository := jobRepo.NewJobRepository(db)
	addedJobs, err := jobRepository.Add(ctx, jobs)
	assert.NoError(t, err)
	assert.EqualValues(t, jobs, addedJobs)
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
		j.Spec().Version().Int() == s.JobMetadata.Version &&
		j.Spec().Owner().String() == s.JobMetadata.Owner &&
		j.Spec().Schedule().Interval() == s.Schedule.Interval
}
