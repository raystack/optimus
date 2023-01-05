//go:build !unit_test
// +build !unit_test

package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	serviceJob "github.com/odpf/optimus/core/job"
	serviceScheduler "github.com/odpf/optimus/core/scheduler"
	serviceTenant "github.com/odpf/optimus/core/tenant"
	repoJob "github.com/odpf/optimus/internal/store/postgres/job"
	repoScheduler "github.com/odpf/optimus/internal/store/postgres/scheduler"
	repoTenant "github.com/odpf/optimus/internal/store/postgres/tenant"
	"github.com/odpf/optimus/tests/setup"
)

func BenchmarkJobRunRepository(b *testing.B) {
	ctx := context.Background()

	proj, err := serviceTenant.NewProject("test-proj",
		map[string]string{
			"bucket":                            "gs://some_folder-2",
			serviceTenant.ProjectSchedulerHost:  "host",
			serviceTenant.ProjectStoragePathKey: "gs://location",
		})
	assert.NoError(b, err)
	namespace, err := serviceTenant.NewNamespace("test-ns", proj.Name(),
		map[string]string{
			"bucket": "gs://ns_bucket",
		})
	assert.NoError(b, err)
	tnnt, err := serviceTenant.NewTenant(proj.Name().String(), namespace.Name().String())
	assert.NoError(b, err)

	dbSetup := func() *pgxpool.Pool {
		pool := setup.TestPool()
		setup.TruncateTablesWith(pool)

		projRepo := repoTenant.NewProjectRepository(pool)
		if err := projRepo.Save(ctx, proj); err != nil {
			panic(err)
		}

		namespaceRepo := repoTenant.NewNamespaceRepository(pool)
		if err := namespaceRepo.Save(ctx, namespace); err != nil {
			panic(err)
		}
		return pool
	}

	b.Run("Create", func(b *testing.B) {
		db := dbSetup()
		jobRepo := repoJob.NewJobRepository(db)
		schedulerJobRunRepo := repoScheduler.NewJobRunRepository(db)

		jobName, err := serviceJob.NameFrom("job_test")
		assert.NoError(b, err)
		destination := serviceJob.ResourceURN("dev.resource.sample")
		job := setup.Job(tnnt, jobName, destination)
		storedJobs, err := jobRepo.Add(ctx, []*serviceJob.Job{job})
		assert.Len(b, storedJobs, 1)
		assert.NoError(b, err)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			jobNameForJobRun, err := serviceScheduler.JobNameFrom(jobName.String())
			assert.NoError(b, err)
			scheduledAt := time.Now().Add(time.Second * time.Duration(i))
			actualError := schedulerJobRunRepo.Create(ctx, tnnt, jobNameForJobRun, scheduledAt, int64(time.Second))
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetByScheduledAt", func(b *testing.B) {
		db := dbSetup()
		jobRepo := repoJob.NewJobRepository(db)
		schedulerJobRunRepo := repoScheduler.NewJobRunRepository(db)

		jobName, err := serviceJob.NameFrom("job_test")
		assert.NoError(b, err)
		destination := serviceJob.ResourceURN("dev.resource.sample")
		job := setup.Job(tnnt, jobName, destination)
		storedJobs, err := jobRepo.Add(ctx, []*serviceJob.Job{job})
		assert.Len(b, storedJobs, 1)
		assert.NoError(b, err)

		maxNumberOfJobRun := 50
		scheduledAts := make([]time.Time, maxNumberOfJobRun)
		for i := 0; i < maxNumberOfJobRun; i++ {
			jobNameForJobRun, err := serviceScheduler.JobNameFrom(jobName.String())
			assert.NoError(b, err)
			scheduledAt := time.Now().Add(time.Second * time.Duration(i))
			actualError := schedulerJobRunRepo.Create(ctx, tnnt, jobNameForJobRun, scheduledAt, int64(time.Second))
			assert.NoError(b, actualError)

			scheduledAts[i] = scheduledAt
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			jobRunIdx := i % maxNumberOfJobRun
			jobNameForJobRun, err := serviceScheduler.JobNameFrom(jobName.String())
			assert.NoError(b, err)

			actualJobRun, actualError := schedulerJobRunRepo.GetByScheduledAt(ctx, tnnt, jobNameForJobRun, scheduledAts[jobRunIdx])
			assert.NotNil(b, actualJobRun)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetByID", func(b *testing.B) {
		db := dbSetup()
		jobRepo := repoJob.NewJobRepository(db)
		schedulerJobRunRepo := repoScheduler.NewJobRunRepository(db)

		jobName, err := serviceJob.NameFrom("job_test")
		assert.NoError(b, err)
		destination := serviceJob.ResourceURN("dev.resource.sample")
		job := setup.Job(tnnt, jobName, destination)
		storedJobs, err := jobRepo.Add(ctx, []*serviceJob.Job{job})
		assert.Len(b, storedJobs, 1)
		assert.NoError(b, err)

		maxNumberOfJobRun := 50
		jobRunIDs := make([]uuid.UUID, maxNumberOfJobRun)
		for i := 0; i < maxNumberOfJobRun; i++ {
			jobNameForJobRun, err := serviceScheduler.JobNameFrom(jobName.String())
			assert.NoError(b, err)
			scheduledAt := time.Now().Add(time.Second * time.Duration(i))
			actualError := schedulerJobRunRepo.Create(ctx, tnnt, jobNameForJobRun, scheduledAt, int64(time.Second))
			assert.NoError(b, actualError)

			storedJobRun, err := schedulerJobRunRepo.GetByScheduledAt(ctx, tnnt, jobNameForJobRun, scheduledAt)
			assert.NoError(b, err)
			jobRunIDs[i] = storedJobRun.ID
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			jobRunIdx := i % maxNumberOfJobRun
			id := jobRunIDs[jobRunIdx]

			actualJobRun, actualError := schedulerJobRunRepo.GetByID(ctx, serviceScheduler.JobRunID(id))
			assert.NotNil(b, actualJobRun)
			assert.NoError(b, actualError)
		}
	})
}
