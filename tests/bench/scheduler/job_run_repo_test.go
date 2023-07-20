//go:build !unit_test

package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	serviceJob "github.com/raystack/optimus/core/job"
	serviceScheduler "github.com/raystack/optimus/core/scheduler"
	serviceTenant "github.com/raystack/optimus/core/tenant"
	repoJob "github.com/raystack/optimus/internal/store/postgres/job"
	repoScheduler "github.com/raystack/optimus/internal/store/postgres/scheduler"
	repoTenant "github.com/raystack/optimus/internal/store/postgres/tenant"
	"github.com/raystack/optimus/tests/setup"
)

func BenchmarkJobRunRepository(b *testing.B) {
	const maxNumberOfJobRuns = 64

	transporterKafkaBrokerKey := "KAFKA_BROKERS"
	config := map[string]string{
		"bucket":                            "gs://folder_for_test",
		transporterKafkaBrokerKey:           "192.168.1.1:8080,192.168.1.1:8081",
		serviceTenant.ProjectSchedulerHost:  "http://localhost:8082",
		serviceTenant.ProjectStoragePathKey: "gs://location",
	}
	project, err := serviceTenant.NewProject("project_for_test", config)
	assert.NoError(b, err)
	namespace, err := serviceTenant.NewNamespace("namespace_for_test", project.Name(), config)
	assert.NoError(b, err)
	tnnt, err := serviceTenant.NewTenant(project.Name().String(), namespace.Name().String())
	assert.NoError(b, err)

	ctx := context.Background()

	dbSetup := func(b *testing.B) *pgxpool.Pool {
		b.Helper()

		pool := setup.TestPool()
		setup.TruncateTablesWith(pool)

		projectRepo := repoTenant.NewProjectRepository(pool)
		err := projectRepo.Save(ctx, project)
		assert.NoError(b, err)

		namespaceRepo := repoTenant.NewNamespaceRepository(pool)
		err = namespaceRepo.Save(ctx, namespace)
		assert.NoError(b, err)

		return pool
	}

	b.Run("Create", func(b *testing.B) {
		db := dbSetup(b)
		jobRepo := repoJob.NewJobRepository(db)
		schedulerJobRunRepo := repoScheduler.NewJobRunRepository(db)

		job := setup.NewDummyJobBuilder().Build(tnnt)
		storedJobs, err := jobRepo.Add(ctx, []*serviceJob.Job{job})
		assert.Len(b, storedJobs, 1)
		assert.NoError(b, err)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			jobNameForJobRun, err := serviceScheduler.JobNameFrom(job.GetName())
			assert.NoError(b, err)

			scheduledAt := time.Now().Add(time.Second * time.Duration(i))

			actualError := schedulerJobRunRepo.Create(ctx, tnnt, jobNameForJobRun, scheduledAt, int64(time.Second))
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetByScheduledAt", func(b *testing.B) {
		db := dbSetup(b)
		jobRepo := repoJob.NewJobRepository(db)
		schedulerJobRunRepo := repoScheduler.NewJobRunRepository(db)

		job := setup.NewDummyJobBuilder().Build(tnnt)
		storedJobs, err := jobRepo.Add(ctx, []*serviceJob.Job{job})
		assert.Len(b, storedJobs, 1)
		assert.NoError(b, err)

		scheduledAts := make([]time.Time, maxNumberOfJobRuns)
		for i := 0; i < maxNumberOfJobRuns; i++ {
			jobNameForJobRun, err := serviceScheduler.JobNameFrom(job.GetName())
			assert.NoError(b, err)

			scheduledAt := time.Now().Add(time.Second * time.Duration(i))

			actualError := schedulerJobRunRepo.Create(ctx, tnnt, jobNameForJobRun, scheduledAt, int64(time.Second))
			assert.NoError(b, actualError)

			scheduledAts[i] = scheduledAt
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			jobRunIdx := i % maxNumberOfJobRuns
			jobNameForJobRun, err := serviceScheduler.JobNameFrom(job.GetName())
			assert.NoError(b, err)

			actualJobRun, actualError := schedulerJobRunRepo.GetByScheduledAt(ctx, tnnt, jobNameForJobRun, scheduledAts[jobRunIdx])
			assert.NotNil(b, actualJobRun)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetByID", func(b *testing.B) {
		db := dbSetup(b)
		jobRepo := repoJob.NewJobRepository(db)
		schedulerJobRunRepo := repoScheduler.NewJobRunRepository(db)

		job := setup.NewDummyJobBuilder().Build(tnnt)
		storedJobs, err := jobRepo.Add(ctx, []*serviceJob.Job{job})
		assert.Len(b, storedJobs, 1)
		assert.NoError(b, err)

		jobRunIDs := make([]uuid.UUID, maxNumberOfJobRuns)
		for i := 0; i < maxNumberOfJobRuns; i++ {
			jobNameForJobRun, err := serviceScheduler.JobNameFrom(job.GetName())
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
			jobRunIdx := i % maxNumberOfJobRuns
			id := jobRunIDs[jobRunIdx]

			actualJobRun, actualError := schedulerJobRunRepo.GetByID(ctx, serviceScheduler.JobRunID(id))
			assert.NotNil(b, actualJobRun)
			assert.NoError(b, actualError)
		}
	})

	b.Run("Update", func(b *testing.B) {
		db := dbSetup(b)
		jobRepo := repoJob.NewJobRepository(db)
		schedulerJobRunRepo := repoScheduler.NewJobRunRepository(db)

		job := setup.NewDummyJobBuilder().Build(tnnt)
		storedJobs, err := jobRepo.Add(ctx, []*serviceJob.Job{job})
		assert.Len(b, storedJobs, 1)
		assert.NoError(b, err)

		jobNameForJobRun, err := serviceScheduler.JobNameFrom(job.GetName())
		assert.NoError(b, err)

		scheduledAt := time.Now()
		actualError := schedulerJobRunRepo.Create(ctx, tnnt, jobNameForJobRun, scheduledAt, int64(time.Second))
		assert.NoError(b, actualError)

		storedJobRun, err := schedulerJobRunRepo.GetByScheduledAt(ctx, tnnt, jobNameForJobRun, scheduledAt)
		assert.NoError(b, err)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			endTime := time.Now().Add(time.Second * time.Duration(i))

			actualError := schedulerJobRunRepo.Update(ctx, storedJobRun.ID, endTime, serviceScheduler.StateAccepted)
			assert.NoError(b, actualError)
		}
	})

	b.Run("UpdateSLA", func(b *testing.B) {
		db := dbSetup(b)
		jobRepo := repoJob.NewJobRepository(db)
		schedulerJobRunRepo := repoScheduler.NewJobRunRepository(db)

		job := setup.NewDummyJobBuilder().Build(tnnt)
		storedJobs, err := jobRepo.Add(ctx, []*serviceJob.Job{job})
		assert.Len(b, storedJobs, 1)
		assert.NoError(b, err)

		scheduledAts := make([]time.Time, maxNumberOfJobRuns)
		for i := 0; i < maxNumberOfJobRuns; i++ {
			jobNameForJobRun, err := serviceScheduler.JobNameFrom(job.GetName())
			assert.NoError(b, err)

			scheduledAt := time.Now().Add(time.Second * time.Duration(i))

			actualError := schedulerJobRunRepo.Create(ctx, tnnt, jobNameForJobRun, scheduledAt, int64(time.Second))
			assert.NoError(b, actualError)

			scheduledAts[i] = scheduledAt
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			jobRunIdx := i % maxNumberOfJobRuns
			jobNameForJobRun, err := serviceScheduler.JobNameFrom(job.GetName())
			assert.NoError(b, err)

			slaObject := serviceScheduler.SLAObject{
				JobName:        jobNameForJobRun,
				JobScheduledAt: scheduledAts[jobRunIdx],
			}

			actualError := schedulerJobRunRepo.UpdateSLA(ctx, []*serviceScheduler.SLAObject{&slaObject})
			assert.NoError(b, actualError)
		}
	})
}
