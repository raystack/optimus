//go:build !unit_test
// +build !unit_test

package scheduler

import (
	"context"
	"fmt"
	"testing"

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

func BenchmarkJobRepository(b *testing.B) {
	const maxNumberOfJobs = 64

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

	b.Run("GetJob", func(b *testing.B) {
		db := dbSetup(b)
		jobRepo := repoJob.NewJobRepository(db)
		schedulerJobRepo := repoScheduler.NewJobProviderRepository(db)
		jobs := make([]*serviceJob.Job, maxNumberOfJobs)
		for i := 0; i < maxNumberOfJobs; i++ {
			name := fmt.Sprintf("job_test_%d", i)
			jobName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)

			jobs[i] = setup.NewDummyJobBuilder().
				OverrideName(jobName).
				Build(tnnt)
		}
		storedJobs, err := jobRepo.Add(ctx, jobs)
		assert.Len(b, storedJobs, maxNumberOfJobs)
		assert.NoError(b, err)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			jobIdx := i % maxNumberOfJobs
			job := jobs[jobIdx]
			jobName, err := serviceScheduler.JobNameFrom(job.GetName())
			assert.NoError(b, err)

			actualJob, actualError := schedulerJobRepo.GetJob(ctx, project.Name(), jobName)
			assert.NotNil(b, actualJob)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetJobDetails", func(b *testing.B) {
		db := dbSetup(b)
		jobRepo := repoJob.NewJobRepository(db)
		schedulerJobRepo := repoScheduler.NewJobProviderRepository(db)

		jobs := make([]*serviceJob.Job, maxNumberOfJobs)
		for i := 0; i < maxNumberOfJobs; i++ {
			name := fmt.Sprintf("job_test_%d", i)
			jobName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)

			jobs[i] = setup.NewDummyJobBuilder().
				OverrideName(jobName).
				Build(tnnt)
		}
		storedJobs, err := jobRepo.Add(ctx, jobs)
		assert.Len(b, storedJobs, maxNumberOfJobs)
		assert.NoError(b, err)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			jobIdx := i % maxNumberOfJobs
			job := jobs[jobIdx]
			jobName, err := serviceScheduler.JobNameFrom(job.GetName())
			assert.NoError(b, err)

			actualJob, actualError := schedulerJobRepo.GetJobDetails(ctx, project.Name(), jobName)
			assert.NotNil(b, actualJob)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetAll", func(b *testing.B) {
		db := dbSetup(b)
		jobRepo := repoJob.NewJobRepository(db)
		schedulerJobRepo := repoScheduler.NewJobProviderRepository(db)

		jobs := make([]*serviceJob.Job, maxNumberOfJobs)
		for i := 0; i < maxNumberOfJobs; i++ {
			name := fmt.Sprintf("job_test_%d", i)
			jobName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)

			jobs[i] = setup.NewDummyJobBuilder().
				OverrideName(jobName).
				Build(tnnt)
		}
		storedJobs, err := jobRepo.Add(ctx, jobs)
		assert.Len(b, storedJobs, maxNumberOfJobs)
		assert.NoError(b, err)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			actualJobs, actualError := schedulerJobRepo.GetAll(ctx, project.Name())
			assert.Len(b, actualJobs, maxNumberOfJobs)
			assert.NoError(b, actualError)
		}
	})
}
