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

	b.Run("GetJob", func(b *testing.B) {
		db := dbSetup()
		jobRepo := repoJob.NewJobRepository(db)
		schedulerJobRepo := repoScheduler.NewJobProviderRepository(db)

		maxNumberOfJobs := 50
		jobs := make([]*serviceJob.Job, maxNumberOfJobs)
		for i := 0; i < maxNumberOfJobs; i++ {
			name := fmt.Sprintf("job_test_%d", i)
			jobName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)
			destination := serviceJob.ResourceURN("dev.resource.sample")
			jobs[i] = setup.Job(tnnt, jobName, destination)
		}
		storedJobs, err := jobRepo.Add(ctx, jobs)
		assert.Len(b, storedJobs, maxNumberOfJobs)
		assert.NoError(b, err)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			jobIdx := i % maxNumberOfJobs
			name := fmt.Sprintf("job_test_%d", jobIdx)
			jobName, err := serviceScheduler.JobNameFrom(name)
			assert.NoError(b, err)

			actualJob, actualError := schedulerJobRepo.GetJob(ctx, proj.Name(), jobName)
			assert.NotNil(b, actualJob)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetJobDetails", func(b *testing.B) {
		db := dbSetup()
		jobRepo := repoJob.NewJobRepository(db)
		schedulerJobRepo := repoScheduler.NewJobProviderRepository(db)

		maxNumberOfJobs := 50
		jobs := make([]*serviceJob.Job, maxNumberOfJobs)
		for i := 0; i < maxNumberOfJobs; i++ {
			name := fmt.Sprintf("job_test_%d", i)
			jobName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)
			destination := serviceJob.ResourceURN("dev.resource.sample")
			jobs[i] = setup.Job(tnnt, jobName, destination)
		}
		storedJobs, err := jobRepo.Add(ctx, jobs)
		assert.Len(b, storedJobs, maxNumberOfJobs)
		assert.NoError(b, err)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			jobIdx := i % maxNumberOfJobs
			name := fmt.Sprintf("job_test_%d", jobIdx)
			jobName, err := serviceScheduler.JobNameFrom(name)
			assert.NoError(b, err)

			actualJob, actualError := schedulerJobRepo.GetJobDetails(ctx, proj.Name(), jobName)
			assert.NotNil(b, actualJob)
			assert.NoError(b, actualError)
		}
	})
}
