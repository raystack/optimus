//go:build !unit_test
// +build !unit_test

package bench

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/models"
	jobRepository "github.com/odpf/optimus/internal/store/postgres/job"
	tenantRepository "github.com/odpf/optimus/internal/store/postgres/tenant"
	"github.com/odpf/optimus/tests/setup"
)

func BenchmarkJobRepository(b *testing.B) {
	ctx := context.Background()

	proj, err := tenant.NewProject("test-proj",
		map[string]string{
			"bucket":                     "gs://some_folder-2",
			tenant.ProjectSchedulerHost:  "host",
			tenant.ProjectStoragePathKey: "gs://location",
		})
	assert.NoError(b, err)
	namespace, err := tenant.NewNamespace("test-ns", proj.Name(),
		map[string]string{
			"bucket": "gs://ns_bucket",
		})
	assert.NoError(b, err)
	assert.NoError(b, err)
	tnnt, err := tenant.NewTenant(proj.Name().String(), namespace.Name().String())
	assert.NoError(b, err)

	dbSetup := func() *gorm.DB {
		dbConn := setup.TestDB()
		setup.TruncateTables(dbConn)

		projRepo := tenantRepository.NewProjectRepository(dbConn)
		if err := projRepo.Save(ctx, proj); err != nil {
			panic(err)
		}

		namespaceRepo := tenantRepository.NewNamespaceRepository(dbConn)
		if err := namespaceRepo.Save(ctx, namespace); err != nil {
			panic(err)
		}
		return dbConn
	}

	b.Run("Add", func(b *testing.B) {
		db := dbSetup()
		repo := jobRepository.NewJobRepository(db)

		b.ResetTimer()

		maxNumberOfJobs := 50
		for i := 0; i < b.N; i++ {
			jobs := make([]*job.Job, maxNumberOfJobs)
			for j := 0; j < maxNumberOfJobs; j++ {
				name := fmt.Sprintf("job_test_%d_%d", i, j)
				jobName, err := job.NameFrom(name)
				assert.NoError(b, err)
				destination := job.ResourceURN("dev.resource.sample")
				jobs[j] = setup.Job(tnnt, jobName, destination)
			}

			actualStoredJobs, actualError := repo.Add(ctx, jobs)
			assert.Len(b, actualStoredJobs, maxNumberOfJobs)
			assert.NoError(b, actualError)
		}
	})

	b.Run("Update", func(b *testing.B) {
		db := dbSetup()
		repo := jobRepository.NewJobRepository(db)
		maxNumberOfJobs := 50
		jobs := make([]*job.Job, maxNumberOfJobs)
		for i := 0; i < maxNumberOfJobs; i++ {
			name := fmt.Sprintf("job_test_%d", i)
			jobName, err := job.NameFrom(name)
			assert.NoError(b, err)
			destination := job.ResourceURN("dev.resource.sample")
			jobs[i] = setup.Job(tnnt, jobName, destination)
		}
		storedJobs, err := repo.Add(ctx, jobs)
		assert.Len(b, storedJobs, maxNumberOfJobs)
		assert.NoError(b, err)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			actualUpdatedJobs, actualError := repo.Update(ctx, jobs)
			assert.Len(b, actualUpdatedJobs, maxNumberOfJobs)
			assert.NoError(b, actualError)
		}
	})

	b.Run("ResolveUpstreams", func(b *testing.B) {
		db := dbSetup()
		repo := jobRepository.NewJobRepository(db)
		maxNumberOfJobs := 50
		for i := 0; i < maxNumberOfJobs; i++ {
			name := fmt.Sprintf("job_treated_as_static_upstream_%d", i)
			staticUpstreamName, err := job.NameFrom(name)
			assert.NoError(b, err)
			staticUpstreamDestination := job.ResourceURN(fmt.Sprintf("dev.resource.sample_static_upstream_%d", i))
			jobTreatedAsStaticUpstream := setup.Job(tnnt, staticUpstreamName, staticUpstreamDestination)

			name = fmt.Sprintf("job_treated_as_inferred_upstream_%d", i)
			inferredUpstreamName, err := job.NameFrom(name)
			assert.NoError(b, err)
			inferredUpstreamDestination := job.ResourceURN(fmt.Sprintf("dev.resource.sample_inferred_upstream_%d", i))
			jobTreatedAsInferredUpstream := setup.Job(tnnt, inferredUpstreamName, inferredUpstreamDestination)

			version, err := job.VersionFrom(1)
			assert.NoError(b, err)
			name = fmt.Sprintf("current_job_%d", i)
			currentJobName, err := job.NameFrom(name)
			assert.NoError(b, err)
			owner, err := job.OwnerFrom("dev_test")
			assert.NoError(b, err)
			retry := job.NewRetry(5, 0, false)
			startDate, err := job.ScheduleDateFrom("2022-10-01")
			assert.NoError(b, err)
			schedule, err := job.NewScheduleBuilder(startDate).WithRetry(retry).Build()
			assert.NoError(b, err)
			window, err := models.NewWindow(version.Int(), "d", "24h", "24h")
			assert.NoError(b, err)
			taskConfig, err := job.NewConfig(map[string]string{"sample_task_key": "sample_value"})
			assert.NoError(b, err)
			task := job.NewTaskBuilder("bq2bq", taskConfig).Build()

			specUpstream, err := job.NewSpecUpstreamBuilder().
				WithUpstreamNames([]job.SpecUpstreamName{
					job.SpecUpstreamNameFrom(staticUpstreamName.String()),
				}).Build()
			assert.NoError(b, err)
			spec := job.NewSpecBuilder(version, currentJobName, owner, schedule, window, task).
				WithSpecUpstream(specUpstream).
				Build()
			currentDestination := job.ResourceURN(fmt.Sprintf("dev.resource.sample_current_job_%d", i))
			currentJob := job.NewJob(tnnt, spec, currentDestination, []job.ResourceURN{inferredUpstreamDestination})

			storedJobs, err := repo.Add(ctx, []*job.Job{
				jobTreatedAsStaticUpstream,
				jobTreatedAsInferredUpstream,
				currentJob,
			})
			assert.Len(b, storedJobs, 3)
			assert.NoError(b, err)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			jobIdx := i % maxNumberOfJobs
			name := fmt.Sprintf("current_job_%d", jobIdx)
			currentJobName, err := job.NameFrom(name)
			assert.NoError(b, err)

			actualUpstreamsPerJobName, actualError := repo.ResolveUpstreams(ctx, proj.Name(), []job.Name{currentJobName})
			assert.Len(b, actualUpstreamsPerJobName, 1)
			assert.Len(b, actualUpstreamsPerJobName[currentJobName], 2)
			assert.NoError(b, actualError)
		}
	})
}
