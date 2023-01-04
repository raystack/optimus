//go:build !unit_test
// +build !unit_test

package bench

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	serviceJob "github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/models"
	repoJob "github.com/odpf/optimus/internal/store/postgres/job"
	repoTenant "github.com/odpf/optimus/internal/store/postgres/tenant"
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
	tnnt, err := tenant.NewTenant(proj.Name().String(), namespace.Name().String())
	assert.NoError(b, err)

	dbSetup := func() *gorm.DB {
		dbConn := setup.TestDB()
		setup.TruncateTables(dbConn)

		projRepo := repoTenant.NewProjectRepository(dbConn)
		if err := projRepo.Save(ctx, proj); err != nil {
			panic(err)
		}

		namespaceRepo := repoTenant.NewNamespaceRepository(dbConn)
		if err := namespaceRepo.Save(ctx, namespace); err != nil {
			panic(err)
		}
		return dbConn
	}

	b.Run("Add", func(b *testing.B) {
		db := dbSetup()
		repo := repoJob.NewJobRepository(db)

		b.ResetTimer()

		maxNumberOfJobs := 50
		for i := 0; i < b.N; i++ {
			jobs := make([]*serviceJob.Job, maxNumberOfJobs)
			for j := 0; j < maxNumberOfJobs; j++ {
				name := fmt.Sprintf("job_test_%d_%d", i, j)
				jobName, err := serviceJob.NameFrom(name)
				assert.NoError(b, err)
				destination := serviceJob.ResourceURN("dev.resource.sample")
				jobs[j] = setup.Job(tnnt, jobName, destination)
			}

			actualStoredJobs, actualError := repo.Add(ctx, jobs)
			assert.Len(b, actualStoredJobs, maxNumberOfJobs)
			assert.NoError(b, actualError)
		}
	})

	b.Run("Update", func(b *testing.B) {
		db := dbSetup()
		repo := repoJob.NewJobRepository(db)
		maxNumberOfJobs := 50
		jobs := make([]*serviceJob.Job, maxNumberOfJobs)
		for i := 0; i < maxNumberOfJobs; i++ {
			name := fmt.Sprintf("job_test_%d", i)
			jobName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)
			destination := serviceJob.ResourceURN("dev.resource.sample")
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
		repo := repoJob.NewJobRepository(db)
		maxNumberOfJobs := 50
		for i := 0; i < maxNumberOfJobs; i++ {
			name := fmt.Sprintf("job_treated_as_static_upstream_%d", i)
			staticUpstreamName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)
			staticUpstreamDestination := serviceJob.ResourceURN(fmt.Sprintf("dev.resource.sample_static_upstream_%d", i))
			jobTreatedAsStaticUpstream := setup.Job(tnnt, staticUpstreamName, staticUpstreamDestination)

			name = fmt.Sprintf("job_treated_as_inferred_upstream_%d", i)
			inferredUpstreamName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)
			inferredUpstreamDestination := serviceJob.ResourceURN(fmt.Sprintf("dev.resource.sample_inferred_upstream_%d", i))
			jobTreatedAsInferredUpstream := setup.Job(tnnt, inferredUpstreamName, inferredUpstreamDestination)

			version, err := serviceJob.VersionFrom(1)
			assert.NoError(b, err)
			name = fmt.Sprintf("current_job_%d", i)
			currentJobName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)
			owner, err := serviceJob.OwnerFrom("dev_test")
			assert.NoError(b, err)
			retry := serviceJob.NewRetry(5, 0, false)
			startDate, err := serviceJob.ScheduleDateFrom("2022-10-01")
			assert.NoError(b, err)
			schedule, err := serviceJob.NewScheduleBuilder(startDate).WithRetry(retry).Build()
			assert.NoError(b, err)
			window, err := models.NewWindow(version.Int(), "d", "24h", "24h")
			assert.NoError(b, err)
			taskConfig, err := serviceJob.NewConfig(map[string]string{"sample_task_key": "sample_value"})
			assert.NoError(b, err)
			task := serviceJob.NewTaskBuilder("bq2bq", taskConfig).Build()

			specUpstream, err := serviceJob.NewSpecUpstreamBuilder().
				WithUpstreamNames([]serviceJob.SpecUpstreamName{
					serviceJob.SpecUpstreamNameFrom(staticUpstreamName.String()),
				}).Build()
			assert.NoError(b, err)
			spec := serviceJob.NewSpecBuilder(version, currentJobName, owner, schedule, window, task).
				WithSpecUpstream(specUpstream).
				Build()
			currentDestination := serviceJob.ResourceURN(fmt.Sprintf("dev.resource.sample_current_job_%d", i))
			currentJob := serviceJob.NewJob(tnnt, spec, currentDestination, []serviceJob.ResourceURN{inferredUpstreamDestination})

			storedJobs, err := repo.Add(ctx, []*serviceJob.Job{
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
			currentJobName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)

			actualUpstreamsPerJobName, actualError := repo.ResolveUpstreams(ctx, proj.Name(), []serviceJob.Name{currentJobName})
			assert.Len(b, actualUpstreamsPerJobName, 1)
			assert.Len(b, actualUpstreamsPerJobName[currentJobName], 2)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetByJobName", func(b *testing.B) {
		db := dbSetup()
		repo := repoJob.NewJobRepository(db)
		maxNumberOfJobs := 50
		jobs := make([]*serviceJob.Job, maxNumberOfJobs)
		for i := 0; i < maxNumberOfJobs; i++ {
			name := fmt.Sprintf("job_test_%d", i)
			jobName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)
			destination := serviceJob.ResourceURN("dev.resource.sample")
			jobs[i] = setup.Job(tnnt, jobName, destination)
		}
		storedJobs, err := repo.Add(ctx, jobs)
		assert.Len(b, storedJobs, maxNumberOfJobs)
		assert.NoError(b, err)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			jobIdx := i % maxNumberOfJobs
			name := fmt.Sprintf("job_test_%d", jobIdx)
			jobName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)

			actualJob, actualError := repo.GetByJobName(ctx, proj.Name(), jobName)
			assert.NotNil(b, actualJob)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetAllByProjectName", func(b *testing.B) {
		db := dbSetup()
		repo := repoJob.NewJobRepository(db)
		maxNumberOfJobs := 50
		jobs := make([]*serviceJob.Job, maxNumberOfJobs)
		for i := 0; i < maxNumberOfJobs; i++ {
			name := fmt.Sprintf("job_test_%d", i)
			jobName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)
			destination := serviceJob.ResourceURN("dev.resource.sample")
			jobs[i] = setup.Job(tnnt, jobName, destination)
		}
		storedJobs, err := repo.Add(ctx, jobs)
		assert.Len(b, storedJobs, maxNumberOfJobs)
		assert.NoError(b, err)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			actualJobs, actualError := repo.GetAllByProjectName(ctx, proj.Name())
			assert.Len(b, actualJobs, maxNumberOfJobs)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetAllByResourceDestination", func(b *testing.B) {
		db := dbSetup()
		repo := repoJob.NewJobRepository(db)
		maxNumberOfJobs := 50
		jobs := make([]*serviceJob.Job, maxNumberOfJobs)
		for i := 0; i < maxNumberOfJobs; i++ {
			name := fmt.Sprintf("job_test_%d", i)
			jobName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)
			destination := serviceJob.ResourceURN(fmt.Sprintf("dev.resource.sample_%d", i))
			jobs[i] = setup.Job(tnnt, jobName, destination)
		}
		storedJobs, err := repo.Add(ctx, jobs)
		assert.Len(b, storedJobs, maxNumberOfJobs)
		assert.NoError(b, err)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			jobIdx := i % maxNumberOfJobs
			destination := serviceJob.ResourceURN(fmt.Sprintf("dev.resource.sample_%d", jobIdx))

			actualJobs, actualError := repo.GetAllByResourceDestination(ctx, destination)
			assert.Len(b, actualJobs, 1)
			assert.NoError(b, actualError)
		}
	})

	b.Run("ReplaceUpstreams", func(b *testing.B) {
		db := dbSetup()
		repo := repoJob.NewJobRepository(db)
		maxNumberOfJobs := 50
		jobs := make([]*serviceJob.Job, maxNumberOfJobs)
		for i := 0; i < maxNumberOfJobs; i++ {
			name := fmt.Sprintf("job_test_%d", i)
			jobName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)
			destination := serviceJob.ResourceURN(fmt.Sprintf("dev.resource.sample_%d", i))
			jobs[i] = setup.Job(tnnt, jobName, destination)
		}
		storedJobs, err := repo.Add(ctx, jobs)
		assert.Len(b, storedJobs, maxNumberOfJobs)
		assert.NoError(b, err)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			withUpstreams := make([]*serviceJob.WithUpstream, maxNumberOfJobs)
			for j := 0; j < maxNumberOfJobs; j++ {
				jobIdx := i % maxNumberOfJobs
				currentJob := jobs[jobIdx]
				currentJobName := currentJob.Spec().Name()

				maxNumberOfUpstreams := 50
				upstreams := make([]*serviceJob.Upstream, maxNumberOfUpstreams)
				for k := 0; k < maxNumberOfUpstreams; k++ {
					resourceURN := serviceJob.ResourceURN(fmt.Sprintf("dev.resource.resource_%d_%d_%d", i, j, k))
					upstream := serviceJob.NewUpstreamResolved(currentJobName, "http://optimus.io", resourceURN, tnnt, serviceJob.UpstreamTypeInferred, "bq2bq", false)
					upstreams[k] = upstream
				}
				withUpstreams[j] = serviceJob.NewWithUpstream(currentJob, upstreams)
			}

			actualError := repo.ReplaceUpstreams(ctx, withUpstreams)
			assert.NoError(b, actualError)
		}
	})

	b.Run("Delete", func(b *testing.B) {
		db := dbSetup()
		repo := repoJob.NewJobRepository(db)
		maxNumberOfJobs := 50
		jobs := make([]*serviceJob.Job, maxNumberOfJobs)
		for i := 0; i < maxNumberOfJobs; i++ {
			name := fmt.Sprintf("job_test_%d", i)
			jobName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)
			destination := serviceJob.ResourceURN("dev.resource.sample")
			jobs[i] = setup.Job(tnnt, jobName, destination)
		}
		storedJobs, err := repo.Add(ctx, jobs)
		assert.Len(b, storedJobs, maxNumberOfJobs)
		assert.NoError(b, err)

		b.ResetTimer()

		for i := b.N; i > 0; i-- {
			var cleanHistory bool
			if i < maxNumberOfJobs {
				cleanHistory = true
			}
			jobIdx := i % maxNumberOfJobs
			name := fmt.Sprintf("job_test_%d", jobIdx)
			jobName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)

			actualError := repo.Delete(ctx, tnnt.ProjectName(), jobName, cleanHistory)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetAllByTenant", func(b *testing.B) {
		db := dbSetup()
		repo := repoJob.NewJobRepository(db)
		maxNumberOfJobs := 50
		jobs := make([]*serviceJob.Job, maxNumberOfJobs)
		for i := 0; i < maxNumberOfJobs; i++ {
			name := fmt.Sprintf("job_test_%d", i)
			jobName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)
			destination := serviceJob.ResourceURN("dev.resource.sample")
			jobs[i] = setup.Job(tnnt, jobName, destination)
		}
		storedJobs, err := repo.Add(ctx, jobs)
		assert.Len(b, storedJobs, maxNumberOfJobs)
		assert.NoError(b, err)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			actualJobs, actualError := repo.GetAllByTenant(ctx, tnnt)
			assert.Len(b, actualJobs, maxNumberOfJobs)
			assert.NoError(b, actualError)
		}
	})
}
