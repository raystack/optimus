//go:build !unit_test

package job

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	serviceJob "github.com/odpf/optimus/core/job"
	serviceTenant "github.com/odpf/optimus/core/tenant"
	repoJob "github.com/odpf/optimus/internal/store/postgres/job"
	repoTenant "github.com/odpf/optimus/internal/store/postgres/tenant"
	"github.com/odpf/optimus/tests/setup"
)

func BenchmarkJobRepository(b *testing.B) {
	const maxNumberOfJobs = 64
	const maxNumberOfUpstreams = 64
	const maxNumberOfDownstreams = 64

	projectName := "project_test"
	transporterKafkaBrokerKey := "KAFKA_BROKERS"
	config := map[string]string{
		"bucket":                            "gs://folder_for_test",
		transporterKafkaBrokerKey:           "192.168.1.1:8080,192.168.1.1:8081",
		serviceTenant.ProjectSchedulerHost:  "http://localhost:8082",
		serviceTenant.ProjectStoragePathKey: "gs://location",
	}
	project, err := serviceTenant.NewProject(projectName, config)
	assert.NoError(b, err)

	namespaceName := "namespace_test"
	namespace, err := serviceTenant.NewNamespace(namespaceName, project.Name(), config)
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

	b.Run("Add", func(b *testing.B) {
		db := dbSetup(b)
		repo := repoJob.NewJobRepository(db)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			jobs := make([]*serviceJob.Job, maxNumberOfJobs)
			for j := 0; j < maxNumberOfJobs; j++ {
				name := fmt.Sprintf("job_test_%d_%d", i, j)
				jobName, err := serviceJob.NameFrom(name)
				assert.NoError(b, err)

				jobs[j] = setup.NewDummyJobBuilder().OverrideName(jobName).Build(tnnt)
			}

			actualStoredJobs, actualError := repo.Add(ctx, jobs)
			assert.Len(b, actualStoredJobs, maxNumberOfJobs)
			assert.NoError(b, actualError)
		}
	})

	b.Run("Update", func(b *testing.B) {
		db := dbSetup(b)
		repo := repoJob.NewJobRepository(db)
		jobs := make([]*serviceJob.Job, maxNumberOfJobs)
		for i := 0; i < maxNumberOfJobs; i++ {
			name := fmt.Sprintf("job_test_%d", i)
			jobName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)

			jobs[i] = setup.NewDummyJobBuilder().OverrideName(jobName).Build(tnnt)
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
		db := dbSetup(b)
		repo := repoJob.NewJobRepository(db)
		currentJobNames := make([]serviceJob.Name, maxNumberOfJobs)
		for i := 0; i < maxNumberOfJobs; i++ {
			name := fmt.Sprintf("job_treated_as_static_upstream_%d", i)
			staticUpstreamName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)

			jobTreatedAsStaticUpstream := setup.NewDummyJobBuilder().
				OverrideName(staticUpstreamName).
				Build(tnnt)

			name = fmt.Sprintf("job_treated_as_inferred_upstream_%d", i)
			inferredUpstreamName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)

			inferredUpstreamDestination := serviceJob.ResourceURN(fmt.Sprintf("dev.resource.sample_inferred_upstream_%d", i))
			jobTreatedAsInferredUpstream := setup.NewDummyJobBuilder().
				OverrideName(inferredUpstreamName).
				OverrideDestinationURN(inferredUpstreamDestination).
				Build(tnnt)

			name = fmt.Sprintf("current_job_%d", i)
			currentJobName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)

			currentJob := setup.NewDummyJobBuilder().
				OverrideName(currentJobName).
				OverrideSpecUpstreamNames([]serviceJob.SpecUpstreamName{serviceJob.SpecUpstreamNameFrom(staticUpstreamName.String())}).
				OverrideSourceURNs([]serviceJob.ResourceURN{inferredUpstreamDestination}).
				Build(tnnt)

			storedJobs, err := repo.Add(ctx, []*serviceJob.Job{
				jobTreatedAsStaticUpstream,
				jobTreatedAsInferredUpstream,
				currentJob,
			})
			assert.Len(b, storedJobs, 3)
			assert.NoError(b, err)

			currentJobNames[i] = currentJobName
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			jobIdx := i % maxNumberOfJobs
			currentJobName := currentJobNames[jobIdx]

			actualUpstreamsPerJobName, actualError := repo.ResolveUpstreams(ctx, project.Name(), []serviceJob.Name{currentJobName})
			assert.Len(b, actualUpstreamsPerJobName, 1)
			assert.Len(b, actualUpstreamsPerJobName[currentJobName], 2)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetByJobName", func(b *testing.B) {
		db := dbSetup(b)
		repo := repoJob.NewJobRepository(db)
		jobs := make([]*serviceJob.Job, maxNumberOfJobs)
		for i := 0; i < maxNumberOfJobs; i++ {
			name := fmt.Sprintf("job_test_%d", i)
			jobName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)

			jobs[i] = setup.NewDummyJobBuilder().OverrideName(jobName).Build(tnnt)
		}
		storedJobs, err := repo.Add(ctx, jobs)
		assert.Len(b, storedJobs, maxNumberOfJobs)
		assert.NoError(b, err)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			jobIdx := i % maxNumberOfJobs
			jobName := jobs[jobIdx].Spec().Name()

			actualJob, actualError := repo.GetByJobName(ctx, project.Name(), jobName)
			assert.NotNil(b, actualJob)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetAllByProjectName", func(b *testing.B) {
		db := dbSetup(b)
		repo := repoJob.NewJobRepository(db)
		jobs := make([]*serviceJob.Job, maxNumberOfJobs)
		for i := 0; i < maxNumberOfJobs; i++ {
			name := fmt.Sprintf("job_test_%d", i)
			jobName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)

			jobs[i] = setup.NewDummyJobBuilder().OverrideName(jobName).Build(tnnt)
		}
		storedJobs, err := repo.Add(ctx, jobs)
		assert.Len(b, storedJobs, maxNumberOfJobs)
		assert.NoError(b, err)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			actualJobs, actualError := repo.GetAllByProjectName(ctx, project.Name())
			assert.Len(b, actualJobs, maxNumberOfJobs)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetAllByResourceDestination", func(b *testing.B) {
		db := dbSetup(b)
		repo := repoJob.NewJobRepository(db)
		jobs := make([]*serviceJob.Job, maxNumberOfJobs)
		for i := 0; i < maxNumberOfJobs; i++ {
			name := fmt.Sprintf("job_test_%d", i)
			jobName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)

			destination := serviceJob.ResourceURN(fmt.Sprintf("dev.resource.sample_%d", i))
			jobs[i] = setup.NewDummyJobBuilder().
				OverrideName(jobName).
				OverrideDestinationURN(destination).
				Build(tnnt)
		}
		storedJobs, err := repo.Add(ctx, jobs)
		assert.Len(b, storedJobs, maxNumberOfJobs)
		assert.NoError(b, err)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			jobIdx := i % maxNumberOfJobs
			destination := jobs[jobIdx].Destination()

			actualJobs, actualError := repo.GetAllByResourceDestination(ctx, destination)
			assert.Len(b, actualJobs, 1)
			assert.NoError(b, actualError)
		}
	})

	b.Run("ReplaceUpstreams", func(b *testing.B) {
		db := dbSetup(b)
		repo := repoJob.NewJobRepository(db)
		jobs := make([]*serviceJob.Job, maxNumberOfJobs)
		for i := 0; i < maxNumberOfJobs; i++ {
			name := fmt.Sprintf("job_test_%d", i)
			jobName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)

			destination := serviceJob.ResourceURN(fmt.Sprintf("dev.resource.sample_%d", i))
			jobs[i] = setup.NewDummyJobBuilder().
				OverrideName(jobName).
				OverrideDestinationURN(destination).
				Build(tnnt)
		}
		storedJobs, err := repo.Add(ctx, jobs)
		assert.Len(b, storedJobs, maxNumberOfJobs)
		assert.NoError(b, err)

		withUpstreams := make([]*serviceJob.WithUpstream, maxNumberOfJobs)
		for i := 0; i < maxNumberOfJobs; i++ {
			job := jobs[i]
			jobName := job.Spec().Name()

			upstreams := make([]*serviceJob.Upstream, maxNumberOfUpstreams)
			for j := 0; j < maxNumberOfUpstreams; j++ {
				resourceURN := serviceJob.ResourceURN(fmt.Sprintf("dev.resource.resource_%d_%d", i, j))
				upstream := serviceJob.NewUpstreamResolved(jobName, "http://optimus.io", resourceURN, tnnt, serviceJob.UpstreamTypeInferred, "bq2bq", false)
				upstreams[j] = upstream
			}
			withUpstreams[i] = serviceJob.NewWithUpstream(job, upstreams)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			jobIdx := i % maxNumberOfJobs
			withUpstream := withUpstreams[jobIdx]

			actualError := repo.ReplaceUpstreams(ctx, []*serviceJob.WithUpstream{withUpstream})
			assert.NoError(b, actualError)
		}
	})

	b.Run("Delete", func(b *testing.B) {
		db := dbSetup(b)
		repo := repoJob.NewJobRepository(db)
		jobs := make([]*serviceJob.Job, maxNumberOfJobs)
		for i := 0; i < maxNumberOfJobs; i++ {
			name := fmt.Sprintf("job_test_%d", i)
			jobName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)

			jobs[i] = setup.NewDummyJobBuilder().
				OverrideName(jobName).
				Build(tnnt)
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
			job := jobs[jobIdx]

			actualError := repo.Delete(ctx, tnnt.ProjectName(), job.Spec().Name(), cleanHistory)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetAllByTenant", func(b *testing.B) {
		db := dbSetup(b)
		repo := repoJob.NewJobRepository(db)
		jobs := make([]*serviceJob.Job, maxNumberOfJobs)
		for i := 0; i < maxNumberOfJobs; i++ {
			name := fmt.Sprintf("job_test_%d", i)
			jobName, err := serviceJob.NameFrom(name)
			assert.NoError(b, err)

			jobs[i] = setup.NewDummyJobBuilder().
				OverrideName(jobName).
				Build(tnnt)
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

	b.Run("GetUpstreams", func(b *testing.B) {
		db := dbSetup(b)
		repo := repoJob.NewJobRepository(db)

		jobName, err := serviceJob.NameFrom("job_test")
		assert.NoError(b, err)
		currentJob := setup.NewDummyJobBuilder().OverrideName(jobName).Build(tnnt)

		_, err = repo.Add(ctx, []*serviceJob.Job{currentJob})
		assert.NoError(b, err)

		upstreams := make([]*serviceJob.Upstream, maxNumberOfUpstreams)
		for i := 0; i < maxNumberOfUpstreams; i++ {
			resourceURN := fmt.Sprintf("dev.resource.sample_%d", i)
			upstreams[i] = serviceJob.NewUpstreamUnresolvedInferred(serviceJob.ResourceURN(resourceURN))
		}
		withUpstream := serviceJob.NewWithUpstream(currentJob, upstreams)

		err = repo.ReplaceUpstreams(ctx, []*serviceJob.WithUpstream{withUpstream})
		assert.NoError(b, err)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			actualUpstreams, actualError := repo.GetUpstreams(ctx, tnnt.ProjectName(), jobName)
			assert.Len(b, actualUpstreams, maxNumberOfUpstreams)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetDownstreamByDestination", func(b *testing.B) {
		db := dbSetup(b)
		repo := repoJob.NewJobRepository(db)

		rootJobName, err := serviceJob.NameFrom("root_job")
		assert.NoError(b, err)
		rootJobDestination := serviceJob.ResourceURN("root_job_destination")
		rootJob := setup.NewDummyJobBuilder().
			OverrideName(rootJobName).
			OverrideDestinationURN(rootJobDestination).
			Build(tnnt)

		_, err = repo.Add(ctx, []*serviceJob.Job{rootJob})
		assert.NoError(b, err)

		for i := 0; i < maxNumberOfDownstreams; i++ {
			currentJobName, err := serviceJob.NameFrom(fmt.Sprintf("downstream_job_%d", i))
			assert.NoError(b, err)
			currentDestinationURN := serviceJob.ResourceURN(fmt.Sprintf("dev.resource.sample_%d", i))

			currentJob := setup.NewDummyJobBuilder().
				OverrideName(currentJobName).
				OverrideDestinationURN(currentDestinationURN).
				OverrideSourceURNs([]serviceJob.ResourceURN{rootJobDestination}).
				Build(tnnt)

			_, err = repo.Add(ctx, []*serviceJob.Job{currentJob})
			assert.NoError(b, err)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			actualDownstreams, actualError := repo.GetDownstreamByDestination(ctx, tnnt.ProjectName(), rootJobDestination)
			assert.Len(b, actualDownstreams, maxNumberOfDownstreams)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetDownstreamByJobName", func(b *testing.B) {
		db := dbSetup(b)
		repo := repoJob.NewJobRepository(db)

		rootJobName, err := serviceJob.NameFrom("root_job")
		assert.NoError(b, err)
		rootJobDestination := serviceJob.ResourceURN("root_job_destination")
		rootJob := setup.NewDummyJobBuilder().
			OverrideName(rootJobName).
			OverrideDestinationURN(rootJobDestination).
			Build(tnnt)

		_, err = repo.Add(ctx, []*serviceJob.Job{rootJob})
		assert.NoError(b, err)

		for i := 0; i < maxNumberOfDownstreams; i++ {
			currentJobName, err := serviceJob.NameFrom(fmt.Sprintf("downstream_job_%d", i))
			assert.NoError(b, err)
			currentDestinationURN := serviceJob.ResourceURN(fmt.Sprintf("dev.resource.sample_%d", i))

			currentJob := setup.NewDummyJobBuilder().
				OverrideName(currentJobName).
				OverrideDestinationURN(currentDestinationURN).
				OverrideSpecUpstreamNames([]serviceJob.SpecUpstreamName{
					serviceJob.SpecUpstreamName(rootJobName),
				}).Build(tnnt)

			_, err = repo.Add(ctx, []*serviceJob.Job{currentJob})
			assert.NoError(b, err)

			upstream := serviceJob.NewUpstreamUnresolvedStatic(rootJobName, tnnt.ProjectName())
			withUpstream := serviceJob.NewWithUpstream(currentJob, []*serviceJob.Upstream{upstream})
			err = repo.ReplaceUpstreams(ctx, []*serviceJob.WithUpstream{withUpstream})
			assert.NoError(b, err)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			actualDownstreams, actualError := repo.GetDownstreamByJobName(ctx, tnnt.ProjectName(), rootJobName)
			assert.Len(b, actualDownstreams, maxNumberOfDownstreams)
			assert.NoError(b, actualError)
		}
	})
}
