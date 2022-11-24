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
				jobs[j] = setup.Job(tnnt, jobName)
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
			jobs[i] = setup.Job(tnnt, jobName)
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
}
