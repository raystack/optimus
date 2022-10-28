//go:build !unit_test

package job_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/dto"
	"github.com/odpf/optimus/core/tenant"
	postgres "github.com/odpf/optimus/internal/store/postgres/job"
	tenantPostgres "github.com/odpf/optimus/internal/store/postgres/tenant"
	"github.com/odpf/optimus/tests/setup"
)

func TestPostgresJobDependencyRepository(t *testing.T) {
	ctx := context.Background()

	proj, _ := tenant.NewProject("test-proj",
		map[string]string{
			"bucket":                     "gs://some_folder-2",
			tenant.ProjectSchedulerHost:  "host",
			tenant.ProjectStoragePathKey: "gs://location",
		})
	namespace, _ := tenant.NewNamespace("test-ns", proj.Name(),
		map[string]string{
			"bucket": "gs://ns_bucket",
		})
	otherNamespace, _ := tenant.NewNamespace("other-ns", proj.Name(),
		map[string]string{
			"bucket": "gs://ns_bucket",
		})

	dbSetup := func() *gorm.DB {
		dbConn := setup.TestDB()
		setup.TruncateTables(dbConn)

		projRepo := tenantPostgres.NewProjectRepository(dbConn)
		assert.Nil(t, projRepo.Save(ctx, proj))

		namespaceRepo := tenantPostgres.NewNamespaceRepository(dbConn)
		assert.Nil(t, namespaceRepo.Save(ctx, namespace))
		assert.Nil(t, namespaceRepo.Save(ctx, otherNamespace))

		return dbConn
	}

	t.Run("Save", func(t *testing.T) {
		t.Run("inserts job dependency", func(t *testing.T) {
			db := dbSetup()

			sampleTenant, err := tenant.NewTenant(proj.Name().String(), namespace.Name().String())
			assert.Nil(t, err)

			dependencyB := dto.NewDependency("jobB", sampleTenant, "", "resource-B")
			dependencyC := dto.NewDependency("jobC", sampleTenant, "", "resource-C")
			dependencies := []*dto.Dependency{dependencyB, dependencyC}
			jobWithDependency := job.NewWithDependency("jobA", proj.Name(), dependencies, nil)

			jobDependencyRepo := postgres.NewJobDependencyRepository(db)
			assert.Nil(t, jobDependencyRepo.Save(ctx, []*job.WithDependency{jobWithDependency}))
		})
		t.Run("deletes existing job dependency and inserts", func(t *testing.T) {
			db := dbSetup()

			sampleTenant, err := tenant.NewTenant(proj.Name().String(), namespace.Name().String())
			assert.Nil(t, err)

			dependencyB := dto.NewDependency("jobB", sampleTenant, "", "resource-B")
			dependencies := []*dto.Dependency{dependencyB}
			jobWithDependency := job.NewWithDependency("jobA", proj.Name(), dependencies, nil)

			jobDependencyRepo := postgres.NewJobDependencyRepository(db)
			assert.Nil(t, jobDependencyRepo.Save(ctx, []*job.WithDependency{jobWithDependency}))

			dependencyC := dto.NewDependency("jobC", sampleTenant, "", "resource-C")
			dependencies = []*dto.Dependency{dependencyC}
			jobWithDependency = job.NewWithDependency("jobA", proj.Name(), dependencies, nil)

			assert.Nil(t, jobDependencyRepo.Save(ctx, []*job.WithDependency{jobWithDependency}))
		})
	})
}
