//go:build !unit_test
// +build !unit_test

package resource

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	serviceResource "github.com/odpf/optimus/core/resource"
	serviceTenant "github.com/odpf/optimus/core/tenant"
	repoResource "github.com/odpf/optimus/internal/store/postgres/resource"
	repoTenant "github.com/odpf/optimus/internal/store/postgres/tenant"
	"github.com/odpf/optimus/tests/setup"
)

func BenchmarkBackupRepository(b *testing.B) {
	ctx := context.Background()
	projectName := "project_test"
	namespaceName := "namespace_test"
	proj, err := serviceTenant.NewProject(projectName,
		map[string]string{
			"bucket":                            "gs://some_folder-2",
			serviceTenant.ProjectSchedulerHost:  "host",
			serviceTenant.ProjectStoragePathKey: "gs://location",
		})
	assert.NoError(b, err)
	namespace, err := serviceTenant.NewNamespace(namespaceName, proj.Name(),
		map[string]string{
			"bucket": "gs://ns_bucket",
		})
	assert.NoError(b, err)
	tnnt, err := serviceTenant.NewTenant(proj.Name().String(), namespace.Name().String())
	assert.NoError(b, err)
	resourceNames := []string{"project.dataset.resource1", "project.dataset.resource2"}
	description := "backup for benchmark testing"

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
		repository := repoResource.NewBackupRepository(db)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			backup, err := serviceResource.NewBackup(serviceResource.Bigquery, tnnt, resourceNames, description, time.Now(), nil)
			assert.NoError(b, err)

			actualError := repository.Create(ctx, backup)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetAll", func(b *testing.B) {
		db := dbSetup()
		repository := repoResource.NewBackupRepository(db)
		maxNumberOfBackups := 50
		for i := 0; i < maxNumberOfBackups; i++ {
			backup, err := serviceResource.NewBackup(serviceResource.Bigquery, tnnt, resourceNames, description, time.Now(), nil)
			assert.NoError(b, err)

			err = repository.Create(ctx, backup)
			assert.NoError(b, err)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			actualBackups, actualError := repository.GetAll(ctx, tnnt, serviceResource.Bigquery)
			assert.Len(b, actualBackups, maxNumberOfBackups)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetByID", func(b *testing.B) {
		db := dbSetup()
		repository := repoResource.NewBackupRepository(db)
		maxNumberOfBackups := 50
		for i := 0; i < maxNumberOfBackups; i++ {
			backup, err := serviceResource.NewBackup(serviceResource.Bigquery, tnnt, resourceNames, description, time.Now(), nil)
			assert.NoError(b, err)

			err = repository.Create(ctx, backup)
			assert.NoError(b, err)
		}
		storedBackups, err := repository.GetAll(ctx, tnnt, serviceResource.Bigquery)
		assert.NoError(b, err)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			for j := 0; j < len(storedBackups); j++ {
				actualBackup, actualError := repository.GetByID(ctx, storedBackups[j].ID())
				assert.NotNil(b, actualBackup)
				assert.NoError(b, actualError)
			}
		}
	})
}
