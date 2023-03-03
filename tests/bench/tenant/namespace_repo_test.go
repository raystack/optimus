//go:build !unit_test

package tenant

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	serviceTenant "github.com/goto/optimus/core/tenant"
	repoTenant "github.com/goto/optimus/internal/store/postgres/tenant"
	"github.com/goto/optimus/tests/setup"
)

func BenchmarkNamespaceRepository(b *testing.B) {
	const maxNumberOfNamespaces = 64

	transporterKafkaBrokerKey := "KAFKA_BROKERS"
	config := map[string]string{
		"bucket":                            "gs://folder_for_test",
		transporterKafkaBrokerKey:           "192.168.1.1:8080,192.168.1.1:8081",
		serviceTenant.ProjectSchedulerHost:  "http://localhost:8082",
		serviceTenant.ProjectStoragePathKey: "gs://location",
	}
	project, err := serviceTenant.NewProject("project_for_test", config)
	assert.NoError(b, err)

	ctx := context.Background()

	dbSetup := func(b *testing.B) *pgxpool.Pool {
		b.Helper()

		pool := setup.TestPool()
		setup.TruncateTablesWith(pool)

		prjRepo := repoTenant.NewProjectRepository(pool)
		err := prjRepo.Save(ctx, project)
		assert.NoError(b, err)

		return pool
	}

	b.Run("Save", func(b *testing.B) {
		db := dbSetup(b)
		repo := repoTenant.NewNamespaceRepository(db)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			name := fmt.Sprintf("namespace_for_test_%d", i)
			namespace, err := serviceTenant.NewNamespace(name, project.Name(), config)
			assert.NoError(b, err)

			actualError := repo.Save(ctx, namespace)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetByName", func(b *testing.B) {
		db := dbSetup(b)
		repo := repoTenant.NewNamespaceRepository(db)
		namespaceNames := make([]string, maxNumberOfNamespaces)
		for i := 0; i < maxNumberOfNamespaces; i++ {
			name := fmt.Sprintf("namespace_for_test_%d", i)
			namespace, err := serviceTenant.NewNamespace(name, project.Name(), config)
			assert.NoError(b, err)

			actualError := repo.Save(ctx, namespace)
			assert.NoError(b, actualError)

			namespaceNames[i] = name
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			namespaceIdx := i % maxNumberOfNamespaces
			name := namespaceNames[namespaceIdx]
			namespaceName, err := serviceTenant.NamespaceNameFrom(name)
			assert.NoError(b, err)

			actualNamespace, actualError := repo.GetByName(ctx, project.Name(), namespaceName)
			assert.NotNil(b, actualNamespace)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetAll", func(b *testing.B) {
		db := dbSetup(b)
		repo := repoTenant.NewNamespaceRepository(db)
		for i := 0; i < maxNumberOfNamespaces; i++ {
			name := fmt.Sprintf("namespace_for_test_%d", i)
			namespace, err := serviceTenant.NewNamespace(name, project.Name(), config)
			assert.NoError(b, err)

			actualError := repo.Save(ctx, namespace)
			assert.NoError(b, actualError)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			actualNamespaces, actualError := repo.GetAll(ctx, project.Name())
			assert.NotNil(b, actualNamespaces)
			assert.NoError(b, actualError)
		}
	})
}
