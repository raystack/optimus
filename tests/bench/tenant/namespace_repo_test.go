//go:build !unit_test
// +build !unit_test

package tenant

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	serviceTenant "github.com/odpf/optimus/core/tenant"
	repoTenant "github.com/odpf/optimus/internal/store/postgres/tenant"
	"github.com/odpf/optimus/tests/setup"
)

func BenchmarkNamespaceRepository(b *testing.B) {
	transporterKafkaBrokerKey := "KAFKA_BROKERS"
	proj, err := serviceTenant.NewProject("t-optimus-1",
		map[string]string{
			"bucket":                            "gs://some_folder-2",
			transporterKafkaBrokerKey:           "10.12.12.12:6668,10.12.12.13:6668",
			serviceTenant.ProjectSchedulerHost:  "host",
			serviceTenant.ProjectStoragePathKey: "gs://location",
		})
	assert.NoError(b, err)

	ctx := context.Background()
	dbSetup := func() *gorm.DB {
		dbConn := setup.TestDB()
		setup.TruncateTables(dbConn)

		prjRepo := repoTenant.NewProjectRepository(dbConn)
		err := prjRepo.Save(ctx, proj)
		if err != nil {
			panic(err)
		}

		return dbConn
	}

	b.Run("Save", func(b *testing.B) {
		db := dbSetup()
		repo := repoTenant.NewNamespaceRepository(db)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			name := fmt.Sprintf("n-optimus-%d", i)
			ns, err := serviceTenant.NewNamespace(name, proj.Name(),
				map[string]string{
					"bucket": "gs://ns_bucket",
				})
			assert.NoError(b, err)

			actualError := repo.Save(ctx, ns)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetByName", func(b *testing.B) {
		db := dbSetup()
		repo := repoTenant.NewNamespaceRepository(db)
		maxNumberOfNamespaces := 50
		for i := 0; i < maxNumberOfNamespaces; i++ {
			name := fmt.Sprintf("t-optimus-%d", i)
			ns, err := serviceTenant.NewNamespace(name, proj.Name(),
				map[string]string{
					"bucket": "gs://ns_bucket",
				})
			assert.NoError(b, err)

			actualError := repo.Save(ctx, ns)
			assert.NoError(b, actualError)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			namespaceIdx := i % maxNumberOfNamespaces
			name := fmt.Sprintf("t-optimus-%d", namespaceIdx)
			namespaceName, err := serviceTenant.NamespaceNameFrom(name)
			assert.NoError(b, err)

			actualNamespace, actualError := repo.GetByName(ctx, proj.Name(), namespaceName)
			assert.NotNil(b, actualNamespace)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetAll", func(b *testing.B) {
		db := dbSetup()
		repo := repoTenant.NewNamespaceRepository(db)
		maxNumberOfNamespaces := 50
		for i := 0; i < maxNumberOfNamespaces; i++ {
			name := fmt.Sprintf("t-optimus-%d", i)
			ns, err := serviceTenant.NewNamespace(name, proj.Name(),
				map[string]string{
					"bucket": "gs://ns_bucket",
				})
			assert.NoError(b, err)

			actualError := repo.Save(ctx, ns)
			assert.NoError(b, actualError)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			actualNamespaces, actualError := repo.GetAll(ctx, proj.Name())
			assert.NotNil(b, actualNamespaces)
			assert.NoError(b, actualError)
		}
	})
}
