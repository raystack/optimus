//go:build !unit_test
// +build !unit_test

package resource

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	serviceResource "github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	repoResource "github.com/odpf/optimus/internal/store/postgres/resource"
	"github.com/odpf/optimus/tests/setup"
)

func BenchmarkBackupRepository(b *testing.B) {
	ctx := context.Background()
	projectName := "project_test"
	namespaceName := "namespace_test"
	tnnt, err := tenant.NewTenant(projectName, namespaceName)
	assert.NoError(b, err)
	resourceNames := []string{"project.dataset.resource1", "project.dataset.resource2"}
	description := "backup for benchmark testing"

	dbSetup := func() *gorm.DB {
		dbConn := setup.TestDB()
		setup.TruncateTables(dbConn)
		return dbConn
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
}
