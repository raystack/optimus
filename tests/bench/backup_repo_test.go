//go:build !unit_test
// +build !unit_test

package bench

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
}
