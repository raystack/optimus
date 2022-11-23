package bench

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	serviceResource "github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/ext/store/bigquery"
	repoResource "github.com/odpf/optimus/internal/store/postgres/resource"
	"github.com/odpf/optimus/tests/setup"
)

func BenchmarkResourceRepo(b *testing.B) {
	ctx := context.Background()
	projectName := "project_test"
	namespaceName := "namespace_test"
	tnnt, err := tenant.NewTenant(projectName, namespaceName)
	assert.NoError(b, err)
	spec := map[string]any{
		"description": "spec for test",
	}
	meta := &serviceResource.Metadata{
		Version:     1,
		Description: "metadata for test",
		Labels: map[string]string{
			"orchestrator": "optimus",
		},
	}

	dbSetup := func() *gorm.DB {
		dbConn := setup.TestDB()
		setup.TruncateTables(dbConn)
		return dbConn
	}

	b.Run("Create", func(b *testing.B) {
		db := dbSetup()
		repository := repoResource.NewRepository(db)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			fullName := fmt.Sprintf("project.dataset_%d", i)
			resourceToCreate, err := serviceResource.NewResource(fullName, bigquery.KindDataset, serviceResource.Bigquery, tnnt, meta, spec)
			assert.NoError(b, err)
			urn := fmt.Sprintf("%s:%s.%s", projectName, namespaceName, fullName)
			err = resourceToCreate.UpdateURN(urn)
			assert.NoError(b, err)

			actualError := repository.Create(ctx, resourceToCreate)
			assert.NoError(b, actualError)
		}
	})

	b.Run("Update", func(b *testing.B) {
		db := dbSetup()
		repository := repoResource.NewRepository(db)
		maxNumberOfResources := 50
		for i := 0; i < maxNumberOfResources; i++ {
			fullName := fmt.Sprintf("project.dataset_%d", i)
			resourceToCreate, err := serviceResource.NewResource(fullName, bigquery.KindDataset, serviceResource.Bigquery, tnnt, meta, spec)
			assert.NoError(b, err)
			urn := fmt.Sprintf("%s:%s.%s", projectName, namespaceName, fullName)
			err = resourceToCreate.UpdateURN(urn)
			assert.NoError(b, err)

			err = repository.Create(ctx, resourceToCreate)
			assert.NoError(b, err)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			resourceIdx := i % maxNumberOfResources
			fullName := fmt.Sprintf("project.dataset_%d", resourceIdx)
			resourceToUpdate, err := serviceResource.NewResource(fullName, bigquery.KindDataset, serviceResource.Bigquery, tnnt, meta, spec)
			assert.NoError(b, err)
			urn := fmt.Sprintf("%s:%s.%s", projectName, namespaceName, fullName)
			err = resourceToUpdate.UpdateURN(urn)
			assert.NoError(b, err)

			actualError := repository.Update(ctx, resourceToUpdate)
			assert.NoError(b, actualError)
		}
	})

	b.Run("ReadByFullName", func(b *testing.B) {
		db := dbSetup()
		repository := repoResource.NewRepository(db)
		maxNumberOfResources := 50
		for i := 0; i < maxNumberOfResources; i++ {
			fullName := fmt.Sprintf("project.dataset_%d", i)
			resourceToCreate, err := serviceResource.NewResource(fullName, bigquery.KindDataset, serviceResource.Bigquery, tnnt, meta, spec)
			assert.NoError(b, err)
			urn := fmt.Sprintf("%s:%s.%s", projectName, namespaceName, fullName)
			err = resourceToCreate.UpdateURN(urn)
			assert.NoError(b, err)

			err = repository.Create(ctx, resourceToCreate)
			assert.NoError(b, err)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			resourceIdx := i % maxNumberOfResources
			fullName := fmt.Sprintf("project.dataset_%d", resourceIdx)

			actualResource, actualError := repository.ReadByFullName(ctx, tnnt, serviceResource.Bigquery, fullName)
			assert.NotNil(b, actualResource)
			assert.Equal(b, fullName, actualResource.FullName())
			assert.NoError(b, actualError)
		}
	})
}
