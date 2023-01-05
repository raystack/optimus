//go:build !unit_test
// +build !unit_test

package resource

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	serviceResource "github.com/odpf/optimus/core/resource"
	serviceTenant "github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/ext/store/bigquery"
	repoResource "github.com/odpf/optimus/internal/store/postgres/resource"
	repoTenant "github.com/odpf/optimus/internal/store/postgres/tenant"
	"github.com/odpf/optimus/tests/setup"
)

func BenchmarkResourceRepository(b *testing.B) {
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

	b.Run("ReadAll", func(b *testing.B) {
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
			actualResources, actualError := repository.ReadAll(ctx, tnnt, serviceResource.Bigquery)
			assert.Len(b, actualResources, maxNumberOfResources)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetResources", func(b *testing.B) {
		db := dbSetup()
		repository := repoResource.NewRepository(db)
		maxNumberOfResources := 50
		fullNames := make([]string, maxNumberOfResources)
		for i := 0; i < maxNumberOfResources; i++ {
			fullName := fmt.Sprintf("project.dataset_%d", i)
			resourceToCreate, err := serviceResource.NewResource(fullName, bigquery.KindDataset, serviceResource.Bigquery, tnnt, meta, spec)
			assert.NoError(b, err)
			urn := fmt.Sprintf("%s:%s.%s", projectName, namespaceName, fullName)
			err = resourceToCreate.UpdateURN(urn)
			assert.NoError(b, err)

			err = repository.Create(ctx, resourceToCreate)
			assert.NoError(b, err)

			fullNames[i] = fullName
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			actualResources, actualError := repository.GetResources(ctx, tnnt, serviceResource.Bigquery, fullNames)
			assert.Len(b, actualResources, maxNumberOfResources)
			assert.NoError(b, actualError)
		}
	})

	b.Run("UpdateStatus", func(b *testing.B) {
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
			incomingResources := make([]*serviceResource.Resource, maxNumberOfResources)
			for j := 0; j < maxNumberOfResources; j++ {
				fullName := fmt.Sprintf("project.dataset_%d", j)
				resourceToCreateOrUpdate, err := serviceResource.NewResource(fullName, bigquery.KindDataset, serviceResource.Bigquery, tnnt, meta, spec)
				assert.NoError(b, err)
				urn := fmt.Sprintf("%s:%s.%s", projectName, namespaceName, fullName)
				err = resourceToCreateOrUpdate.UpdateURN(urn)
				assert.NoError(b, err)
				incomingResources[j] = serviceResource.FromExisting(resourceToCreateOrUpdate, serviceResource.ReplaceStatus(serviceResource.StatusSuccess))
			}

			actualError := repository.UpdateStatus(ctx, incomingResources...)
			assert.NoError(b, actualError)
		}
	})
}
