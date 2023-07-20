//go:build !unit_test

package resource

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	serviceResource "github.com/raystack/optimus/core/resource"
	serviceTenant "github.com/raystack/optimus/core/tenant"
	"github.com/raystack/optimus/ext/store/bigquery"
	repoResource "github.com/raystack/optimus/internal/store/postgres/resource"
	repoTenant "github.com/raystack/optimus/internal/store/postgres/tenant"
	"github.com/raystack/optimus/tests/setup"
)

func BenchmarkResourceRepository(b *testing.B) {
	const maxNumberOfResources = 64

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

	b.Run("Create", func(b *testing.B) {
		db := dbSetup(b)
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
		db := dbSetup(b)
		repository := repoResource.NewRepository(db)
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

		updatedMeta := &serviceResource.Metadata{
			Version:     1,
			Description: "updated metadata for test",
			Labels: map[string]string{
				"orchestrator": "optimus",
			},
		}
		resourcesToUpdate := make([]*serviceResource.Resource, maxNumberOfResources)
		for i := 0; i < maxNumberOfResources; i++ {
			resourceIdx := i % maxNumberOfResources
			fullName := fullNames[resourceIdx]

			resourceToUpdate, err := serviceResource.NewResource(fullName, bigquery.KindDataset, serviceResource.Bigquery, tnnt, updatedMeta, spec)
			assert.NoError(b, err)

			urn := fmt.Sprintf("%s:%s.%s", projectName, namespaceName, fullName)
			err = resourceToUpdate.UpdateURN(urn)
			assert.NoError(b, err)

			resourcesToUpdate[i] = resourceToUpdate
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			resourceIdx := i % maxNumberOfResources
			resourceToUpdate := resourcesToUpdate[resourceIdx]

			actualError := repository.Update(ctx, resourceToUpdate)
			assert.NoError(b, actualError)
		}
	})

	b.Run("ReadByFullName", func(b *testing.B) {
		db := dbSetup(b)
		repository := repoResource.NewRepository(db)
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
			resourceIdx := i % maxNumberOfResources
			fullName := fullNames[resourceIdx]

			actualResource, actualError := repository.ReadByFullName(ctx, tnnt, serviceResource.Bigquery, fullName)
			assert.NotNil(b, actualResource)
			assert.Equal(b, fullName, actualResource.FullName())
			assert.NoError(b, actualError)
		}
	})

	b.Run("ReadAll", func(b *testing.B) {
		db := dbSetup(b)
		repository := repoResource.NewRepository(db)
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
		db := dbSetup(b)
		repository := repoResource.NewRepository(db)
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
		db := dbSetup(b)
		repository := repoResource.NewRepository(db)
		resources := make([]*serviceResource.Resource, maxNumberOfResources)
		for i := 0; i < maxNumberOfResources; i++ {
			fullName := fmt.Sprintf("project.dataset_%d", i)
			resourceToCreate, err := serviceResource.NewResource(fullName, bigquery.KindDataset, serviceResource.Bigquery, tnnt, meta, spec)
			assert.NoError(b, err)

			urn := fmt.Sprintf("%s:%s.%s", projectName, namespaceName, fullName)
			err = resourceToCreate.UpdateURN(urn)
			assert.NoError(b, err)

			err = repository.Create(ctx, resourceToCreate)
			assert.NoError(b, err)

			resources[i] = resourceToCreate
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			incomingResources := make([]*serviceResource.Resource, maxNumberOfResources)
			for j := 0; j < maxNumberOfResources; j++ {
				resourceToUpdate := resources[j]
				incomingResources[j] = serviceResource.FromExisting(resourceToUpdate, serviceResource.ReplaceStatus(serviceResource.StatusSuccess))
			}

			actualError := repository.UpdateStatus(ctx, incomingResources...)
			assert.NoError(b, actualError)
		}
	})
}
