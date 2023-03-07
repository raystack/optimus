//go:build !unit_test

package tenant

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	serviceTenant "github.com/odpf/optimus/core/tenant"
	repoTenant "github.com/odpf/optimus/internal/store/postgres/tenant"
	"github.com/odpf/optimus/tests/setup"
)

func BenchmarkProjectRepository(b *testing.B) {
	const maxNumberOfProjects = 64

	ctx := context.Background()
	dbSetup := func() *pgxpool.Pool {
		pool := setup.TestPool()
		setup.TruncateTablesWith(pool)
		return pool
	}

	transporterKafkaBrokerKey := "KAFKA_BROKERS"
	config := map[string]string{
		"bucket":                            "gs://folder_for_test",
		transporterKafkaBrokerKey:           "192.168.1.1:8080,192.168.1.1:8081",
		serviceTenant.ProjectSchedulerHost:  "http://localhost:8082",
		serviceTenant.ProjectStoragePathKey: "gs://location",
	}

	b.Run("Save", func(b *testing.B) {
		db := dbSetup()
		repo := repoTenant.NewProjectRepository(db)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			name := fmt.Sprintf("project_for_test_%d", i)
			project, err := serviceTenant.NewProject(name, config)
			assert.NoError(b, err)

			actualError := repo.Save(ctx, project)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetByName", func(b *testing.B) {
		db := dbSetup()
		repo := repoTenant.NewProjectRepository(db)
		projectNames := make([]string, maxNumberOfProjects)
		for i := 0; i < maxNumberOfProjects; i++ {
			name := fmt.Sprintf("project_for_test_%d", i)
			project, err := serviceTenant.NewProject(name, config)
			assert.NoError(b, err)

			actualError := repo.Save(ctx, project)
			assert.NoError(b, actualError)

			projectNames[i] = name
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			projectIdx := i % maxNumberOfProjects
			name := projectNames[projectIdx]
			projectName, err := serviceTenant.ProjectNameFrom(name)
			assert.NoError(b, err)

			actualProject, actualError := repo.GetByName(ctx, projectName)
			assert.NotNil(b, actualProject)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetAll", func(b *testing.B) {
		db := dbSetup()
		repo := repoTenant.NewProjectRepository(db)
		for i := 0; i < maxNumberOfProjects; i++ {
			name := fmt.Sprintf("project_for_test_%d", i)
			project, err := serviceTenant.NewProject(name, config)
			assert.NoError(b, err)

			actualError := repo.Save(ctx, project)
			assert.NoError(b, actualError)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			actualProjects, actualError := repo.GetAll(ctx)
			assert.Len(b, actualProjects, maxNumberOfProjects)
			assert.NoError(b, actualError)
		}
	})
}
