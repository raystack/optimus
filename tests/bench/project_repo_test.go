//go:build !unit_test
// +build !unit_test

package bench

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

func BenchmarkProjectRepository(b *testing.B) {
	ctx := context.Background()
	dbSetup := func() *gorm.DB {
		dbConn := setup.TestDB()
		setup.TruncateTables(dbConn)

		return dbConn
	}

	transporterKafkaBrokerKey := "KAFKA_BROKERS"
	config := map[string]string{
		"bucket":                            "gs://some_folder-2",
		transporterKafkaBrokerKey:           "10.12.12.12:6668,10.12.12.13:6668",
		serviceTenant.ProjectSchedulerHost:  "host",
		serviceTenant.ProjectStoragePathKey: "gs://location",
	}

	b.Run("Save", func(b *testing.B) {
		db := dbSetup()
		repo := repoTenant.NewProjectRepository(db)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			name := fmt.Sprintf("t-optimus-%d", i)
			project, err := serviceTenant.NewProject(name, config)
			assert.NoError(b, err)

			actualError := repo.Save(ctx, project)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetByName", func(b *testing.B) {
		db := dbSetup()
		repo := repoTenant.NewProjectRepository(db)
		maxNumberOfProjects := 50
		for i := 0; i < maxNumberOfProjects; i++ {
			name := fmt.Sprintf("t-optimus-%d", i)
			project, err := serviceTenant.NewProject(name, config)
			assert.NoError(b, err)

			actualError := repo.Save(ctx, project)
			assert.NoError(b, actualError)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			projectIdx := i % maxNumberOfProjects
			name := fmt.Sprintf("t-optimus-%d", projectIdx)
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
		maxNumberOfProjects := 50
		for i := 0; i < maxNumberOfProjects; i++ {
			name := fmt.Sprintf("t-optimus-%d", i)
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
