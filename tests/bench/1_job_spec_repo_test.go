//go:build !unit_test
// +build !unit_test

package bench

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	"github.com/odpf/optimus/internal/store/postgres"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/tests/setup"
)

func BenchmarkJobRepository(b *testing.B) {
	ctx := context.Background()
	pluginRepo := setup.InMemoryPluginRegistry()
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")
	adapter := postgres.NewAdapter(pluginRepo)

	project := setup.Project(1)
	project.ID = models.ProjectID(uuid.New())

	namespace := setup.Namespace(1, project)
	namespace.ID = uuid.New()

	bq2bq := setup.MockPluginBQ{}

	dbSetup := func() *gorm.DB {
		dbConn := setup.TestDB()
		setup.TruncateTables(dbConn)

		projRepo := postgres.NewProjectRepository(dbConn, hash)
		assert.Nil(b, projRepo.Save(ctx, project))

		nsRepo := postgres.NewNamespaceRepository(dbConn, hash)
		assert.Nil(b, nsRepo.Save(ctx, project, namespace))

		secretRepo := postgres.NewSecretRepository(dbConn, hash)
		for i := 0; i < 5; i++ {
			assert.Nil(b, secretRepo.Save(ctx, project, namespace, setup.Secret(i)))
		}

		return dbConn
	}

	b.Run("Save", func(t *testing.B) {
		db := dbSetup()

		jobSpecRepository, err := postgres.NewJobSpecRepository(db, adapter)
		if err != nil {
			panic(err)
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			dest := fmt.Sprintf("bigquery://integration:playground.table%d", i)
			jobSpec := setup.Job(i, namespace, bq2bq, nil)
			jobSpec.ResourceDestination = dest
			err := jobSpecRepository.Save(ctx, jobSpec)
			if err != nil {
				panic(err)
			}
		}
	})

	b.Run("GetByNameAndProjectName", func(t *testing.B) {
		db := dbSetup()

		jobSpecRepository, err := postgres.NewJobSpecRepository(db, adapter)
		if err != nil {
			panic(err)
		}

		for i := 0; i < 1000; i++ {
			dest := fmt.Sprintf("bigquery://integration:playground.table%d", i)
			job := setup.Job(i, namespace, bq2bq, nil)
			job.ResourceDestination = dest
			err := jobSpecRepository.Save(ctx, job)
			if err != nil {
				panic(err)
			}
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			num := i % 1000
			name := fmt.Sprintf("job_%d", num)

			jb, err := jobSpecRepository.GetByNameAndProjectName(ctx, name, project.Name, false)
			if err != nil {
				panic(err)
			}
			if jb.Name != name {
				panic("Job name does not match")
			}
		}
	})

	b.Run("GetAllByProjectName", func(t *testing.B) {
		db := dbSetup()

		jobSpecRepository, err := postgres.NewJobSpecRepository(db, adapter)
		if err != nil {
			panic(err)
		}

		for i := 0; i < 1000; i++ {
			dest := fmt.Sprintf("bigquery://integration:playground.table%d", i)
			job := setup.Job(i, namespace, bq2bq, nil)
			job.ResourceDestination = dest
			err := jobSpecRepository.Save(ctx, job)
			if err != nil {
				panic(err)
			}
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			jbs, err := jobSpecRepository.GetAllByProjectName(ctx, project.Name, false)
			if err != nil {
				panic(err)
			}
			if len(jbs) != 1000 {
				panic("Job length does not match")
			}
		}
	})
}
