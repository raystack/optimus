//go:build !unit_test
// +build !unit_test

package bench

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/odpf/optimus/store/postgres"
	"github.com/odpf/optimus/tests/setup"
)

func BenchmarkJobDependencyRepository(b *testing.B) {
	ctx := context.Background()
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")

	proj := setup.Project(1)
	proj.ID = models.ProjectID(uuid.New())

	namespace := setup.Namespace(1, proj)
	namespace.ID = uuid.New()

	mod := setup.MockPluginBQ{}

	dbSetup := func() *gorm.DB {
		dbConn := setup.TestDB()
		setup.TruncateTables(dbConn)

		projRepo := postgres.NewProjectRepository(dbConn, hash)
		err := projRepo.Save(ctx, proj)
		assert.Nil(b, err)

		nsRepo := postgres.NewNamespaceRepository(dbConn, hash)
		err = nsRepo.Save(ctx, proj, namespace)
		assert.Nil(b, err)

		secretRepo := postgres.NewSecretRepository(dbConn, hash)
		for s := 0; s < 5; s++ {
			secret := setup.Secret(s)
			err = secretRepo.Save(ctx, proj, models.NamespaceSpec{}, secret)
			assert.Nil(b, err)
		}

		return dbConn
	}

	b.Run("Save", func(b *testing.B) {
		db := dbSetup()

		job := setup.Job(1, namespace, mod, nil)
		job.ID = uuid.New()

		jobDependencies := models.JobSpecDependency{
			Job:     &job,
			Project: &proj,
			Type:    models.JobSpecDependencyTypeIntra,
		}
		var repo store.JobDependencyRepository = postgres.NewJobDependencyRepository(db)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			err := repo.Save(ctx, proj.ID, uuid.New(), jobDependencies)
			if err != nil {
				panic(err)
			}
		}
	})

	b.Run("GetAll", func(b *testing.B) {
		db := dbSetup()

		job := setup.Job(1, namespace, mod, nil)
		job.ID = uuid.New()

		jobDependencies := models.JobSpecDependency{
			Job:     &job,
			Project: &proj,
			Type:    models.JobSpecDependencyTypeIntra,
		}
		var repo store.JobDependencyRepository = postgres.NewJobDependencyRepository(db)
		b.ResetTimer()

		for i := 0; i < 100; i++ {
			err := repo.Save(ctx, proj.ID, uuid.New(), jobDependencies)
			if err != nil {
				panic(err)
			}
		}

		for i := 0; i < b.N; i++ {
			deps, err := repo.GetAll(ctx, proj.ID)
			if err != nil {
				panic(err)
			}
			if len(deps) != 100 {
				panic("Number of deps is not same")
			}
		}
	})
}
