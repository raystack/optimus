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

	"github.com/odpf/optimus/internal/store"
	"github.com/odpf/optimus/internal/store/postgres"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/tests/setup"
)

func BenchmarkProjectRepository(b *testing.B) {
	ctx := context.Background()
	dbSetup := func() *gorm.DB {
		dbConn := setup.TestDB()
		setup.TruncateTables(dbConn)

		return dbConn
	}

	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")

	b.Run("Save", func(b *testing.B) {
		db := dbSetup()
		var repo store.ProjectRepository = postgres.NewProjectRepository(db, hash)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			project := setup.Project(i)
			err := repo.Save(ctx, project)
			if err != nil {
				panic(err)
			}
		}
	})

	b.Run("GetByName", func(b *testing.B) {
		db := dbSetup()
		var repo store.ProjectRepository = postgres.NewProjectRepository(db, hash)
		secretRepo := postgres.NewSecretRepository(db, hash)
		for i := 0; i < 10; i++ {
			project := setup.Project(i)
			project.ID = models.ProjectID(uuid.New())
			err := repo.Save(ctx, project)
			assert.Nil(b, err)

			for s := 0; s < 5; s++ {
				secret := setup.Secret(s)
				err = secretRepo.Save(ctx, project, models.NamespaceSpec{}, secret)
				assert.Nil(b, err)
			}
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			num := i % 10
			projName := fmt.Sprintf("t-optimus-%d", num)
			prj, err := repo.GetByName(ctx, projName)
			if err != nil {
				panic(err)
			}
			if prj.Name != projName {
				panic("Project name is not same")
			}
		}
	})

	b.Run("GetAllWithUpstreams", func(b *testing.B) {
		db := dbSetup()
		var repo store.ProjectRepository = postgres.NewProjectRepository(db, hash)
		secretRepo := postgres.NewSecretRepository(db, hash)
		for i := 0; i < 10; i++ {
			project := setup.Project(i)
			project.ID = models.ProjectID(uuid.New())
			err := repo.Save(ctx, project)
			assert.Nil(b, err)

			for s := 0; s < 5; s++ {
				secret := setup.Secret(s)
				err = secretRepo.Save(ctx, project, models.NamespaceSpec{}, secret)
				assert.Nil(b, err)
			}
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			prj, err := repo.GetAll(ctx)
			if err != nil {
				panic(err)
			}
			if len(prj) != 10 {
				panic("Project list is not same")
			}
		}
	})
}
