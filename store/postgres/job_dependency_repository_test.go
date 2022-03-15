//go:build !unit_test
// +build !unit_test

package postgres

import (
	"context"
	"github.com/google/uuid"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
	"os"
	"testing"
)

func TestJobDependencyRepository(t *testing.T) {
	projectSpec := models.ProjectSpec{
		ID:   uuid.Must(uuid.NewRandom()),
		Name: "t-optimus-project",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
	}
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")
	ctx := context.Background()

	DBSetup := func() *gorm.DB {
		dbURL, ok := os.LookupEnv("TEST_OPTIMUS_DB_URL")
		if !ok {
			panic("unable to find TEST_OPTIMUS_DB_URL env var")
		}
		dbConn, err := Connect(dbURL, 1, 1, os.Stdout)
		if err != nil {
			panic(err)
		}
		m, err := NewHTTPFSMigrator(dbURL)
		if err != nil {
			panic(err)
		}
		if err := m.Drop(); err != nil {
			panic(err)
		}
		if err := Migrate(dbURL); err != nil {
			panic(err)
		}

		projRepo := NewProjectRepository(dbConn, hash)
		assert.Nil(t, projRepo.Save(ctx, projectSpec))
		return dbConn
	}

	t.Run("Save", func(t *testing.T) {
		db := DBSetup()
		sqlDB, _ := db.DB()
		defer sqlDB.Close()

		jobDependencies := []store.JobDependency{
			{
				JobID:          uuid.Must(uuid.NewRandom()),
				ProjectID:      projectSpec.ID,
				DependentJobID: uuid.Must(uuid.NewRandom()),
				Type:           models.JobSpecDependencyTypeIntra.String(),
			},
			{
				JobID:          uuid.Must(uuid.NewRandom()),
				ProjectID:      projectSpec.ID,
				DependentJobID: uuid.Must(uuid.NewRandom()),
				Type:           models.JobSpecDependencyTypeIntra.String(),
			},
		}
		repo := NewJobDependencyRepository(db, projectSpec)

		err := repo.Save(ctx, jobDependencies[0])
		assert.Nil(t, err)

		err = repo.Save(ctx, jobDependencies[1])
		assert.Nil(t, err)

		var storedJobDependencies []store.JobDependency
		storedJobDependencies, err = repo.GetAll(ctx)
		assert.Nil(t, err)
		assert.EqualValues(t, []uuid.UUID{jobDependencies[0].JobID, jobDependencies[1].JobID}, []uuid.UUID{storedJobDependencies[0].JobID, storedJobDependencies[1].JobID})

		err = repo.DeleteByJobID(ctx, jobDependencies[0].JobID)
		assert.Nil(t, err)

		storedJobDependencies, err = repo.GetAll(ctx)
		assert.Nil(t, err)
		assert.Equal(t, jobDependencies[1].JobID, storedJobDependencies[0].JobID)
	})
}
