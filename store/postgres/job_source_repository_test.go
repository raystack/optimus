//go:build !unit_test
// +build !unit_test

package postgres_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/postgres"
)

func TestIntegrationJobSourceRepository(t *testing.T) {
	projectSpec := models.ProjectSpec{
		ID:   models.ProjectID(uuid.New()),
		Name: "t-optimus-project",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
	}
	externalProjectSpec := models.ProjectSpec{
		ID:   models.ProjectID(uuid.New()),
		Name: "t-optimus-project-2",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
	}
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")
	ctx := context.Background()

	DBSetup := func() *gorm.DB {
		dbConn := setupDB()
		truncateTables(dbConn)

		projRepo := postgres.NewProjectRepository(dbConn, hash)
		assert.Nil(t, projRepo.Save(ctx, projectSpec))
		assert.Nil(t, projRepo.Save(ctx, externalProjectSpec))
		return dbConn
	}

	t.Run("Save", func(t *testing.T) {
		db := DBSetup()

		jobID1 := uuid.New()
		jobID2 := uuid.New()
		jobID3 := uuid.New()

		jobSources := []models.JobSource{
			{
				JobID:       jobID1,
				ProjectID:   projectSpec.ID,
				ResourceURN: "resource-1",
			},
			{
				JobID:       jobID1,
				ProjectID:   projectSpec.ID,
				ResourceURN: "resource-2",
			},
			{
				JobID:       jobID2,
				ProjectID:   projectSpec.ID,
				ResourceURN: "resource-3",
			},
			{
				JobID:       jobID3,
				ProjectID:   externalProjectSpec.ID,
				ResourceURN: "resource-4",
			},
		}
		repo := postgres.NewJobSourceRepository(db)

		for _, source := range jobSources {
			err := repo.Save(ctx, source)
			assert.Nil(t, err)
		}

		storedJobSources, err := repo.GetAll(ctx, projectSpec.ID)
		assert.Nil(t, err)
		assert.EqualValues(t, []models.JobSource{jobSources[0], jobSources[1], jobSources[2]}, storedJobSources)

		storedJobSources, err = repo.GetAll(ctx, externalProjectSpec.ID)
		assert.Nil(t, err)
		assert.EqualValues(t, []models.JobSource{jobSources[3]}, storedJobSources)

		err = repo.DeleteByJobID(ctx, jobID1)
		assert.Nil(t, err)

		storedJobSources, err = repo.GetAll(ctx, projectSpec.ID)
		assert.Nil(t, err)
		assert.EqualValues(t, []models.JobSource{jobSources[2]}, storedJobSources)

		storedJobSources, err = repo.GetAll(ctx, externalProjectSpec.ID)
		assert.Nil(t, err)
		assert.EqualValues(t, []models.JobSource{jobSources[3]}, storedJobSources)
	})
}
