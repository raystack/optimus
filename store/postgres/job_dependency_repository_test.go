//go:build !unit_test
// +build !unit_test

package postgres

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

func TestIntegrationJobDependencyRepository(t *testing.T) {
	projectSpec := models.ProjectSpec{
		ID:   models.ProjectID(uuid.New()),
		Name: "t-optimus-project",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
	}
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")
	ctx := context.Background()

	DBSetup := func() *gorm.DB {
		dbConn := setupDB()
		truncateTables(dbConn)

		projRepo := NewProjectRepository(dbConn, hash)
		assert.Nil(t, projRepo.Save(ctx, projectSpec))
		return dbConn
	}

	t.Run("Save", func(t *testing.T) {
		db := DBSetup()

		jobID1 := uuid.New()
		jobID2 := uuid.New()
		jobID3 := uuid.New()
		jobDependencies := []models.JobSpecDependency{
			{
				Job:     &models.JobSpec{ID: jobID2},
				Project: &projectSpec,
				Type:    models.JobSpecDependencyTypeIntra,
			},
			{
				Job:     &models.JobSpec{ID: jobID3},
				Project: &projectSpec,
				Type:    models.JobSpecDependencyTypeIntra,
			},
		}
		repo := NewJobDependencyRepository(db)

		err := repo.Save(ctx, projectSpec.ID, jobID1, jobDependencies[0])
		assert.Nil(t, err)

		err = repo.Save(ctx, projectSpec.ID, jobID2, jobDependencies[1])
		assert.Nil(t, err)

		var storedJobDependencies []models.JobIDDependenciesPair
		storedJobDependencies, err = repo.GetAll(ctx, projectSpec.ID)
		assert.Nil(t, err)
		assert.EqualValues(t, []uuid.UUID{jobID1, jobID2}, []uuid.UUID{storedJobDependencies[0].JobID, storedJobDependencies[1].JobID})
		assert.EqualValues(t, []uuid.UUID{jobDependencies[0].Job.ID, jobDependencies[1].Job.ID}, []uuid.UUID{storedJobDependencies[0].DependentJobID, storedJobDependencies[1].DependentJobID})

		err = repo.DeleteByJobID(ctx, jobID1)
		assert.Nil(t, err)

		storedJobDependencies, err = repo.GetAll(ctx, projectSpec.ID)
		assert.Nil(t, err)
		assert.Equal(t, jobID2, storedJobDependencies[0].JobID)
		assert.Equal(t, jobDependencies[1].Job.ID, storedJobDependencies[0].DependentJobID)
	})
}
