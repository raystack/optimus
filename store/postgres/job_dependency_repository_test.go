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

func TestIntegrationJobDependencyRepository(t *testing.T) {
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
		jobID4 := uuid.New()
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
			{
				Job:     &models.JobSpec{ID: jobID4},
				Project: &externalProjectSpec,
				Type:    models.JobSpecDependencyTypeInter,
			},
		}
		repo := postgres.NewJobDependencyRepository(db)

		err := repo.Save(ctx, projectSpec.ID, jobID1, jobDependencies[0])
		assert.Nil(t, err)

		err = repo.Save(ctx, projectSpec.ID, jobID2, jobDependencies[1])
		assert.Nil(t, err)

		err = repo.Save(ctx, projectSpec.ID, jobID1, jobDependencies[2])
		assert.Nil(t, err)

		var storedJobDependencies []models.JobIDDependenciesPair
		storedJobDependencies, err = repo.GetAll(ctx, projectSpec.ID)
		assert.Nil(t, err)

		expectedJobDependencies := []models.JobIDDependenciesPair{
			{
				JobID:            jobID1,
				DependentProject: *jobDependencies[0].Project,
				DependentJobID:   jobDependencies[0].Job.ID,
				Type:             jobDependencies[0].Type,
			},
			{
				JobID:            jobID2,
				DependentProject: *jobDependencies[1].Project,
				DependentJobID:   jobDependencies[1].Job.ID,
				Type:             jobDependencies[1].Type,
			},
			{
				JobID:            jobID1,
				DependentProject: *jobDependencies[2].Project,
				DependentJobID:   jobDependencies[2].Job.ID,
				Type:             jobDependencies[2].Type,
			},
		}
		assert.EqualValues(t, expectedJobDependencies, storedJobDependencies)

		err = repo.DeleteByJobID(ctx, jobID1)
		assert.Nil(t, err)

		storedJobDependencies, err = repo.GetAll(ctx, projectSpec.ID)
		assert.Nil(t, err)
		assert.Equal(t, jobID2, storedJobDependencies[0].JobID)
		assert.Equal(t, jobDependencies[1].Job.ID, storedJobDependencies[0].DependentJobID)
	})
}
