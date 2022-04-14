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

func TestIntegrationJobDeploymentRepository(t *testing.T) {
	projectSpec1 := models.ProjectSpec{
		ID:   models.ProjectID(uuid.New()),
		Name: "t-optimus-project",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
	}
	projectSpec2 := models.ProjectSpec{
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
		assert.Nil(t, projRepo.Save(ctx, projectSpec1))
		assert.Nil(t, projRepo.Save(ctx, projectSpec2))
		return dbConn
	}

	t.Run("Save", func(t *testing.T) {
		db := DBSetup()

		jobDeployments := []models.JobDeployment{
			{
				ID:      uuid.New(),
				Project: projectSpec1,
				Status:  models.JobDeploymentStatusInProgress,
				Details: models.JobDeploymentDetail{},
			},
			{
				ID:      uuid.New(),
				Project: projectSpec2,
				Status:  models.JobDeploymentStatusFailed,
				Details: models.JobDeploymentDetail{
					Failures: []models.JobDeploymentFailure{
						{
							JobName: "job-1",
							Message: "internal error",
						},
					},
				},
			},
		}
		repo := postgres.NewJobDeploymentRepository(db)

		err := repo.Save(ctx, jobDeployments[0])
		assert.Nil(t, err)

		err = repo.Save(ctx, jobDeployments[1])
		assert.Nil(t, err)

		storedDeployment1, err := repo.GetByID(ctx, jobDeployments[0].ID)
		storedDeployment2, err := repo.GetByID(ctx, jobDeployments[1].ID)
		assert.Nil(t, err)
		assert.EqualValues(t, []uuid.UUID{jobDeployments[0].ID, jobDeployments[1].ID}, []uuid.UUID{storedDeployment1.ID, storedDeployment2.ID})
	})
	t.Run("UpdateByID", func(t *testing.T) {
		db := DBSetup()

		jobDeployments := []models.JobDeployment{
			{
				ID:      uuid.New(),
				Project: projectSpec1,
				Status:  models.JobDeploymentStatusInProgress,
				Details: models.JobDeploymentDetail{},
			},
			{
				ID:      uuid.New(),
				Project: projectSpec2,
				Status:  models.JobDeploymentStatusFailed,
				Details: models.JobDeploymentDetail{
					Failures: []models.JobDeploymentFailure{
						{
							JobName: "job-1",
							Message: "internal error",
						},
					},
				},
			},
		}
		repo := postgres.NewJobDeploymentRepository(db)

		err := repo.Save(ctx, jobDeployments[0])
		assert.Nil(t, err)

		err = repo.Save(ctx, jobDeployments[1])
		assert.Nil(t, err)

		storedDeployment1, err := repo.GetByID(ctx, jobDeployments[0].ID)
		storedDeployment2, err := repo.GetByID(ctx, jobDeployments[1].ID)
		assert.Nil(t, err)
		assert.EqualValues(t, []uuid.UUID{jobDeployments[0].ID, jobDeployments[1].ID}, []uuid.UUID{storedDeployment1.ID, storedDeployment2.ID})

		jobDeployments[0].Status = models.JobDeploymentStatusSucceed

		err = repo.UpdateByID(ctx, jobDeployments[0])
		assert.Nil(t, err)

		modifiedDeployment1, err := repo.GetByID(ctx, jobDeployments[0].ID)
		unmodifiedDeployment2, err := repo.GetByID(ctx, jobDeployments[1].ID)
		assert.Nil(t, err)
		assert.Equal(t, []models.JobDeploymentStatus{jobDeployments[0].Status, jobDeployments[1].Status}, []models.JobDeploymentStatus{modifiedDeployment1.Status, unmodifiedDeployment2.Status})
	})
	t.Run("GetByStatusAndProjectID", func(t *testing.T) {
		db := DBSetup()

		jobDeployments := []models.JobDeployment{
			{
				ID:      uuid.New(),
				Project: projectSpec1,
				Status:  models.JobDeploymentStatusInProgress,
				Details: models.JobDeploymentDetail{},
			},
			{
				ID:      uuid.New(),
				Project: projectSpec2,
				Status:  models.JobDeploymentStatusFailed,
				Details: models.JobDeploymentDetail{
					Failures: []models.JobDeploymentFailure{
						{
							JobName: "job-1",
							Message: "internal error",
						},
					},
				},
			},
		}
		repo := postgres.NewJobDeploymentRepository(db)

		err := repo.Save(ctx, jobDeployments[0])
		assert.Nil(t, err)

		err = repo.Save(ctx, jobDeployments[1])
		assert.Nil(t, err)

		storedDeployment1, err := repo.GetByStatusAndProjectID(ctx, jobDeployments[0].Status, jobDeployments[0].Project.ID)
		storedDeployment2, err := repo.GetByStatusAndProjectID(ctx, jobDeployments[1].Status, jobDeployments[1].Project.ID)
		assert.Nil(t, err)
		assert.EqualValues(t, []uuid.UUID{jobDeployments[0].ID, jobDeployments[1].ID}, []uuid.UUID{storedDeployment1.ID, storedDeployment2.ID})
	})
}
