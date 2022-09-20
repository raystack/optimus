//go:build !unit_test
// +build !unit_test

package postgres_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	"github.com/odpf/optimus/internal/store/postgres"
	"github.com/odpf/optimus/models"
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
		t.Run("should return error if context is nil", func(t *testing.T) {
			db := DBSetup()
			repo := postgres.NewJobSourceRepository(db)

			jobID := uuid.New()
			var ctx context.Context
			jobSourceURNs := []string{"urn1", "urn2"}

			actualError := repo.Save(ctx, projectSpec.ID, jobID, jobSourceURNs)

			assert.Error(t, actualError)
		})

		t.Run("should return error if error during delete by job id", func(t *testing.T) {
			db := DBSetup()
			repo := postgres.NewJobSourceRepository(db)

			jobID := uuid.New()
			ctx, cancelFn := context.WithCancel(context.Background())
			cancelFn()
			jobSourceURNs := []string{"urn1", "urn2"}

			actualError := repo.Save(ctx, projectSpec.ID, jobID, jobSourceURNs)

			assert.Error(t, actualError)
		})

		t.Run("should not save the job sources and return error if resource urns contain duplication", func(t *testing.T) {
			db := DBSetup()
			repo := postgres.NewJobSourceRepository(db)

			jobID := uuid.New()
			ctx := context.Background()
			jobSourceURNs := []string{"urn1", "urn1"}

			actualError := repo.Save(ctx, projectSpec.ID, jobID, jobSourceURNs)
			var jobSources []postgres.JobSource
			if err := db.Find(&jobSources).Error; err != nil {
				panic(err)
			}

			expectedLength := 0

			assert.Error(t, actualError)
			assert.Len(t, jobSources, expectedLength)
		})

		t.Run("should save the job sources and return nil if no error is encountered", func(t *testing.T) {
			db := DBSetup()
			repo := postgres.NewJobSourceRepository(db)

			jobID := uuid.New()
			ctx := context.Background()
			jobSourceURNs := []string{"urn1", "urn2"}

			actualError := repo.Save(ctx, projectSpec.ID, jobID, jobSourceURNs)
			var jobSources []postgres.JobSource
			if err := db.Find(&jobSources).Error; err != nil {
				panic(err)
			}

			expectedLength := 2

			assert.NoError(t, actualError)
			assert.Len(t, jobSources, expectedLength)
		})
	})

	t.Run("GetAll", func(t *testing.T) {
		t.Run("should return nil and error if context is nil", func(t *testing.T) {
			db := DBSetup()
			repo := postgres.NewJobSourceRepository(db)

			var ctx context.Context

			actualJobSources, actualError := repo.GetAll(ctx, projectSpec.ID)

			assert.Nil(t, actualJobSources)
			assert.Error(t, actualError)
		})

		t.Run("should return nil and error if encountered any error when reading from database", func(t *testing.T) {
			db := DBSetup()
			repo := postgres.NewJobSourceRepository(db)

			ctx, cancelFn := context.WithCancel(context.Background())
			cancelFn()

			actualJobSources, actualError := repo.GetAll(ctx, projectSpec.ID)

			assert.Nil(t, actualJobSources)
			assert.Error(t, actualError)
		})

		t.Run("should return job sources and nil if no error is encountered", func(t *testing.T) {
			jobSource := &postgres.JobSource{
				JobID:       uuid.New(),
				ProjectID:   projectSpec.ID.UUID(),
				ResourceURN: "urn1",
			}
			db := DBSetup()
			if err := db.Create(jobSource).Error; err != nil {
				panic(err)
			}
			repo := postgres.NewJobSourceRepository(db)

			ctx := context.Background()

			actualJobSources, actualError := repo.GetAll(ctx, projectSpec.ID)

			expectedLen := 1

			assert.Len(t, actualJobSources, expectedLen)
			assert.NoError(t, actualError)
		})
	})

	t.Run("GetByResourceURN", func(t *testing.T) {
		sampleResourceURN := "resource-a"
		t.Run("should return nil and error if context is nil", func(t *testing.T) {
			db := DBSetup()
			repo := postgres.NewJobSourceRepository(db)

			var ctx context.Context

			actualJobSources, actualError := repo.GetByResourceURN(ctx, sampleResourceURN)

			assert.Nil(t, actualJobSources)
			assert.Error(t, actualError)
		})

		t.Run("should return nil and error if encountered any error when reading from database", func(t *testing.T) {
			db := DBSetup()
			repo := postgres.NewJobSourceRepository(db)

			ctx, cancelFn := context.WithCancel(context.Background())
			cancelFn()

			actualJobSources, actualError := repo.GetByResourceURN(ctx, sampleResourceURN)

			assert.Nil(t, actualJobSources)
			assert.Error(t, actualError)
		})

		t.Run("should return job sources based on resource URN and nil if no error is encountered", func(t *testing.T) {
			jobSource := &postgres.JobSource{
				JobID:       uuid.New(),
				ProjectID:   projectSpec.ID.UUID(),
				ResourceURN: sampleResourceURN,
			}
			db := DBSetup()
			if err := db.Create(jobSource).Error; err != nil {
				panic(err)
			}
			repo := postgres.NewJobSourceRepository(db)

			ctx := context.Background()

			actualJobSources, actualError := repo.GetByResourceURN(ctx, sampleResourceURN)

			expectedLen := 1

			assert.Len(t, actualJobSources, expectedLen)
			assert.NoError(t, actualError)
		})
	})

	t.Run("DeleteByJobID", func(t *testing.T) {
		t.Run("should return error if context is nil", func(t *testing.T) {
			db := DBSetup()
			repo := postgres.NewJobSourceRepository(db)

			var ctx context.Context
			jobID := uuid.New()

			actualError := repo.DeleteByJobID(ctx, jobID)

			assert.Error(t, actualError)
		})

		t.Run("should return nil and error if encountered any error when deleting from database", func(t *testing.T) {
			db := DBSetup()
			repo := postgres.NewJobSourceRepository(db)

			ctx, cancelFn := context.WithCancel(context.Background())
			cancelFn()
			jobID := uuid.New()

			actualError := repo.DeleteByJobID(ctx, jobID)

			assert.Error(t, actualError)
		})

		t.Run("should return job sources and nil if no error is encountered", func(t *testing.T) {
			jobSource := &postgres.JobSource{
				JobID:       uuid.New(),
				ProjectID:   projectSpec.ID.UUID(),
				ResourceURN: "urn1",
			}
			db := DBSetup()
			if err := db.Create(jobSource).Error; err != nil {
				panic(err)
			}
			repo := postgres.NewJobSourceRepository(db)

			ctx := context.Background()

			actualError := repo.DeleteByJobID(ctx, jobSource.JobID)
			var storedSources []postgres.JobSource
			if err := db.Find(&storedSources).Error; err != nil {
				panic(err)
			}

			expectedLen := 0

			assert.Len(t, storedSources, expectedLen)
			assert.NoError(t, actualError)
		})
	})

	t.Run("GetResourceURNsPerJobID", func(t *testing.T) {
		t.Run("should return nil and error if context is nil", func(t *testing.T) {
			db := DBSetup()
			repo := postgres.NewJobSourceRepository(db)

			var ctx context.Context

			actualJobSources, actualError := repo.GetResourceURNsPerJobID(ctx, projectSpec.ID)

			assert.Nil(t, actualJobSources)
			assert.Error(t, actualError)
		})

		t.Run("should return nil and error if encountered any error when reading from database", func(t *testing.T) {
			db := DBSetup()
			repo := postgres.NewJobSourceRepository(db)

			ctx, cancelFn := context.WithCancel(context.Background())
			cancelFn()

			actualJobSources, actualError := repo.GetResourceURNsPerJobID(ctx, projectSpec.ID)

			assert.Nil(t, actualJobSources)
			assert.Error(t, actualError)
		})

		t.Run("should return resource URNs per job ID and nil if no error is encountered", func(t *testing.T) {
			jobID1 := uuid.New()
			jobID2 := uuid.New()
			resourceURNsPerJobID := map[uuid.UUID][]string{
				jobID1: {"resource-a", "resource-b"},
				jobID2: {"resource-b"},
			}

			db := DBSetup()
			projRepo := postgres.NewProjectRepository(db, hash)
			assert.Nil(t, projRepo.Save(ctx, projectSpec))

			repo := postgres.NewJobSourceRepository(db)

			err := repo.Save(ctx, projectSpec.ID, jobID1, resourceURNsPerJobID[jobID1])
			assert.NoError(t, err)

			err = repo.Save(ctx, projectSpec.ID, jobID2, resourceURNsPerJobID[jobID2])
			assert.NoError(t, err)

			actualResourceURNsPerJobID, actualError := repo.GetResourceURNsPerJobID(ctx, projectSpec.ID)

			assert.EqualValues(t, resourceURNsPerJobID, actualResourceURNsPerJobID)
			assert.NoError(t, actualError)
		})
	})
}
