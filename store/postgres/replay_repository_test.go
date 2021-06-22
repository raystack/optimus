// +build !unit_test

package postgres

import (
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jinzhu/gorm"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
)

func TestReplayRepository(t *testing.T) {
	DBSetup := func() *gorm.DB {
		dbURL, ok := os.LookupEnv("TEST_OPTIMUS_DB_URL")
		if !ok {
			panic("unable to find TEST_OPTIMUS_DB_URL env var")
		}
		dbConn, err := Connect(dbURL, 1, 1)
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

		return dbConn
	}

	jobSpec := models.JobSpec{
		Name: "job-name",
	}

	startTime, _ := time.Parse(job.ReplayDateFormat, "2020-01-15")
	endTime, _ := time.Parse(job.ReplayDateFormat, "2020-01-20")
	uuid := uuid.Must(uuid.NewRandom())
	testConfigs := []*models.ReplaySpec{
		{
			ID:        uuid,
			Job:       jobSpec,
			StartDate: startTime,
			EndDate:   endTime,
			Status:    models.ReplayStatusAccepted,
		},
	}

	t.Run("Insert and GetByID", func(t *testing.T) {
		db := DBSetup()
		defer db.Close()
		testModels := []*models.ReplaySpec{}
		testModels = append(testModels, testConfigs...)

		repo := NewReplayRepository(db, jobSpec)

		err := repo.Insert(testModels[0])
		assert.Nil(t, err)

		checkModel, err := repo.GetByID(testModels[0].ID)
		assert.Nil(t, err)
		assert.Equal(t, uuid, checkModel.ID)
	})

	t.Run("UpdateStatus", func(t *testing.T) {
		db := DBSetup()
		defer db.Close()
		testModels := []*models.ReplaySpec{}
		testModels = append(testModels, testConfigs...)

		repo := NewReplayRepository(db, jobSpec)

		err := repo.Insert(testModels[0])
		assert.Nil(t, err)

		errMessage := "failed to execute"
		replayMessage := models.ReplayMessage{
			Status:  models.ReplayStatusFailed,
			Message: errMessage,
		}
		err = repo.UpdateStatus(uuid, models.ReplayStatusFailed, replayMessage)
		assert.Nil(t, err)

		checkModel, err := repo.GetByID(testModels[0].ID)
		assert.Nil(t, err)
		assert.Equal(t, models.ReplayStatusFailed, checkModel.Status)
		assert.Equal(t, errMessage, checkModel.Message.Message)
	})
}
