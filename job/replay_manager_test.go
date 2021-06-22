package job_test

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/optimus/core/logger"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestReplayManager(t *testing.T) {
	t.Run("Close", func(t *testing.T) {
		logger.Init(logger.ERROR)
		manager := job.NewManager(nil, nil, nil, 5)
		err := manager.Close()
		assert.Nil(t, err)
	})
	t.Run("Replay", func(t *testing.T) {
		dagStartTime, _ := time.Parse(job.ReplayDateFormat, "2020-04-05")
		startDate, _ := time.Parse(job.ReplayDateFormat, "2020-08-22")
		endDate, _ := time.Parse(job.ReplayDateFormat, "2020-08-26")
		jobSpec := models.JobSpec{
			Name: "job-name",
			Schedule: models.JobSpecSchedule{
				StartDate: dagStartTime,
				Interval:  "0 2 * * *",
			},
		}
		replayRequest := &models.ReplayRequestInput{
			Job:   jobSpec,
			Start: startDate,
			End:   endDate,
			Project: models.ProjectSpec{
				Name: "project-name",
			},
			DagSpecMap: map[string]models.JobSpec{
				"job-name": jobSpec,
			},
		}
		t.Run("should throw error if uuid provider returns failure", func(t *testing.T) {
			logger.Init(logger.ERROR)
			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)
			objUUID := uuid.Must(uuid.NewRandom())
			errMessage := "error while generating uuid"
			uuidProvider.On("NewUUID").Return(objUUID, errors.New(errMessage))

			replayManager := job.NewManager(nil, nil, uuidProvider, 5)
			_, err := replayManager.Replay(replayRequest)
			assert.NotNil(t, err)
			assert.True(t, strings.Contains(err.Error(), errMessage))
		})
		t.Run("should throw an error if replay repo throws error", func(t *testing.T) {
			logger.Init(logger.ERROR)
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", replayRequest.Job).Return(replayRepository)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)
			objUUID := uuid.Must(uuid.NewRandom())
			uuidProvider.On("NewUUID").Return(objUUID, nil)

			errMessage := "error with replay repo"
			toInsertReplaySpec := &models.ReplaySpec{
				ID:        objUUID,
				Job:       jobSpec,
				StartDate: startDate,
				EndDate:   endDate,
				Status:    models.ReplayStatusAccepted,
			}
			replayRepository.On("Insert", toInsertReplaySpec).Return(errors.New(errMessage))

			replayManager := job.NewManager(nil, replaySpecRepoFac, uuidProvider, 5)
			_, err := replayManager.Replay(replayRequest)
			assert.NotNil(t, err)
			assert.True(t, strings.Contains(err.Error(), errMessage))
		})
	})
}
