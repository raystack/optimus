package job_test

import (
	"context"
	"io/ioutil"
	"testing"
	"time"

	"github.com/odpf/optimus/core/logger"

	"github.com/google/uuid"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestReplayWorker(t *testing.T) {
	logger.InitWithWriter(logger.DEBUG, ioutil.Discard)
	dagStartTime, _ := time.Parse(job.ReplayDateFormat, "2020-04-05")
	startDate, _ := time.Parse(job.ReplayDateFormat, "2020-08-22")
	endDate, _ := time.Parse(job.ReplayDateFormat, "2020-08-26")
	currUUID := uuid.Must(uuid.NewRandom())
	dagRunStartTime := time.Date(2020, time.Month(8), 22, 2, 0, 0, 0, time.UTC)
	dagRunEndTime := time.Date(2020, time.Month(8), 26, 2, 0, 0, 0, time.UTC)
	jobSpec := models.JobSpec{
		Name: "job-name",
		Schedule: models.JobSpecSchedule{
			StartDate: dagStartTime,
			Interval:  "0 2 * * *",
		},
	}
	replayRequest := &models.ReplayWorkerRequest{
		ID:    currUUID,
		Job:   jobSpec,
		Start: startDate,
		End:   endDate,
		Project: models.ProjectSpec{
			Name: "project-name",
		},
		JobSpecMap: map[string]models.JobSpec{
			"job-name": jobSpec,
		},
	}
	t.Run("Process", func(t *testing.T) {
		t.Run("should throw an error when replayRepo throws an error", func(t *testing.T) {
			ctx := context.Background()
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			errMessage := "replay repo error"
			replayRepository.On("UpdateStatus", currUUID, models.ReplayStatusInProgress, models.ReplayMessage{}).Return(errors.New(errMessage))

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", replayRequest.Job).Return(replayRepository)

			worker := job.NewReplayWorker(replaySpecRepoFac, nil)
			err := worker.Process(ctx, replayRequest)
			assert.NotNil(t, err)
			assert.Equal(t, errMessage, err.Error())
		})
		t.Run("should throw an error when scheduler throws an error", func(t *testing.T) {
			ctx := context.Background()
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("UpdateStatus", currUUID, models.ReplayStatusInProgress, models.ReplayMessage{}).Return(nil)
			errMessage := "error while clearing dag runs for job job-name: scheduler clear error"
			failedReplayMessage := models.ReplayMessage{
				Type:    job.AirflowClearDagRunFailed,
				Message: errMessage,
			}
			replayRepository.On("UpdateStatus", currUUID, models.ReplayStatusFailed, failedReplayMessage).Return(nil)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", replayRequest.Job).Return(replayRepository)

			scheduler := new(mock.MockScheduler)
			defer scheduler.AssertExpectations(t)
			errorMessage := "scheduler clear error"
			scheduler.On("Clear", ctx, replayRequest.Project, "job-name", dagRunStartTime, dagRunEndTime).Return(errors.New(errorMessage))

			worker := job.NewReplayWorker(replaySpecRepoFac, scheduler)
			err := worker.Process(ctx, replayRequest)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), errorMessage)
		})
		t.Run("should throw an error when updatestatus throws an error for failed request", func(t *testing.T) {
			ctx := context.Background()
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("UpdateStatus", currUUID, models.ReplayStatusInProgress, models.ReplayMessage{}).Return(nil)
			errMessage := "error while clearing dag runs for job job-name: scheduler clear error"
			failedReplayMessage := models.ReplayMessage{
				Type:    job.AirflowClearDagRunFailed,
				Message: errMessage,
			}
			updateStatusErr := errors.New("error while updating status to failed")
			replayRepository.On("UpdateStatus", currUUID, models.ReplayStatusFailed, failedReplayMessage).Return(updateStatusErr)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", replayRequest.Job).Return(replayRepository)

			scheduler := new(mock.MockScheduler)
			defer scheduler.AssertExpectations(t)
			errorMessage := "scheduler clear error"
			scheduler.On("Clear", ctx, replayRequest.Project, "job-name", dagRunStartTime, dagRunEndTime).Return(errors.New(errorMessage))

			worker := job.NewReplayWorker(replaySpecRepoFac, scheduler)
			err := worker.Process(ctx, replayRequest)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), updateStatusErr.Error())
		})
		t.Run("should throw an error when updatestatus throws an error for successful request", func(t *testing.T) {
			ctx := context.Background()
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("UpdateStatus", currUUID, models.ReplayStatusInProgress, models.ReplayMessage{}).Return(nil)
			updateSuccessStatusErr := errors.New("error while updating replay request")
			replayRepository.On("UpdateStatus", currUUID, models.ReplayStatusSuccess, models.ReplayMessage{}).Return(updateSuccessStatusErr)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", replayRequest.Job).Return(replayRepository)

			scheduler := new(mock.MockScheduler)
			defer scheduler.AssertExpectations(t)
			scheduler.On("Clear", ctx, replayRequest.Project, "job-name", dagRunStartTime, dagRunEndTime).Return(nil)

			worker := job.NewReplayWorker(replaySpecRepoFac, scheduler)
			err := worker.Process(ctx, replayRequest)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), updateSuccessStatusErr.Error())
		})
		t.Run("should update replay status if successful", func(t *testing.T) {
			ctx := context.Background()
			replayRepository := new(mock.ReplayRepository)
			replayRepository.On("UpdateStatus", currUUID, models.ReplayStatusInProgress, models.ReplayMessage{}).Return(nil)
			replayRepository.On("UpdateStatus", currUUID, models.ReplayStatusSuccess, models.ReplayMessage{}).Return(nil)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", replayRequest.Job).Return(replayRepository)

			scheduler := new(mock.MockScheduler)
			defer scheduler.AssertExpectations(t)
			scheduler.On("Clear", ctx, replayRequest.Project, "job-name", dagRunStartTime, dagRunEndTime).Return(nil)

			worker := job.NewReplayWorker(replaySpecRepoFac, scheduler)
			err := worker.Process(ctx, replayRequest)
			assert.Nil(t, err)
		})
		t.Run("should throw an error when prepareTree throws an error", func(t *testing.T) {
			replayRequest.JobSpecMap = make(map[string]models.JobSpec)
			ctx := context.Background()
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("UpdateStatus", currUUID, models.ReplayStatusInProgress, models.ReplayMessage{}).Return(nil)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", replayRequest.Job).Return(replayRepository)

			scheduler := new(mock.MockScheduler)
			defer scheduler.AssertExpectations(t)

			worker := job.NewReplayWorker(replaySpecRepoFac, scheduler)
			err := worker.Process(ctx, replayRequest)
			assert.NotNil(t, err)
		})
	})
}
