package job_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/odpf/optimus/core/tree"

	"github.com/odpf/salt/log"

	"github.com/google/uuid"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
)

func TestReplayWorker(t *testing.T) {
	log := log.NewNoop()
	dagStartTime, _ := time.Parse(job.ReplayDateFormat, "2020-04-05")
	startDate, _ := time.Parse(job.ReplayDateFormat, "2020-08-22")
	endDate, _ := time.Parse(job.ReplayDateFormat, "2020-08-26")
	currUUID := uuid.Must(uuid.NewRandom())
	dagRunStartTime := time.Date(2020, time.Month(8), 22, 2, 0, 0, 0, time.UTC)
	dagRunEndTime := time.Date(2020, time.Month(8), 26, 2, 0, 0, 0, time.UTC)
	projectSpec := models.ProjectSpec{
		Name: "project-name",
	}
	jobSpec := models.JobSpec{
		Name: "job-name",
		Schedule: models.JobSpecSchedule{
			StartDate: dagStartTime,
			Interval:  "0 2 * * *",
		},
	}

	executionTree := tree.NewTreeNode(jobSpec)
	executionTree.Runs.Add(time.Date(2020, time.Month(8), 22, 2, 0, 0, 0, time.UTC))
	executionTree.Runs.Add(time.Date(2020, time.Month(8), 23, 2, 0, 0, 0, time.UTC))
	executionTree.Runs.Add(time.Date(2020, time.Month(8), 24, 2, 0, 0, 0, time.UTC))
	executionTree.Runs.Add(time.Date(2020, time.Month(8), 25, 2, 0, 0, 0, time.UTC))
	executionTree.Runs.Add(time.Date(2020, time.Month(8), 26, 2, 0, 0, 0, time.UTC))

	replayRequest := models.ReplayRequest{
		ID:      currUUID,
		Project: projectSpec,
	}

	replaySpec := models.ReplaySpec{
		ID:            currUUID,
		Job:           jobSpec,
		StartDate:     startDate,
		EndDate:       endDate,
		Status:        models.ReplayStatusInProgress,
		ExecutionTree: executionTree,
	}

	t.Run("Process", func(t *testing.T) {
		t.Run("should throw an error when replayRepo throws an error", func(t *testing.T) {
			ctx := context.Background()
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			errMessage := "replay repo error"
			replayRepository.On("UpdateStatus", ctx, currUUID, models.ReplayStatusInProgress, models.ReplayMessage{}).Return(errors.New(errMessage))

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New").Return(replayRepository)

			worker := job.NewReplayWorker(log, replaySpecRepoFac, nil)
			err := worker.Process(ctx, replayRequest)
			assert.NotNil(t, err)
			assert.Equal(t, errMessage, err.Error())
		})
		t.Run("should throw an error when batchScheduler throws an error", func(t *testing.T) {
			ctx := context.Background()
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("UpdateStatus", ctx, currUUID, models.ReplayStatusInProgress, models.ReplayMessage{}).Return(nil)
			errMessage := "error while clearing dag runs for job job-name: batchScheduler clear error"
			failedReplayMessage := models.ReplayMessage{
				Type:    job.AirflowClearDagRunFailed,
				Message: errMessage,
			}
			replayRepository.On("UpdateStatus", ctx, currUUID, models.ReplayStatusFailed, failedReplayMessage).Return(nil)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New").Return(replayRepository)
			replayRepository.On("GetByID", ctx, currUUID).Return(replaySpec, nil)

			scheduler := new(mock.Scheduler)
			defer scheduler.AssertExpectations(t)
			errorMessage := "batchScheduler clear error"
			scheduler.On("Clear", ctx, projectSpec, "job-name", dagRunStartTime, dagRunEndTime).Return(errors.New(errorMessage))

			worker := job.NewReplayWorker(log, replaySpecRepoFac, scheduler)
			err := worker.Process(ctx, replayRequest)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), errorMessage)
		})
		t.Run("should throw an error when updatestatus throws an error for failed request", func(t *testing.T) {
			ctx := context.Background()
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("UpdateStatus", ctx, currUUID, models.ReplayStatusInProgress, models.ReplayMessage{}).Return(nil)
			errMessage := "error while clearing dag runs for job job-name: batchScheduler clear error"
			failedReplayMessage := models.ReplayMessage{
				Type:    job.AirflowClearDagRunFailed,
				Message: errMessage,
			}
			updateStatusErr := errors.New("error while updating status to failed")
			replayRepository.On("UpdateStatus", ctx, currUUID, models.ReplayStatusFailed, failedReplayMessage).Return(updateStatusErr)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New").Return(replayRepository)
			replayRepository.On("GetByID", ctx, currUUID).Return(replaySpec, nil)

			scheduler := new(mock.Scheduler)
			defer scheduler.AssertExpectations(t)
			errorMessage := "batchScheduler clear error"
			scheduler.On("Clear", ctx, projectSpec, "job-name", dagRunStartTime, dagRunEndTime).Return(errors.New(errorMessage))

			worker := job.NewReplayWorker(log, replaySpecRepoFac, scheduler)
			err := worker.Process(ctx, replayRequest)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), updateStatusErr.Error())
		})
		t.Run("should throw an error when updatestatus throws an error for successful request", func(t *testing.T) {
			ctx := context.Background()
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("UpdateStatus", ctx, currUUID, models.ReplayStatusInProgress, models.ReplayMessage{}).Return(nil)
			updateSuccessStatusErr := errors.New("error while updating replay request")
			replayRepository.On("UpdateStatus", ctx, currUUID, models.ReplayStatusReplayed, models.ReplayMessage{}).Return(updateSuccessStatusErr)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New").Return(replayRepository)
			replayRepository.On("GetByID", ctx, currUUID).Return(replaySpec, nil)

			scheduler := new(mock.Scheduler)
			defer scheduler.AssertExpectations(t)
			scheduler.On("Clear", ctx, projectSpec, "job-name", dagRunStartTime, dagRunEndTime).Return(nil)

			worker := job.NewReplayWorker(log, replaySpecRepoFac, scheduler)
			err := worker.Process(ctx, replayRequest)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), updateSuccessStatusErr.Error())
		})
		t.Run("should update replay status if successful", func(t *testing.T) {
			ctx := context.Background()
			replayRepository := new(mock.ReplayRepository)
			replayRepository.On("UpdateStatus", ctx, currUUID, models.ReplayStatusInProgress, models.ReplayMessage{}).Return(nil)
			replayRepository.On("UpdateStatus", ctx, currUUID, models.ReplayStatusReplayed, models.ReplayMessage{}).Return(nil)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New").Return(replayRepository)
			replayRepository.On("GetByID", ctx, currUUID).Return(replaySpec, nil)

			scheduler := new(mock.Scheduler)
			defer scheduler.AssertExpectations(t)
			scheduler.On("Clear", ctx, projectSpec, "job-name", dagRunStartTime, dagRunEndTime).Return(nil)

			worker := job.NewReplayWorker(log, replaySpecRepoFac, scheduler)
			err := worker.Process(ctx, replayRequest)
			assert.Nil(t, err)
		})
		t.Run("should throw an error when getting replay spec throws an error", func(t *testing.T) {
			ctx := context.Background()
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("UpdateStatus", ctx, currUUID, models.ReplayStatusInProgress, models.ReplayMessage{}).Return(nil)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New").Return(replayRepository)
			errMessage := "fetch replay failed"
			replayRepository.On("GetByID", ctx, currUUID).Return(models.ReplaySpec{}, errors.New(errMessage))

			scheduler := new(mock.Scheduler)
			defer scheduler.AssertExpectations(t)

			worker := job.NewReplayWorker(log, replaySpecRepoFac, scheduler)
			err := worker.Process(ctx, replayRequest)
			assert.Equal(t, errMessage, err.Error())
		})
	})
}
