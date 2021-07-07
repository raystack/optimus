package job_test

import (
	"context"
	"io/ioutil"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/optimus/core/logger"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestReplayManager(t *testing.T) {
	logger.InitWithWriter(logger.DEBUG, ioutil.Discard)
	replayManagerConfig := job.ReplayManagerConfig{
		NumWorkers:    5,
		WorkerTimeout: 1000,
	}
	t.Run("Close", func(t *testing.T) {
		manager := job.NewManager(nil, nil, nil, replayManagerConfig, nil)
		err := manager.Close()
		assert.Nil(t, err)
	})
	t.Run("Replay", func(t *testing.T) {
		dagStartTime, _ := time.Parse(job.ReplayDateFormat, "2020-04-05")
		startDate, _ := time.Parse(job.ReplayDateFormat, "2020-08-22")
		endDate, _ := time.Parse(job.ReplayDateFormat, "2020-08-26")
		reqBatchEndDate := endDate.AddDate(0, 0, 1)
		reqBatchSize := 100
		schedule := models.JobSpecSchedule{
			StartDate: dagStartTime,
			Interval:  "0 2 * * *",
		}
		jobSpec := models.JobSpec{
			Name:     "job-name",
			Schedule: schedule,
		}
		jobSpec2 := models.JobSpec{
			ID:       uuid.Must(uuid.NewRandom()),
			Name:     "job-name-2",
			Schedule: schedule,
		}
		replayRequest := &models.ReplayWorkerRequest{
			Job:   jobSpec,
			Start: startDate,
			End:   endDate,
			Project: models.ProjectSpec{
				Name: "project-name",
			},
			JobSpecMap: map[string]models.JobSpec{
				jobSpec.Name:  jobSpec,
				jobSpec2.Name: jobSpec2,
			},
		}
		t.Run("should throw error if uuid provider returns failure", func(t *testing.T) {
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			statusToCheck := []string{models.ReplayStatusInProgress, models.ReplayStatusAccepted}
			replayRepository.On("GetByStatus", statusToCheck).Return([]models.ReplaySpec{}, store.ErrResourceNotFound)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", replayRequest.Job).Return(replayRepository)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)
			objUUID := uuid.Must(uuid.NewRandom())
			errMessage := "error while generating uuid"
			uuidProvider.On("NewUUID").Return(objUUID, errors.New(errMessage))

			scheduler := new(mock.MockScheduler)
			defer scheduler.AssertExpectations(t)
			ctx := context.Background()
			scheduler.On("GetDagRunStatus", ctx, replayRequest.Project, jobSpec.Name, startDate, reqBatchEndDate, reqBatchSize).Return([]models.JobStatus{}, nil)

			replayManager := job.NewManager(nil, replaySpecRepoFac, uuidProvider, replayManagerConfig, scheduler)
			_, err := replayManager.Replay(replayRequest)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), errMessage)
		})
		t.Run("should throw an error if replay repo throws error", func(t *testing.T) {
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			statusToCheck := []string{models.ReplayStatusInProgress, models.ReplayStatusAccepted}
			replayRepository.On("GetByStatus", statusToCheck).Return([]models.ReplaySpec{}, store.ErrResourceNotFound)

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

			scheduler := new(mock.MockScheduler)
			defer scheduler.AssertExpectations(t)
			ctx := context.Background()
			scheduler.On("GetDagRunStatus", ctx, replayRequest.Project, jobSpec.Name, startDate, reqBatchEndDate, reqBatchSize).Return([]models.JobStatus{}, nil)

			replayManager := job.NewManager(nil, replaySpecRepoFac, uuidProvider, replayManagerConfig, scheduler)
			_, err := replayManager.Replay(replayRequest)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), errMessage)
		})
		t.Run("should throw an error if unable to fetch active replays", func(t *testing.T) {
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			statusToCheck := []string{models.ReplayStatusInProgress, models.ReplayStatusAccepted}
			errMessage := "error checking other replays"
			replayRepository.On("GetByStatus", statusToCheck).Return([]models.ReplaySpec{}, errors.New(errMessage))

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", replayRequest.Job).Return(replayRepository)

			scheduler := new(mock.MockScheduler)
			defer scheduler.AssertExpectations(t)
			ctx := context.Background()
			scheduler.On("GetDagRunStatus", ctx, replayRequest.Project, jobSpec.Name, startDate, reqBatchEndDate, reqBatchSize).Return([]models.JobStatus{}, nil)

			replayManager := job.NewManager(nil, replaySpecRepoFac, nil, replayManagerConfig, scheduler)
			_, err := replayManager.Replay(replayRequest)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), errMessage)
		})
		t.Run("should throw an error if conflicting replays found", func(t *testing.T) {
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			statusToCheck := []string{models.ReplayStatusInProgress, models.ReplayStatusAccepted}

			activeReplayUUID := uuid.Must(uuid.NewRandom())
			activeJobUUID := uuid.Must(uuid.NewRandom())
			activeJobSpec := models.JobSpec{
				ID:       activeJobUUID,
				Name:     "job-name",
				Schedule: schedule,
			}
			activeReplaySpec := []models.ReplaySpec{
				{
					ID:        activeReplayUUID,
					Job:       activeJobSpec,
					StartDate: startDate,
					EndDate:   endDate,
					Status:    models.ReplayStatusInProgress,
				},
			}
			replayRepository.On("GetByStatus", statusToCheck).Return(activeReplaySpec, nil)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", replayRequest.Job).Return(replayRepository)

			scheduler := new(mock.MockScheduler)
			defer scheduler.AssertExpectations(t)
			ctx := context.Background()
			scheduler.On("GetDagRunStatus", ctx, replayRequest.Project, jobSpec.Name, startDate, reqBatchEndDate, reqBatchSize).Return([]models.JobStatus{}, nil)

			replayManager := job.NewManager(nil, replaySpecRepoFac, nil, replayManagerConfig, scheduler)

			_, err := replayManager.Replay(replayRequest)
			assert.Equal(t, err, job.ErrConflictedJobRun)
		})
		t.Run("should pass replay validation when no conflicting dag found", func(t *testing.T) {
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			statusToCheck := []string{models.ReplayStatusInProgress, models.ReplayStatusAccepted}

			activeReplayUUID := uuid.Must(uuid.NewRandom())
			activeJobSpec := jobSpec2
			activeReplaySpec := []models.ReplaySpec{
				{
					ID:        activeReplayUUID,
					Job:       activeJobSpec,
					StartDate: startDate,
					EndDate:   endDate,
					Status:    models.ReplayStatusInProgress,
				},
			}
			replayRepository.On("GetByStatus", statusToCheck).Return(activeReplaySpec, nil)

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

			scheduler := new(mock.MockScheduler)
			defer scheduler.AssertExpectations(t)
			ctx := context.Background()
			scheduler.On("GetDagRunStatus", ctx, replayRequest.Project, jobSpec.Name, startDate, reqBatchEndDate, reqBatchSize).Return([]models.JobStatus{}, nil)

			replayManager := job.NewManager(nil, replaySpecRepoFac, uuidProvider, replayManagerConfig, scheduler)

			_, err := replayManager.Replay(replayRequest)
			assert.Equal(t, errMessage, err.Error())
		})
		t.Run("should pass replay validation when no conflicting runs found", func(t *testing.T) {
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			statusToCheck := []string{models.ReplayStatusInProgress, models.ReplayStatusAccepted}

			activeReplayUUID := uuid.Must(uuid.NewRandom())
			activeStartDate, _ := time.Parse(job.ReplayDateFormat, "2021-01-01")
			activeEndDate, _ := time.Parse(job.ReplayDateFormat, "2021-02-01")
			activeReplaySpec := []models.ReplaySpec{
				{
					ID:        activeReplayUUID,
					Job:       jobSpec,
					StartDate: activeStartDate,
					EndDate:   activeEndDate,
					Status:    models.ReplayStatusInProgress,
				},
			}
			replayRepository.On("GetByStatus", statusToCheck).Return(activeReplaySpec, nil)

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

			scheduler := new(mock.MockScheduler)
			defer scheduler.AssertExpectations(t)
			ctx := context.Background()
			scheduler.On("GetDagRunStatus", ctx, replayRequest.Project, jobSpec.Name, startDate, reqBatchEndDate, reqBatchSize).Return([]models.JobStatus{}, nil)

			replayManager := job.NewManager(nil, replaySpecRepoFac, uuidProvider, replayManagerConfig, scheduler)

			_, err := replayManager.Replay(replayRequest)
			assert.Equal(t, errMessage, err.Error())
		})
		t.Run("should return error when unable to get status from scheduler", func(t *testing.T) {
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", replayRequest.Job).Return(replayRepository)

			scheduler := new(mock.MockScheduler)
			defer scheduler.AssertExpectations(t)
			ctx := context.Background()
			errMessage := "unable to get status"
			scheduler.On("GetDagRunStatus", ctx, replayRequest.Project, jobSpec.Name, startDate, reqBatchEndDate, reqBatchSize).Return([]models.JobStatus{}, errors.New(errMessage))

			replayManager := job.NewManager(nil, replaySpecRepoFac, nil, replayManagerConfig, scheduler)

			_, err := replayManager.Replay(replayRequest)
			assert.Equal(t, errMessage, err.Error())
		})
		t.Run("should return error when same job and run in the running state is found", func(t *testing.T) {
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", replayRequest.Job).Return(replayRepository)

			scheduler := new(mock.MockScheduler)
			defer scheduler.AssertExpectations(t)
			ctx := context.Background()
			jobStatus := []models.JobStatus{
				{
					ScheduledAt: time.Date(2020, time.Month(8), 22, 2, 0, 0, 0, time.UTC),
					State:       models.JobStatusStateSuccess,
				},
				{
					ScheduledAt: time.Date(2020, time.Month(8), 23, 2, 0, 0, 0, time.UTC),
					State:       models.JobStatusStateRunning,
				},
			}
			scheduler.On("GetDagRunStatus", ctx, replayRequest.Project, jobSpec.Name, startDate, reqBatchEndDate, reqBatchSize).Return(jobStatus, nil)

			replayManager := job.NewManager(nil, replaySpecRepoFac, nil, replayManagerConfig, scheduler)

			_, err := replayManager.Replay(replayRequest)
			assert.Equal(t, job.ErrConflictedJobRun, err)
		})
		t.Run("should return error when no running instance found in scheduler but accepted in replay", func(t *testing.T) {
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			statusToCheck := []string{models.ReplayStatusInProgress, models.ReplayStatusAccepted}

			activeReplayUUID := uuid.Must(uuid.NewRandom())
			activeJobUUID := uuid.Must(uuid.NewRandom())
			activeJobSpec := models.JobSpec{
				ID:       activeJobUUID,
				Name:     "job-name",
				Schedule: schedule,
			}
			activeReplaySpec := []models.ReplaySpec{
				{
					ID:        activeReplayUUID,
					Job:       activeJobSpec,
					StartDate: startDate,
					EndDate:   endDate,
					Status:    models.ReplayStatusInProgress,
				},
			}
			replayRepository.On("GetByStatus", statusToCheck).Return(activeReplaySpec, nil)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", replayRequest.Job).Return(replayRepository)

			scheduler := new(mock.MockScheduler)
			defer scheduler.AssertExpectations(t)
			ctx := context.Background()
			jobStatus := []models.JobStatus{
				{
					ScheduledAt: time.Date(2021, time.Month(1), 1, 2, 0, 0, 0, time.UTC),
					State:       models.JobStatusStateRunning,
				},
			}
			scheduler.On("GetDagRunStatus", ctx, replayRequest.Project, jobSpec.Name, startDate, reqBatchEndDate, reqBatchSize).Return(jobStatus, nil)

			replayManager := job.NewManager(nil, replaySpecRepoFac, nil, replayManagerConfig, scheduler)

			_, err := replayManager.Replay(replayRequest)
			assert.Equal(t, job.ErrConflictedJobRun, err)
		})
	})
}
