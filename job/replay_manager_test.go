package job_test

import (
	"context"
	"fmt"
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
	ctx := context.Background()
	logger.InitWithWriter(logger.DEBUG, ioutil.Discard)
	t.Run("Close", func(t *testing.T) {
		replayManagerConfig := job.ReplayManagerConfig{
			NumWorkers:    5,
			WorkerTimeout: 1000,
		}

		replayRepository := new(mock.ReplayRepository)
		defer replayRepository.AssertExpectations(t)
		replayRepository.On("GetByStatus", job.ReplayStatusToValidate).Return([]models.ReplaySpec{}, nil)

		replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
		defer replaySpecRepoFac.AssertExpectations(t)
		replaySpecRepoFac.On("New", models.JobSpec{}).Return(replayRepository)

		manager := job.NewManager(nil, replaySpecRepoFac, nil, replayManagerConfig, nil)
		err := manager.Close()
		assert.Nil(t, err)
	})
	t.Run("Init", func(t *testing.T) {
		replayManagerConfig := job.ReplayManagerConfig{
			NumWorkers:    0,
			WorkerTimeout: 1000,
			RunTimeout:    time.Hour * 8,
		}
		dagStartTime, _ := time.Parse(job.ReplayDateFormat, "2020-04-05")
		startDate, _ := time.Parse(job.ReplayDateFormat, "2020-08-22")
		endDate, _ := time.Parse(job.ReplayDateFormat, "2020-08-26")
		schedule := models.JobSpecSchedule{
			StartDate: dagStartTime,
			Interval:  "0 2 * * *",
		}
		jobSpec := models.JobSpec{
			Name:     "job-name",
			Schedule: schedule,
		}
		t.Run("should mark long running replay as failed", func(t *testing.T) {
			activeReplayUUID := uuid.Must(uuid.NewRandom())
			currentTime := time.Now()
			activeReplaySpecs := []models.ReplaySpec{
				{
					ID:        activeReplayUUID,
					Job:       jobSpec,
					StartDate: startDate,
					EndDate:   endDate,
					Status:    models.ReplayStatusInProgress,
					CreatedAt: currentTime.Add(time.Hour * -10),
				},
			}
			failedReplayMessage := models.ReplayMessage{
				Type:    job.ReplayRunTimeout,
				Message: fmt.Sprintf("replay has been running since %s", activeReplaySpecs[0].CreatedAt.UTC().Format(job.TimestampLogFormat)),
			}

			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByStatus", job.ReplayStatusToValidate).Return(activeReplaySpecs, nil)
			replayRepository.On("UpdateStatus", activeReplayUUID, models.ReplayStatusFailed, failedReplayMessage).Return(nil)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", models.JobSpec{}).Return(replayRepository)

			replayManager := job.NewManager(nil, replaySpecRepoFac, nil, replayManagerConfig, nil)
			replayManager.Init()
		})
	})
	t.Run("Replay", func(t *testing.T) {
		replayManagerConfig := job.ReplayManagerConfig{
			NumWorkers:    5,
			WorkerTimeout: 1000,
		}
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
			ID:       uuid.Must(uuid.NewRandom()),
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
			replayRepository.On("GetByStatus", job.ReplayStatusToValidate).Return([]models.ReplaySpec{}, store.ErrResourceNotFound).Twice()

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", models.JobSpec{}).Return(replayRepository)
			replaySpecRepoFac.On("New", replayRequest.Job).Return(replayRepository)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)
			objUUID := uuid.Must(uuid.NewRandom())
			errMessage := "error while generating uuid"
			uuidProvider.On("NewUUID").Return(objUUID, errors.New(errMessage))

			scheduler := new(mock.MockScheduler)
			defer scheduler.AssertExpectations(t)
			scheduler.On("GetDagRunStatus", ctx, replayRequest.Project, jobSpec.Name, startDate, reqBatchEndDate, reqBatchSize).Return([]models.JobStatus{}, nil)

			replayManager := job.NewManager(nil, replaySpecRepoFac, uuidProvider, replayManagerConfig, scheduler)
			_, err := replayManager.Replay(ctx, replayRequest)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), errMessage)
		})
		t.Run("should throw an error if replay repo throws error", func(t *testing.T) {
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			// worker init
			replayRepository.On("GetByStatus", job.ReplayStatusToValidate).Return([]models.ReplaySpec{}, store.ErrResourceNotFound).Twice()

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", models.JobSpec{}).Return(replayRepository)
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
			scheduler.On("GetDagRunStatus", ctx, replayRequest.Project, jobSpec.Name, startDate, reqBatchEndDate, reqBatchSize).Return([]models.JobStatus{}, nil)

			replayManager := job.NewManager(nil, replaySpecRepoFac, uuidProvider, replayManagerConfig, scheduler)
			_, err := replayManager.Replay(ctx, replayRequest)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), errMessage)
		})
		t.Run("should throw an error if unable to fetch active replays", func(t *testing.T) {
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByStatus", job.ReplayStatusToValidate).Return([]models.ReplaySpec{}, store.ErrResourceNotFound).Once()
			errMessage := "error checking other replays"
			replayRepository.On("GetByStatus", job.ReplayStatusToValidate).Return([]models.ReplaySpec{}, errors.New(errMessage))

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", models.JobSpec{}).Return(replayRepository)
			replaySpecRepoFac.On("New", replayRequest.Job).Return(replayRepository)

			scheduler := new(mock.MockScheduler)
			defer scheduler.AssertExpectations(t)
			scheduler.On("GetDagRunStatus", ctx, replayRequest.Project, jobSpec.Name, startDate, reqBatchEndDate, reqBatchSize).Return([]models.JobStatus{}, nil)

			replayManager := job.NewManager(nil, replaySpecRepoFac, nil, replayManagerConfig, scheduler)
			_, err := replayManager.Replay(ctx, replayRequest)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), errMessage)
		})
		t.Run("should throw an error if conflicting replays found", func(t *testing.T) {
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

			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByStatus", job.ReplayStatusToValidate).Return([]models.ReplaySpec{}, store.ErrResourceNotFound).Once()
			replayRepository.On("GetByStatus", job.ReplayStatusToValidate).Return(activeReplaySpec, nil)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", models.JobSpec{}).Return(replayRepository)
			replaySpecRepoFac.On("New", replayRequest.Job).Return(replayRepository)

			scheduler := new(mock.MockScheduler)
			defer scheduler.AssertExpectations(t)
			scheduler.On("GetDagRunStatus", ctx, replayRequest.Project, jobSpec.Name, startDate, reqBatchEndDate, reqBatchSize).Return([]models.JobStatus{}, nil)

			replayManager := job.NewManager(nil, replaySpecRepoFac, nil, replayManagerConfig, scheduler)

			_, err := replayManager.Replay(ctx, replayRequest)
			assert.Equal(t, err, job.ErrConflictedJobRun)
		})
		t.Run("should pass replay validation when no conflicting dag found", func(t *testing.T) {
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

			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByStatus", job.ReplayStatusToValidate).Return([]models.ReplaySpec{}, store.ErrResourceNotFound).Once()
			replayRepository.On("GetByStatus", job.ReplayStatusToValidate).Return(activeReplaySpec, nil)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", models.JobSpec{}).Return(replayRepository)
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
			scheduler.On("GetDagRunStatus", ctx, replayRequest.Project, jobSpec.Name, startDate, reqBatchEndDate, reqBatchSize).Return([]models.JobStatus{}, nil)

			replayManager := job.NewManager(nil, replaySpecRepoFac, uuidProvider, replayManagerConfig, scheduler)
			_, err := replayManager.Replay(ctx, replayRequest)
			assert.Equal(t, errMessage, err.Error())
		})
		t.Run("should pass replay validation when no conflicting runs found", func(t *testing.T) {
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

			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByStatus", job.ReplayStatusToValidate).Return([]models.ReplaySpec{}, store.ErrResourceNotFound).Once()
			replayRepository.On("GetByStatus", job.ReplayStatusToValidate).Return(activeReplaySpec, nil)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", models.JobSpec{}).Return(replayRepository)
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
			scheduler.On("GetDagRunStatus", ctx, replayRequest.Project, jobSpec.Name, startDate, reqBatchEndDate, reqBatchSize).Return([]models.JobStatus{}, nil)

			replayManager := job.NewManager(nil, replaySpecRepoFac, uuidProvider, replayManagerConfig, scheduler)
			_, err := replayManager.Replay(ctx, replayRequest)
			assert.Equal(t, errMessage, err.Error())
		})
		t.Run("should return error when unable to get status from scheduler", func(t *testing.T) {
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByStatus", job.ReplayStatusToValidate).Return([]models.ReplaySpec{}, store.ErrResourceNotFound).Once()

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", models.JobSpec{}).Return(replayRepository)
			replaySpecRepoFac.On("New", replayRequest.Job).Return(replayRepository)

			scheduler := new(mock.MockScheduler)
			defer scheduler.AssertExpectations(t)
			errMessage := "unable to get status"
			scheduler.On("GetDagRunStatus", ctx, replayRequest.Project, jobSpec.Name, startDate, reqBatchEndDate, reqBatchSize).Return([]models.JobStatus{}, errors.New(errMessage))

			replayManager := job.NewManager(nil, replaySpecRepoFac, nil, replayManagerConfig, scheduler)

			_, err := replayManager.Replay(ctx, replayRequest)
			assert.Equal(t, errMessage, err.Error())
		})
		t.Run("should return error when same job and run in the running state is found", func(t *testing.T) {
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByStatus", job.ReplayStatusToValidate).Return([]models.ReplaySpec{}, store.ErrResourceNotFound).Once()

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", models.JobSpec{}).Return(replayRepository)
			replaySpecRepoFac.On("New", replayRequest.Job).Return(replayRepository)

			scheduler := new(mock.MockScheduler)
			defer scheduler.AssertExpectations(t)
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
			_, err := replayManager.Replay(ctx, replayRequest)
			assert.Equal(t, job.ErrConflictedJobRun, err)
		})
		t.Run("should return error when no running instance found in scheduler but accepted in replay", func(t *testing.T) {
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

			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByStatus", job.ReplayStatusToValidate).Return([]models.ReplaySpec{}, store.ErrResourceNotFound).Once()
			replayRepository.On("GetByStatus", job.ReplayStatusToValidate).Return(activeReplaySpec, nil)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", models.JobSpec{}).Return(replayRepository)
			replaySpecRepoFac.On("New", replayRequest.Job).Return(replayRepository)

			scheduler := new(mock.MockScheduler)
			defer scheduler.AssertExpectations(t)
			jobStatus := []models.JobStatus{
				{
					ScheduledAt: time.Date(2021, time.Month(1), 1, 2, 0, 0, 0, time.UTC),
					State:       models.JobStatusStateRunning,
				},
			}
			scheduler.On("GetDagRunStatus", ctx, replayRequest.Project, jobSpec.Name, startDate, reqBatchEndDate, reqBatchSize).Return(jobStatus, nil)

			replayManager := job.NewManager(nil, replaySpecRepoFac, nil, replayManagerConfig, scheduler)
			_, err := replayManager.Replay(ctx, replayRequest)
			assert.Equal(t, job.ErrConflictedJobRun, err)
		})
		t.Run("should not validate conflicting dags but cancel conflicting replay when force enabled", func(t *testing.T) {
			activeReplayUUID := uuid.Must(uuid.NewRandom())
			activeReplaySpec := []models.ReplaySpec{
				{
					ID:        activeReplayUUID,
					Job:       jobSpec,
					StartDate: startDate,
					EndDate:   endDate,
					Status:    models.ReplayStatusInProgress,
				},
			}

			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByStatus", job.ReplayStatusToValidate).Return([]models.ReplaySpec{}, store.ErrResourceNotFound).Once()
			replayRepository.On("GetByJobIDAndStatus", activeReplaySpec[0].Job.ID, job.ReplayStatusToValidate).Return(activeReplaySpec, nil)

			cancelledReplayMessage := models.ReplayMessage{
				Type:    job.ErrConflictedJobRun.Error(),
				Message: fmt.Sprintf("force started replay with ID: %s", replayRequest.ID),
			}
			replayRepository.On("UpdateStatus", activeReplayUUID, models.ReplayStatusCancelled, cancelledReplayMessage).Return(nil)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", models.JobSpec{}).Return(replayRepository)
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

			replayRequest.Force = true
			replayManager := job.NewManager(nil, replaySpecRepoFac, uuidProvider, replayManagerConfig, nil)
			_, err := replayManager.Replay(ctx, replayRequest)
			assert.Equal(t, errMessage, err.Error())
		})
	})
}
