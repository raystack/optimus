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
	mocklib "github.com/stretchr/testify/mock"
)

func TestReplayManager(t *testing.T) {
	ctx := context.Background()
	logger.InitWithWriter(logger.DEBUG, ioutil.Discard)
	t.Run("Close", func(t *testing.T) {
		replayManagerConfig := job.ReplayManagerConfig{
			NumWorkers:    5,
			WorkerTimeout: 1000,
		}

		manager := job.NewManager(nil, nil, nil, replayManagerConfig, nil, nil, nil)
		err := manager.Close()
		assert.Nil(t, err)
	})
	t.Run("Replay", func(t *testing.T) {
		replayManagerConfig := job.ReplayManagerConfig{
			NumWorkers:    5,
			WorkerTimeout: 1000,
		}
		dagStartTime, _ := time.Parse(job.ReplayDateFormat, "2020-04-05")
		startDate, _ := time.Parse(job.ReplayDateFormat, "2020-08-22")
		endDate, _ := time.Parse(job.ReplayDateFormat, "2020-08-26")
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
		replayRequest := &models.ReplayRequest{
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

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New").Return(replayRepository)

			replayValidator := new(mock.ReplayValidator)
			replayValidator.On("Validate", mocklib.Anything, replayRepository, replayRequest).Return(nil)
			defer replayValidator.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)
			objUUID := uuid.Must(uuid.NewRandom())
			errMessage := "error while generating uuid"
			uuidProvider.On("NewUUID").Return(objUUID, errors.New(errMessage))

			replayManager := job.NewManager(nil, replaySpecRepoFac, uuidProvider, replayManagerConfig, nil, replayValidator, nil)
			_, err := replayManager.Replay(ctx, replayRequest)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), errMessage)
		})
		t.Run("should throw an error if replay repo throws error", func(t *testing.T) {
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New").Return(replayRepository)

			replayValidator := new(mock.ReplayValidator)
			replayValidator.On("Validate", mocklib.Anything, replayRepository, replayRequest).Return(nil)
			defer replayValidator.AssertExpectations(t)

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

			replayManager := job.NewManager(nil, replaySpecRepoFac, uuidProvider, replayManagerConfig, nil, replayValidator, nil)
			_, err := replayManager.Replay(ctx, replayRequest)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), errMessage)
		})
		t.Run("should throw an error if conflicting replays found", func(t *testing.T) {
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New").Return(replayRepository)

			replayValidator := new(mock.ReplayValidator)
			replayValidator.On("Validate", mocklib.Anything, replayRepository, replayRequest).Return(job.ErrConflictedJobRun)
			defer replayValidator.AssertExpectations(t)

			replayManager := job.NewManager(nil, replaySpecRepoFac, nil, replayManagerConfig, nil, replayValidator, nil)

			_, err := replayManager.Replay(ctx, replayRequest)
			assert.Equal(t, err, job.ErrConflictedJobRun)
		})
		t.Run("should not throw validation error when no conflicting replays found", func(t *testing.T) {
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New").Return(replayRepository)

			replayValidator := new(mock.ReplayValidator)
			replayValidator.On("Validate", mocklib.Anything, replayRepository, replayRequest).Return(nil)
			defer replayValidator.AssertExpectations(t)

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

			replayManager := job.NewManager(nil, replaySpecRepoFac, uuidProvider, replayManagerConfig, nil, replayValidator, nil)
			_, err := replayManager.Replay(ctx, replayRequest)
			assert.Equal(t, errMessage, err.Error())
		})
	})
	t.Run("GetReplay", func(t *testing.T) {
		t.Run("should return replay given a valid UUID", func(t *testing.T) {
			replayUUID := uuid.Must(uuid.NewRandom())
			replayJob := models.JobSpec{
				Name: "sample-job",
			}
			replaySpec := models.ReplaySpec{
				ID:        replayUUID,
				Job:       replayJob,
				StartDate: time.Date(2020, time.Month(8), 20, 2, 0, 0, 0, time.UTC),
				EndDate:   time.Date(2020, time.Month(8), 22, 2, 0, 0, 0, time.UTC),
				Status:    models.ReplayStatusAccepted,
			}

			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByID", replayUUID).Return(replaySpec, nil)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New").Return(replayRepository)

			replayManager := job.NewManager(nil, replaySpecRepoFac, nil, job.ReplayManagerConfig{}, nil, nil, nil)
			replayResult, err := replayManager.GetReplay(replayUUID)

			assert.Nil(t, err)
			assert.Equal(t, &replaySpec, replayResult)
		})
		t.Run("should return error when replay is not found", func(t *testing.T) {
			replayUUID := uuid.Must(uuid.NewRandom())

			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByID", replayUUID).Return(models.ReplaySpec{}, store.ErrResourceNotFound)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New").Return(replayRepository)

			replayManager := job.NewManager(nil, replaySpecRepoFac, nil, job.ReplayManagerConfig{}, nil, nil, nil)
			replayResult, err := replayManager.GetReplay(replayUUID)

			assert.Equal(t, err, store.ErrResourceNotFound)
			assert.Nil(t, replayResult)
		})
	})
	t.Run("GetRunStatus", func(t *testing.T) {
		t.Run("should return status of every runs in every jobs", func(t *testing.T) {
			replayUUID := uuid.Must(uuid.NewRandom())
			jobName := "dag1-no-deps"
			jobSpec := models.JobSpec{Name: jobName, Dependencies: map[string]models.JobSpecDependency{}}
			jobStatusList := []models.JobStatus{
				{
					ScheduledAt: time.Date(2020, time.Month(8), 20, 2, 0, 0, 0, time.UTC),
					State:       models.JobStatusStateSuccess,
				},
				{
					ScheduledAt: time.Date(2020, time.Month(8), 21, 2, 0, 0, 0, time.UTC),
					State:       models.JobStatusStateSuccess,
				},
			}
			startDate := time.Date(2020, time.Month(8), 20, 0, 0, 0, 0, time.UTC)
			endDate := time.Date(2020, time.Month(8), 22, 0, 0, 0, 0, time.UTC)
			replayRequest := &models.ReplayRequest{
				ID:    replayUUID,
				Job:   jobSpec,
				Start: startDate,
				End:   endDate,
				Project: models.ProjectSpec{
					Name: "project-name",
				},
				Force: false,
			}

			scheduler := new(mock.Scheduler)
			defer scheduler.AssertExpectations(t)
			batchEndDate := endDate.AddDate(0, 0, 1).Add(time.Second * -1)
			scheduler.On("GetDagRunStatus", ctx, replayRequest.Project, jobName, startDate, batchEndDate, 100).Return(jobStatusList, nil)

			replayManager := job.NewManager(nil, nil, nil, job.ReplayManagerConfig{}, scheduler, nil, nil)
			jobStatusMap, err := replayManager.GetRunStatus(context.TODO(), replayRequest, jobName)

			assert.Nil(t, err)
			assert.Equal(t, jobStatusList, jobStatusMap)
		})
	})
}
