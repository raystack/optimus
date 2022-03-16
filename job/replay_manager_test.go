package job_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
	mocklib "github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

func TestReplayManager(t *testing.T) {
	ctx := context.Background()
	log := log.NewNoop()
	t.Run("Close", func(t *testing.T) {
		replayManagerConfig := job.ReplayManagerConfig{
			NumWorkers:    3,
			WorkerTimeout: time.Second,
		}

		worker := mock.NewReplayWorker()
		replayWorkerFact := new(mock.ReplayWorkerFactory)
		replayWorkerFact.On("New").Return(worker)
		defer replayWorkerFact.AssertExpectations(t)

		manager := job.NewManager(log, replayWorkerFact, nil, nil, replayManagerConfig, nil, nil, nil)
		worker.Close()

		err := manager.Close()
		assert.Nil(t, err)
	})
	t.Run("Replay", func(t *testing.T) {
		replayManagerConfig := job.ReplayManagerConfig{
			NumWorkers:    3,
			WorkerTimeout: time.Second * 10,
		}
		dagStartTime := time.Date(2020, time.Month(4), 5, 0, 0, 0, 0, time.UTC)
		startDate := time.Date(2020, time.Month(8), 22, 0, 0, 0, 0, time.UTC)
		endDate := time.Date(2020, time.Month(8), 26, 0, 0, 0, 0, time.UTC)
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
		replayRequest := models.ReplayRequest{
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
			replayValidator.On("Validate", mocklib.Anything, replayRepository, replayRequest, mocklib.Anything).Return(nil)
			defer replayValidator.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)
			objUUID := uuid.Must(uuid.NewRandom())
			errMessage := "error while generating uuid"
			uuidProvider.On("NewUUID").Return(objUUID, errors.New(errMessage))

			worker := mock.NewReplayWorker()
			replayWorkerFact := new(mock.ReplayWorkerFactory)
			replayWorkerFact.On("New").Return(worker)
			defer replayWorkerFact.AssertExpectations(t)

			replayManager := job.NewManager(log, replayWorkerFact, replaySpecRepoFac, uuidProvider, replayManagerConfig, nil, replayValidator, nil)
			_, err := replayManager.Replay(ctx, replayRequest)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), errMessage)

			worker.Close()
			err = replayManager.Close()
			assert.Nil(t, err)
		})
		t.Run("should throw an error if replay repo throws error", func(t *testing.T) {
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New").Return(replayRepository)

			replayValidator := new(mock.ReplayValidator)
			replayValidator.On("Validate", mocklib.Anything, replayRepository, replayRequest, mocklib.Anything).Return(nil)
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
			replayRepository.On("Insert", ctx, toInsertReplaySpec).Return(errors.New(errMessage))

			worker := mock.NewReplayWorker()
			replayWorkerFact := new(mock.ReplayWorkerFactory)
			replayWorkerFact.On("New").Return(worker)
			defer replayWorkerFact.AssertExpectations(t)

			replayManager := job.NewManager(log, replayWorkerFact, replaySpecRepoFac, uuidProvider, replayManagerConfig, nil, replayValidator, nil)
			_, err := replayManager.Replay(ctx, replayRequest)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), errMessage)

			worker.Close()
			err = replayManager.Close()
			assert.Nil(t, err)
		})
		t.Run("should throw an error if conflicting replays found", func(t *testing.T) {
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New").Return(replayRepository)

			replayValidator := new(mock.ReplayValidator)
			replayValidator.On("Validate", mocklib.Anything, replayRepository, replayRequest, mocklib.Anything).Return(job.ErrConflictedJobRun)
			defer replayValidator.AssertExpectations(t)

			worker := mock.NewReplayWorker()
			replayWorkerFact := new(mock.ReplayWorkerFactory)
			replayWorkerFact.On("New").Return(worker)
			defer replayWorkerFact.AssertExpectations(t)

			replayManager := job.NewManager(log, replayWorkerFact, replaySpecRepoFac, nil, replayManagerConfig, nil, replayValidator, nil)

			_, err := replayManager.Replay(ctx, replayRequest)
			assert.Equal(t, err, job.ErrConflictedJobRun)

			worker.Close()
			err = replayManager.Close()
			assert.Nil(t, err)
		})
		t.Run("should not throw validation error when no conflicting replays found", func(t *testing.T) {
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New").Return(replayRepository)

			replayValidator := new(mock.ReplayValidator)
			replayValidator.On("Validate", mocklib.Anything, replayRepository, replayRequest, mocklib.Anything).Return(nil)
			defer replayValidator.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)
			objUUID := uuid.Must(uuid.NewRandom())
			uuidProvider.On("NewUUID").Return(objUUID, nil)

			toInsertReplaySpec := &models.ReplaySpec{
				ID:        objUUID,
				Job:       jobSpec,
				StartDate: startDate,
				EndDate:   endDate,
				Status:    models.ReplayStatusAccepted,
			}
			replayRepository.On("Insert", ctx, toInsertReplaySpec).Return(nil)

			worker := mock.NewReplayWorker()
			replayRequestToProcess := replayRequest
			replayRequestToProcess.ID = objUUID
			worker.On("Process", mocklib.Anything, replayRequestToProcess).Return(nil)
			defer worker.AssertExpectations(t)

			replayWorkerFact := new(mock.ReplayWorkerFactory)
			replayWorkerFact.On("New").Return(worker)
			defer replayWorkerFact.AssertExpectations(t)

			replayManager := job.NewManager(log, replayWorkerFact, replaySpecRepoFac, uuidProvider, job.ReplayManagerConfig{
				NumWorkers:    1,
				WorkerTimeout: time.Second * 5,
			}, nil, replayValidator, nil)
			_, err := replayManager.Replay(ctx, replayRequest)
			assert.Nil(t, err)

			worker.Close()
			err = replayManager.Close()
			assert.Nil(t, err)
		})
		// TODO fix this test
		//t.Run("should throw an error if workers are busy", func(t *testing.T) {
		//	replayRepository := new(mock.ReplayRepository)
		//	defer replayRepository.AssertExpectations(t)
		//
		//	replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
		//	defer replaySpecRepoFac.AssertExpectations(t)
		//	replaySpecRepoFac.On("New").Return(replayRepository)
		//
		//	replayValidator := new(mock.ReplayValidator)
		//	replayValidator.On("Validate", mocklib.Anything, replayRepository, replayRequest, mocklib.Anything).Return(nil).Times(4)
		//	defer replayValidator.AssertExpectations(t)
		//
		//	uuidProvider := new(mock.UUIDProvider)
		//	defer uuidProvider.AssertExpectations(t)
		//	objUUID := uuid.Must(uuid.NewRandom())
		//	uuidProvider.On("NewUUID").Return(objUUID, nil).Times(4)
		//
		//	toInsertReplaySpec := &models.ReplaySpec{
		//		ID:        objUUID,
		//		Job:       jobSpec,
		//		StartDate: startDate,
		//		EndDate:   endDate,
		//		Status:    models.ReplayStatusAccepted,
		//	}
		//
		//	replayRepository.On("Insert", ctx, toInsertReplaySpec).Return(nil).Times(4)
		//
		//	// other workers should not be closed before encounter full state.
		//	// replay will be cancelled when workers are full.
		//	var wg sync.WaitGroup
		//	wg.Add(1)
		//	cancelledMsg := models.ReplayMessage{
		//		Type:    models.ReplayStatusCancelled,
		//		Message: job.ErrRequestQueueFull.Error(),
		//	}
		//	replayRepository.On("UpdateStatus", objUUID, models.ReplayStatusCancelled, cancelledMsg).
		//		Return(nil).Once().Run(func(args mocklib.Arguments) {
		//		wg.Done()
		//	})
		//
		//	var replayWorkers []interface{}
		//	for i := 0; i < 3; i++ {
		//		replayWorker := mock.NewReplayWorker()
		//		replayRequestToProcess := replayRequest
		//		replayRequestToProcess.ID = objUUID
		//		// worker will not finish process immediately
		//		replayWorker.On("Process", mocklib.Anything, replayRequestToProcess).Return(nil).
		//			Times(1).After(time.Second)
		//		replayWorkers = append(replayWorkers, replayWorker)
		//	}
		//	replayWorkerFact := &mock.ReplayWorkerFactoryIndexed{
		//		Workers: replayWorkers,
		//	}
		//	replayWorkerFact.On("New").Times(replayManagerConfig.NumWorkers)
		//	defer replayWorkerFact.AssertExpectations(t)
		//
		//	replayManager := job.NewManager(log, replayWorkerFact, replaySpecRepoFac, uuidProvider, replayManagerConfig, nil, replayValidator, nil)
		//
		//	_, err := replayManager.Replay(ctx, replayRequest)
		//	assert.Nil(t, err)
		//	_, err = replayManager.Replay(ctx, replayRequest)
		//	assert.Nil(t, err)
		//	_, err = replayManager.Replay(ctx, replayRequest)
		//	assert.Nil(t, err)
		//
		//	_, err = replayManager.Replay(ctx, replayRequest)
		//	assert.Equal(t, job.ErrRequestQueueFull, err)
		//
		//	wg.Wait()
		//	for _, w := range replayWorkers {
		//		rw := w.(*mock.ReplayWorker)
		//		rw.Close()
		//		rw.AssertExpectations(t)
		//	}
		//
		//	err = replayManager.Close()
		//	assert.Nil(t, err)
		//})
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
			replayRepository.On("GetByID", ctx, replayUUID).Return(replaySpec, nil)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New").Return(replayRepository)

			replayManager := job.NewManager(log, nil, replaySpecRepoFac, nil, job.ReplayManagerConfig{}, nil, nil, nil)
			replayResult, err := replayManager.GetReplay(ctx, replayUUID)

			assert.Nil(t, err)
			assert.Equal(t, replaySpec, replayResult)

			err = replayManager.Close()
			assert.Nil(t, err)
		})
		t.Run("should return error when replay is not found", func(t *testing.T) {
			replayUUID := uuid.Must(uuid.NewRandom())

			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByID", ctx, replayUUID).Return(models.ReplaySpec{}, store.ErrResourceNotFound)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New").Return(replayRepository)

			replayManager := job.NewManager(log, nil, replaySpecRepoFac, nil, job.ReplayManagerConfig{}, nil, nil, nil)
			replayResult, err := replayManager.GetReplay(ctx, replayUUID)

			assert.Equal(t, err, store.ErrResourceNotFound)
			assert.Equal(t, models.ReplaySpec{}, replayResult)

			err = replayManager.Close()
			assert.Nil(t, err)
		})
	})
	t.Run("GetReplayList", func(t *testing.T) {
		t.Run("should return replay list given a valid project UUID", func(t *testing.T) {
			projectUUID := uuid.Must(uuid.NewRandom())
			replayJob := models.JobSpec{
				Name: "sample-job",
			}
			startDate := time.Date(2020, time.Month(8), 5, 0, 0, 0, 0, time.UTC)
			endDate := time.Date(2020, time.Month(8), 7, 0, 0, 0, 0, time.UTC)
			replaySpecs := []models.ReplaySpec{
				{
					ID:        uuid.Must(uuid.NewRandom()),
					Job:       replayJob,
					StartDate: startDate,
					EndDate:   endDate,
					Status:    models.ReplayStatusReplayed,
					CreatedAt: time.Now().UTC().Add(time.Hour * -1),
				},
			}

			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByProjectID", ctx, projectUUID).Return(replaySpecs, nil)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New").Return(replayRepository)

			replayManager := job.NewManager(log, nil, replaySpecRepoFac, nil, job.ReplayManagerConfig{}, nil, nil, nil)
			replayListResult, err := replayManager.GetReplayList(ctx, projectUUID)

			assert.Nil(t, err)
			assert.Equal(t, replaySpecs, replayListResult)

			err = replayManager.Close()
			assert.Nil(t, err)
		})
		t.Run("should only return recent replays given a valid project UUID", func(t *testing.T) {
			projectUUID := uuid.Must(uuid.NewRandom())
			replayJob := models.JobSpec{
				Name: "sample-job",
			}
			startDate := time.Date(2020, time.Month(8), 5, 0, 0, 0, 0, time.UTC)
			endDate := time.Date(2020, time.Month(8), 7, 0, 0, 0, 0, time.UTC)
			replaySpecs := []models.ReplaySpec{
				{
					ID:        uuid.Must(uuid.NewRandom()),
					Job:       replayJob,
					StartDate: startDate,
					EndDate:   endDate,
					Status:    models.ReplayStatusReplayed,
					CreatedAt: time.Now().UTC().Add(time.Hour * -1),
				},
				{
					ID:        uuid.Must(uuid.NewRandom()),
					Job:       replayJob,
					StartDate: startDate,
					EndDate:   endDate,
					Status:    models.ReplayStatusReplayed,
					CreatedAt: time.Now().UTC().Add(time.Hour * -24 * 100),
				},
			}

			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByProjectID", ctx, projectUUID).Return(replaySpecs, nil)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New").Return(replayRepository)

			replayManager := job.NewManager(log, nil, replaySpecRepoFac, nil, job.ReplayManagerConfig{}, nil, nil, nil)
			replayListResult, err := replayManager.GetReplayList(ctx, projectUUID)

			expectedReplaySpecs := []models.ReplaySpec{replaySpecs[0]}
			assert.Nil(t, err)
			assert.Equal(t, expectedReplaySpecs, replayListResult)

			err = replayManager.Close()
			assert.Nil(t, err)
		})
		t.Run("should not return error when replay is not found", func(t *testing.T) {
			projectUUID := uuid.Must(uuid.NewRandom())

			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByProjectID", ctx, projectUUID).Return([]models.ReplaySpec{}, store.ErrResourceNotFound)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New").Return(replayRepository)

			replayManager := job.NewManager(log, nil, replaySpecRepoFac, nil, job.ReplayManagerConfig{}, nil, nil, nil)
			replayResult, err := replayManager.GetReplayList(ctx, projectUUID)

			assert.Nil(t, err)
			assert.Equal(t, []models.ReplaySpec{}, replayResult)

			err = replayManager.Close()
			assert.Nil(t, err)
		})
		t.Run("should return error when unable to get replay list", func(t *testing.T) {
			projectUUID := uuid.Must(uuid.NewRandom())

			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			errorMsg := "unable to get list of replays"
			replayRepository.On("GetByProjectID", ctx, projectUUID).Return([]models.ReplaySpec{}, errors.New(errorMsg))

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New").Return(replayRepository)

			replayManager := job.NewManager(log, nil, replaySpecRepoFac, nil, job.ReplayManagerConfig{}, nil, nil, nil)
			replayResult, err := replayManager.GetReplayList(ctx, projectUUID)

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, []models.ReplaySpec{}, replayResult)

			err = replayManager.Close()
			assert.Nil(t, err)
		})
	})
	t.Run("GetRunStatus", func(t *testing.T) {
		projectSpec := models.ProjectSpec{
			Name: "project-name",
		}
		t.Run("should return status of every runs in every jobs", func(t *testing.T) {
			replayUUID := uuid.Must(uuid.NewRandom())
			jobName := "dag1-no-deps"
			jobSpec := models.JobSpec{
				Name:         jobName,
				Dependencies: map[string]models.JobSpecDependency{},
			}
			jobStatusList := []models.JobStatus{
				{
					ScheduledAt: time.Date(2020, time.Month(8), 20, 2, 0, 0, 0, time.UTC),
					State:       models.RunStateSuccess,
				},
				{
					ScheduledAt: time.Date(2020, time.Month(8), 21, 2, 0, 0, 0, time.UTC),
					State:       models.RunStateSuccess,
				},
			}
			startDate := time.Date(2020, time.Month(8), 20, 0, 0, 0, 0, time.UTC)
			endDate := time.Date(2020, time.Month(8), 22, 0, 0, 0, 0, time.UTC)
			replaySpec := models.ReplaySpec{
				ID:        replayUUID,
				Job:       jobSpec,
				StartDate: startDate,
				EndDate:   endDate,
			}

			scheduler := new(mock.Scheduler)
			defer scheduler.AssertExpectations(t)
			batchEndDate := endDate.AddDate(0, 0, 1).Add(time.Second * -1)
			scheduler.On("GetJobRunStatus", ctx, projectSpec, jobSpec.Name, startDate, batchEndDate, 100).Return(jobStatusList, nil)

			replayManager := job.NewManager(log, nil, nil, nil, job.ReplayManagerConfig{}, scheduler, nil, nil)
			jobStatusMap, err := replayManager.GetRunStatus(context.TODO(), projectSpec, replaySpec.StartDate, replaySpec.EndDate, jobSpec.Name)

			assert.Nil(t, err)
			assert.Equal(t, jobStatusList, jobStatusMap)

			err = replayManager.Close()
			assert.Nil(t, err)
		})
	})
}
