package job_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/odpf/optimus/core/tree"

	"github.com/google/uuid"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
)

func TestReplayValidator(t *testing.T) {
	t.Run("Validate", func(t *testing.T) {
		ctx := context.TODO()
		reqBatchSize := 100
		dagStartTime := time.Date(2020, time.Month(4), 05, 0, 0, 0, 0, time.UTC)
		startDate := time.Date(2020, time.Month(8), 22, 0, 0, 0, 0, time.UTC)
		endDate := time.Date(2020, time.Month(8), 26, 0, 0, 0, 0, time.UTC)
		batchEndDate := endDate.AddDate(0, 0, 1).Add(time.Second * -1)
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
		projectSpec := models.ProjectSpec{
			Name: "project-name",
		}
		replayRequest := models.ReplayRequest{
			Job:     jobSpec,
			Start:   startDate,
			End:     endDate,
			Project: projectSpec,
			JobSpecMap: map[string]models.JobSpec{
				jobSpec.Name:  jobSpec,
				jobSpec2.Name: jobSpec2,
			},
		}
		executionTree := tree.NewTreeNode(jobSpec)
		executionTree.Runs.Add(time.Date(2020, time.Month(8), 22, 2, 0, 0, 0, time.UTC))
		executionTree.Runs.Add(time.Date(2020, time.Month(8), 23, 2, 0, 0, 0, time.UTC))
		executionTree.Runs.Add(time.Date(2020, time.Month(8), 24, 2, 0, 0, 0, time.UTC))
		executionTree.Runs.Add(time.Date(2020, time.Month(8), 25, 2, 0, 0, 0, time.UTC))
		executionTree.Runs.Add(time.Date(2020, time.Month(8), 26, 2, 0, 0, 0, time.UTC))

		t.Run("should throw an error if unable to fetch active replays", func(t *testing.T) {
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			errMessage := "error checking other replays"
			replayRepository.On("GetByStatus", ctx, job.ReplayStatusToValidate).Return([]models.ReplaySpec{}, errors.New(errMessage))

			scheduler := new(mock.Scheduler)
			defer scheduler.AssertExpectations(t)
			scheduler.On("GetJobRunStatus", ctx, projectSpec, jobSpec.Name, startDate, batchEndDate, reqBatchSize).Return([]models.JobStatus{}, nil)

			replayValidator := job.NewReplayValidator(scheduler)
			err := replayValidator.Validate(ctx, replayRepository, replayRequest, executionTree)

			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), errMessage)
		})
		t.Run("should throw an error if conflicting replays found", func(t *testing.T) {
			activeReplaySpec := []models.ReplaySpec{
				{
					ID: uuid.Must(uuid.NewRandom()),
					Job: models.JobSpec{
						ID:       uuid.Must(uuid.NewRandom()),
						Name:     "job-name",
						Schedule: schedule,
					},
					StartDate: startDate,
					EndDate:   endDate,
					Status:    models.ReplayStatusInProgress,
				},
			}

			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByStatus", ctx, job.ReplayStatusToValidate).Return(activeReplaySpec, nil)

			scheduler := new(mock.Scheduler)
			defer scheduler.AssertExpectations(t)
			scheduler.On("GetJobRunStatus", ctx, projectSpec, jobSpec.Name, startDate, batchEndDate, reqBatchSize).Return([]models.JobStatus{}, nil)

			replayValidator := job.NewReplayValidator(scheduler)
			err := replayValidator.Validate(ctx, replayRepository, replayRequest, executionTree)

			assert.Equal(t, err, job.ErrConflictedJobRun)
		})
		t.Run("should throw an error if multiple replays are active and conflicting replays found", func(t *testing.T) {
			activeReplaySpec := []models.ReplaySpec{
				{
					ID:        uuid.Must(uuid.NewRandom()),
					Job:       jobSpec2,
					StartDate: startDate,
					EndDate:   endDate,
					Status:    models.ReplayStatusInProgress,
				},
				{
					ID:        uuid.Must(uuid.NewRandom()),
					Job:       jobSpec,
					StartDate: startDate,
					EndDate:   endDate,
					Status:    models.ReplayStatusInProgress,
				},
			}

			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByStatus", ctx, job.ReplayStatusToValidate).Return(activeReplaySpec, nil)

			scheduler := new(mock.Scheduler)
			defer scheduler.AssertExpectations(t)
			scheduler.On("GetJobRunStatus", ctx, projectSpec, jobSpec.Name, startDate, batchEndDate, reqBatchSize).Return([]models.JobStatus{}, nil)

			replayValidator := job.NewReplayValidator(scheduler)
			err := replayValidator.Validate(ctx, replayRepository, replayRequest, executionTree)

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
			replayRepository.On("GetByStatus", ctx, job.ReplayStatusToValidate).Return(activeReplaySpec, nil)

			scheduler := new(mock.Scheduler)
			defer scheduler.AssertExpectations(t)
			scheduler.On("GetJobRunStatus", ctx, projectSpec, jobSpec.Name, startDate, batchEndDate, reqBatchSize).Return([]models.JobStatus{}, nil)

			replayValidator := job.NewReplayValidator(scheduler)
			err := replayValidator.Validate(ctx, replayRepository, replayRequest, executionTree)

			assert.Nil(t, err)
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
			replayRepository.On("GetByStatus", ctx, job.ReplayStatusToValidate).Return(activeReplaySpec, nil)

			scheduler := new(mock.Scheduler)
			defer scheduler.AssertExpectations(t)
			scheduler.On("GetJobRunStatus", ctx, projectSpec, jobSpec.Name, startDate, batchEndDate, reqBatchSize).Return([]models.JobStatus{}, nil)

			replayValidator := job.NewReplayValidator(scheduler)
			err := replayValidator.Validate(ctx, replayRepository, replayRequest, executionTree)

			assert.Nil(t, err)
		})
		t.Run("should return error when unable to get status from batchScheduler", func(t *testing.T) {
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			scheduler := new(mock.Scheduler)
			defer scheduler.AssertExpectations(t)
			errMessage := "unable to get status"
			scheduler.On("GetJobRunStatus", ctx, projectSpec, jobSpec.Name, startDate, batchEndDate, reqBatchSize).Return([]models.JobStatus{}, errors.New(errMessage))

			replayValidator := job.NewReplayValidator(scheduler)
			err := replayValidator.Validate(ctx, replayRepository, replayRequest, executionTree)

			assert.Equal(t, errMessage, err.Error())
		})
		t.Run("should return error when same job and run in the running state is found", func(t *testing.T) {
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			scheduler := new(mock.Scheduler)
			defer scheduler.AssertExpectations(t)
			jobStatus := []models.JobStatus{
				{
					ScheduledAt: time.Date(2020, time.Month(8), 22, 2, 0, 0, 0, time.UTC),
					State:       models.RunStateSuccess,
				},
				{
					ScheduledAt: time.Date(2020, time.Month(8), 23, 2, 0, 0, 0, time.UTC),
					State:       models.RunStateRunning,
				},
			}
			scheduler.On("GetJobRunStatus", ctx, projectSpec, jobSpec.Name, startDate, batchEndDate, reqBatchSize).Return(jobStatus, nil)

			replayValidator := job.NewReplayValidator(scheduler)
			err := replayValidator.Validate(ctx, replayRepository, replayRequest, executionTree)

			assert.Equal(t, job.ErrConflictedJobRun, err)
		})
		t.Run("should return error when no running instance found in batchScheduler but accepted in replay", func(t *testing.T) {
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
			replayRepository.On("GetByStatus", ctx, job.ReplayStatusToValidate).Return(activeReplaySpec, nil)

			scheduler := new(mock.Scheduler)
			defer scheduler.AssertExpectations(t)
			jobStatus := []models.JobStatus{
				{
					ScheduledAt: time.Date(2021, time.Month(1), 1, 2, 0, 0, 0, time.UTC),
					State:       models.RunStateRunning,
				},
			}
			scheduler.On("GetJobRunStatus", ctx, projectSpec, jobSpec.Name, startDate, batchEndDate, reqBatchSize).Return(jobStatus, nil)

			replayValidator := job.NewReplayValidator(scheduler)
			err := replayValidator.Validate(ctx, replayRepository, replayRequest, executionTree)

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
			replayRepository.On("GetByJobIDAndStatus", ctx, activeReplaySpec[0].Job.ID, job.ReplayStatusToValidate).Return(activeReplaySpec, nil)

			cancelledReplayMessage := models.ReplayMessage{
				Type:    job.ErrConflictedJobRun.Error(),
				Message: fmt.Sprintf("force started replay with ID: %s", replayRequest.ID),
			}
			replayRepository.On("UpdateStatus", ctx, activeReplayUUID, models.ReplayStatusCancelled, cancelledReplayMessage).Return(nil)

			scheduler := new(mock.Scheduler)
			defer scheduler.AssertExpectations(t)

			replayRequest.Force = true
			replayValidator := job.NewReplayValidator(scheduler)
			err := replayValidator.Validate(ctx, replayRepository, replayRequest, executionTree)

			assert.Nil(t, err)
		})
	})
}
