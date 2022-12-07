package job_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/internal/lib/tree"
	"github.com/odpf/optimus/internal/store"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

func TestReplaySyncer(t *testing.T) {
	l := log.NewNoop()
	ctx := context.Background()
	reqBatchSize := 100
	runTimeout := time.Hour * 5
	activeReplayUUID := uuid.New()
	startDate := time.Date(2020, time.Month(8), 22, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2020, time.Month(8), 23, 0, 0, 0, 0, time.UTC)
	batchEndDate := endDate.AddDate(0, 0, 1).Add(time.Second * -1)
	dagStartTime := time.Date(2020, time.Month(4), 5, 0, 0, 0, 0, time.UTC)

	specs := make(map[string]models.JobSpec)
	spec1 := "dag1"
	spec2 := "dag2"
	noDependency := map[string]models.JobSpecDependency{}
	twoAMSchedule := models.JobSpecSchedule{
		StartDate: dagStartTime,
		Interval:  "0 2 * * *",
	}
	oneDayWindow, err := models.NewWindow(1, "", "", "24h")
	if err != nil {
		panic(err)
	}
	oneDayTaskWindow := models.JobSpecTask{
		Window: oneDayWindow,
	}
	threeDaysWindow, err := models.NewWindow(1, "", "", "72h")
	if err != nil {
		panic(err)
	}
	threeDayTaskWindow := models.JobSpecTask{
		Window: threeDaysWindow,
	}
	specs[spec1] = models.JobSpec{ID: uuid.New(), Name: spec1, Dependencies: noDependency, Schedule: twoAMSchedule, Task: oneDayTaskWindow}
	specs[spec2] = models.JobSpec{ID: uuid.New(), Name: spec2, Dependencies: getDependencyObject(specs, spec1), Schedule: twoAMSchedule, Task: threeDayTaskWindow}

	executionTreeDependent := tree.NewTreeNode(specs[spec2])
	executionTreeDependent.Runs.Add(time.Date(2020, time.Month(8), 22, 2, 0, 0, 0, time.UTC))
	executionTreeDependent.Runs.Add(time.Date(2020, time.Month(8), 23, 2, 0, 0, 0, time.UTC))

	executionTree := tree.NewTreeNode(specs[spec1])
	executionTree.Runs.Add(time.Date(2020, time.Month(8), 22, 2, 0, 0, 0, time.UTC))
	executionTree.Runs.Add(time.Date(2020, time.Month(8), 23, 2, 0, 0, 0, time.UTC))
	executionTree.AddDependent(executionTreeDependent)

	activeReplaySpec := []models.ReplaySpec{
		{
			ID:            activeReplayUUID,
			Job:           specs[spec1],
			StartDate:     startDate,
			EndDate:       endDate,
			Status:        models.ReplayStatusReplayed,
			ExecutionTree: executionTree,
		},
	}

	projectSpecs := []models.ProjectSpec{
		{
			ID:   models.ProjectID(uuid.New()),
			Name: "project-sample",
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
		},
	}
	t.Run("Sync", func(t *testing.T) {
		t.Run("should not return error when no replay with sync criteria found", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetAllWithUpstreams", ctx).Return(projectSpecs, nil)
			defer projectRepository.AssertExpectations(t)

			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByProjectIDAndStatus", ctx, projectSpecs[0].ID, job.ReplayStatusToSynced).Return([]models.ReplaySpec{}, store.ErrResourceNotFound)

			replaySyncer := job.NewReplaySyncer(l, replayRepository, projectRepository, nil, time.Now)
			err := replaySyncer.Sync(ctx, runTimeout)

			assert.Nil(t, err)
		})
		t.Run("should return error when fetching replays failed", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetAllWithUpstreams", ctx).Return(projectSpecs, nil)
			defer projectRepository.AssertExpectations(t)

			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			errorMsg := "fetching replay error"
			replayRepository.On("GetByProjectIDAndStatus", ctx, projectSpecs[0].ID, job.ReplayStatusToSynced).Return([]models.ReplaySpec{}, errors.New(errorMsg))

			replaySyncer := job.NewReplaySyncer(l, replayRepository, projectRepository, nil, time.Now)
			err := replaySyncer.Sync(ctx, runTimeout)

			assert.Equal(t, errorMsg, err.Error())
		})
		t.Run("should mark state of running replay to success if all instances are success", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetAllWithUpstreams", ctx).Return(projectSpecs, nil)
			defer projectRepository.AssertExpectations(t)

			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByProjectIDAndStatus", ctx, projectSpecs[0].ID, job.ReplayStatusToSynced).Return(activeReplaySpec, nil)

			jobStatus := []models.JobStatus{
				{
					ScheduledAt: time.Date(2020, time.Month(8), 22, 2, 0, 0, 0, time.UTC),
					State:       models.RunStateSuccess,
				},
				{
					ScheduledAt: time.Date(2020, time.Month(8), 23, 2, 0, 0, 0, time.UTC),
					State:       models.RunStateSuccess,
				},
			}
			scheduler := new(mock.Scheduler)
			defer scheduler.AssertExpectations(t)
			scheduler.On("GetJobRunStatus", ctx, projectSpecs[0], specs[spec1].Name, startDate, batchEndDate, reqBatchSize).Return(jobStatus, nil).Once()
			scheduler.On("GetJobRunStatus", ctx, projectSpecs[0], specs[spec2].Name, startDate, batchEndDate, reqBatchSize).Return(jobStatus, nil).Once()

			successReplayMessage := models.ReplayMessage{
				Type:    models.ReplayStatusSuccess,
				Message: job.ReplayMessageSuccess,
			}
			replayRepository.On("UpdateStatus", ctx, activeReplayUUID, models.ReplayStatusSuccess, successReplayMessage).Return(nil)

			replaySyncer := job.NewReplaySyncer(l, replayRepository, projectRepository, scheduler, time.Now)
			err := replaySyncer.Sync(ctx, runTimeout)

			assert.Nil(t, err)
		})
		t.Run("should mark state of running replay to failed if no longer running instance and one of instances is failed", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetAllWithUpstreams", ctx).Return(projectSpecs, nil)
			defer projectRepository.AssertExpectations(t)

			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByProjectIDAndStatus", ctx, projectSpecs[0].ID, job.ReplayStatusToSynced).Return(activeReplaySpec, nil)

			jobStatus := []models.JobStatus{
				{
					ScheduledAt: time.Date(2020, time.Month(8), 22, 2, 0, 0, 0, time.UTC),
					State:       models.RunStateSuccess,
				},
				{
					ScheduledAt: time.Date(2020, time.Month(8), 23, 2, 0, 0, 0, time.UTC),
					State:       models.RunStateFailed,
				},
			}
			scheduler := new(mock.Scheduler)
			defer scheduler.AssertExpectations(t)
			scheduler.On("GetJobRunStatus", ctx, projectSpecs[0], specs[spec1].Name, startDate, batchEndDate, reqBatchSize).Return(jobStatus, nil).Once()
			scheduler.On("GetJobRunStatus", ctx, projectSpecs[0], specs[spec2].Name, startDate, batchEndDate, reqBatchSize).Return(jobStatus, nil).Once()

			failedReplayMessage := models.ReplayMessage{
				Type:    models.ReplayStatusFailed,
				Message: job.ReplayMessageFailed,
			}
			replayRepository.On("UpdateStatus", ctx, activeReplayUUID, models.ReplayStatusFailed, failedReplayMessage).Return(nil)

			replaySyncer := job.NewReplaySyncer(l, replayRepository, projectRepository, scheduler, time.Now)
			err := replaySyncer.Sync(ctx, runTimeout)

			assert.Nil(t, err)
		})
		t.Run("should not update replay status if instances are still running", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetAllWithUpstreams", ctx).Return(projectSpecs, nil)
			defer projectRepository.AssertExpectations(t)

			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByProjectIDAndStatus", ctx, projectSpecs[0].ID, job.ReplayStatusToSynced).Return(activeReplaySpec, nil)

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
			scheduler := new(mock.Scheduler)
			defer scheduler.AssertExpectations(t)
			scheduler.On("GetJobRunStatus", ctx, projectSpecs[0], specs[spec1].Name, startDate, batchEndDate, reqBatchSize).Return(jobStatus, nil).Once()
			scheduler.On("GetJobRunStatus", ctx, projectSpecs[0], specs[spec2].Name, startDate, batchEndDate, reqBatchSize).Return(jobStatus, nil).Once()

			replaySyncer := job.NewReplaySyncer(l, replayRepository, projectRepository, scheduler, time.Now)
			err := replaySyncer.Sync(ctx, runTimeout)

			assert.Nil(t, err)
		})
		t.Run("should mark timeout replay as failed", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetAllWithUpstreams", ctx).Return(projectSpecs, nil)
			defer projectRepository.AssertExpectations(t)

			replayCreatedAt := time.Now().Add(time.Hour * -5)
			replaySpec := []models.ReplaySpec{
				{
					ID:        activeReplayUUID,
					Job:       specs[spec1],
					StartDate: startDate,
					EndDate:   endDate,
					Status:    models.ReplayStatusAccepted,
					CreatedAt: replayCreatedAt,
				},
			}

			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByProjectIDAndStatus", ctx, projectSpecs[0].ID, job.ReplayStatusToSynced).Return(replaySpec, nil)

			failedReplayMessage := models.ReplayMessage{
				Type:    job.ReplayRunTimeout,
				Message: fmt.Sprintf("replay has been running since %s", replayCreatedAt.UTC().Format(job.TimestampLogFormat)),
			}
			replayRepository.On("UpdateStatus", ctx, activeReplayUUID, models.ReplayStatusFailed, failedReplayMessage).Return(nil)

			replaySyncer := job.NewReplaySyncer(l, replayRepository, projectRepository, nil, time.Now)
			err := replaySyncer.Sync(ctx, runTimeout)

			assert.Nil(t, err)
		})
		t.Run("should return error when unable to get dag run status from batchScheduler", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetAllWithUpstreams", ctx).Return(projectSpecs, nil)
			defer projectRepository.AssertExpectations(t)

			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByProjectIDAndStatus", ctx, projectSpecs[0].ID, job.ReplayStatusToSynced).Return(activeReplaySpec, nil)

			scheduler := new(mock.Scheduler)
			defer scheduler.AssertExpectations(t)
			errorMsg := "fetch dag run status from batchScheduler failed"
			scheduler.On("GetJobRunStatus", ctx, projectSpecs[0], specs[spec1].Name, startDate, batchEndDate, reqBatchSize).Return([]models.JobStatus{}, errors.New(errorMsg)).Once()

			replaySyncer := job.NewReplaySyncer(l, replayRepository, projectRepository, scheduler, time.Now)
			err := replaySyncer.Sync(ctx, runTimeout)

			assert.Contains(t, err.Error(), errorMsg)
		})
	})
}
