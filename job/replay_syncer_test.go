package job_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestReplaySyncer(t *testing.T) {
	ctx := context.TODO()
	reqBatchSize := 100
	runTimeout := time.Hour * 5
	dumpAssets := func(jobSpec models.JobSpec, _ time.Time) (models.JobAssets, error) {
		return jobSpec.Assets, nil
	}
	t.Run("Sync", func(t *testing.T) {
		t.Run("should not return error when no replay with sync criteria found", func(t *testing.T) {
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByStatus", job.ReplayStatusToSynced).Return([]models.ReplaySpec{}, store.ErrResourceNotFound)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", models.JobSpec{}).Return(replayRepository)

			replaySyncer := job.NewReplaySyncer(replaySpecRepoFac, nil, nil, nil, nil, nil)
			err := replaySyncer.Sync(context.TODO(), runTimeout)

			assert.Nil(t, err)
		})
		t.Run("should return error when fetching replays failed", func(t *testing.T) {
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			errorMsg := "fetching replay error"
			replayRepository.On("GetByStatus", job.ReplayStatusToSynced).Return([]models.ReplaySpec{}, errors.New(errorMsg))

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", models.JobSpec{}).Return(replayRepository)

			replaySyncer := job.NewReplaySyncer(replaySpecRepoFac, nil, nil, nil, nil, nil)
			err := replaySyncer.Sync(context.TODO(), runTimeout)

			assert.Equal(t, errorMsg, err.Error())
		})
		t.Run("should mark state of running replay to success if all instances are success", func(t *testing.T) {
			activeReplayUUID := uuid.Must(uuid.NewRandom())
			startDate := time.Date(2020, time.Month(8), 22, 0, 0, 0, 0, time.UTC)
			endDate := time.Date(2020, time.Month(8), 23, 0, 0, 0, 0, time.UTC)
			batchEndDate := endDate.AddDate(0, 0, 1).Add(time.Second * -1)
			dagStartTime := time.Date(2020, time.Month(4), 05, 0, 0, 0, 0, time.UTC)

			projectSpec := models.ProjectSpec{
				Name: "project-sample",
			}
			specs := make(map[string]models.JobSpec)
			dagSpec := make([]models.JobSpec, 0)
			spec1 := "dag1"
			spec2 := "dag2"
			noDependency := map[string]models.JobSpecDependency{}
			twoAMSchedule := models.JobSpecSchedule{
				StartDate: dagStartTime,
				Interval:  "0 2 * * *",
			}
			oneDayTaskWindow := models.JobSpecTask{
				Window: models.JobSpecTaskWindow{
					Size: time.Hour * 24,
				},
			}
			threeDayTaskWindow := models.JobSpecTask{
				Window: models.JobSpecTaskWindow{
					Size: time.Hour * 24 * 3,
				},
			}
			specs[spec1] = models.JobSpec{ID: uuid.Must(uuid.NewRandom()), Name: spec1, Dependencies: noDependency, Schedule: twoAMSchedule, Task: oneDayTaskWindow, Project: projectSpec}
			dagSpec = append(dagSpec, specs[spec1])
			specs[spec2] = models.JobSpec{ID: uuid.Must(uuid.NewRandom()), Name: spec2, Dependencies: getDependencyObject(specs, spec1), Schedule: twoAMSchedule, Task: threeDayTaskWindow, Project: projectSpec}
			dagSpec = append(dagSpec, specs[spec2])

			registeredProjects := []models.ProjectSpec{
				projectSpec,
			}
			activeReplaySpec := []models.ReplaySpec{
				{
					ID:        activeReplayUUID,
					Job:       specs[spec1],
					StartDate: startDate,
					EndDate:   endDate,
					Status:    models.ReplayStatusReplayed,
				},
			}

			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByStatus", job.ReplayStatusToSynced).Return(activeReplaySpec, nil)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", models.JobSpec{}).Return(replayRepository)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			projectJobSpecRepo.On("GetAll").Return(dagSpec, nil)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			projJobSpecRepoFac.On("New", projectSpec).Return(projectJobSpecRepo)
			defer projJobSpecRepoFac.AssertExpectations(t)

			depenResolver := new(mock.DependencyResolver)
			depenResolver.On("Resolve", projectSpec, projectJobSpecRepo, specs[spec1], nil).Return(specs[spec1], nil).Once()
			depenResolver.On("Resolve", projectSpec, projectJobSpecRepo, specs[spec2], nil).Return(specs[spec2], nil).Once()
			defer depenResolver.AssertExpectations(t)

			jobStatus := []models.JobStatus{
				{
					ScheduledAt: time.Date(2020, time.Month(8), 22, 2, 0, 0, 0, time.UTC),
					State:       models.InstanceStateSuccess,
				},
				{
					ScheduledAt: time.Date(2020, time.Month(8), 23, 2, 0, 0, 0, time.UTC),
					State:       models.InstanceStateSuccess,
				},
			}
			scheduler := new(mock.Scheduler)
			defer scheduler.AssertExpectations(t)
			scheduler.On("GetDagRunStatus", ctx, projectSpec, specs[spec1].Name, startDate, batchEndDate, reqBatchSize).Return(jobStatus, nil).Once()
			scheduler.On("GetDagRunStatus", ctx, projectSpec, specs[spec2].Name, startDate, batchEndDate, reqBatchSize).Return(jobStatus, nil).Once()

			successReplayMessage := models.ReplayMessage{
				Type:    models.ReplayStatusSuccess,
				Message: job.ReplayMessageSuccess,
			}
			replayRepository.On("UpdateStatus", activeReplayUUID, models.ReplayStatusSuccess, successReplayMessage).Return(nil)

			replaySyncer := job.NewReplaySyncer(replaySpecRepoFac, scheduler, depenResolver, projJobSpecRepoFac, dumpAssets, registeredProjects)
			err := replaySyncer.Sync(context.TODO(), runTimeout)

			assert.Nil(t, err)
		})
		t.Run("should mark state of running replay to failed if no longer running instance and one of instances is failed", func(t *testing.T) {
			activeReplayUUID := uuid.Must(uuid.NewRandom())
			startDate := time.Date(2020, time.Month(8), 22, 0, 0, 0, 0, time.UTC)
			endDate := time.Date(2020, time.Month(8), 23, 0, 0, 0, 0, time.UTC)
			batchEndDate := endDate.AddDate(0, 0, 1).Add(time.Second * -1)
			dagStartTime := time.Date(2020, time.Month(4), 05, 0, 0, 0, 0, time.UTC)

			projectSpec := models.ProjectSpec{
				Name: "project-sample",
			}
			specs := make(map[string]models.JobSpec)
			dagSpec := make([]models.JobSpec, 0)
			spec1 := "dag1"
			spec2 := "dag2"
			noDependency := map[string]models.JobSpecDependency{}
			twoAMSchedule := models.JobSpecSchedule{
				StartDate: dagStartTime,
				Interval:  "0 2 * * *",
			}
			oneDayTaskWindow := models.JobSpecTask{
				Window: models.JobSpecTaskWindow{
					Size: time.Hour * 24,
				},
			}
			threeDayTaskWindow := models.JobSpecTask{
				Window: models.JobSpecTaskWindow{
					Size: time.Hour * 24 * 3,
				},
			}
			specs[spec1] = models.JobSpec{ID: uuid.Must(uuid.NewRandom()), Name: spec1, Dependencies: noDependency, Schedule: twoAMSchedule, Task: oneDayTaskWindow, Project: projectSpec}
			dagSpec = append(dagSpec, specs[spec1])
			specs[spec2] = models.JobSpec{ID: uuid.Must(uuid.NewRandom()), Name: spec2, Dependencies: getDependencyObject(specs, spec1), Schedule: twoAMSchedule, Task: threeDayTaskWindow, Project: projectSpec}
			dagSpec = append(dagSpec, specs[spec2])

			registeredProjects := []models.ProjectSpec{
				projectSpec,
			}
			activeReplaySpec := []models.ReplaySpec{
				{
					ID:        activeReplayUUID,
					Job:       specs[spec1],
					StartDate: startDate,
					EndDate:   endDate,
					Status:    models.ReplayStatusReplayed,
				},
			}

			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByStatus", job.ReplayStatusToSynced).Return(activeReplaySpec, nil)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", models.JobSpec{}).Return(replayRepository)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			projectJobSpecRepo.On("GetAll").Return(dagSpec, nil)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			projJobSpecRepoFac.On("New", projectSpec).Return(projectJobSpecRepo)
			defer projJobSpecRepoFac.AssertExpectations(t)

			depenResolver := new(mock.DependencyResolver)
			depenResolver.On("Resolve", projectSpec, projectJobSpecRepo, specs[spec1], nil).Return(specs[spec1], nil).Once()
			depenResolver.On("Resolve", projectSpec, projectJobSpecRepo, specs[spec2], nil).Return(specs[spec2], nil).Once()
			defer depenResolver.AssertExpectations(t)

			jobStatus := []models.JobStatus{
				{
					ScheduledAt: time.Date(2020, time.Month(8), 22, 2, 0, 0, 0, time.UTC),
					State:       models.InstanceStateSuccess,
				},
				{
					ScheduledAt: time.Date(2020, time.Month(8), 23, 2, 0, 0, 0, time.UTC),
					State:       models.InstanceStateFailed,
				},
			}
			scheduler := new(mock.Scheduler)
			defer scheduler.AssertExpectations(t)
			scheduler.On("GetDagRunStatus", ctx, projectSpec, specs[spec1].Name, startDate, batchEndDate, reqBatchSize).Return(jobStatus, nil).Once()
			scheduler.On("GetDagRunStatus", ctx, projectSpec, specs[spec2].Name, startDate, batchEndDate, reqBatchSize).Return(jobStatus, nil).Once()

			failedReplayMessage := models.ReplayMessage{
				Type:    models.ReplayStatusFailed,
				Message: job.ReplayMessageFailed,
			}
			replayRepository.On("UpdateStatus", activeReplayUUID, models.ReplayStatusFailed, failedReplayMessage).Return(nil)

			replaySyncer := job.NewReplaySyncer(replaySpecRepoFac, scheduler, depenResolver, projJobSpecRepoFac, dumpAssets, registeredProjects)
			err := replaySyncer.Sync(context.TODO(), runTimeout)

			assert.Nil(t, err)
		})
		t.Run("should not update replay status if instances are still running", func(t *testing.T) {
			activeReplayUUID := uuid.Must(uuid.NewRandom())
			startDate := time.Date(2020, time.Month(8), 22, 0, 0, 0, 0, time.UTC)
			endDate := time.Date(2020, time.Month(8), 23, 0, 0, 0, 0, time.UTC)
			batchEndDate := endDate.AddDate(0, 0, 1).Add(time.Second * -1)
			dagStartTime := time.Date(2020, time.Month(4), 05, 0, 0, 0, 0, time.UTC)

			projectSpec := models.ProjectSpec{
				Name: "project-sample",
			}
			specs := make(map[string]models.JobSpec)
			dagSpec := make([]models.JobSpec, 0)
			spec1 := "dag1"
			spec2 := "dag2"
			noDependency := map[string]models.JobSpecDependency{}
			twoAMSchedule := models.JobSpecSchedule{
				StartDate: dagStartTime,
				Interval:  "0 2 * * *",
			}
			oneDayTaskWindow := models.JobSpecTask{
				Window: models.JobSpecTaskWindow{
					Size: time.Hour * 24,
				},
			}
			threeDayTaskWindow := models.JobSpecTask{
				Window: models.JobSpecTaskWindow{
					Size: time.Hour * 24 * 3,
				},
			}
			specs[spec1] = models.JobSpec{ID: uuid.Must(uuid.NewRandom()), Name: spec1, Dependencies: noDependency, Schedule: twoAMSchedule, Task: oneDayTaskWindow, Project: projectSpec}
			dagSpec = append(dagSpec, specs[spec1])
			specs[spec2] = models.JobSpec{ID: uuid.Must(uuid.NewRandom()), Name: spec2, Dependencies: getDependencyObject(specs, spec1), Schedule: twoAMSchedule, Task: threeDayTaskWindow, Project: projectSpec}
			dagSpec = append(dagSpec, specs[spec2])

			registeredProjects := []models.ProjectSpec{
				projectSpec,
			}
			activeReplaySpec := []models.ReplaySpec{
				{
					ID:        activeReplayUUID,
					Job:       specs[spec1],
					StartDate: startDate,
					EndDate:   endDate,
					Status:    models.ReplayStatusReplayed,
				},
			}

			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByStatus", job.ReplayStatusToSynced).Return(activeReplaySpec, nil)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", models.JobSpec{}).Return(replayRepository)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			projectJobSpecRepo.On("GetAll").Return(dagSpec, nil)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			projJobSpecRepoFac.On("New", projectSpec).Return(projectJobSpecRepo)
			defer projJobSpecRepoFac.AssertExpectations(t)

			depenResolver := new(mock.DependencyResolver)
			depenResolver.On("Resolve", projectSpec, projectJobSpecRepo, specs[spec1], nil).Return(specs[spec1], nil).Once()
			depenResolver.On("Resolve", projectSpec, projectJobSpecRepo, specs[spec2], nil).Return(specs[spec2], nil).Once()
			defer depenResolver.AssertExpectations(t)

			jobStatus := []models.JobStatus{
				{
					ScheduledAt: time.Date(2020, time.Month(8), 22, 2, 0, 0, 0, time.UTC),
					State:       models.InstanceStateSuccess,
				},
				{
					ScheduledAt: time.Date(2020, time.Month(8), 23, 2, 0, 0, 0, time.UTC),
					State:       models.InstanceStateRunning,
				},
			}
			scheduler := new(mock.Scheduler)
			defer scheduler.AssertExpectations(t)
			scheduler.On("GetDagRunStatus", ctx, projectSpec, specs[spec1].Name, startDate, batchEndDate, reqBatchSize).Return(jobStatus, nil).Once()
			scheduler.On("GetDagRunStatus", ctx, projectSpec, specs[spec2].Name, startDate, batchEndDate, reqBatchSize).Return(jobStatus, nil).Once()

			replaySyncer := job.NewReplaySyncer(replaySpecRepoFac, scheduler, depenResolver, projJobSpecRepoFac, dumpAssets, registeredProjects)
			err := replaySyncer.Sync(context.TODO(), runTimeout)

			assert.Nil(t, err)
		})
		t.Run("should mark timeout replay as failed", func(t *testing.T) {
			activeReplayUUID := uuid.Must(uuid.NewRandom())
			startDate := time.Date(2020, time.Month(8), 22, 0, 0, 0, 0, time.UTC)
			endDate := time.Date(2020, time.Month(8), 23, 0, 0, 0, 0, time.UTC)
			dagStartTime := time.Date(2020, time.Month(4), 05, 0, 0, 0, 0, time.UTC)
			replayCreatedAt := time.Now().Add(time.Hour * -5)

			projectSpec := models.ProjectSpec{
				Name: "project-sample",
			}
			specs := make(map[string]models.JobSpec)
			spec1 := "dag1"
			spec2 := "dag2"
			noDependency := map[string]models.JobSpecDependency{}
			twoAMSchedule := models.JobSpecSchedule{
				StartDate: dagStartTime,
				Interval:  "0 2 * * *",
			}
			oneDayTaskWindow := models.JobSpecTask{
				Window: models.JobSpecTaskWindow{
					Size: time.Hour * 24,
				},
			}
			threeDayTaskWindow := models.JobSpecTask{
				Window: models.JobSpecTaskWindow{
					Size: time.Hour * 24 * 3,
				},
			}
			specs[spec1] = models.JobSpec{ID: uuid.Must(uuid.NewRandom()), Name: spec1, Dependencies: noDependency, Schedule: twoAMSchedule, Task: oneDayTaskWindow, Project: projectSpec}
			specs[spec2] = models.JobSpec{ID: uuid.Must(uuid.NewRandom()), Name: spec2, Dependencies: getDependencyObject(specs, spec1), Schedule: twoAMSchedule, Task: threeDayTaskWindow, Project: projectSpec}

			registeredProjects := []models.ProjectSpec{
				projectSpec,
			}
			activeReplaySpec := []models.ReplaySpec{
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
			replayRepository.On("GetByStatus", job.ReplayStatusToSynced).Return(activeReplaySpec, nil)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", models.JobSpec{}).Return(replayRepository)

			failedReplayMessage := models.ReplayMessage{
				Type:    job.ReplayRunTimeout,
				Message: fmt.Sprintf("replay has been running since %s", replayCreatedAt.UTC().Format(job.TimestampLogFormat)),
			}
			replayRepository.On("UpdateStatus", activeReplayUUID, models.ReplayStatusFailed, failedReplayMessage).Return(nil)

			replaySyncer := job.NewReplaySyncer(replaySpecRepoFac, nil, nil, nil, dumpAssets, registeredProjects)
			err := replaySyncer.Sync(context.TODO(), runTimeout)

			assert.Nil(t, err)
		})
		t.Run("should return error when unable to retrieve jobs from a project", func(t *testing.T) {
			activeReplayUUID := uuid.Must(uuid.NewRandom())
			startDate := time.Date(2020, time.Month(8), 22, 0, 0, 0, 0, time.UTC)
			endDate := time.Date(2020, time.Month(8), 23, 0, 0, 0, 0, time.UTC)
			dagStartTime := time.Date(2020, time.Month(4), 05, 0, 0, 0, 0, time.UTC)

			projectSpec := models.ProjectSpec{
				Name: "project-sample",
			}
			specs := make(map[string]models.JobSpec)
			spec1 := "dag1"
			spec2 := "dag2"
			noDependency := map[string]models.JobSpecDependency{}
			twoAMSchedule := models.JobSpecSchedule{
				StartDate: dagStartTime,
				Interval:  "0 2 * * *",
			}
			oneDayTaskWindow := models.JobSpecTask{
				Window: models.JobSpecTaskWindow{
					Size: time.Hour * 24,
				},
			}
			threeDayTaskWindow := models.JobSpecTask{
				Window: models.JobSpecTaskWindow{
					Size: time.Hour * 24 * 3,
				},
			}
			specs[spec1] = models.JobSpec{ID: uuid.Must(uuid.NewRandom()), Name: spec1, Dependencies: noDependency, Schedule: twoAMSchedule, Task: oneDayTaskWindow, Project: projectSpec}
			specs[spec2] = models.JobSpec{ID: uuid.Must(uuid.NewRandom()), Name: spec2, Dependencies: getDependencyObject(specs, spec1), Schedule: twoAMSchedule, Task: threeDayTaskWindow, Project: projectSpec}

			registeredProjects := []models.ProjectSpec{
				projectSpec,
			}
			activeReplaySpec := []models.ReplaySpec{
				{
					ID:        activeReplayUUID,
					Job:       specs[spec1],
					StartDate: startDate,
					EndDate:   endDate,
					Status:    models.ReplayStatusReplayed,
				},
			}

			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByStatus", job.ReplayStatusToSynced).Return(activeReplaySpec, nil)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", models.JobSpec{}).Return(replayRepository)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			errorMsg := "failed to retrieve jobs"
			projectJobSpecRepo.On("GetAll").Return(nil, errors.New(errorMsg))
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			projJobSpecRepoFac.On("New", projectSpec).Return(projectJobSpecRepo)
			defer projJobSpecRepoFac.AssertExpectations(t)

			depenResolver := new(mock.DependencyResolver)
			defer depenResolver.AssertExpectations(t)

			scheduler := new(mock.Scheduler)
			defer scheduler.AssertExpectations(t)

			replaySyncer := job.NewReplaySyncer(replaySpecRepoFac, scheduler, depenResolver, projJobSpecRepoFac, dumpAssets, registeredProjects)
			err := replaySyncer.Sync(context.TODO(), runTimeout)

			assert.Contains(t, err.Error(), errorMsg)
		})
		t.Run("should return error when unable to get dag run status from scheduler", func(t *testing.T) {
			activeReplayUUID := uuid.Must(uuid.NewRandom())
			startDate := time.Date(2020, time.Month(8), 22, 0, 0, 0, 0, time.UTC)
			endDate := time.Date(2020, time.Month(8), 23, 0, 0, 0, 0, time.UTC)
			batchEndDate := endDate.AddDate(0, 0, 1).Add(time.Second * -1)
			dagStartTime := time.Date(2020, time.Month(4), 05, 0, 0, 0, 0, time.UTC)

			projectSpec := models.ProjectSpec{
				Name: "project-sample",
			}
			specs := make(map[string]models.JobSpec)
			dagSpec := make([]models.JobSpec, 0)
			spec1 := "dag1"
			spec2 := "dag2"
			noDependency := map[string]models.JobSpecDependency{}
			twoAMSchedule := models.JobSpecSchedule{
				StartDate: dagStartTime,
				Interval:  "0 2 * * *",
			}
			oneDayTaskWindow := models.JobSpecTask{
				Window: models.JobSpecTaskWindow{
					Size: time.Hour * 24,
				},
			}
			threeDayTaskWindow := models.JobSpecTask{
				Window: models.JobSpecTaskWindow{
					Size: time.Hour * 24 * 3,
				},
			}
			specs[spec1] = models.JobSpec{ID: uuid.Must(uuid.NewRandom()), Name: spec1, Dependencies: noDependency, Schedule: twoAMSchedule, Task: oneDayTaskWindow, Project: projectSpec}
			dagSpec = append(dagSpec, specs[spec1])
			specs[spec2] = models.JobSpec{ID: uuid.Must(uuid.NewRandom()), Name: spec2, Dependencies: getDependencyObject(specs, spec1), Schedule: twoAMSchedule, Task: threeDayTaskWindow, Project: projectSpec}
			dagSpec = append(dagSpec, specs[spec2])

			registeredProjects := []models.ProjectSpec{
				projectSpec,
			}
			activeReplaySpec := []models.ReplaySpec{
				{
					ID:        activeReplayUUID,
					Job:       specs[spec1],
					StartDate: startDate,
					EndDate:   endDate,
					Status:    models.ReplayStatusReplayed,
				},
			}

			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			replayRepository.On("GetByStatus", job.ReplayStatusToSynced).Return(activeReplaySpec, nil)

			replaySpecRepoFac := new(mock.ReplaySpecRepoFactory)
			defer replaySpecRepoFac.AssertExpectations(t)
			replaySpecRepoFac.On("New", models.JobSpec{}).Return(replayRepository)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			projectJobSpecRepo.On("GetAll").Return(dagSpec, nil)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			projJobSpecRepoFac.On("New", projectSpec).Return(projectJobSpecRepo)
			defer projJobSpecRepoFac.AssertExpectations(t)

			depenResolver := new(mock.DependencyResolver)
			depenResolver.On("Resolve", projectSpec, projectJobSpecRepo, specs[spec1], nil).Return(specs[spec1], nil).Once()
			depenResolver.On("Resolve", projectSpec, projectJobSpecRepo, specs[spec2], nil).Return(specs[spec2], nil).Once()
			defer depenResolver.AssertExpectations(t)

			scheduler := new(mock.Scheduler)
			defer scheduler.AssertExpectations(t)
			errorMsg := "fetch dag run status from scheduler failed"
			scheduler.On("GetDagRunStatus", ctx, projectSpec, specs[spec1].Name, startDate, batchEndDate, reqBatchSize).Return([]models.JobStatus{}, errors.New(errorMsg)).Once()

			replaySyncer := job.NewReplaySyncer(replaySpecRepoFac, scheduler, depenResolver, projJobSpecRepoFac, dumpAssets, registeredProjects)
			err := replaySyncer.Sync(context.TODO(), runTimeout)

			assert.Contains(t, err.Error(), errorMsg)
		})
	})
}
