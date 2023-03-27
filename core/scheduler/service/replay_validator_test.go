package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/service"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/cron"
)

func TestReplayValidator(t *testing.T) {
	ctx := context.Background()
	tnnt, _ := tenant.NewTenant("sample-project", "sample-namespace")
	jobName := scheduler.JobName("sample_select")
	startTimeStr := "2023-01-02T15:00:00Z"
	startTime, _ := time.Parse(scheduler.ISODateFormat, startTimeStr)
	endTime := startTime.Add(48 * time.Hour)
	parallel := true
	description := "sample backfill"
	replayJobConfig := map[string]string{"EXECUTION_PROJECT": "example_project"}
	replayConfig := scheduler.NewReplayConfig(startTime, endTime, parallel, replayJobConfig, description)
	runsCriteriaJobA := &scheduler.JobRunsCriteria{
		Name:      jobName.String(),
		StartDate: startTime,
		EndDate:   endTime,
	}
	jobCronStr := "0 12 * * *"
	jobCron, _ := cron.ParseCronSchedule(jobCronStr)
	scheduledTimeStr1 := "2023-01-02T12:00:00Z"
	scheduledTime1, _ := time.Parse(scheduler.ISODateFormat, scheduledTimeStr1)
	replayStatusToValidate := []scheduler.ReplayState{
		scheduler.ReplayStateCreated, scheduler.ReplayStateInProgress,
		scheduler.ReplayStatePartialReplayed, scheduler.ReplayStateReplayed,
	}
	replayReq := scheduler.NewReplayRequest(jobName, tnnt, replayConfig, scheduler.ReplayStateCreated)

	t.Run("Validate", func(t *testing.T) {
		t.Run("should return nil if no conflict replay or conflict run found", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			onGoingReplayConfig := scheduler.NewReplayConfig(time.Now(), time.Now(), parallel, replayJobConfig, description)
			onGoingReplay := []*scheduler.Replay{
				scheduler.NewReplayRequest(jobName, tnnt, onGoingReplayConfig, scheduler.ReplayStateCreated),
				scheduler.NewReplayRequest("other-job", tnnt, onGoingReplayConfig, scheduler.ReplayStateCreated),
			}
			currentRuns := []*scheduler.JobRunStatus{
				{
					ScheduledAt: scheduledTime1,
					State:       scheduler.StateSuccess,
				},
			}

			replayRepository.On("GetReplayRequestsByStatus", ctx, replayStatusToValidate).Return(onGoingReplay, nil)
			sch.On("GetJobRuns", ctx, tnnt, runsCriteriaJobA, jobCron).Return(currentRuns, nil)

			validator := service.NewValidator(replayRepository, sch)
			err := validator.Validate(ctx, replayReq, jobCron)
			assert.NoError(t, err)
		})
		t.Run("should return error if conflict replay found", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			onGoingReplay := []*scheduler.Replay{
				scheduler.NewReplayRequest(jobName, tnnt, replayConfig, scheduler.ReplayStateInProgress),
			}

			replayRepository.On("GetReplayRequestsByStatus", ctx, replayStatusToValidate).Return(onGoingReplay, nil)

			validator := service.NewValidator(replayRepository, sch)
			err := validator.Validate(ctx, replayReq, jobCron)
			assert.ErrorContains(t, err, "conflicted replay")
		})
		t.Run("should return error if conflict run found", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			onGoingReplayConfig := scheduler.NewReplayConfig(time.Now(), time.Now(), parallel, replayJobConfig, description)
			onGoingReplay := []*scheduler.Replay{
				scheduler.NewReplayRequest(jobName, tnnt, onGoingReplayConfig, scheduler.ReplayStateCreated),
			}
			currentRuns := []*scheduler.JobRunStatus{
				{
					ScheduledAt: scheduledTime1,
					State:       scheduler.StateRunning,
				},
			}

			replayRepository.On("GetReplayRequestsByStatus", ctx, replayStatusToValidate).Return(onGoingReplay, nil)
			sch.On("GetJobRuns", ctx, tnnt, runsCriteriaJobA, jobCron).Return(currentRuns, nil)

			validator := service.NewValidator(replayRepository, sch)
			err := validator.Validate(ctx, replayReq, jobCron)
			assert.ErrorContains(t, err, "conflicted job run found")
		})
		t.Run("should return error if unable to GetReplayRequestsByStatus", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			internalErr := errors.New("internal error")
			replayRepository.On("GetReplayRequestsByStatus", ctx, replayStatusToValidate).Return(nil, internalErr)

			validator := service.NewValidator(replayRepository, sch)
			err := validator.Validate(ctx, replayReq, jobCron)
			assert.ErrorIs(t, err, internalErr)
		})
		t.Run("should return error if unable to get job runs", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			onGoingReplayConfig := scheduler.NewReplayConfig(time.Now(), time.Now(), parallel, map[string]string{}, description)
			onGoingReplay := []*scheduler.Replay{
				scheduler.NewReplayRequest(jobName, tnnt, onGoingReplayConfig, scheduler.ReplayStateCreated),
			}

			replayRepository.On("GetReplayRequestsByStatus", ctx, replayStatusToValidate).Return(onGoingReplay, nil)

			internalErr := errors.New("internal error")
			sch.On("GetJobRuns", ctx, tnnt, runsCriteriaJobA, jobCron).Return(nil, internalErr)

			validator := service.NewValidator(replayRepository, sch)
			err := validator.Validate(ctx, replayReq, jobCron)
			assert.ErrorIs(t, err, internalErr)
		})
	})
}
