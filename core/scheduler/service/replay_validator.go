package service

import (
	"golang.org/x/net/context"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/cron"
)

var replayStatusToValidate = []scheduler.ReplayState{
	scheduler.ReplayStateCreated, scheduler.ReplayStateInProgress,
	scheduler.ReplayStatePartialReplayed, scheduler.ReplayStateReplayed,
}

type Validator struct {
	replayRepository ReplayRepository
	scheduler        ReplayScheduler
}

func NewValidator(replayRepository ReplayRepository, scheduler ReplayScheduler) *Validator {
	return &Validator{replayRepository: replayRepository, scheduler: scheduler}
}

func (v Validator) Validate(ctx context.Context, replayRequest *scheduler.Replay, jobCron *cron.ScheduleSpec) error {
	if err := v.validateConflictedReplay(ctx, replayRequest); err != nil {
		return err
	}

	return v.validateConflictedRun(ctx, replayRequest, jobCron)
}

func (v Validator) validateConflictedReplay(ctx context.Context, replayRequest *scheduler.Replay) error {
	onGoingReplays, err := v.replayRepository.GetReplayRequestsByStatus(ctx, replayStatusToValidate)
	if err != nil {
		return err
	}
	for _, onGoingReplay := range onGoingReplays {
		if onGoingReplay.Tenant() != replayRequest.Tenant() || onGoingReplay.JobName() != replayRequest.JobName() {
			continue
		}

		// Check any intersection of date range
		if (onGoingReplay.Config().StartTime.Equal(replayRequest.Config().EndTime) || onGoingReplay.Config().StartTime.Before(replayRequest.Config().EndTime)) &&
			(onGoingReplay.Config().EndTime.Equal(replayRequest.Config().StartTime) || onGoingReplay.Config().EndTime.After(replayRequest.Config().StartTime)) {
			return errors.NewError(errors.ErrFailedPrecond, scheduler.EntityJobRun, "conflicted replay found")
		}
	}
	return nil
}

func (v Validator) validateConflictedRun(ctx context.Context, replayRequest *scheduler.Replay, jobCron *cron.ScheduleSpec) error {
	jobRunCriteria := &scheduler.JobRunsCriteria{
		Name:      replayRequest.JobName().String(),
		StartDate: replayRequest.Config().StartTime,
		EndDate:   replayRequest.Config().EndTime,
	}
	runs, err := v.scheduler.GetJobRuns(ctx, replayRequest.Tenant(), jobRunCriteria, jobCron)
	if err != nil {
		return err
	}
	for _, run := range runs {
		if run.State == scheduler.StateQueued || run.State == scheduler.StateRunning {
			return errors.NewError(errors.ErrFailedPrecond, scheduler.EntityJobRun, "conflicted job run found")
		}
	}
	return nil
}
