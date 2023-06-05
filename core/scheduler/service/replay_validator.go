package service

import (
	"fmt"
	"time"

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
	jobRepo          JobRepository
}

func NewValidator(replayRepository ReplayRepository, scheduler ReplayScheduler, jobRepo JobRepository) *Validator {
	return &Validator{replayRepository: replayRepository, scheduler: scheduler, jobRepo: jobRepo}
}

func (v Validator) Validate(ctx context.Context, replayRequest *scheduler.Replay, jobCron *cron.ScheduleSpec) error {
	if err := v.validateDateRange(ctx, replayRequest); err != nil {
		return err
	}

	if err := v.validateConflictedReplay(ctx, replayRequest); err != nil {
		return err
	}

	return v.validateConflictedRun(ctx, replayRequest, jobCron)
}

func (v Validator) validateDateRange(ctx context.Context, replayRequest *scheduler.Replay) error {
	jobSpec, err := v.jobRepo.GetJobDetails(ctx, replayRequest.Tenant().ProjectName(), replayRequest.JobName())
	if err != nil {
		return err
	}
	replayStartDate := replayRequest.Config().StartTime.UTC()
	replayEndDate := replayRequest.Config().EndTime.UTC()
	jobStartDate := jobSpec.Schedule.StartDate.UTC()
	jobEndDate := time.Now().UTC()
	// time bound for end date
	if jobSpec.Schedule.EndDate != nil && jobSpec.Schedule.EndDate.UTC().Before(jobEndDate) {
		jobEndDate = jobSpec.Schedule.EndDate.UTC()
	}

	if replayStartDate.Before(jobStartDate) {
		return errors.NewError(errors.ErrFailedPrecond, scheduler.EntityReplay, fmt.Sprintf("replay start date (%s) is not allowed to be set before job start date (%s)", replayStartDate.String(), jobStartDate.String()))
	}

	if replayEndDate.After(jobEndDate) {
		return errors.NewError(errors.ErrFailedPrecond, scheduler.EntityReplay, fmt.Sprintf("replay end date (%s) is not allowed to be set after the date (%s)", replayEndDate.String(), jobEndDate.String()))
	}

	return nil
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
