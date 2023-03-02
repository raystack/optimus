package service

import (
	"golang.org/x/net/context"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/internal/lib/cron"
)

var (
	replayStatusToValidate = []scheduler.ReplayState{scheduler.ReplayStateCreated, scheduler.ReplayStateInProgress,
		scheduler.ReplayStatePartialReplayed, scheduler.ReplayStateReplayed}
	// ErrConflictedJobRun = errors.New("conflicted job run found")
	//ErrConflictedReplay = errors.New("conflicted replay found")
)

type Validator struct {
	replayRepository ReplayRepository
	scheduler        ReplayScheduler
}

func NewValidator(replayRepository ReplayRepository, scheduler ReplayScheduler) *Validator {
	return &Validator{replayRepository: replayRepository, scheduler: scheduler}
}

func (v Validator) Validate(ctx context.Context, tenant tenant.Tenant, jobName scheduler.JobName, config *scheduler.ReplayConfig, jobCron *cron.ScheduleSpec) error {
	if err := v.validateConflictedReplay(ctx, tenant, jobName, config); err != nil {
		return err
	}

	return v.validateConflictedRun(ctx, tenant, jobName, config, jobCron)
}

func (v Validator) validateConflictedReplay(ctx context.Context, tenant tenant.Tenant, jobName scheduler.JobName, config *scheduler.ReplayConfig) error {
	onGoingReplays, err := v.replayRepository.GetReplayByStatus(ctx, replayStatusToValidate)
	if err != nil {
		return err
	}
	for _, onGoingReplay := range onGoingReplays {
		if onGoingReplay.Replay.Tenant != tenant || onGoingReplay.Replay.JobName != jobName {
			continue
		}

		// Check any intersection of date range
		if (onGoingReplay.Replay.Config.StartTime.Equal(config.EndTime) || onGoingReplay.Replay.Config.StartTime.Before(config.EndTime)) &&
			(onGoingReplay.Replay.Config.EndTime.Equal(config.StartTime) || onGoingReplay.Replay.Config.EndTime.After(config.StartTime)) {
			return errors.NewError(errors.ErrFailedPrecond, scheduler.EntityJobRun, "conflicted replay found")
		}
	}
	return nil
}

func (v Validator) validateConflictedRun(ctx context.Context, tenant tenant.Tenant, jobName scheduler.JobName, config *scheduler.ReplayConfig, jobCron *cron.ScheduleSpec) error {
	jobRunCriteria := &scheduler.JobRunsCriteria{
		Name:      jobName.String(),
		StartDate: config.StartTime,
		EndDate:   config.EndTime,
	}
	runs, err := v.scheduler.GetJobRuns(ctx, tenant, jobRunCriteria, jobCron)
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
