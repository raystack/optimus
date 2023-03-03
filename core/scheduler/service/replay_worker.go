package service

import (
	"fmt"
	"time"

	"github.com/odpf/salt/log"
	"golang.org/x/net/context"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/lib/cron"
)

type ReplayScheduler interface {
	Clear(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) error
	ClearBatch(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, startTime, endTime time.Time) error

	GetJobRuns(ctx context.Context, t tenant.Tenant, criteria *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error)
}

type ReplayWorker struct {
	l log.Logger

	replayRepo ReplayRepository
	scheduler  ReplayScheduler

	jobRepo JobRepository
}

func NewReplayWorker(l log.Logger, replayRepo ReplayRepository, scheduler ReplayScheduler, jobRepo JobRepository) *ReplayWorker {
	return &ReplayWorker{l: l, replayRepo: replayRepo, scheduler: scheduler, jobRepo: jobRepo}
}

type JobReplayRunService interface {
	GetJobRuns(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, criteria *scheduler.JobRunsCriteria) ([]*scheduler.JobRunStatus, error)
}

func (w ReplayWorker) Process(ctx context.Context, replayReq *scheduler.Replay) {
	jobCron, err := w.getJobCron(ctx, replayReq)
	if err != nil {
		w.l.Error(fmt.Sprintf("unable to get cron value for job %s: %s", replayReq.Replay.JobName, err.Error()), "replay_id", replayReq.ID)
	}

	switch replayReq.Replay.State {
	case scheduler.ReplayStateCreated:
		err = w.processNewReplayRequest(ctx, replayReq, jobCron)
	case scheduler.ReplayStatePartialReplayed:
		err = w.processPartialReplayedRequest(ctx, replayReq, jobCron)
	case scheduler.ReplayStateReplayed:
		err = w.processReplayedRequest(ctx, replayReq, jobCron)
	}

	if err != nil {
		if err := w.replayRepo.UpdateReplayStatus(ctx, replayReq.ID, scheduler.ReplayStateFailed, err.Error()); err != nil {
			w.l.Error("unable to update replay state", "replay_id", replayReq.ID)
			return
		}
	}
}

func (w ReplayWorker) processNewReplayRequest(ctx context.Context, replayReq *scheduler.Replay, jobCron *cron.ScheduleSpec) error {
	state := scheduler.ReplayStateReplayed
	if replayReq.Replay.Config.Parallel {
		startLogicalTime := replayReq.GetFirstExecutableRun().GetLogicalTime(jobCron)
		endLogicalTime := replayReq.GetLastExecutableRun().GetLogicalTime(jobCron)
		if err := w.scheduler.ClearBatch(ctx, replayReq.Replay.Tenant, replayReq.Replay.JobName, startLogicalTime, endLogicalTime); err != nil {
			w.l.Error("unable to clear job run for replay", "replay_id", replayReq.ID)
			return err
		}
	} else {
		logicalTimeToClear := replayReq.GetFirstExecutableRun().GetLogicalTime(jobCron)
		if err := w.scheduler.Clear(ctx, replayReq.Replay.Tenant, replayReq.Replay.JobName, logicalTimeToClear); err != nil {
			w.l.Error("unable to clear job run for replay", "replay_id", replayReq.ID)
			return err
		}
		if len(replayReq.Runs) > 1 {
			state = scheduler.ReplayStatePartialReplayed
		}
	}

	jobRuns, err := w.fetchRuns(ctx, replayReq, jobCron)
	if err != nil {
		w.l.Error(fmt.Sprintf("unable to get runs: %s", err.Error()), "replay_id", replayReq.ID)
		return err
	}

	if err := w.replayRepo.UpdateReplay(ctx, replayReq.ID, state, jobRuns, ""); err != nil {
		w.l.Error("unable to update replay state", "replay_id", replayReq.ID)
		return err
	}
	return nil
}

func (w ReplayWorker) processPartialReplayedRequest(ctx context.Context, replayReq *scheduler.Replay, jobCron *cron.ScheduleSpec) error {
	jobRuns, err := w.fetchRuns(ctx, replayReq, jobCron)
	if err != nil {
		w.l.Error(fmt.Sprintf("unable to get runs: %s", err.Error()), "replay_id", replayReq.ID)
		return err
	}

	executableRuns := scheduler.JobRunStatusList(jobRuns).GetSortedRunsByStates([]scheduler.State{scheduler.StatePending})
	if !w.isOnGoingRunsExist(jobRuns) && len(executableRuns) > 0 {
		logicalTimeToClear := executableRuns[0].GetLogicalTime(jobCron)
		if err := w.scheduler.Clear(ctx, replayReq.Replay.Tenant, replayReq.Replay.JobName, logicalTimeToClear); err != nil {
			w.l.Error("unable to clear job run for replay", "replay_id", replayReq.ID)
			return err
		}
		jobRuns, err = w.fetchRuns(ctx, replayReq, jobCron)
		if err != nil {
			w.l.Error(fmt.Sprintf("unable to get runs: %s", err.Error()), "replay_id", replayReq.ID)
			return err
		}
	}

	replayState := scheduler.ReplayStatePartialReplayed
	if len(executableRuns) <= 1 {
		replayState = scheduler.ReplayStateReplayed
	}

	if err := w.replayRepo.UpdateReplay(ctx, replayReq.ID, replayState, jobRuns, ""); err != nil {
		w.l.Error("unable to update replay state", "replay_id", replayReq.ID)
		return err
	}
	return nil
}

func (w ReplayWorker) processReplayedRequest(ctx context.Context, replayReq *scheduler.Replay, jobCron *cron.ScheduleSpec) error {
	runs, err := w.fetchRuns(ctx, replayReq, jobCron)
	if err != nil {
		w.l.Error(fmt.Sprintf("unable to get runs: %s", err.Error()), "replay_id", replayReq.ID)
		return err
	}

	inProgressRuns := scheduler.JobRunStatusList(runs).GetSortedRunsByStates([]scheduler.State{scheduler.StateQueued, scheduler.StateRunning})
	failedRuns := scheduler.JobRunStatusList(runs).GetSortedRunsByStates([]scheduler.State{scheduler.StateFailed})
	state := scheduler.ReplayStateReplayed
	if len(inProgressRuns) == 0 && len(failedRuns) == 0 {
		state = scheduler.ReplayStateSuccess
	} else if len(inProgressRuns) == 0 && len(failedRuns) > 0 {
		state = scheduler.ReplayStateFailed
	}

	if err := w.replayRepo.UpdateReplay(ctx, replayReq.ID, state, runs, ""); err != nil {
		w.l.Error("unable to update replay state", "replay_id", replayReq.ID)
		return err
	}
	return nil
}

func (w ReplayWorker) getJobCron(ctx context.Context, replayReq *scheduler.Replay) (*cron.ScheduleSpec, error) {
	jobWithDetails, err := w.jobRepo.GetJobDetails(ctx, replayReq.Replay.Tenant.ProjectName(), replayReq.Replay.JobName)
	if err != nil {
		return nil, fmt.Errorf("unable to get job details from DB for jobName: %s, project: %s, error: %w ",
			replayReq.Replay.JobName, replayReq.Replay.Tenant.ProjectName(), err)
	}
	interval := jobWithDetails.Schedule.Interval
	if interval == "" {
		return nil, fmt.Errorf("job schedule interval not found")
	}
	jobCron, err := cron.ParseCronSchedule(interval)
	if err != nil {
		return nil, fmt.Errorf("unable to parse job cron interval %w", err)
	}
	return jobCron, nil
}

func (w ReplayWorker) fetchRuns(ctx context.Context, replayReq *scheduler.Replay, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error) {
	jobRunCriteria := &scheduler.JobRunsCriteria{
		Name:      replayReq.Replay.JobName.String(),
		StartDate: replayReq.Replay.Config.StartTime,
		EndDate:   replayReq.Replay.Config.EndTime,
	}
	return w.scheduler.GetJobRuns(ctx, replayReq.Replay.Tenant, jobRunCriteria, jobCron)
}

func (ReplayWorker) isOnGoingRunsExist(jobRuns []*scheduler.JobRunStatus) bool {
	inProgressRuns := scheduler.JobRunStatusList(jobRuns).GetSortedRunsByStates([]scheduler.State{scheduler.StateRunning, scheduler.StateQueued})
	return len(inProgressRuns) > 0
}
