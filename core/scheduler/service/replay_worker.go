package service

import (
	"fmt"
	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/lib/cron"
	"github.com/odpf/salt/log"
	"golang.org/x/net/context"
	"time"
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

	jobRunService JobRunService
}

func NewReplayWorker(l log.Logger, replayRepo ReplayRepository, scheduler ReplayScheduler) *ReplayWorker {
	return &ReplayWorker{l: l, replayRepo: replayRepo, scheduler: scheduler}
}

func (w ReplayWorker) Process(ctx context.Context, replays []*scheduler.StoredReplay) {
	for _, replayReq := range replays {
		if err := w.replayRepo.UpdateReplay(ctx, replayReq.ID, scheduler.ReplayStateInProgress, replayReq.Replay.Runs, ""); err != nil {
			w.l.Error("unable to update replay state", "replay_id", replayReq.ID)
			return
		}

		var err error
		switch replayReq.Replay.State {
		case scheduler.ReplayStateCreated:
			err = w.processNewReplayRequest(ctx, replayReq)
		case scheduler.ReplayStatePartialReplayed:
			err = w.processPartialReplayedRequest(ctx, replayReq)
		case scheduler.ReplayStateReplayed:
			err = w.processReplayedRequest(ctx, replayReq)
		}

		if err != nil {
			if err := w.replayRepo.UpdateReplay(ctx, replayReq.ID, scheduler.ReplayStateFailed, replayReq.Replay.Runs, err.Error()); err != nil {
				w.l.Error("unable to update replay state", "replay_id", replayReq.ID)
				return
			}
		}
	}
}

func (w ReplayWorker) processNewReplayRequest(ctx context.Context, replayReq *scheduler.StoredReplay) error {
	state := scheduler.ReplayStateReplayed
	if replayReq.Replay.Config.Parallel {
		startLogicalTime := replayReq.Replay.GetFirstExecutableRun().LogicalTime
		endLogicalTime := replayReq.Replay.GetLastExecutableRun().LogicalTime
		if err := w.scheduler.ClearBatch(ctx, replayReq.Replay.Tenant, replayReq.Replay.JobName, startLogicalTime, endLogicalTime); err != nil {
			w.l.Error("unable to clear job run for replay", "replay_id", replayReq.ID)
			return err
		}
	} else {
		logicalTimeToClear := replayReq.Replay.GetFirstExecutableRun().LogicalTime
		if err := w.scheduler.Clear(ctx, replayReq.Replay.Tenant, replayReq.Replay.JobName, logicalTimeToClear); err != nil {
			w.l.Error("unable to clear job run for replay", "replay_id", replayReq.ID)
			return err
		}
		if len(replayReq.Replay.Runs) > 1 {
			state = scheduler.ReplayStatePartialReplayed
		}
	}

	jobRuns, err := w.fetchRuns(ctx, replayReq)
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

func (w ReplayWorker) processPartialReplayedRequest(ctx context.Context, replayReq *scheduler.StoredReplay) error {
	jobRuns, err := w.fetchRuns(ctx, replayReq)
	if err != nil {
		w.l.Error(fmt.Sprintf("unable to get runs: %s", err.Error()), "replay_id", replayReq.ID)
		return err
	}

	executableRuns := scheduler.JobRunStatusList(jobRuns).GetSortedRunsByStates([]scheduler.State{scheduler.StatePending})
	if !w.isOnGoingRunsExist(jobRuns) && len(executableRuns) > 0 {
		logicalTimeToClear := executableRuns[0].LogicalTime
		if err := w.scheduler.Clear(ctx, replayReq.Replay.Tenant, replayReq.Replay.JobName, logicalTimeToClear); err != nil {
			w.l.Error("unable to clear job run for replay", "replay_id", replayReq.ID)
			return err
		}
		jobRuns, err = w.fetchRuns(ctx, replayReq)
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

func (w ReplayWorker) processReplayedRequest(ctx context.Context, replayReq *scheduler.StoredReplay) error {
	runs, err := w.fetchRuns(ctx, replayReq)
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

func (w ReplayWorker) fetchRuns(ctx context.Context, replayReq *scheduler.StoredReplay) ([]*scheduler.JobRunStatus, error) {
	jobRunCriteria := &scheduler.JobRunsCriteria{
		Name:      replayReq.Replay.JobName.String(),
		StartDate: replayReq.Replay.Config.StartTime,
		EndDate:   replayReq.Replay.Config.EndTime,
	}
	return w.jobRunService.GetJobRuns(ctx, replayReq.Replay.Tenant.ProjectName(), replayReq.Replay.JobName, jobRunCriteria)
}

func (w ReplayWorker) isOnGoingRunsExist(jobRuns []*scheduler.JobRunStatus) bool {
	inProgressRuns := scheduler.JobRunStatusList(jobRuns).GetSortedRunsByStates([]scheduler.State{scheduler.StateRunning, scheduler.StateQueued})
	if len(inProgressRuns) > 0 {
		return true
	}
	return false
}
