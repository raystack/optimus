package service

import (
	"fmt"
	"time"

	"github.com/odpf/salt/log"
	"golang.org/x/net/context"

	"github.com/odpf/optimus/config"
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

	config config.ReplayConfig
}

func NewReplayWorker(l log.Logger, replayRepo ReplayRepository, scheduler ReplayScheduler, jobRepo JobRepository, config config.ReplayConfig) *ReplayWorker {
	return &ReplayWorker{l: l, replayRepo: replayRepo, scheduler: scheduler, jobRepo: jobRepo, config: config}
}

type JobReplayRunService interface {
	GetJobRuns(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, criteria *scheduler.JobRunsCriteria) ([]*scheduler.JobRunStatus, error)
}

func (w ReplayWorker) Process(replayReq *scheduler.ReplayWithRun) {
	ctx, cancelCtx := context.WithTimeout(context.Background(), w.config.WorkerTimeout)
	defer cancelCtx()

	w.l.Debug("processing replay request %s with status %s", replayReq.Replay.ID().String(), replayReq.Replay.State().String())
	jobCron, err := w.getJobCron(ctx, replayReq)
	if err != nil {
		w.l.Error(fmt.Sprintf("unable to get cron value for job %s: %s", replayReq.Replay.JobName(), err.Error()), "replay_id", replayReq.Replay.ID())
	}

	switch replayReq.Replay.State() {
	case scheduler.ReplayStateCreated:
		err = w.processNewReplayRequest(ctx, replayReq, jobCron)
	case scheduler.ReplayStatePartialReplayed:
		err = w.processPartialReplayedRequest(ctx, replayReq, jobCron)
	case scheduler.ReplayStateReplayed:
		err = w.processReplayedRequest(ctx, replayReq, jobCron)
	}

	if err != nil {
		if err := w.replayRepo.UpdateReplayStatus(ctx, replayReq.Replay.ID(), scheduler.ReplayStateFailed, err.Error()); err != nil {
			w.l.Error("unable to update replay state to failed", "replay_id", replayReq.Replay.ID())
			return
		}
	}
}

func (w ReplayWorker) processNewReplayRequest(ctx context.Context, replayReq *scheduler.ReplayWithRun, jobCron *cron.ScheduleSpec) (err error) {
	state := scheduler.ReplayStateReplayed
	var updatedRuns []*scheduler.JobRunStatus
	if replayReq.Replay.Config().Parallel {
		updatedRuns, err = w.processNewReplayRequestParallel(ctx, replayReq, jobCron)
	} else {
		updatedRuns, err = w.processNewReplayRequestSequential(ctx, replayReq, jobCron)
		if len(replayReq.Runs) > 1 {
			state = scheduler.ReplayStatePartialReplayed
		}
	}
	if err != nil {
		return err
	}
	if err := w.replayRepo.UpdateReplay(ctx, replayReq.Replay.ID(), state, updatedRuns, ""); err != nil {
		w.l.Error("unable to update replay state", "replay_id", replayReq.Replay.ID())
		return err
	}
	return nil
}

func (w ReplayWorker) processNewReplayRequestParallel(ctx context.Context, replayReq *scheduler.ReplayWithRun, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error) {
	startLogicalTime := replayReq.GetFirstExecutableRun().GetLogicalTime(jobCron)
	endLogicalTime := replayReq.GetLastExecutableRun().GetLogicalTime(jobCron)
	if err := w.scheduler.ClearBatch(ctx, replayReq.Replay.Tenant(), replayReq.Replay.JobName(), startLogicalTime, endLogicalTime); err != nil {
		w.l.Error("unable to clear job run for replay", "replay_id", replayReq.Replay.ID())
		return nil, err
	}

	w.l.Debug("cleared [%s] runs for replay [%s]", replayReq.Replay.JobName().String(), replayReq.Replay.ID().String())
	updatedReplayMap := make(map[time.Time]scheduler.State)
	for _, run := range replayReq.Runs {
		updatedReplayMap[run.ScheduledAt] = scheduler.StateReplayed
	}

	// TODO: merge might not be needed in parallel scenario
	updatedRuns := scheduler.JobRunStatusList(replayReq.Runs).MergeWithUpdatedRuns(updatedReplayMap)
	return updatedRuns, nil
}

func (w ReplayWorker) processNewReplayRequestSequential(ctx context.Context, replayReq *scheduler.ReplayWithRun, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error) {
	runToClear := replayReq.GetFirstExecutableRun()
	if err := w.scheduler.Clear(ctx, replayReq.Replay.Tenant(), replayReq.Replay.JobName(), runToClear.GetLogicalTime(jobCron)); err != nil {
		w.l.Error("unable to clear job run for replay", "replay_id", replayReq.Replay.ID())
		return nil, err
	}

	w.l.Debug("cleared [%s] [%s] run for replay %s", replayReq.Replay.JobName().String(), runToClear.ScheduledAt, replayReq.Replay.ID().String())
	updatedReplayMap := map[time.Time]scheduler.State{
		runToClear.ScheduledAt: scheduler.StateReplayed,
	}
	updatedRuns := scheduler.JobRunStatusList(replayReq.Runs).MergeWithUpdatedRuns(updatedReplayMap)
	return updatedRuns, nil
}

/*
- 1: running
- 2: pending
*/

func (w ReplayWorker) processPartialReplayedRequest(ctx context.Context, replayReq *scheduler.ReplayWithRun, jobCron *cron.ScheduleSpec) error {
	incomingRuns, err := w.fetchRuns(ctx, replayReq, jobCron)
	if err != nil {
		w.l.Error(fmt.Sprintf("unable to get runs: %s", err.Error()), "replay_id", replayReq.Replay.ID())
		return err
	}

	updatedReplayMap := identifyUpdatedRunStatus(replayReq.Runs, incomingRuns)
	updatedRuns := scheduler.JobRunStatusList(replayReq.Runs).MergeWithUpdatedRuns(updatedReplayMap)

	replayedRuns := scheduler.JobRunStatusList(updatedRuns).GetSortedRunsByStates([]scheduler.State{scheduler.StateReplayed})
	// TODO: rename to toBeReplayedRuns
	executableRuns := scheduler.JobRunStatusList(updatedRuns).GetSortedRunsByStates([]scheduler.State{scheduler.StatePending})

	replayState := scheduler.ReplayStatePartialReplayed
	if len(replayedRuns) == 0 && len(executableRuns) > 0 {
		logicalTimeToClear := executableRuns[0].GetLogicalTime(jobCron)
		if err := w.scheduler.Clear(ctx, replayReq.Replay.Tenant(), replayReq.Replay.JobName(), logicalTimeToClear); err != nil {
			w.l.Error("unable to clear job run for replay", "replay_id", replayReq.Replay.ID())
			return err
		}
		w.l.Debug("cleared [%s] [%s] run for replay %s", replayReq.Replay.JobName().String(), executableRuns[0].ScheduledAt, replayReq.Replay.ID().String())

		updatedReplayMap[executableRuns[0].ScheduledAt] = scheduler.StateReplayed
		updatedRuns = scheduler.JobRunStatusList(incomingRuns).MergeWithUpdatedRuns(updatedReplayMap)
	}

	pendingRuns := scheduler.JobRunStatusList(updatedRuns).GetSortedRunsByStates([]scheduler.State{scheduler.StatePending})
	if len(pendingRuns) == 0 {
		replayState = scheduler.ReplayStateReplayed
	}

	if err := w.replayRepo.UpdateReplay(ctx, replayReq.Replay.ID(), replayState, updatedRuns, ""); err != nil {
		w.l.Error("unable to update replay state", "replay_id", replayReq.Replay.ID())
		return err
	}
	return nil
}

func (w ReplayWorker) processReplayedRequest(ctx context.Context, replayReq *scheduler.ReplayWithRun, jobCron *cron.ScheduleSpec) error {
	incomingRuns, err := w.fetchRuns(ctx, replayReq, jobCron)
	if err != nil {
		w.l.Error(fmt.Sprintf("unable to get runs: %s", err.Error()), "replay_id", replayReq.Replay.ID())
		return err
	}

	updatedReplayMap := identifyUpdatedRunStatus(replayReq.Runs, incomingRuns)
	updatedRuns := scheduler.JobRunStatusList(incomingRuns).MergeWithUpdatedRuns(updatedReplayMap)
	inProgressRuns := scheduler.JobRunStatusList(updatedRuns).GetSortedRunsByStates([]scheduler.State{scheduler.StateReplayed})
	failedRuns := scheduler.JobRunStatusList(updatedRuns).GetSortedRunsByStates([]scheduler.State{scheduler.StateFailed})

	state := scheduler.ReplayStateReplayed
	message := replayReq.Replay.Message()
	if len(inProgressRuns) == 0 && len(failedRuns) == 0 {
		state = scheduler.ReplayStateSuccess
		w.l.Debug("marking replay %s as success", replayReq.Replay.ID().String())
	} else if len(inProgressRuns) == 0 && len(failedRuns) > 0 {
		state = scheduler.ReplayStateFailed
		message = fmt.Sprintf("found %d failed runs. %s", len(failedRuns), message)
		w.l.Debug("marking replay %s as failed", replayReq.Replay.ID().String())
	}

	if err := w.replayRepo.UpdateReplay(ctx, replayReq.Replay.ID(), state, updatedRuns, message); err != nil {
		w.l.Error("unable to update replay state", "replay_id", replayReq.Replay.ID())
		return err
	}
	return nil
}

func identifyUpdatedRunStatus(existingJobRuns []*scheduler.JobRunStatus, incomingJobRuns []*scheduler.JobRunStatus) map[time.Time]scheduler.State {
	incomingRunStatusMap := scheduler.JobRunStatusList(incomingJobRuns).ToRunStatusMap()

	updatedReplayMap := make(map[time.Time]scheduler.State)
	for _, run := range existingJobRuns {
		if run.State != scheduler.StateReplayed {
			continue
		}
		if incomingRunStatusMap[run.ScheduledAt.UTC()] == scheduler.StateSuccess || incomingRunStatusMap[run.ScheduledAt.UTC()] == scheduler.StateFailed {
			updatedReplayMap[run.ScheduledAt.UTC()] = incomingRunStatusMap[run.ScheduledAt.UTC()]
		}
	}
	return updatedReplayMap
}

func (w ReplayWorker) getJobCron(ctx context.Context, replayReq *scheduler.ReplayWithRun) (*cron.ScheduleSpec, error) {
	jobWithDetails, err := w.jobRepo.GetJobDetails(ctx, replayReq.Replay.Tenant().ProjectName(), replayReq.Replay.JobName())
	if err != nil || jobWithDetails == nil {
		return nil, fmt.Errorf("unable to get job details from DB for jobName: %s, project: %s, error: %w ",
			replayReq.Replay.JobName(), replayReq.Replay.Tenant().ProjectName(), err)
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

func (w ReplayWorker) fetchRuns(ctx context.Context, replayReq *scheduler.ReplayWithRun, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error) {
	jobRunCriteria := &scheduler.JobRunsCriteria{
		Name:      replayReq.Replay.JobName().String(),
		StartDate: replayReq.Replay.Config().StartTime,
		EndDate:   replayReq.Replay.Config().EndTime,
	}
	return w.scheduler.GetJobRuns(ctx, replayReq.Replay.Tenant(), jobRunCriteria, jobCron)
}
