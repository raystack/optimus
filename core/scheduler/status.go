package scheduler

import (
	"sort"
	"strings"
	"time"

	"github.com/raystack/optimus/internal/errors"
	"github.com/raystack/optimus/internal/lib/cron"
)

const (
	StatePending State = "pending"

	StateAccepted State = "accepted"
	StateRunning  State = "running"
	StateQueued   State = "queued"

	StateRetry State = "retried"

	StateSuccess State = "success"
	StateFailed  State = "failed"

	StateWaitUpstream State = "wait_upstream"
	StateInProgress   State = "in_progress"
)

var TaskEndStates = []State{StateSuccess, StateFailed, StateRetry}

type State string

func StateFromString(state string) (State, error) {
	switch strings.ToLower(state) {
	case string(StatePending):
		return StatePending, nil
	case string(StateAccepted):
		return StateAccepted, nil
	case string(StateRunning):
		return StateRunning, nil
	case string(StateRetry):
		return StateRetry, nil
	case string(StateQueued):
		return StateQueued, nil
	case string(StateSuccess):
		return StateSuccess, nil
	case string(StateFailed):
		return StateFailed, nil
	case string(StateWaitUpstream):
		return StateWaitUpstream, nil
	case string(StateInProgress):
		return StateInProgress, nil
	default:
		return "", errors.InvalidArgument(EntityJobRun, "invalid state for run "+state)
	}
}

func (j State) String() string {
	return string(j)
}

type JobRunStatus struct {
	ScheduledAt time.Time
	State       State
}

func JobRunStatusFrom(scheduledAt time.Time, state string) (JobRunStatus, error) {
	runState, err := StateFromString(state)
	if err != nil {
		return JobRunStatus{}, err
	}

	return JobRunStatus{
		ScheduledAt: scheduledAt,
		State:       runState,
	}, nil
}

func (j JobRunStatus) GetLogicalTime(jobCron *cron.ScheduleSpec) time.Time {
	return jobCron.Prev(j.ScheduledAt)
}

type JobRunStatusList []*JobRunStatus

func (j JobRunStatusList) GetSortedRunsByStates(states []State) []*JobRunStatus {
	stateMap := make(map[State]bool, len(states))
	for _, state := range states {
		stateMap[state] = true
	}

	var result []*JobRunStatus
	for _, run := range j {
		if stateMap[run.State] {
			result = append(result, run)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ScheduledAt.Before(result[j].ScheduledAt)
	})
	return result
}

func (j JobRunStatusList) GetSortedRunsByScheduledAt() []*JobRunStatus {
	result := []*JobRunStatus(j)
	sort.Slice(result, func(i, j int) bool {
		return result[i].ScheduledAt.Before(result[j].ScheduledAt)
	})
	return result
}

func (j JobRunStatusList) MergeWithUpdatedRuns(updatedRunMap map[time.Time]State) []*JobRunStatus {
	var updatedRuns []*JobRunStatus
	for _, run := range j {
		if updatedStatus, ok := updatedRunMap[run.ScheduledAt.UTC()]; ok {
			updatedRun := run
			updatedRun.State = updatedStatus
			updatedRuns = append(updatedRuns, updatedRun)
			continue
		}
		updatedRuns = append(updatedRuns, run)
	}
	return updatedRuns
}

func (j JobRunStatusList) ToRunStatusMap() map[time.Time]State {
	runStatusMap := make(map[time.Time]State, len(j))
	for _, run := range j {
		runStatusMap[run.ScheduledAt.UTC()] = run.State
	}
	return runStatusMap
}

// JobRunsCriteria represents the filter condition to get run status from scheduler
type JobRunsCriteria struct {
	Name        string
	StartDate   time.Time
	EndDate     time.Time
	Filter      []string
	OnlyLastRun bool
}

func (c *JobRunsCriteria) ExecutionStart(cron *cron.ScheduleSpec) time.Time {
	return cron.Prev(c.StartDate)
}

func (c *JobRunsCriteria) ExecutionEndDate(jobCron *cron.ScheduleSpec) time.Time {
	scheduleEndTime := c.EndDate

	// when the current time matches one of the schedule times execution time means previous schedule.
	if jobCron.Next(scheduleEndTime.Add(-time.Second * 1)).Equal(scheduleEndTime) {
		return jobCron.Prev(scheduleEndTime)
	}
	// else it is previous to previous schedule.
	return jobCron.Prev(jobCron.Prev(scheduleEndTime))
}
