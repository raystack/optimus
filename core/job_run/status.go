package job_run

import (
	"strings"
	"time"

	"github.com/odpf/optimus/internal/errors"
)

const (
	StatePending State = "pending"

	StateAccepted State = "accepted"
	StateRunning  State = "running"
	StateQueued   State = "queued"

	StateSuccess State = "success"
	StateFailed  State = "failed"
)

type State string

func StateFromString(state string) (State, error) {
	switch strings.ToLower(state) {
	case string(StatePending):
		return StatePending, nil
	case string(StateAccepted):
		return StateAccepted, nil
	case string(StateRunning):
		return StateRunning, nil
	case string(StateQueued):
		return StateQueued, nil
	case string(StateSuccess):
		return StateSuccess, nil
	case string(StateFailed):
		return StateFailed, nil
	default:
		return "", errors.InvalidArgument(EntityJobRun, "invalid state for job run "+state)
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
