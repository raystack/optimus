package scheduler

import (
	"github.com/google/uuid"
	"sort"
	"strings"
	"time"

	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

const (
	ReplayStateCreated ReplayState = "created"

	ReplayStateInProgress      ReplayState = "in progress"
	ReplayStateInvalid         ReplayState = "invalid"
	ReplayStatePartialReplayed ReplayState = "partial replayed"
	ReplayStateReplayed        ReplayState = "replayed"

	ReplayStateSuccess ReplayState = "success"
	ReplayStateFailed  ReplayState = "failed"
)

type ReplayState string

func ReplayStateFromString(state string) (ReplayState, error) {
	switch strings.ToLower(state) {
	case string(ReplayStateCreated):
		return ReplayStateCreated, nil
	case string(ReplayStateInProgress):
		return ReplayStateInProgress, nil
	case string(ReplayStateInvalid):
		return ReplayStateInvalid, nil
	case string(ReplayStatePartialReplayed):
		return ReplayStatePartialReplayed, nil
	case string(ReplayStateReplayed):
		return ReplayStateReplayed, nil
	case string(ReplayStateSuccess):
		return ReplayStateSuccess, nil
	case string(ReplayStateFailed):
		return ReplayStateFailed, nil
	default:
		return "", errors.InvalidArgument(EntityJobRun, "invalid state for replay "+state)
	}
}

func (j ReplayState) String() string {
	return string(j)
}

type Replay struct {
	JobName JobName
	Tenant  tenant.Tenant
	Config  *ReplayConfig

	Runs []*JobRunStatus // TODO: JobRunStatus does not have `message/log`

	State   ReplayState
	Message string
}

func NewReplay(jobName JobName, tenant tenant.Tenant, config *ReplayConfig, runs []*JobRunStatus, state ReplayState) *Replay {
	return &Replay{JobName: jobName, Tenant: tenant, Config: config, Runs: runs, State: state}
}

func (r Replay) GetFirstExecutableRun() *JobRunStatus {
	sort.Slice(r.Runs, func(i, j int) bool {
		return r.Runs[i].ScheduledAt.Before(r.Runs[j].ScheduledAt)
	})
	return r.Runs[0]
}

func (r Replay) GetLastExecutableRun() *JobRunStatus {
	sort.Slice(r.Runs, func(i, j int) bool {
		return r.Runs[i].ScheduledAt.After(r.Runs[j].ScheduledAt)
	})
	return r.Runs[0]
}

type StoredReplay struct {
	ID     uuid.UUID
	Replay *Replay
}

func NewStoredReplay(ID uuid.UUID, replay *Replay) *StoredReplay {
	return &StoredReplay{ID: ID, Replay: replay}
}

type ReplayConfig struct {
	StartTime   time.Time
	EndTime     time.Time
	Parallel    bool
	Description string
}

func NewReplayConfig(startTime time.Time, endTime time.Time, parallel bool, description string) *ReplayConfig {
	return &ReplayConfig{StartTime: startTime, EndTime: endTime, Parallel: parallel, Description: description}
}
