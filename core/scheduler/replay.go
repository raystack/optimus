package scheduler

import (
	"strings"
	"time"

	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

const (
	ReplayStateCreated ReplayState = "created"

	ReplayStateInProgress ReplayState = "in progress"
	ReplayStateInvalid    ReplayState = "invalid"
	ReplayStateReplayed   ReplayState = "replayed"

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

type ReplayConfig struct {
	StartTime   time.Time
	EndTime     time.Time
	Parallel    bool
	Description string
}

func NewReplayConfig(startTime time.Time, endTime time.Time, parallel bool, description string) *ReplayConfig {
	return &ReplayConfig{StartTime: startTime, EndTime: endTime, Parallel: parallel, Description: description}
}
