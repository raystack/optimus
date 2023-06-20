package scheduler

import (
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

const (
	// initial state
	ReplayStateCreated ReplayState = "created"

	// running state
	ReplayStateInProgress      ReplayState = "in progress"
	ReplayStatePartialReplayed ReplayState = "partial replayed"
	ReplayStateReplayed        ReplayState = "replayed"

	// terminal state
	ReplayStateInvalid ReplayState = "invalid"
	ReplayStateSuccess ReplayState = "success"
	ReplayStateFailed  ReplayState = "failed"

	// state on presentation layer
	ReplayUserStateCreated    ReplayUserState = "created"
	ReplayUserStateInProgress ReplayUserState = "in progress"
	ReplayUserStateInvalid    ReplayUserState = "invalid"
	ReplayUserStateSuccess    ReplayUserState = "success"
	ReplayUserStateFailed     ReplayUserState = "failed"

	EntityReplay = "replay"
)

type (
	ReplayState     string // contract status for business layer
	ReplayUserState string // contract status for presentation layer
)

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

func (j ReplayUserState) String() string {
	return string(j)
}

type Replay struct {
	id uuid.UUID

	jobName JobName
	tenant  tenant.Tenant
	config  *ReplayConfig

	state   ReplayState
	message string

	createdAt time.Time
}

func (r *Replay) ID() uuid.UUID {
	return r.id
}

func (r *Replay) JobName() JobName {
	return r.jobName
}

func (r *Replay) Tenant() tenant.Tenant {
	return r.tenant
}

func (r *Replay) Config() *ReplayConfig {
	return r.config
}

func (r *Replay) State() ReplayState {
	return r.state
}

func (r *Replay) UserState() ReplayUserState {
	switch r.state {
	case ReplayStateCreated:
		return ReplayUserStateCreated
	case ReplayStateInProgress, ReplayStatePartialReplayed, ReplayStateReplayed:
		return ReplayUserStateInProgress
	case ReplayStateInvalid:
		return ReplayUserStateInvalid
	case ReplayStateSuccess:
		return ReplayUserStateSuccess
	case ReplayStateFailed:
		return ReplayUserStateFailed
	default:
		return ""
	}
}

func (r *Replay) Message() string {
	return r.message
}

func (r *Replay) CreatedAt() time.Time {
	return r.createdAt
}

func NewReplayRequest(jobName JobName, tenant tenant.Tenant, config *ReplayConfig, state ReplayState) *Replay {
	return &Replay{jobName: jobName, tenant: tenant, config: config, state: state}
}

func NewReplay(id uuid.UUID, jobName JobName, tenant tenant.Tenant, config *ReplayConfig, state ReplayState, createdAt time.Time) *Replay {
	return &Replay{id: id, jobName: jobName, tenant: tenant, config: config, state: state, createdAt: createdAt}
}

type ReplayWithRun struct {
	Replay *Replay
	Runs   []*JobRunStatus // TODO: JobRunStatus does not have `message/log`
}

func (r *ReplayWithRun) GetFirstExecutableRun() *JobRunStatus {
	runs := JobRunStatusList(r.Runs).GetSortedRunsByStates([]State{StatePending})
	if len(runs) > 0 {
		return runs[0]
	}
	return nil
}

func (r *ReplayWithRun) GetLastExecutableRun() *JobRunStatus {
	runs := JobRunStatusList(r.Runs).GetSortedRunsByStates([]State{StatePending})
	if len(runs) > 0 {
		return runs[len(runs)-1]
	}
	return nil
}

type ReplayConfig struct {
	StartTime   time.Time
	EndTime     time.Time
	Parallel    bool
	JobConfig   map[string]string
	Description string
}

func NewReplayConfig(startTime, endTime time.Time, parallel bool, jobConfig map[string]string, description string) *ReplayConfig {
	return &ReplayConfig{StartTime: startTime.UTC(), EndTime: endTime.UTC(), Parallel: parallel, JobConfig: jobConfig, Description: description}
}
