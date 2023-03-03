package scheduler

import (
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

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

type ReplayRequest struct {
	ID uuid.UUID

	JobName JobName
	Tenant  tenant.Tenant
	Config  *ReplayConfig

	State   ReplayState
	Message string

	CreatedAt time.Time
}

func NewReplayRequest(jobName JobName, tenant tenant.Tenant, config *ReplayConfig, state ReplayState) *ReplayRequest {
	return &ReplayRequest{JobName: jobName, Tenant: tenant, Config: config, State: state}
}

func NewReplayRequestWithMetadata(id uuid.UUID, jobName JobName, tenant tenant.Tenant, config *ReplayConfig, state ReplayState, createdAt time.Time) *ReplayRequest {
	return &ReplayRequest{ID: id, JobName: jobName, Tenant: tenant, Config: config, State: state, CreatedAt: createdAt}
}

type Replay struct {
	ID     uuid.UUID
	Replay *ReplayRequest
	Runs   []*JobRunStatus // TODO: JobRunStatus does not have `message/log`
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

func NewStoredReplay(id uuid.UUID, replay *ReplayRequest) *Replay {
	return &Replay{ID: id, Replay: replay}
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
