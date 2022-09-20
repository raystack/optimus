package models

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/odpf/optimus/internal/lib/tree"
)

const (
	// ReplayStatusAccepted worker picked up the request
	ReplayStatusAccepted   = "accepted"
	ReplayStatusInProgress = "inprogress"
	// ReplayStatusReplayed worker finished clear up the run instances
	ReplayStatusReplayed = "replayed"
	// ReplayStatusFailed worker fail while processing the replay request
	ReplayStatusFailed    = "failed"    // end state
	ReplayStatusSuccess   = "success"   // end state
	ReplayStatusCancelled = "cancelled" // end state
)

type ReplayMessage struct {
	Type    string
	Message string
}

type ReplayRequest struct {
	ID                          uuid.UUID
	Job                         JobSpec
	Start                       time.Time
	End                         time.Time
	Project                     ProjectSpec
	JobSpecMap                  map[string]JobSpec
	JobNamespaceMap             map[string]string
	Force                       bool
	AllowedDownstreamNamespaces []string
}

type ReplayPlan struct {
	ExecutionTree *tree.TreeNode
	IgnoredJobs   []string
}

type ReplaySpec struct {
	ID            uuid.UUID
	Job           JobSpec
	StartDate     time.Time
	EndDate       time.Time
	Config        map[string]string
	ExecutionTree *tree.TreeNode
	Status        string
	Message       ReplayMessage
	CreatedAt     time.Time
}

type ReplayState struct {
	Status string
	Node   *tree.TreeNode
}

type ReplayResult struct {
	ID          uuid.UUID
	IgnoredJobs []string
}

type ReplayService interface {
	// ReplayDryRun returns the execution tree of jobSpec and its dependencies between start and endDate, and the ignored jobs
	ReplayDryRun(context.Context, ReplayRequest) (ReplayPlan, error)
	// Replay replays the jobSpec and its dependencies between start and endDate
	Replay(context.Context, ReplayRequest) (ReplayResult, error)
	// GetReplayStatus of a replay using its ID
	GetReplayStatus(context.Context, ReplayRequest) (ReplayState, error)
	// GetReplayList of a project
	GetReplayList(ctx context.Context, projectID ProjectID) ([]ReplaySpec, error)
}
