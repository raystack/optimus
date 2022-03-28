package models

import (
	"time"

	"github.com/google/uuid"

	"github.com/odpf/optimus/core/tree"
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
