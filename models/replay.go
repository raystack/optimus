package models

import (
	"time"

	"github.com/odpf/optimus/core/tree"

	"github.com/google/uuid"
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
	ID                uuid.UUID
	Job               JobSpec
	Start             time.Time
	End               time.Time
	Project           ProjectSpec
	JobSpecMap        map[string]JobSpec
	Force             bool
	IgnoreDownstream  bool
	AllowedDownstream string
}

type ReplaySpec struct {
	ID               uuid.UUID
	Job              JobSpec
	StartDate        time.Time
	EndDate          time.Time
	IgnoreDownstream bool
	ExecutionTree    *tree.TreeNode
	Status           string
	Message          ReplayMessage
	CreatedAt        time.Time
}

type ReplayState struct {
	Status string
	Node   *tree.TreeNode
}
