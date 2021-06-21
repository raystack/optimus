package models

import (
	"time"

	"github.com/google/uuid"
)

const (
	// ReplayStatusAccepted worker picked up the request
	ReplayStatusAccepted = "Accepted"
)

type ReplayRequestInput struct {
	ID         uuid.UUID
	Job        JobSpec
	Start      time.Time
	End        time.Time
	Project    ProjectSpec
	DagSpecMap map[string]JobSpec
}

type ReplaySpec struct {
	ID        uuid.UUID
	Job       JobSpec
	StartDate time.Time
	EndDate   time.Time
	Status    string
	Message   string
	CommitID  string
	Project   ProjectSpec
}

type Syncer interface {
	SyncReplayStatusWithAirflow(ReplaySpec) error
}

type ReplayRepository interface {
	Insert(replay *ReplaySpec) error
	GetByID(id uuid.UUID) (ReplaySpec, error)
}
