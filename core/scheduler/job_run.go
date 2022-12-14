package scheduler

import (
	"time"

	"github.com/google/uuid"

	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

type JobRunID uuid.UUID

func JobRunIDFromString(runID string) (JobRunID, error) {
	if runID == "" {
		return JobRunID(uuid.Nil), nil
	}

	parsed, err := uuid.Parse(runID)
	if err != nil {
		return JobRunID{}, errors.InvalidArgument(EntityJobRun, "invalid value for job run id "+runID)
	}

	return JobRunID(parsed), nil
}

func (i JobRunID) UUID() uuid.UUID {
	return uuid.UUID(i)
}

func (i JobRunID) IsEmpty() bool {
	return i.UUID() == uuid.Nil
}

type JobRun struct {
	ID uuid.UUID

	JobName JobName
	Tenant  tenant.Tenant

	StartTime time.Time
}

type OperatorRun struct {
	ID           uuid.UUID
	Name         string
	JobRunID     uuid.UUID
	OperatorType OperatorType
	Status       State
	StartTime    time.Time
	EndTime      time.Time
}

type NotifyAttrs struct {
	Owner    string
	JobEvent Event
	Route    string
	Secret   string
}
