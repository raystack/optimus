package resource

import (
	"time"

	"github.com/google/uuid"

	"github.com/odpf/optimus/internal/errors"
)

const (
	EntityBackup = "backup"
)

type BackupID uuid.UUID

func BackupIDFrom(id string) (BackupID, error) {
	parsedID, err := uuid.Parse(id)
	if err != nil {
		return BackupID(uuid.Nil), errors.InvalidArgument(EntityBackup, "invalid id for backup "+id)
	}

	return BackupID(parsedID), nil
}

func (i BackupID) String() string {
	return i.UUID().String()
}

func (i BackupID) IsInvalid() bool {
	return i.UUID() == uuid.Nil
}

func (i BackupID) UUID() uuid.UUID {
	return uuid.UUID(i)
}

type BackupConfiguration struct {
	ResourceNames []Name

	Description string
	Config      map[string]string
}

type IgnoredResource struct {
	Name   Name
	Reason string
}

type BackupInfo struct {
	ResourceNames    []Name
	IgnoredResources []IgnoredResource
}

func (b BackupInfo) ResourceNamesAsString() []string {
	var names []string
	for _, name := range b.ResourceNames {
		names = append(names, name.String())
	}
	return names
}

type BackupDetails struct {
	ID BackupID

	ResourceNames []Name
	Description   string
	CreatedAt     time.Time
	Config        map[string]string
}

func (d BackupDetails) ResourceNamesAsString() []string {
	var names []string
	for _, name := range d.ResourceNames {
		names = append(names, name.String())
	}
	return names
}
