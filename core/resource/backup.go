package resource

import (
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

func (i BackupID) UUID() uuid.UUID {
	return uuid.UUID(i)
}

type BackupConfiguration struct {
	ResourceName Name

	Description                 string
	AllowedDownstreamNamespaces []string
	Config                      map[string]string
}

type BackupInfo struct {
	Resources        []string
	IgnoredResources []string
}
