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
	ResourceName Name

	Description                 string
	AllowedDownstreamNamespaces []string // This should be allowed namespaces, default current
	Config                      map[string]string
}

type BackupInfo struct { // TODO: check if we really need it
	ResourceURNs     []string
	IgnoredResources []string
}

type TableInfo struct {
	TableName Name
	Dataset   Dataset
}

func (t TableInfo) URN() string {
	return t.Dataset.URN() + "." + t.TableName.String()
}

type TableBackupInfo struct {
	Source      TableInfo
	Destination TableInfo
}

type BackupDetails struct {
	ID BackupID

	ResourceName   string
	Description    string
	CreatedAt      time.Time
	Config         map[string]string
	BackedUpTables []TableBackupInfo
}

func (d *BackupDetails) SourceURNs() []string {
	var URNs []string
	for _, backup := range d.BackedUpTables {
		URNs = append(URNs, backup.Source.URN())
	}
	return URNs
}

func (d *BackupDetails) DestinationURNs() []string {
	var URNs []string
	for _, backup := range d.BackedUpTables {
		URNs = append(URNs, backup.Destination.URN())
	}
	return URNs
}
