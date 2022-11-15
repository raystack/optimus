package resource

import (
	"time"

	"github.com/google/uuid"

	"github.com/odpf/optimus/core/tenant"
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

type IgnoredResource struct {
	Name   string
	Reason string
}

type BackupResult struct {
	ID               BackupID
	ResourceNames    []string
	IgnoredResources []IgnoredResource
}

type Backup struct {
	id BackupID

	store  Store
	tenant tenant.Tenant

	resourceNames []string
	description   string
	createdAt     time.Time
	config        map[string]string
}

func NewBackup(store Store, t tenant.Tenant, resNames []string, desc string, createdAt time.Time, conf map[string]string) (*Backup, error) {
	if len(resNames) == 0 {
		return nil, errors.InvalidArgument(EntityBackup, "list of resources to backup is empty")
	}

	for _, resourceName := range resNames {
		if resourceName == "" {
			return nil, errors.InvalidArgument(EntityBackup, "one of resource names is empty")
		}
	}

	return &Backup{
		store:         store,
		tenant:        t,
		resourceNames: nil,
		description:   desc,
		createdAt:     createdAt,
		config:        conf,
	}, nil
}

func (b *Backup) GetConfigOrDefaultFor(key string, fallback string) string {
	value, ok := b.config[key]
	if ok {
		return value
	}

	b.config[key] = fallback
	return fallback
}

func (b *Backup) UpdateID(id uuid.UUID) error {
	if id == uuid.Nil {
		return errors.InvalidArgument(EntityBackup, "id to update is invalid")
	}

	if !b.id.IsInvalid() {
		return errors.InvalidStateTransition(EntityBackup, "trying to replace valid id "+b.id.String())
	}
	b.id = BackupID(id)
	return nil
}

func (b *Backup) ID() BackupID {
	return b.id
}

func (b *Backup) Store() Store {
	return b.store
}

func (b *Backup) Tenant() tenant.Tenant {
	return b.tenant
}

func (b *Backup) ResourceNames() []string {
	return b.resourceNames
}

func (b *Backup) Description() string {
	return b.description
}

func (b *Backup) CreatedAt() time.Time {
	return b.createdAt
}

func (b *Backup) Config() map[string]string {
	return b.config
}
