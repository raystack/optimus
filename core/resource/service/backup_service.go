package service

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

// recentBackupWindowMonths contains the window interval to consider for recent backups
const recentBackupWindowMonths = -3

type BackupRepository interface {
	GetByID(ctx context.Context, store resource.Store, id resource.BackupID) (*resource.BackupDetails, error)
	GetAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.BackupDetails, error)
	Save(ctx context.Context, details *resource.BackupDetails) error
}

type ResourceProvider interface {
	GetResources(ctx context.Context, tnnt tenant.Tenant, store resource.Store, names []string) ([]*resource.Resource, error)
}

type BackupManager interface {
	Backup(ctx context.Context, store resource.Store, config *resource.BackupDetails, resources []*resource.Resource) (*resource.BackupInfo, error)
}

type IDProvider interface {
	NewUUID() (uuid.UUID, error)
}

type BackupService struct {
	repo BackupRepository

	resources     ResourceProvider
	idProvider    IDProvider
	backupManager BackupManager
}

func (s BackupService) Backup(ctx context.Context, tnnt tenant.Tenant, store resource.Store, details *resource.BackupDetails) (*resource.BackupInfo, error) {
	id, err := s.idProvider.NewUUID()
	if err != nil {
		return nil, errors.InternalError(resource.EntityBackup, "unable to generate backup id", err)
	}
	details.ID = resource.BackupID(id)

	resources, err := s.resources.GetResources(ctx, tnnt, store, details.ResourceNames)
	if err != nil {
		return nil, err
	}

	ignored := findMissingResources(details.ResourceNames, resources)
	backupInfo, err := s.backupManager.Backup(ctx, store, details, resources)
	if err != nil {
		return nil, err
	}

	backupInfo.IgnoredResources = append(backupInfo.IgnoredResources, ignored...)
	err = s.repo.Save(ctx, details)
	if err != nil {
		return backupInfo, err
	}

	return backupInfo, nil
}

func (s BackupService) Get(ctx context.Context, _ tenant.Tenant, store resource.Store, backupID resource.BackupID) (*resource.BackupDetails, error) {
	if backupID.IsInvalid() {
		return nil, errors.InvalidArgument("backup", "the backup id is not valid")
	}
	return s.repo.GetByID(ctx, store, backupID)
}

func (s BackupService) List(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.BackupDetails, error) {
	backups, err := s.repo.GetAll(ctx, tnnt, store)
	if err != nil {
		return nil, err
	}

	var recentBackups []*resource.BackupDetails
	cutoffDate := time.Now().AddDate(0, recentBackupWindowMonths, 0)
	for _, backup := range backups {
		if backup.CreatedAt.After(cutoffDate) {
			recentBackups = append(recentBackups, backup)
		}
	}

	return recentBackups, nil
}

func findMissingResources(names []string, resources []*resource.Resource) []resource.IgnoredResource {
	if len(resources) == len(names) {
		return nil
	}

	var resourcesMap map[string]struct{}
	for _, r := range resources {
		resourcesMap[r.FullName()] = struct{}{}
	}

	var ignored []resource.IgnoredResource
	for _, name := range names {
		if _, ok := resourcesMap[name]; !ok {
			ignored = append(ignored, resource.IgnoredResource{
				Name:   name,
				Reason: "no record found in tenant",
			})
		}
	}
	return ignored
}
