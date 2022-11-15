package service

import (
	"context"
	"time"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

// recentBackupWindowMonths contains the window interval to consider for recent backups
const recentBackupWindowMonths = -3

type BackupRepository interface {
	GetByID(ctx context.Context, id resource.BackupID) (*resource.Backup, error)
	GetAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.Backup, error)
	Save(ctx context.Context, details *resource.Backup) error
}

type ResourceProvider interface {
	GetResources(ctx context.Context, tnnt tenant.Tenant, store resource.Store, names []string) ([]*resource.Resource, error)
}

type BackupManager interface {
	Backup(ctx context.Context, backup *resource.Backup, resources []*resource.Resource) (*resource.BackupResult, error)
}

type BackupService struct {
	repo BackupRepository

	resources     ResourceProvider
	backupManager BackupManager
}

func (s BackupService) Create(ctx context.Context, backup *resource.Backup) (*resource.BackupResult, error) {
	resources, err := s.resources.GetResources(ctx, backup.Tenant(), backup.Store(), backup.ResourceNames())
	if err != nil {
		return nil, err
	}
	ignored := findMissingResources(backup.ResourceNames(), resources)

	backupInfo, err := s.backupManager.Backup(ctx, backup, resources)
	if err != nil {
		return nil, err
	}

	backupInfo.IgnoredResources = append(backupInfo.IgnoredResources, ignored...)
	err = s.repo.Save(ctx, backup)
	if err != nil {
		return backupInfo, err
	}

	backupInfo.ID = backup.ID()
	return backupInfo, nil
}

func (s BackupService) Get(ctx context.Context, backupID resource.BackupID) (*resource.Backup, error) {
	if backupID.IsInvalid() {
		return nil, errors.InvalidArgument("backup", "the backup id is not valid")
	}
	return s.repo.GetByID(ctx, backupID)
}

func (s BackupService) List(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.Backup, error) {
	backups, err := s.repo.GetAll(ctx, tnnt, store)
	if err != nil {
		return nil, err
	}

	var recentBackups []*resource.Backup
	cutoffDate := time.Now().AddDate(0, recentBackupWindowMonths, 0)
	for _, backup := range backups {
		if backup.CreatedAt().After(cutoffDate) {
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
