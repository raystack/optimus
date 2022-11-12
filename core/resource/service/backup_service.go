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
	GetByID(ctx context.Context, store resource.Store, id resource.BackupID) (*resource.BackupDetails, error)
	GetAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.BackupDetails, error)
}

type ResourceProvider interface { // Will actually get the resources to back up
}

type BackupService struct {
	repo BackupRepository

	resourceProvider ResourceProvider
}

func (BackupService) Backup(ctx context.Context, tnnt tenant.Tenant, store resource.Store, config resource.BackupConfiguration) (resource.BackupInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (s BackupService) DryRun(ctx context.Context, tnnt tenant.Tenant, store resource.Store, config resource.BackupConfiguration) (resource.BackupInfo, error) {
	//TODO implement me
	panic("implement me")
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
