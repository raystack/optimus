package service

import (
	"context"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
)

type BackupRepository interface {
}

type BackupService struct {
	repo BackupRepository
}

func (BackupService) Backup(ctx context.Context, tnnt tenant.Tenant, store resource.Store, config resource.BackupConfiguration) (resource.BackupInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (s BackupService) DryRun(ctx context.Context, tnnt tenant.Tenant, store resource.Store, config resource.BackupConfiguration) (resource.BackupInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (s BackupService) Get(ctx context.Context, tnnt tenant.Tenant, store resource.Store, backupID resource.BackupID) (interface{}, error) {
	//TODO implement me
	panic("implement me")
}

func (s BackupService) List(ctx context.Context, tnnt tenant.Tenant, store resource.Store) (interface{}, error) {
	//TODO implement me
	panic("implement me")
}
