package service

import (
	"context"
	"strings"
	"time"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/telemetry"
)

const (
	// recentBackupWindowMonths contains the window interval to consider for recent backups
	recentBackupWindowMonths = -3

	metricBackupRequest        = "resource_backup_requests_total"
	backupRequestStatusSuccess = "success"
	backupRequestStatusFailed  = "failed"
)

type BackupRepository interface {
	GetByID(ctx context.Context, id resource.BackupID) (*resource.Backup, error)
	GetAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.Backup, error)
	Create(ctx context.Context, backup *resource.Backup) error
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

	logger log.Logger
}

func (s BackupService) Create(ctx context.Context, backup *resource.Backup) (*resource.BackupResult, error) {
	resources, err := s.resources.GetResources(ctx, backup.Tenant(), backup.Store(), backup.ResourceNames())
	if err != nil {
		s.logger.Error("error getting resources [%s] from db: %s", strings.Join(backup.ResourceNames(), ", "), err)
		return nil, err
	}
	ignored := findMissingResources(backup.ResourceNames(), resources)

	backupInfo, err := s.backupManager.Backup(ctx, backup, resources)
	if err != nil {
		s.logger.Error("error backup up through manager: %s", err)
		return nil, err
	}

	backupInfo.IgnoredResources = append(backupInfo.IgnoredResources, ignored...)
	err = s.repo.Create(ctx, backup)
	if err != nil {
		s.logger.Error("error creating backup record to db: %s", err)
		return backupInfo, err
	}

	raiseBackupRequestMetrics(backup.Tenant(), backupInfo)

	backupInfo.ID = backup.ID()
	return backupInfo, nil
}

func (s BackupService) Get(ctx context.Context, backupID resource.BackupID) (*resource.Backup, error) {
	if backupID.IsInvalid() {
		s.logger.Error("backup id [%s] is invalid", backupID.String())
		return nil, errors.InvalidArgument("backup", "the backup id is not valid")
	}
	return s.repo.GetByID(ctx, backupID)
}

func (s BackupService) List(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.Backup, error) {
	backups, err := s.repo.GetAll(ctx, tnnt, store)
	if err != nil {
		s.logger.Error("error getting all backups from db: %s", err)
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

	resourcesMap := map[string]struct{}{}
	for _, r := range resources {
		resourcesMap[r.FullName()] = struct{}{}
	}

	var ignored []resource.IgnoredResource
	for _, name := range names {
		if _, ok := resourcesMap[name]; !ok {
			ignored = append(ignored, resource.IgnoredResource{
				Name:   name,
				Reason: "no resource found in namespace",
			})
		}
	}
	return ignored
}

func NewBackupService(repo BackupRepository, resources ResourceProvider, manager BackupManager, logger log.Logger) *BackupService {
	return &BackupService{
		repo:          repo,
		resources:     resources,
		backupManager: manager,
		logger:        logger,
	}
}

func raiseBackupRequestMetrics(jobTenant tenant.Tenant, backupResult *resource.BackupResult) {
	for _, ignoredResource := range backupResult.IgnoredResources {
		raiseBackupRequestMetric(jobTenant, ignoredResource.Name, backupRequestStatusFailed)
	}
	for _, resourceName := range backupResult.ResourceNames {
		raiseBackupRequestMetric(jobTenant, resourceName, backupRequestStatusSuccess)
	}
}

func raiseBackupRequestMetric(jobTenant tenant.Tenant, resourceName, state string) {
	telemetry.NewCounter(metricBackupRequest, map[string]string{
		"project":   jobTenant.ProjectName().String(),
		"namespace": jobTenant.NamespaceName().String(),
		"resource":  resourceName,
		"status":    state,
	}).Inc()
}
