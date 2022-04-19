package mock

import (
	"context"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type BackupRepo struct {
	mock.Mock
}

func (repo *BackupRepo) Save(ctx context.Context, spec models.BackupSpec) error {
	return repo.Called(ctx, spec).Error(0)
}

func (repo *BackupRepo) GetAll(ctx context.Context) ([]models.BackupSpec, error) {
	args := repo.Called(ctx)
	return args.Get(0).([]models.BackupSpec), args.Error(1)
}

func (repo *BackupRepo) GetByID(ctx context.Context, id uuid.UUID) (models.BackupSpec, error) {
	args := repo.Called(ctx, id)
	return args.Get(0).(models.BackupSpec), args.Error(1)
}

type BackupRepoFactory struct {
	mock.Mock
}

func (fac *BackupRepoFactory) New(projectSpec models.ProjectSpec, storer models.Datastorer) store.BackupRepository {
	return fac.Called(projectSpec, storer).Get(0).(store.BackupRepository)
}

type BackupService struct {
	mock.Mock
}

func (d *BackupService) BackupResourceDryRun(ctx context.Context, req models.BackupRequest, jobSpecs []models.JobSpec) (models.BackupPlan, error) {
	args := d.Called(ctx, req, jobSpecs)
	return args.Get(0).(models.BackupPlan), args.Error(1)
}

func (d *BackupService) BackupResource(ctx context.Context, req models.BackupRequest, jobSpecs []models.JobSpec) (models.BackupResult, error) {
	args := d.Called(ctx, req, jobSpecs)
	return args.Get(0).(models.BackupResult), args.Error(1)
}

func (d *BackupService) ListResourceBackups(ctx context.Context, projectSpec models.ProjectSpec, datastoreName string) ([]models.BackupSpec, error) {
	args := d.Called(ctx, projectSpec, datastoreName)
	return args.Get(0).([]models.BackupSpec), args.Error(1)
}

func (d *BackupService) GetResourceBackup(ctx context.Context, projectSpec models.ProjectSpec, datastoreName string, id uuid.UUID) (models.BackupSpec, error) {
	args := d.Called(ctx, projectSpec, datastoreName, id)
	return args.Get(0).(models.BackupSpec), args.Error(1)
}
