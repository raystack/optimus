package mock

import (
	"context"

	"github.com/google/uuid"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/stretchr/testify/mock"
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
