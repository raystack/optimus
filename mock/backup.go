package mock

import (
	"context"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/models"
)

type BackupRepo struct {
	mock.Mock
}

func (repo *BackupRepo) Save(ctx context.Context, spec models.BackupSpec) error {
	return repo.Called(ctx, spec).Error(0)
}

func (repo *BackupRepo) GetAll(ctx context.Context, project models.ProjectSpec, ds models.Datastorer) ([]models.BackupSpec, error) {
	args := repo.Called(ctx, project, ds)
	return args.Get(0).([]models.BackupSpec), args.Error(1)
}

func (repo *BackupRepo) GetByID(ctx context.Context, id uuid.UUID, ds models.Datastorer) (models.BackupSpec, error) {
	args := repo.Called(ctx, id, ds)
	return args.Get(0).(models.BackupSpec), args.Error(1)
}
