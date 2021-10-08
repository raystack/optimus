package mock

import (
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/stretchr/testify/mock"
)

type BackupRepo struct {
	mock.Mock
}

func (repo *BackupRepo) Save(spec models.BackupSpec) error {
	return repo.Called(spec).Error(0)
}

func (repo *BackupRepo) GetAll() ([]models.BackupSpec, error) {
	args := repo.Called()
	return args.Get(0).([]models.BackupSpec), args.Error(1)
}

type BackupRepoFactory struct {
	mock.Mock
}

func (fac *BackupRepoFactory) New(projectSpec models.ProjectSpec, storer models.Datastorer) store.BackupRepository {
	return fac.Called(projectSpec, storer).Get(0).(store.BackupRepository)
}
