package mock

import (
	"github.com/stretchr/testify/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type NamespaceRepository struct {
	mock.Mock
}

func (pr *NamespaceRepository) Save(spec models.NamespaceSpec) error {
	return pr.Called(spec).Error(0)
}

func (pr *NamespaceRepository) GetByName(name string) (models.NamespaceSpec, error) {
	args := pr.Called(name)
	return args.Get(0).(models.NamespaceSpec), args.Error(1)
}

func (pr *NamespaceRepository) GetAll() ([]models.NamespaceSpec, error) {
	args := pr.Called()
	return args.Get(0).([]models.NamespaceSpec), args.Error(1)
}

type NamespaceRepoFactory struct {
	mock.Mock
}

func (fac *NamespaceRepoFactory) New(proj models.ProjectSpec) store.NamespaceRepository {
	args := fac.Called(proj)
	return args.Get(0).(store.NamespaceRepository)
}
