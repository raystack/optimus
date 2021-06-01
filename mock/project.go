package mock

import (
	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/stretchr/testify/mock"
)

type ProjectRepository struct {
	mock.Mock
}

func (pr *ProjectRepository) Save(spec models.ProjectSpec) error {
	return pr.Called(spec).Error(0)
}

func (pr *ProjectRepository) GetByName(name string) (models.ProjectSpec, error) {
	args := pr.Called(name)
	return args.Get(0).(models.ProjectSpec), args.Error(1)
}

func (pr *ProjectRepository) GetAll() ([]models.ProjectSpec, error) {
	args := pr.Called()
	return args.Get(0).([]models.ProjectSpec), args.Error(1)
}

type ProjectRepoFactory struct {
	mock.Mock
}

func (fac *ProjectRepoFactory) New() store.ProjectRepository {
	args := fac.Called()
	return args.Get(0).(store.ProjectRepository)
}

type ProjectSecretRepoFactory struct {
	mock.Mock
}

func (fac *ProjectSecretRepoFactory) New(p models.ProjectSpec) store.ProjectSecretRepository {
	args := fac.Called(p)
	return args.Get(0).(store.ProjectSecretRepository)
}

type ProjectSecretRepository struct {
	mock.Mock
}

func (pr *ProjectSecretRepository) Save(spec models.ProjectSecretItem) error {
	return pr.Called(spec).Error(0)
}

func (pr *ProjectSecretRepository) GetByName(name string) (models.ProjectSecretItem, error) {
	args := pr.Called(name)
	return args.Get(0).(models.ProjectSecretItem), args.Error(1)
}

func (pr *ProjectSecretRepository) GetAll() ([]models.ProjectSecretItem, error) {
	args := pr.Called()
	return args.Get(0).([]models.ProjectSecretItem), args.Error(1)
}

type PipelineLogObserver struct {
	mock.Mock
}

func (obs *PipelineLogObserver) Notify(evt progress.Event) {
	obs.Called(evt)
}
