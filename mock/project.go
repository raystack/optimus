package mock

import (
	"github.com/stretchr/testify/mock"
	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
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

type PipelineLogObserver struct {
	mock.Mock
}

func (obs *PipelineLogObserver) Notify(evt progress.Event) {
	obs.Called(evt)
}
