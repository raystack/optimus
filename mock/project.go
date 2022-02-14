package mock

import (
	"context"

	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/stretchr/testify/mock"
)

type ProjectRepository struct {
	mock.Mock
}

func (pr *ProjectRepository) Save(ctx context.Context, spec models.ProjectSpec) error {
	return pr.Called(ctx, spec).Error(0)
}

func (pr *ProjectRepository) GetByName(ctx context.Context, name string) (models.ProjectSpec, error) {
	args := pr.Called(ctx, name)
	return args.Get(0).(models.ProjectSpec), args.Error(1)
}

func (pr *ProjectRepository) GetAll(ctx context.Context) ([]models.ProjectSpec, error) {
	args := pr.Called(ctx)
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

type ProjectService struct {
	mock.Mock
}

func (pr *ProjectService) Get(ctx context.Context, name string) (models.ProjectSpec, error) {
	args := pr.Called(ctx, name)
	return args.Get(0).(models.ProjectSpec), args.Error(1)
}
