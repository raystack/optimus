package mock

import (
	"context"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/internal/lib/progress"
	"github.com/odpf/optimus/models"
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

func (pr *ProjectRepository) GetByID(ctx context.Context, projectID uuid.UUID) (models.ProjectSpec, error) {
	args := pr.Called(ctx, projectID)
	return args.Get(0).(models.ProjectSpec), args.Error(1)
}

func (pr *ProjectRepository) GetAll(ctx context.Context) ([]models.ProjectSpec, error) {
	args := pr.Called(ctx)
	return args.Get(0).([]models.ProjectSpec), args.Error(1)
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

func (pr *ProjectService) Save(ctx context.Context, project models.ProjectSpec) error {
	args := pr.Called(ctx, project)
	return args.Error(0)
}

func (pr *ProjectService) GetAll(ctx context.Context) ([]models.ProjectSpec, error) {
	args := pr.Called(ctx)
	return args.Get(0).([]models.ProjectSpec), args.Error(1)
}
