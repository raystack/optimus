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

type ProjectSecretRepoFactory struct {
	mock.Mock
}

func (fac *ProjectSecretRepoFactory) New(p models.ProjectSpec, n models.NamespaceSpec) store.ProjectSecretRepository {
	args := fac.Called(p, n)
	return args.Get(0).(store.ProjectSecretRepository)
}

type ProjectSecretRepository struct {
	mock.Mock
}

func (pr *ProjectSecretRepository) Save(ctx context.Context, spec models.ProjectSecretItem) error {
	return pr.Called(ctx, spec).Error(0)
}

func (pr *ProjectSecretRepository) Update(ctx context.Context, spec models.ProjectSecretItem) error {
	return pr.Called(ctx, spec).Error(0)
}

func (pr *ProjectSecretRepository) GetByName(ctx context.Context, name string) (models.ProjectSecretItem, error) {
	args := pr.Called(ctx, name)
	return args.Get(0).(models.ProjectSecretItem), args.Error(1)
}

func (pr *ProjectSecretRepository) GetAll(ctx context.Context) ([]models.ProjectSecretItem, error) {
	args := pr.Called(ctx)
	return args.Get(0).([]models.ProjectSecretItem), args.Error(1)
}

type PipelineLogObserver struct {
	mock.Mock
}

func (obs *PipelineLogObserver) Notify(evt progress.Event) {
	obs.Called(evt)
}
