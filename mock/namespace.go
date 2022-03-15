package mock

import (
	"context"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/stretchr/testify/mock"
)

type NamespaceRepository struct {
	mock.Mock
}

func (pr *NamespaceRepository) Save(ctx context.Context, spec models.NamespaceSpec) error {
	return pr.Called(ctx, spec).Error(0)
}

func (pr *NamespaceRepository) GetByName(ctx context.Context, name string) (models.NamespaceSpec, error) {
	args := pr.Called(ctx, name)
	return args.Get(0).(models.NamespaceSpec), args.Error(1)
}

func (pr *NamespaceRepository) GetAll(ctx context.Context) ([]models.NamespaceSpec, error) {
	args := pr.Called(ctx)
	return args.Get(0).([]models.NamespaceSpec), args.Error(1)
}

func (pr *NamespaceRepository) Get(ctx context.Context, prjName, nsName string) (models.NamespaceSpec, error) {
	args := pr.Called(ctx, prjName, nsName)
	return args.Get(0).(models.NamespaceSpec), args.Error(1)
}

type NamespaceRepoFactory struct {
	mock.Mock
}

func (fac *NamespaceRepoFactory) New(proj models.ProjectSpec) store.NamespaceRepository {
	args := fac.Called(proj)
	return args.Get(0).(store.NamespaceRepository)
}

type NamespaceService struct {
	mock.Mock
}

func (n *NamespaceService) Get(ctx context.Context, projectName, namespaceName string) (models.NamespaceSpec, error) {
	args := n.Called(ctx, projectName, namespaceName)
	return args.Get(0).(models.NamespaceSpec), args.Error(1)
}

func (n *NamespaceService) GetNamespaceOptionally(ctx context.Context, projectName, namespaceName string) (models.ProjectSpec, models.NamespaceSpec, error) {
	args := n.Called(ctx, projectName, namespaceName)
	return args.Get(0).(models.ProjectSpec), args.Get(1).(models.NamespaceSpec), args.Error(2)
}

func (n *NamespaceService) Save(ctx context.Context, prjName string, namespace models.NamespaceSpec) error {
	args := n.Called(ctx, prjName, namespace)
	return args.Error(0)
}

func (n *NamespaceService) GetAll(ctx context.Context, prjName string) ([]models.NamespaceSpec, error) {
	args := n.Called(ctx, prjName)
	return args.Get(0).([]models.NamespaceSpec), args.Error(1)
}
