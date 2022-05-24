package mock

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/models"
)

type NamespaceRepository struct {
	mock.Mock
}

func (pr *NamespaceRepository) Save(ctx context.Context, project models.ProjectSpec, spec models.NamespaceSpec) error {
	return pr.Called(ctx, project, spec).Error(0)
}

func (pr *NamespaceRepository) GetByName(ctx context.Context, project models.ProjectSpec, name string) (models.NamespaceSpec, error) {
	args := pr.Called(ctx, project, name)
	return args.Get(0).(models.NamespaceSpec), args.Error(1)
}

func (pr *NamespaceRepository) GetAll(ctx context.Context, project models.ProjectSpec) ([]models.NamespaceSpec, error) {
	args := pr.Called(ctx, project)
	return args.Get(0).([]models.NamespaceSpec), args.Error(1)
}

func (pr *NamespaceRepository) Get(ctx context.Context, prjName, nsName string) (models.NamespaceSpec, error) {
	args := pr.Called(ctx, prjName, nsName)
	return args.Get(0).(models.NamespaceSpec), args.Error(1)
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

func (n *NamespaceService) GetByName(ctx context.Context, project models.ProjectSpec, name string) (models.NamespaceSpec, error) {
	args := n.Called(ctx, project, name)
	return args.Get(0).(models.NamespaceSpec), args.Error(1)
}
