package mock

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/models"
)

type ProjectSecretRepository struct {
	mock.Mock
}

func (pr *ProjectSecretRepository) Save(ctx context.Context, project models.ProjectSpec, namespace models.NamespaceSpec, spec models.ProjectSecretItem) error {
	return pr.Called(ctx, project, namespace, spec).Error(0)
}

func (pr *ProjectSecretRepository) Update(ctx context.Context, project models.ProjectSpec, namespace models.NamespaceSpec, spec models.ProjectSecretItem) error {
	return pr.Called(ctx, project, namespace, spec).Error(0)
}

func (pr *ProjectSecretRepository) GetByName(ctx context.Context, project models.ProjectSpec, name string) (models.ProjectSecretItem, error) {
	args := pr.Called(ctx, project, name)
	return args.Get(0).(models.ProjectSecretItem), args.Error(1)
}

func (pr *ProjectSecretRepository) GetAll(ctx context.Context, project models.ProjectSpec) ([]models.SecretItemInfo, error) {
	args := pr.Called(ctx, project)
	return args.Get(0).([]models.SecretItemInfo), args.Error(1)
}

func (pr *ProjectSecretRepository) GetSecrets(ctx context.Context, project models.ProjectSpec, namespace models.NamespaceSpec) ([]models.ProjectSecretItem, error) {
	args := pr.Called(ctx, project, namespace)
	return args.Get(0).([]models.ProjectSecretItem), args.Error(1)
}

func (pr *ProjectSecretRepository) Delete(ctx context.Context, project models.ProjectSpec, namespace models.NamespaceSpec, secretName string) error {
	return pr.Called(ctx, project, namespace, secretName).Error(0)
}

type SecretService struct {
	mock.Mock
}

func (s *SecretService) Save(ctx context.Context, prjName, nsName string, item models.ProjectSecretItem) error {
	return s.Called(ctx, prjName, nsName, item).Error(0)
}

func (s *SecretService) Update(ctx context.Context, prjName, nsName string, item models.ProjectSecretItem) error {
	return s.Called(ctx, prjName, nsName, item).Error(0)
}

func (s *SecretService) List(ctx context.Context, prjName string) ([]models.SecretItemInfo, error) {
	args := s.Called(ctx, prjName)
	return args.Get(0).([]models.SecretItemInfo), args.Error(1)
}

func (s *SecretService) GetSecrets(ctx context.Context, ns models.NamespaceSpec) ([]models.ProjectSecretItem, error) {
	args := s.Called(ctx, ns)
	return args.Get(0).([]models.ProjectSecretItem), args.Error(1)
}

func (s *SecretService) Delete(ctx context.Context, prjName, namespaceName, secretName string) error {
	return s.Called(ctx, prjName, namespaceName, secretName).Error(0)
}
