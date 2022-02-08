package mock

import (
	"context"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/stretchr/testify/mock"
)

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

func (pr *ProjectSecretRepository) Save(ctx context.Context, namespace models.NamespaceSpec, spec models.ProjectSecretItem) error {
	return pr.Called(ctx, namespace, spec).Error(0)
}

func (pr *ProjectSecretRepository) Update(ctx context.Context, namespace models.NamespaceSpec, spec models.ProjectSecretItem) error {
	return pr.Called(ctx, namespace, spec).Error(0)
}

func (pr *ProjectSecretRepository) GetByName(ctx context.Context, name string) (models.ProjectSecretItem, error) {
	args := pr.Called(ctx, name)
	return args.Get(0).(models.ProjectSecretItem), args.Error(1)
}

func (pr *ProjectSecretRepository) GetAll(ctx context.Context) ([]models.SecretItemInfo, error) {
	args := pr.Called(ctx)
	return args.Get(0).([]models.SecretItemInfo), args.Error(1)
}

func (pr *ProjectSecretRepository) GetSecrets(ctx context.Context) ([]models.ProjectSecretItem, error) {
	args := pr.Called(ctx)
	return args.Get(0).([]models.ProjectSecretItem), args.Error(1)
}

type SecretService struct {
	mock.Mock
}

func (s *SecretService) Save(ctx context.Context, prjName string, nsName string, item models.ProjectSecretItem) error {
	return s.Called(ctx, prjName, nsName, item).Error(0)
}

func (s *SecretService) Update(ctx context.Context, prjName string, nsName string, item models.ProjectSecretItem) error {
	return s.Called(ctx, prjName, nsName, item).Error(0)
}

func (s *SecretService) List(ctx context.Context, prjName string) ([]models.SecretItemInfo, error) {
	args := s.Called(ctx, prjName)
	return args.Get(0).([]models.SecretItemInfo), args.Error(1)
}

func (s *SecretService) GetSecrets(ctx context.Context, spec models.ProjectSpec) ([]models.ProjectSecretItem, error) {
	args := s.Called(ctx, spec)
	return args.Get(0).([]models.ProjectSecretItem), args.Error(1)
}
