package service

import (
	"context"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type SecretService interface {
	Save(context.Context, string, string, models.ProjectSecretItem) error
	Update(context.Context, string, string, models.ProjectSecretItem) error
	List(context.Context, string) ([]models.SecretItemInfo, error)
	GetSecrets(ctx context.Context) ([]models.ProjectSecretItem, error)
}

type SecretRepoFactory interface {
	New(projectSpec models.ProjectSpec) store.ProjectSecretRepository
}

type secretService struct {
	projService   ProjectService
	nsService     NamespaceService
	secretRepoFac SecretRepoFactory
}

func NewSecretService(projectService ProjectService, namespaceService NamespaceService, factory SecretRepoFactory) *secretService {
	return &secretService{
		projService:   projectService,
		nsService:     namespaceService,
		secretRepoFac: factory,
	}
}

func (s secretService) Save(ctx context.Context, projectName string, namespaceName string, item models.ProjectSecretItem) error {
	if item.Name == "" {
		return NewError(models.SecretEntity, ErrInvalidArgument, "secret name cannot be empty")
	}

	// TODO: Add new service method to get only project and namespace id for names
	namespaceSpec, err := s.nsService.Get(ctx, projectName, namespaceName)
	if err != nil {
		return err
	}

	repo := s.secretRepoFac.New(namespaceSpec.ProjectSpec)
	err = repo.Save(ctx, namespaceSpec, item)
	if err != nil {
		return FromError(err, models.SecretEntity, "error while saving secret")
	}
	return nil
}

func (s secretService) Update(ctx context.Context, projectName string, namespaceName string, item models.ProjectSecretItem) error {
	if item.Name == "" {
		return NewError(models.SecretEntity, ErrInvalidArgument, "secret name cannot be empty")
	}

	namespaceSpec, err := s.nsService.Get(ctx, projectName, namespaceName)
	if err != nil {
		return err
	}

	repo := s.secretRepoFac.New(namespaceSpec.ProjectSpec)
	err = repo.Update(ctx, namespaceSpec, item)
	if err != nil {
		return FromError(err, models.SecretEntity, "error while updating secret")
	}
	return nil
}

func (s secretService) List(ctx context.Context, projectName string) ([]models.SecretItemInfo, error) {
	projectSpec, err := s.projService.Get(ctx, projectName)
	if err != nil {
		return nil, err
	}

	repo := s.secretRepoFac.New(projectSpec)
	secretItems, err := repo.GetAll(ctx)
	if err != nil {
		return []models.SecretItemInfo{}, FromError(err, models.SecretEntity, "error while saving secret")
	}
	return secretItems, nil
}

func (s secretService) GetSecrets(ctx context.Context, projectName string) ([]models.ProjectSecretItem, error) {
	projectSpec, err := s.projService.Get(ctx, projectName)
	if err != nil {
		return nil, err
	}

	repo := s.secretRepoFac.New(projectSpec)
	secretItems, err := repo.GetSecrets(ctx)
	if err != nil {
		return []models.ProjectSecretItem{}, FromStoreError(err, "secret", "error while getting secret")
	}
	return secretItems, nil
}
