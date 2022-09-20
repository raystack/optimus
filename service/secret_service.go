package service

import (
	"context"

	"github.com/odpf/optimus/internal/store"
	"github.com/odpf/optimus/models"
)

type SecretService interface {
	Save(context.Context, string, string, models.ProjectSecretItem) error
	Update(context.Context, string, string, models.ProjectSecretItem) error
	List(context.Context, string) ([]models.SecretItemInfo, error)
	GetSecrets(context.Context, models.NamespaceSpec) ([]models.ProjectSecretItem, error)
	Delete(context.Context, string, string, string) error
}

type secretService struct {
	projService ProjectService
	nsService   NamespaceService
	repo        store.SecretRepository
}

func NewSecretService(projectService ProjectService, namespaceService NamespaceService, repo store.SecretRepository) *secretService {
	return &secretService{
		projService: projectService,
		nsService:   namespaceService,
		repo:        repo,
	}
}

func (s secretService) Save(ctx context.Context, projectName, namespaceName string, item models.ProjectSecretItem) error {
	if item.Name == "" {
		return NewError(models.SecretEntity, ErrInvalidArgument, "secret name cannot be empty")
	}

	proj, namespace, err := s.nsService.GetNamespaceOptionally(ctx, projectName, namespaceName)
	if err != nil {
		return err
	}

	err = s.repo.Save(ctx, proj, namespace, item)
	if err != nil {
		return FromError(err, models.SecretEntity, "error while saving secret")
	}
	return nil
}

func (s secretService) Update(ctx context.Context, projectName, namespaceName string, item models.ProjectSecretItem) error {
	if item.Name == "" {
		return NewError(models.SecretEntity, ErrInvalidArgument, "secret name cannot be empty")
	}

	proj, namespace, err := s.nsService.GetNamespaceOptionally(ctx, projectName, namespaceName)
	if err != nil {
		return err
	}

	err = s.repo.Update(ctx, proj, namespace, item)
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

	secretItems, err := s.repo.GetAll(ctx, projectSpec)
	if err != nil {
		return []models.SecretItemInfo{}, FromError(err, models.SecretEntity, "error while saving secret")
	}
	return secretItems, nil
}

func (s secretService) GetSecrets(ctx context.Context, namespace models.NamespaceSpec) ([]models.ProjectSecretItem, error) {
	secretItems, err := s.repo.GetSecrets(ctx, namespace.ProjectSpec, namespace)
	if err != nil {
		return []models.ProjectSecretItem{}, FromError(err, models.SecretEntity, "error while getting secrets")
	}
	return secretItems, nil
}

func (s secretService) Delete(ctx context.Context, projectName, namespaceName, secretName string) error {
	proj, namespace, err := s.nsService.GetNamespaceOptionally(ctx, projectName, namespaceName)
	if err != nil {
		return err
	}

	err = s.repo.Delete(ctx, proj, namespace, secretName)
	if err != nil {
		return FromError(err, models.SecretEntity, "error while deleting secret")
	}
	return nil
}
