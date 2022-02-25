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
	Delete(context.Context, string, string) error
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

	var projectSpec models.ProjectSpec
	namespaceSpec := models.NamespaceSpec{}
	var err error
	if namespaceName == "" { // Namespace is optional for secrets
		if projectSpec, err = s.projService.Get(ctx, projectName); err != nil {
			return err
		}
	} else {
		namespaceSpec, err = s.nsService.Get(ctx, projectName, namespaceName)
		if err != nil {
			return err
		}
		projectSpec = namespaceSpec.ProjectSpec
	}

	repo := s.secretRepoFac.New(projectSpec)
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

	var projectSpec models.ProjectSpec
	namespaceSpec := models.NamespaceSpec{}
	var err error
	if namespaceName == "" { // Namespace is optional for secrets
		if projectSpec, err = s.projService.Get(ctx, projectName); err != nil {
			return err
		}
	} else {
		namespaceSpec, err = s.nsService.Get(ctx, projectName, namespaceName)
		if err != nil {
			return err
		}
		projectSpec = namespaceSpec.ProjectSpec
	}

	repo := s.secretRepoFac.New(projectSpec)
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

func (s secretService) Delete(ctx context.Context, prjName string, secretName string) error {
	projectSpec, err := s.projService.Get(ctx, prjName)
	if err != nil {
		return err
	}

	repo := s.secretRepoFac.New(projectSpec)
	err = repo.Delete(ctx, secretName)
	if err != nil {
		return FromError(err, models.SecretEntity, "error while deleting secret")
	}
	return nil
}
