package service

import (
	"context"
	"errors"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type SecretService interface {
	Save(context.Context, string, string, models.ProjectSecretItem) error
	Update(context.Context, string, string, models.ProjectSecretItem) error
	List(context.Context, string) ([]models.SecretItemInfo, error)
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
		return errors.New("secret name cannot be empty")
	}

	// TODO: Add new service method to get only project and namespace id for names
	projectSpec, namespaceSpec, err := s.nsService.GetProjectAndNamespace(ctx, projectName, namespaceName)
	if err != nil {
		return err
	}

	repo := s.secretRepoFac.New(projectSpec)
	return repo.Save(ctx, namespaceSpec, item)
}

func (s secretService) Update(ctx context.Context, projectName string, namespaceName string, item models.ProjectSecretItem) error {
	if item.Name == "" {
		return errors.New("secret name cannot be empty")
	}

	projectSpec, namespaceSpec, err := s.nsService.GetProjectAndNamespace(ctx, projectName, namespaceName)
	if err != nil {
		return err
	}

	repo := s.secretRepoFac.New(projectSpec)
	return repo.Update(ctx, namespaceSpec, item)
}

func (s secretService) List(ctx context.Context, projectName string) ([]models.SecretItemInfo, error) {
	projectSpec, err := s.projService.Get(ctx, projectName)
	if err != nil {
		return nil, err
	}

	repo := s.secretRepoFac.New(projectSpec)
	return repo.GetAll(ctx)
}
