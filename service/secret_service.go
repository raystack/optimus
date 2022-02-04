package service

import (
	"context"
	"errors"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type SecretService interface {
	Save(context.Context, string, string, models.ProjectSecretItem) error
}

type SecretRepoFactory interface {
	New(projectSpec models.ProjectSpec) store.ProjectSecretRepository
}

type secretService struct {
	nsService     NamespaceService
	secretRepoFac SecretRepoFactory
}

func NewSecretService(namespaceService NamespaceService, factory SecretRepoFactory) *secretService {
	return &secretService{
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
