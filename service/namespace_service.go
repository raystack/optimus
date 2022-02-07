package service

import (
	"context"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type NamespaceRepoFactory interface {
	New(spec models.ProjectSpec) store.NamespaceRepository
}

type NamespaceService interface {
	GetProjectAndNamespace(context.Context, string, string) (models.ProjectSpec, models.NamespaceSpec, error)
}

type namespaceService struct {
	projectService   ProjectService
	namespaceRepoFac NamespaceRepoFactory
}

func NewNamespaceService(projectService ProjectService, factory NamespaceRepoFactory) *namespaceService {
	return &namespaceService{
		projectService:   projectService,
		namespaceRepoFac: factory,
	}
}

// GetProjectAndNamespace This function is inefficient, it gets the project from repo along with secrets
// Then we only use the id from the project to fetch the Namespace, along with project and secrets
// Repository can provide a method to query both together.
func (s namespaceService) GetProjectAndNamespace(ctx context.Context, projectName string, namespaceName string) (models.ProjectSpec, models.NamespaceSpec, error) {
	if namespaceName == "" {
		return models.ProjectSpec{}, models.NamespaceSpec{},
			NewError("namespace", ErrInvalidArgument, "namespace name cannot be empty")
	}

	projectSpec, err := s.projectService.Get(ctx, projectName)
	if err != nil {
		return models.ProjectSpec{}, models.NamespaceSpec{}, err
	}

	nsRepo := s.namespaceRepoFac.New(projectSpec)
	nsSpec, err := nsRepo.GetByName(ctx, namespaceName)
	if err != nil {
		return models.ProjectSpec{}, models.NamespaceSpec{}, FromStoreError(err, "namespace", "")
	}

	return projectSpec, nsSpec, nil
}
