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
	Get(context.Context, string, string) (models.NamespaceSpec, error)
	Save(context.Context, string, models.NamespaceSpec) error
	GetAll(context.Context, string) ([]models.NamespaceSpec, error)
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

// Get This function is inefficient, it gets the project from repo along with secrets
// Then we only use the id from the project to fetch the Namespace, along with project and secrets
// Repository can provide a method to query both together.
func (s namespaceService) Get(ctx context.Context, projectName string, namespaceName string) (models.NamespaceSpec, error) {
	if namespaceName == "" {
		return models.NamespaceSpec{},
			NewError(models.NamespaceEntity, ErrInvalidArgument, "namespace name cannot be empty")
	}

	projectSpec, err := s.projectService.Get(ctx, projectName)
	if err != nil {
		return models.NamespaceSpec{}, err
	}

	nsRepo := s.namespaceRepoFac.New(projectSpec)
	nsSpec, err := nsRepo.GetByName(ctx, namespaceName)
	if err != nil {
		return models.NamespaceSpec{}, FromError(err, models.NamespaceEntity, "")
	}

	return nsSpec, nil
}

func (s namespaceService) Save(ctx context.Context, projName string, namespace models.NamespaceSpec) error {
	if namespace.Name == "" {
		return NewError(models.NamespaceEntity, ErrInvalidArgument, "namespace name cannot be empty")
	}

	if len(namespace.Config) == 0 {
		return NewError(models.NamespaceEntity, ErrInvalidArgument, "namespace config cannot be empty")
	}

	projectSpec, err := s.projectService.Get(ctx, projName)
	if err != nil {
		return err
	}

	nsRepo := s.namespaceRepoFac.New(projectSpec)
	err = nsRepo.Save(ctx, namespace)
	if err != nil {
		return FromError(err, models.NamespaceEntity, "")
	}
	return nil
}

func (s namespaceService) GetAll(ctx context.Context, projName string) ([]models.NamespaceSpec, error) {
	projectSpec, err := s.projectService.Get(ctx, projName)
	if err != nil {
		return []models.NamespaceSpec{}, err
	}

	namespaceRepo := s.namespaceRepoFac.New(projectSpec)
	namespaces, err := namespaceRepo.GetAll(ctx)
	if err != nil {
		return []models.NamespaceSpec{}, FromError(err, models.NamespaceEntity, "")
	}
	return namespaces, nil
}
