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
	GetByName(context.Context, models.ProjectSpec, string) (models.NamespaceSpec, error)
	GetNamespaceOptionally(context.Context, string, string) (models.ProjectSpec, models.NamespaceSpec, error)
	Save(context.Context, string, models.NamespaceSpec) error
	GetAll(context.Context, string) ([]models.NamespaceSpec, error)
}

type namespaceService struct {
	projectService ProjectService
	namespaceRepo  store.NamespaceRepository
}

func NewNamespaceService(projectService ProjectService, nsRepo store.NamespaceRepository) NamespaceService {
	return &namespaceService{
		projectService: projectService,
		namespaceRepo:  nsRepo,
	}
}

func (s namespaceService) Get(ctx context.Context, projectName, namespaceName string) (models.NamespaceSpec, error) {
	if projectName == "" {
		return models.NamespaceSpec{},
			NewError(models.ProjectEntity, ErrInvalidArgument, "project name cannot be empty")
	}
	if namespaceName == "" {
		return models.NamespaceSpec{},
			NewError(models.NamespaceEntity, ErrInvalidArgument, "namespace name cannot be empty")
	}

	nsSpec, err := s.namespaceRepo.Get(ctx, projectName, namespaceName)
	if err != nil {
		return models.NamespaceSpec{}, FromError(err, models.NamespaceEntity, "")
	}

	return nsSpec, nil
}

func (s namespaceService) GetByName(ctx context.Context, project models.ProjectSpec, namespaceName string) (models.NamespaceSpec, error) {
	if namespaceName == "" {
		return models.NamespaceSpec{},
			NewError(models.NamespaceEntity, ErrInvalidArgument, "namespace name cannot be empty")
	}

	nsSpec, err := s.namespaceRepo.GetByName(ctx, project, namespaceName)
	if err != nil {
		return models.NamespaceSpec{}, FromError(err, models.NamespaceEntity, "")
	}

	return nsSpec, nil
}

// GetNamespaceOptionally is used for optionally getting namespace if name is present, otherwise get only project
func (s namespaceService) GetNamespaceOptionally(ctx context.Context, projectName, namespaceName string) (models.ProjectSpec, models.NamespaceSpec, error) {
	projectSpec, err := s.projectService.Get(ctx, projectName)
	if err != nil {
		return models.ProjectSpec{}, models.NamespaceSpec{}, err
	}

	if namespaceName == "" {
		return projectSpec, models.NamespaceSpec{}, nil
	}

	nsSpec, err := s.namespaceRepo.GetByName(ctx, projectSpec, namespaceName)
	if err != nil {
		return models.ProjectSpec{}, models.NamespaceSpec{}, FromError(err, models.NamespaceEntity, "")
	}

	return projectSpec, nsSpec, nil
}

func (s namespaceService) Save(ctx context.Context, projName string, namespace models.NamespaceSpec) error {
	if namespace.Name == "" {
		return NewError(models.NamespaceEntity, ErrInvalidArgument, "namespace name cannot be empty")
	}

	projectSpec, err := s.projectService.Get(ctx, projName)
	if err != nil {
		return err
	}

	err = s.namespaceRepo.Save(ctx, projectSpec, namespace)
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

	namespaces, err := s.namespaceRepo.GetAll(ctx, projectSpec)
	if err != nil {
		return []models.NamespaceSpec{}, FromError(err, models.NamespaceEntity, "")
	}
	return namespaces, nil
}
