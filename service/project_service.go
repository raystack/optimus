package service

import (
	"context"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type ProjectRepoFactory interface {
	New() store.ProjectRepository
}

type ProjectService interface {
	Get(context.Context, string) (models.ProjectSpec, error)
}

type projectService struct {
	projectRepoFac ProjectRepoFactory
}

func NewProjectService(factory ProjectRepoFactory) *projectService {
	return &projectService{
		projectRepoFac: factory,
	}
}

func (s projectService) Get(ctx context.Context, projectName string) (models.ProjectSpec, error) {
	if projectName == "" {
		return models.ProjectSpec{},
			NewError("project", ErrInvalidArgument, "project name cannot be empty")
	}

	projectRepo := s.projectRepoFac.New()
	projSpec, err := projectRepo.GetByName(ctx, projectName)
	if err != nil {
		return models.ProjectSpec{}, FromStoreError(err, "project", "")
	}
	return projSpec, nil
}
