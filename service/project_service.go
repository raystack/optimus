package service

import (
	"context"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

const ProjectEntity = "project"

type ProjectRepoFactory interface {
	New() store.ProjectRepository
}

type ProjectService interface {
	Get(context.Context, string) (models.ProjectSpec, error)
	Save(context.Context, models.ProjectSpec) error
	GetAll(context.Context) ([]models.ProjectSpec, error)
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
			NewError(ProjectEntity, ErrInvalidArgument, "project name cannot be empty")
	}

	projectRepo := s.projectRepoFac.New()
	projSpec, err := projectRepo.GetByName(ctx, projectName)
	if err != nil {
		return models.ProjectSpec{}, FromError(err, ProjectEntity, "")
	}
	return projSpec, nil
}

func (s projectService) Save(ctx context.Context, project models.ProjectSpec) error {
	if project.Name == "" {
		return NewError(ProjectEntity, ErrInvalidArgument, "project name cannot be empty")
	}

	if len(project.Config) == 0 {
		return NewError(ProjectEntity, ErrInvalidArgument, "project config cannot be empty")
	}

	projectRepo := s.projectRepoFac.New()
	err := projectRepo.Save(ctx, project)
	if err != nil {
		return FromError(err, ProjectEntity, "")
	}
	return nil
}

func (s projectService) GetAll(ctx context.Context) ([]models.ProjectSpec, error) {
	projectRepo := s.projectRepoFac.New()
	prjs, err := projectRepo.GetAll(ctx)
	if err != nil {
		return []models.ProjectSpec{}, FromError(err, ProjectEntity, "")
	}
	return prjs, nil
}
