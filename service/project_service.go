package service

import (
	"context"

	"github.com/odpf/optimus/internal/store"
	"github.com/odpf/optimus/models"
)

type ProjectService interface {
	Get(context.Context, string) (models.ProjectSpec, error)
	Save(context.Context, models.ProjectSpec) error
	GetAll(context.Context) ([]models.ProjectSpec, error)
}

type projectService struct {
	projectRepo store.ProjectRepository
}

func NewProjectService(projectRepo store.ProjectRepository) ProjectService {
	return &projectService{
		projectRepo: projectRepo,
	}
}

func (s projectService) Get(ctx context.Context, projectName string) (models.ProjectSpec, error) {
	if projectName == "" {
		return models.ProjectSpec{},
			NewError(models.ProjectEntity, ErrInvalidArgument, "project name cannot be empty")
	}

	projSpec, err := s.projectRepo.GetByName(ctx, projectName)
	if err != nil {
		return models.ProjectSpec{}, FromError(err, models.ProjectEntity, "")
	}
	return projSpec, nil
}

func (s projectService) Save(ctx context.Context, project models.ProjectSpec) error {
	if project.Name == "" {
		return NewError(models.ProjectEntity, ErrInvalidArgument, "project name cannot be empty")
	}

	err := s.projectRepo.Save(ctx, project)
	if err != nil {
		return FromError(err, models.ProjectEntity, "")
	}
	return nil
}

func (s projectService) GetAll(ctx context.Context) ([]models.ProjectSpec, error) {
	prjs, err := s.projectRepo.GetAll(ctx)
	if err != nil {
		return []models.ProjectSpec{}, FromError(err, models.ProjectEntity, "")
	}
	return prjs, nil
}
