package service

import (
	"context"

	"github.com/raystack/optimus/core/tenant"
)

type ProjectService struct {
	projectRepo ProjectRepository
}

func NewProjectService(projectRepo ProjectRepository) *ProjectService {
	return &ProjectService{
		projectRepo: projectRepo,
	}
}

type ProjectRepository interface {
	Save(context.Context, *tenant.Project) error
	GetByName(context.Context, tenant.ProjectName) (*tenant.Project, error)
	GetAll(context.Context) ([]*tenant.Project, error)
}

func (s ProjectService) Save(ctx context.Context, project *tenant.Project) error {
	return s.projectRepo.Save(ctx, project)
}

func (s ProjectService) Get(ctx context.Context, name tenant.ProjectName) (*tenant.Project, error) {
	return s.projectRepo.GetByName(ctx, name)
}

func (s ProjectService) GetAll(ctx context.Context) ([]*tenant.Project, error) {
	return s.projectRepo.GetAll(ctx)
}
