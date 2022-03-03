package postgres

import (
	"context"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"gorm.io/gorm"
)

type jobDependencyRepository struct {
	db      *gorm.DB
	project models.ProjectSpec
}

func (repo *jobDependencyRepository) Save(ctx context.Context, jobDependency store.JobDependency) error {
	return repo.db.WithContext(ctx).Create(&jobDependency).Error
}

func (repo *jobDependencyRepository) GetAll(ctx context.Context) ([]store.JobDependency, error) {
	var jobDependencies []store.JobDependency
	if err := repo.db.WithContext(ctx).Where("project_id = ?", repo.project.ID).Find(&jobDependencies).Error; err != nil {
		return jobDependencies, err
	}

	return jobDependencies, nil
}

func NewJobDependencyRepository(db *gorm.DB, projectSpec models.ProjectSpec) *jobDependencyRepository {
	return &jobDependencyRepository{
		db:      db,
		project: projectSpec,
	}
}
