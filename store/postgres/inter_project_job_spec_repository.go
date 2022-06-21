package postgres

import (
	"context"
	"errors"

	"gorm.io/gorm"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type interProjectJobSpecRepository struct {
	db      *gorm.DB
	adapter *JobSpecAdapter
}

func (repo interProjectJobSpecRepository) GetWithFilters(ctx context.Context, projectName, jobName, resourceDestination string) ([]models.JobSpec, error) {
	var project Project
	var jobs []Job
	var specs []models.JobSpec
	query := repo.db.WithContext(ctx).Preload("Namespace").Preload("Project")
	if projectName != "" {
		if err := repo.db.WithContext(ctx).Where("name = ?", projectName).First(&project).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return []models.JobSpec{}, store.ErrResourceNotFound
			}
			return []models.JobSpec{}, err
		}
		query = query.Where("project_id = ?", project.ID)
	}
	if jobName != "" {
		query = query.Where("name = ?", jobName)
	}
	if resourceDestination != "" {
		query = query.Where("destination = ?", resourceDestination)
	}
	if err := query.Find(&jobs).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return []models.JobSpec{}, store.ErrResourceNotFound
		}
		return []models.JobSpec{}, err
	}

	if len(jobs) == 0 {
		return []models.JobSpec{}, store.ErrResourceNotFound
	}

	for _, job := range jobs {
		adapt, err := repo.adapter.ToSpec(job)
		if err != nil {
			return specs, err
		}
		specs = append(specs, adapt)
	}
	return specs, nil
}

func NewInterProjectJobSpecRepository(db *gorm.DB, adapter *JobSpecAdapter) *interProjectJobSpecRepository {
	return &interProjectJobSpecRepository{
		db:      db,
		adapter: adapter,
	}
}
