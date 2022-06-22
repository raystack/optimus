package postgres

import (
	"context"

	"gorm.io/gorm"

	"github.com/odpf/optimus/models"
)

type interProjectJobSpecRepository struct {
	db      *gorm.DB
	adapter *JobSpecAdapter
}

func (repo interProjectJobSpecRepository) GetJobByName(ctx context.Context, jobName string) ([]models.JobSpec, error) {
	var jobs []Job
	var specs []models.JobSpec
	if err := repo.db.WithContext(ctx).Preload("Namespace").Preload("Project").Where("name = ?", jobName).Find(&jobs).Error; err != nil {
		return []models.JobSpec{}, err
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

func (repo interProjectJobSpecRepository) GetJobByResourceDestination(ctx context.Context, resourceDestination string) (models.JobSpec, error) {
	var job Job
	if err := repo.db.WithContext(ctx).Preload("Namespace").Preload("Project").Where("destination = ?", resourceDestination).First(&job).Error; err != nil {
		return models.JobSpec{}, err
	}
	jobSpec, err := repo.adapter.ToSpec(job)
	if err != nil {
		return models.JobSpec{}, err
	}
	return jobSpec, nil
}

func NewInterProjectJobSpecRepository(db *gorm.DB, adapter *JobSpecAdapter) *interProjectJobSpecRepository {
	return &interProjectJobSpecRepository{
		db:      db,
		adapter: adapter,
	}
}
