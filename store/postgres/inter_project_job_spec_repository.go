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

func (repo interProjectJobSpecRepository) GetJobByName(ctx context.Context, jobName string) (models.JobSpec, error) {
	var r Job
	if err := repo.db.WithContext(ctx).Preload("Namespace").Preload("Project").Where("name = ?", jobName).First(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.JobSpec{}, store.ErrResourceNotFound
		}
		return models.JobSpec{}, err
	}
	jobSpec, err := repo.adapter.ToSpec(r)
	if err != nil {
		return models.JobSpec{}, err
	}
	return jobSpec, nil
}

func (repo interProjectJobSpecRepository) GetJobByResourceDestination(ctx context.Context, resourceDestination string) (models.JobSpec, error) {
	var r Job
	if err := repo.db.WithContext(ctx).Preload("Namespace").Preload("Project").Where("destination = ?", resourceDestination).First(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.JobSpec{}, store.ErrResourceNotFound
		}
		return models.JobSpec{}, err
	}
	jobSpec, err := repo.adapter.ToSpec(r)
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
