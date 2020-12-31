package store

import (
	"github.com/odpf/optimus/models"
)

// JobSpecRepository represents a storage interface for Job specifications
type JobSpecRepository interface {
	Save(models.JobSpec) error
	GetByName(string) (models.JobSpec, error)
	GetAll() ([]models.JobSpec, error)
}

type TaskSpecRepository interface {
	Save(models.JobSpecTask, models.JobSpec) error
	GetByName(string, models.JobSpec) (models.JobSpecTask, error)
	GetAll() ([]models.JobSpecTask, error)
}

type AssetSpecRepository interface {
	Save(models.JobSpecAsset, models.JobSpec) error
	GetByName(string, models.JobSpec) (models.JobSpecAsset, error)
	GetAll() ([]models.JobSpecAsset, error)
}
