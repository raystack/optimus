package job

import "github.com/odpf/optimus/models"

// SpecRepository represents a storage interface for Job specifications at a namespace level
type SpecRepository interface {
	Save(models.JobSpec) error
	GetByName(string) (models.JobSpec, error)
	GetAll() ([]models.JobSpec, error)
	Delete(string) error
}
