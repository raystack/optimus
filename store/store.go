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

// ProjectRepository represents a storage interface for registered projects
type ProjectRepository interface {
	Save(models.ProjectSpec) error
	GetByName(string) (models.ProjectSpec, error)
	GetAll() ([]models.ProjectSpec, error)
}

// JobRepository represents a storage interface for compiled specifications for
// JobSpecs
type JobRepository interface {
	Save(models.Job) error
	GetByName(string) (models.Job, error)
	GetAll() ([]models.Job, error)
	Delete(string) error
}
