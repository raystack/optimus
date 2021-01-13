package store

import (
	"github.com/odpf/optimus/models"
)

// JobRepository represents a storage interface for Job specifications
type JobRepository interface {
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
