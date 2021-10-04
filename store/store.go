package store

import (
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/odpf/optimus/models"
)

var (
	ErrResourceNotFound = errors.New("resource not found")
)

// ProjectJobSpecRepository represents a storage interface for Job specifications at a project level
type ProjectJobSpecRepository interface {
	GetByName(string) (models.JobSpec, models.NamespaceSpec, error)
	GetByNameForProject(projectName, jobName string) (models.JobSpec, models.ProjectSpec, error)
	GetAll() ([]models.JobSpec, error)
	GetByDestination(string) (models.JobSpec, models.ProjectSpec, error)
}

// ProjectRepository represents a storage interface for registered projects
type ProjectRepository interface {
	Save(models.ProjectSpec) error
	GetByName(string) (models.ProjectSpec, error)
	GetAll() ([]models.ProjectSpec, error)
}

// ProjectSecretRepository stores secrets attached to projects
type ProjectSecretRepository interface {
	Save(item models.ProjectSecretItem) error
	GetByName(string) (models.ProjectSecretItem, error)
	GetAll() ([]models.ProjectSecretItem, error)
}

// NamespaceRepository represents a storage interface for registered namespaces
type NamespaceRepository interface {
	Save(models.NamespaceSpec) error
	GetByName(string) (models.NamespaceSpec, error)
	GetAll() ([]models.NamespaceSpec, error)
}

// JobRunSpecRepository represents a storage interface for Job runs generated to
// represent a job in running state
type JobRunRepository interface {
	// Save updates the instance in place if it can else insert new
	Save(models.NamespaceSpec, models.JobRun) error
	GetByScheduledAt(jobID uuid.UUID, scheduledAt time.Time) (models.JobRun, models.NamespaceSpec, error)
	GetByID(uuid.UUID) (models.JobRun, models.NamespaceSpec, error)
	UpdateStatus(uuid.UUID, models.JobRunState) error
	GetByStatus(state ...models.JobRunState) ([]models.JobRun, error)
	GetByTrigger(trigger models.JobRunTrigger, state ...models.JobRunState) ([]models.JobRun, error)
	Delete(uuid.UUID) error

	AddInstance(namespace models.NamespaceSpec, run models.JobRun, spec models.InstanceSpec) error

	// Clear will not delete the record but will reset all the run details
	// for fresh start
	Clear(runID uuid.UUID) error
	ClearInstance(runID uuid.UUID, instanceType models.InstanceType, instanceName string) error
	ClearInstances(jobID uuid.UUID, scheduled time.Time) error
}

// JobRunSpecRepository represents a storage interface for Job run instances created
// during execution
type InstanceRepository interface {
	Save(run models.JobRun, spec models.InstanceSpec) error
	UpdateStatus(id uuid.UUID, status models.JobRunState) error
	GetByName(runID uuid.UUID, instanceName, instanceType string) (models.InstanceSpec, error)
	Delete(id uuid.UUID) error
}

// ProjectResourceSpecRepository represents a storage interface for Resource specifications at project level
type ProjectResourceSpecRepository interface {
	GetByName(string) (models.ResourceSpec, models.NamespaceSpec, error)
	GetAll() ([]models.ResourceSpec, error)
}

// ResourceSpecRepository represents a storage interface for Resource specifications at namespace level
type ResourceSpecRepository interface {
	Save(models.ResourceSpec) error
	GetByName(string) (models.ResourceSpec, error)
	GetByURN(string) (models.ResourceSpec, error)
	GetAll() ([]models.ResourceSpec, error)
	Delete(string) error
}

// ReplaySpecRepository represents a storage interface for replay objects
type ReplaySpecRepository interface {
	Insert(replay *models.ReplaySpec) error
	GetByID(id uuid.UUID) (models.ReplaySpec, error)
	UpdateStatus(replayID uuid.UUID, status string, message models.ReplayMessage) error
	GetByStatus(status []string) ([]models.ReplaySpec, error)
	GetByJobIDAndStatus(jobID uuid.UUID, status []string) ([]models.ReplaySpec, error)
	GetByProjectIDAndStatus(projectID uuid.UUID, status []string) ([]models.ReplaySpec, error)
	GetByProjectID(projectID uuid.UUID) ([]models.ReplaySpec, error)
}
