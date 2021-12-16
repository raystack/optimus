package store

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/odpf/optimus/models"
)

var (
	ErrResourceNotFound = errors.New("resource not found")
	ErrEmptyConfig      = errors.New("empty config")
)

// ProjectJobSpecRepository represents a storage interface for Job specifications at a project level
type ProjectJobSpecRepository interface {
	GetByName(context.Context, string) (models.JobSpec, models.NamespaceSpec, error)
	GetByNameForProject(ctx context.Context, projectName, jobName string) (models.JobSpec, models.ProjectSpec, error)
	GetAll(context.Context) ([]models.JobSpec, error)
	GetByDestination(context.Context, string) (models.JobSpec, models.ProjectSpec, error)

	// GetJobNamespaces returns [namespace name] -> []{job name,...} in a project
	GetJobNamespaces(ctx context.Context) (map[string][]string, error)
}

// ProjectRepository represents a storage interface for registered projects
type ProjectRepository interface {
	Save(context.Context, models.ProjectSpec) error
	GetByName(context.Context, string) (models.ProjectSpec, error)
	GetAll(context.Context) ([]models.ProjectSpec, error)
}

// ProjectSecretRepository stores secrets attached to projects
type ProjectSecretRepository interface {
	Save(ctx context.Context, item models.ProjectSecretItem) error
	GetByName(context.Context, string) (models.ProjectSecretItem, error)
	GetAll(context.Context) ([]models.ProjectSecretItem, error)
}

// NamespaceRepository represents a storage interface for registered namespaces
type NamespaceRepository interface {
	Save(context.Context, models.NamespaceSpec) error
	GetByName(context.Context, string) (models.NamespaceSpec, error)
	GetAll(context.Context) ([]models.NamespaceSpec, error)
}

// JobRunSpecRepository represents a storage interface for Job runs generated to
// represent a job in running state
type JobRunRepository interface {
	// Save updates the run in place if it can else insert new
	// Note: it doesn't insert the instances attached to job run in db
	Save(context.Context, models.NamespaceSpec, models.JobRun) error

	GetByScheduledAt(ctx context.Context, jobID uuid.UUID, scheduledAt time.Time) (models.JobRun, models.NamespaceSpec, error)
	GetByID(context.Context, uuid.UUID) (models.JobRun, models.NamespaceSpec, error)
	UpdateStatus(context.Context, uuid.UUID, models.JobRunState) error
	GetByStatus(ctx context.Context, state ...models.JobRunState) ([]models.JobRun, error)
	GetByTrigger(ctx context.Context, trigger models.JobRunTrigger, state ...models.JobRunState) ([]models.JobRun, error)
	Delete(context.Context, uuid.UUID) error

	AddInstance(ctx context.Context, namespace models.NamespaceSpec, run models.JobRun, spec models.InstanceSpec) error

	// Clear will not delete the record but will reset all the run details
	// for fresh start
	Clear(ctx context.Context, runID uuid.UUID) error
	ClearInstance(ctx context.Context, runID uuid.UUID, instanceType models.InstanceType, instanceName string) error
}

// JobRunSpecRepository represents a storage interface for Job run instances created
// during execution
type InstanceRepository interface {
	Save(ctx context.Context, run models.JobRun, spec models.InstanceSpec) error
	UpdateStatus(ctx context.Context, id uuid.UUID, status models.JobRunState) error
	GetByName(ctx context.Context, runID uuid.UUID, instanceName, instanceType string) (models.InstanceSpec, error)

	DeleteByJobRun(ctx context.Context, id uuid.UUID) error
}

// ProjectResourceSpecRepository represents a storage interface for Resource specifications at project level
type ProjectResourceSpecRepository interface {
	GetByName(context.Context, string) (models.ResourceSpec, models.NamespaceSpec, error)
	GetByURN(context.Context, string) (models.ResourceSpec, models.NamespaceSpec, error)
	GetAll(context.Context) ([]models.ResourceSpec, error)
}

// ResourceSpecRepository represents a storage interface for Resource specifications at namespace level
type ResourceSpecRepository interface {
	Save(context.Context, models.ResourceSpec) error
	GetByName(context.Context, string) (models.ResourceSpec, error)
	GetAll(context.Context) ([]models.ResourceSpec, error)
	Delete(context.Context, string) error
}

// ReplaySpecRepository represents a storage interface for replay objects
type ReplaySpecRepository interface {
	Insert(ctx context.Context, replay *models.ReplaySpec) error
	GetByID(ctx context.Context, id uuid.UUID) (models.ReplaySpec, error)
	UpdateStatus(ctx context.Context, replayID uuid.UUID, status string, message models.ReplayMessage) error
	GetByStatus(ctx context.Context, status []string) ([]models.ReplaySpec, error)
	GetByJobIDAndStatus(ctx context.Context, jobID uuid.UUID, status []string) ([]models.ReplaySpec, error)
	GetByProjectIDAndStatus(ctx context.Context, projectID uuid.UUID, status []string) ([]models.ReplaySpec, error)
	GetByProjectID(ctx context.Context, projectID uuid.UUID) ([]models.ReplaySpec, error)
}

// BackupRepository represents a storage interface for backup objects
type BackupRepository interface {
	Save(ctx context.Context, spec models.BackupSpec) error
	GetAll(context.Context) ([]models.BackupSpec, error)
}
