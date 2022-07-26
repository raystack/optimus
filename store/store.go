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
	ErrResourceExists   = errors.New("resource already exists")
	ErrEmptyConfig      = errors.New("empty config")
)

type ProjectJobPair struct {
	Project models.ProjectSpec
	Job     models.JobSpec
}

// ProjectJobSpecRepository represents a storage interface for Job specifications at a project level
// This will be deprecated and avoid having multiple job spec repositories
type ProjectJobSpecRepository interface {
	GetByName(context.Context, string) (models.JobSpec, models.NamespaceSpec, error)
	GetByNameForProject(ctx context.Context, projectName, jobName string) (models.JobSpec, models.ProjectSpec, error)
	GetAll(context.Context) ([]models.JobSpec, error)

	// GetByIDs returns all the jobs as requested by its ID
	GetByIDs(context.Context, []uuid.UUID) ([]models.JobSpec, error)

	// GetJobNamespaces returns [namespace name] -> []{job name,...} in a project
	GetJobNamespaces(ctx context.Context) (map[string][]string, error)
}

// JobSpecRepository represents a storage interface for Job specification
type JobSpecRepository interface {
	GetAllByProjectID(context.Context, models.ProjectID) ([]models.JobSpec, error)
	// TODO: change to GetJobsByName as it returns multiple jobs
	GetJobByName(context.Context, string) ([]models.JobSpec, error)
	// TODO: allow to also GetJobByResourceDestination for a specific project
	GetJobByResourceDestination(context.Context, string) (models.JobSpec, error)
	GetDependentJobs(context.Context, *models.JobSpec) ([]models.JobSpec, error)
	GetInferredDependenciesPerJobID(context.Context, models.ProjectID) (map[uuid.UUID][]models.JobSpec, error)
	GetStaticDependenciesPerJobID(context.Context, models.ProjectID) (map[uuid.UUID][]models.JobSpec, error)
}

// NamespaceJobSpecRepository represents a storage interface for Job specifications at a namespace level
// This will be deprecated and avoid having multiple job spec repositories
type NamespaceJobSpecRepository interface {
	Save(context.Context, models.JobSpec, string) error
	GetByName(context.Context, string) (models.JobSpec, error)
	GetAll(context.Context) ([]models.JobSpec, error)
	Delete(context.Context, uuid.UUID) error
}

// ProjectRepository represents a storage interface for registered projects
type ProjectRepository interface {
	Save(context.Context, models.ProjectSpec) error
	GetByName(context.Context, string) (models.ProjectSpec, error)
	GetAll(context.Context) ([]models.ProjectSpec, error)
}

// NamespaceRepository represents a storage interface for registered namespaces
type NamespaceRepository interface {
	Save(context.Context, models.ProjectSpec, models.NamespaceSpec) error
	GetByName(context.Context, models.ProjectSpec, string) (models.NamespaceSpec, error)
	GetAll(context.Context, models.ProjectSpec) ([]models.NamespaceSpec, error)
	Get(ctx context.Context, projectName, namespaceName string) (models.NamespaceSpec, error)
}

// SecretRepository stores secrets attached to projects
type SecretRepository interface {
	GetSecrets(context.Context, models.ProjectSpec, models.NamespaceSpec) ([]models.ProjectSecretItem, error)
	Save(ctx context.Context, project models.ProjectSpec, namespace models.NamespaceSpec, item models.ProjectSecretItem) error
	Update(ctx context.Context, project models.ProjectSpec, namespace models.NamespaceSpec, item models.ProjectSecretItem) error
	GetAll(context.Context, models.ProjectSpec) ([]models.SecretItemInfo, error)
	Delete(context.Context, models.ProjectSpec, models.NamespaceSpec, string) error
}

// JobRunRepository represents a storage interface for Job runs generated to
// represent a job in running state
type JobRunRepository interface {
	// Save updates the run in place if it can else insert new
	// Note: it doesn't insert the instances attached to job run in db
	Save(context.Context, models.NamespaceSpec, models.JobRun, string) error
	GetByScheduledAt(ctx context.Context, jobID uuid.UUID, scheduledAt time.Time) (models.JobRun, models.NamespaceSpec, error)
	GetByID(context.Context, uuid.UUID) (models.JobRun, models.NamespaceSpec, error)
	UpdateStatus(context.Context, uuid.UUID, models.JobRunState) error
	GetByTrigger(ctx context.Context, trigger models.JobRunTrigger, state ...models.JobRunState) ([]models.JobRun, error)
	AddInstance(ctx context.Context, namespace models.NamespaceSpec, run models.JobRun, spec models.InstanceSpec) error

	// Clear will not delete the record but will reset all the run details
	// for fresh start
	Clear(ctx context.Context, runID uuid.UUID) error
	ClearInstance(ctx context.Context, runID uuid.UUID, instanceType models.InstanceType, instanceName string) error
}

// JobRunMetricsRepository represents a storage interface for Job runs generated to
type JobRunMetricsRepository interface {
	Save(context.Context, models.JobEvent, models.NamespaceSpec, models.JobSpec, int64) error
	Update(context.Context, models.JobEvent, models.NamespaceSpec, models.JobSpec) error
	Get(context.Context, models.JobEvent, models.NamespaceSpec, models.JobSpec) (models.JobRunSpec, error)
	GetActiveJobRun(context.Context, string, models.NamespaceSpec, models.JobSpec) (models.JobRunSpec, error)
}

// TaskRunRepository represents a storage interface for Job runs generated to
type TaskRunRepository interface {
	Save(context.Context, models.JobEvent, models.JobRunSpec) error
	Update(context.Context, models.JobEvent, models.JobRunSpec) error
	GetTaskRun(context.Context, models.JobRunSpec) (models.TaskRunSpec, error)
}

type SensorRunRepository interface {
	Save(context.Context, models.JobEvent, models.JobRunSpec) error
	Update(context.Context, models.JobEvent, models.JobRunSpec) error
	GetSensorRun(context.Context, models.JobRunSpec) (models.SensorRunSpec, error)
}

type HookRunRepository interface {
	Save(context.Context, models.JobEvent, models.JobRunSpec) error
	Update(context.Context, models.JobEvent, models.JobRunSpec) error
	GetHookRun(context.Context, models.JobRunSpec) (models.HookRunSpec, error)
}

// JobRunSpecRepository represents a storage interface for Job run instances created
// during execution
type InstanceRepository interface {
	UpdateStatus(ctx context.Context, id uuid.UUID, status models.JobRunState) error
}

// ProjectResourceSpecRepository represents a storage interface for Resource specifications at project level
type ProjectResourceSpecRepository interface {
	GetByName(context.Context, string) (models.ResourceSpec, models.NamespaceSpec, error)
	GetByURN(context.Context, string) (models.ResourceSpec, models.NamespaceSpec, error)
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
	GetByProjectIDAndStatus(ctx context.Context, projectID models.ProjectID, status []string) ([]models.ReplaySpec, error)
	GetByProjectID(ctx context.Context, projectID models.ProjectID) ([]models.ReplaySpec, error)
}

// BackupRepository represents a storage interface for backup objects
type BackupRepository interface {
	Save(ctx context.Context, spec models.BackupSpec) error
	GetAll(context.Context, models.ProjectSpec, models.Datastorer) ([]models.BackupSpec, error)
	GetByID(context.Context, uuid.UUID, models.Datastorer) (models.BackupSpec, error)
}

type JobDeploymentRepository interface {
	Save(ctx context.Context, deployment models.JobDeployment) error
	GetByID(ctx context.Context, deployID models.DeploymentID) (models.JobDeployment, error)
	GetByStatusAndProjectID(context.Context, models.JobDeploymentStatus, models.ProjectID) (models.JobDeployment, error)
	Update(ctx context.Context, deploymentSpec models.JobDeployment) error
	GetByStatus(ctx context.Context, status models.JobDeploymentStatus) ([]models.JobDeployment, error)
	GetFirstExecutableRequest(ctx context.Context) (models.JobDeployment, error)
}

// JobSourceRepository represents a storage interface for job sources
type JobSourceRepository interface {
	// Save replaces old job sources records for the particular project id and job id with newer ones
	Save(ctx context.Context, projectID models.ProjectID, jobID uuid.UUID, jobSourceURNs []string) error
	GetAll(context.Context, models.ProjectID) ([]models.JobSource, error)
	GetByResourceURN(context.Context, string) ([]models.JobSource, error)
	DeleteByJobID(context.Context, uuid.UUID) error
	GetResourceURNsPerJobID(context.Context, models.ProjectID) (map[uuid.UUID][]string, error)
}
