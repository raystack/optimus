package mock

import (
	"time"

	"github.com/odpf/optimus/store"

	"github.com/google/uuid"

	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/mock"
)

type JobRunRepoFactory struct {
	mock.Mock
}

func (repo *JobRunRepoFactory) New() store.JobRunRepository {
	args := repo.Called()
	return args.Get(0).(store.JobRunRepository)
}

type JobRunRepository struct {
	mock.Mock
}

func (r *JobRunRepository) Save(spec models.NamespaceSpec, run models.JobRun) error {
	args := r.Called(spec, run)
	return args.Error(0)
}

func (r *JobRunRepository) GetByScheduledAt(jobID uuid.UUID, scheduledAt time.Time) (models.JobRun, models.NamespaceSpec, error) {
	args := r.Called(jobID, scheduledAt)
	return args.Get(0).(models.JobRun), args.Get(1).(models.NamespaceSpec), args.Error(2)
}

func (r *JobRunRepository) GetByID(u uuid.UUID) (models.JobRun, models.NamespaceSpec, error) {
	args := r.Called(u)
	return args.Get(0).(models.JobRun), args.Get(1).(models.NamespaceSpec), args.Error(2)
}

func (r *JobRunRepository) UpdateStatus(u uuid.UUID, state models.JobRunState) error {
	args := r.Called(u, state)
	return args.Error(0)
}

func (r *JobRunRepository) GetByStatus(state ...models.JobRunState) ([]models.JobRun, error) {
	args := r.Called(state)
	return args.Get(0).([]models.JobRun), args.Error(1)
}

func (r *JobRunRepository) GetByTrigger(trig models.JobRunTrigger, state ...models.JobRunState) ([]models.JobRun, error) {
	args := r.Called(trig, state)
	return args.Get(0).([]models.JobRun), args.Error(1)
}

func (r *JobRunRepository) Delete(u uuid.UUID) error {
	args := r.Called(u)
	return args.Error(0)
}

func (r *JobRunRepository) AddInstance(namespace models.NamespaceSpec, run models.JobRun, spec models.InstanceSpec) error {
	args := r.Called(namespace, run, spec)
	return args.Error(0)
}

func (r *JobRunRepository) Clear(runID uuid.UUID) error {
	args := r.Called(runID)
	return args.Error(0)
}

func (r *JobRunRepository) ClearInstance(runID uuid.UUID, instanceType models.InstanceType, instanceName string) error {
	args := r.Called(runID, instanceType, instanceName)
	return args.Error(0)
}

func (r *JobRunRepository) ClearInstances(jobID uuid.UUID, scheduled time.Time) error {
	args := r.Called(jobID, scheduled)
	return args.Error(0)
}

type InstanceSpecRepoFactory struct {
	mock.Mock
}

func (repo *InstanceSpecRepoFactory) New() store.InstanceRepository {
	args := repo.Called()
	return args.Get(0).(store.InstanceRepository)
}

// InstanceSpecRepository to store mock instance specs
type InstanceSpecRepository struct {
	mock.Mock
}

func (repo *InstanceSpecRepository) Save(t models.InstanceSpec) error {
	return repo.Called(t).Error(0)
}

func (repo *InstanceSpecRepository) GetByScheduledAt(st time.Time) (models.InstanceSpec, error) {
	args := repo.Called(st)
	if args.Get(0) != nil {
		return args.Get(0).(models.InstanceSpec), args.Error(1)
	}
	return models.InstanceSpec{}, args.Error(1)
}

func (repo *InstanceSpecRepository) Clear(st time.Time) error {
	return repo.Called(st).Error(0)
}

type RunService struct {
	mock.Mock
}

func (s *RunService) GetScheduledRun(namespaceSpec models.NamespaceSpec, JobID models.JobSpec, scheduledAt time.Time) (models.JobRun, error) {
	args := s.Called(namespaceSpec, JobID, scheduledAt)
	return args.Get(0).(models.JobRun), args.Error(1)
}

func (s *RunService) GetByID(JobRunID uuid.UUID) (models.JobRun, models.NamespaceSpec, error) {
	args := s.Called(JobRunID)
	return args.Get(0).(models.JobRun), args.Get(1).(models.NamespaceSpec), args.Error(2)
}

func (s *RunService) Register(namespace models.NamespaceSpec, jobRun models.JobRun, instanceType models.InstanceType, instanceName string) (models.InstanceSpec, error) {
	args := s.Called(namespace, jobRun, instanceType, instanceName)
	return args.Get(0).(models.InstanceSpec), args.Error(1)
}

func (s *RunService) Compile(namespaceSpec models.NamespaceSpec, jobRun models.JobRun, instanceSpec models.InstanceSpec) (envMap map[string]string, fileMap map[string]string, err error) {
	args := s.Called(namespaceSpec, jobRun, instanceSpec)
	return args.Get(0).(map[string]string), args.Get(1).(map[string]string), args.Error(2)
}
