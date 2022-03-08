package mock

import (
	"context"
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

func (r *JobRunRepository) Save(ctx context.Context, spec models.NamespaceSpec, run models.JobRun) error {
	args := r.Called(ctx, spec, run)
	return args.Error(0)
}

func (r *JobRunRepository) GetByScheduledAt(ctx context.Context, jobID uuid.UUID, scheduledAt time.Time) (models.JobRun, models.NamespaceSpec, error) {
	args := r.Called(ctx, jobID, scheduledAt)
	return args.Get(0).(models.JobRun), args.Get(1).(models.NamespaceSpec), args.Error(2)
}

func (r *JobRunRepository) GetByID(ctx context.Context, u uuid.UUID) (models.JobRun, models.NamespaceSpec, error) {
	args := r.Called(ctx, u)
	return args.Get(0).(models.JobRun), args.Get(1).(models.NamespaceSpec), args.Error(2)
}

func (r *JobRunRepository) UpdateStatus(ctx context.Context, u uuid.UUID, state models.JobRunState) error {
	args := r.Called(ctx, u, state)
	return args.Error(0)
}

func (r *JobRunRepository) GetByStatus(ctx context.Context, state ...models.JobRunState) ([]models.JobRun, error) {
	args := r.Called(ctx, state)
	return args.Get(0).([]models.JobRun), args.Error(1)
}

func (r *JobRunRepository) GetByTrigger(ctx context.Context, trig models.JobRunTrigger, state ...models.JobRunState) ([]models.JobRun, error) {
	args := r.Called(ctx, trig, state)
	return args.Get(0).([]models.JobRun), args.Error(1)
}

func (r *JobRunRepository) Delete(ctx context.Context, u uuid.UUID) error {
	args := r.Called(ctx, u)
	return args.Error(0)
}

func (r *JobRunRepository) AddInstance(ctx context.Context, namespace models.NamespaceSpec, run models.JobRun, spec models.InstanceSpec) error {
	args := r.Called(ctx, namespace, run, spec)
	return args.Error(0)
}

func (r *JobRunRepository) Clear(ctx context.Context, runID uuid.UUID) error {
	args := r.Called(ctx, runID)
	return args.Error(0)
}

func (r *JobRunRepository) ClearInstance(ctx context.Context, runID uuid.UUID, instanceType models.InstanceType, instanceName string) error {
	args := r.Called(ctx, runID, instanceType, instanceName)
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

func (repo *InstanceSpecRepository) Save(ctx context.Context, t models.InstanceSpec) error {
	return repo.Called(ctx, t).Error(0)
}

func (repo *InstanceSpecRepository) GetByScheduledAt(ctx context.Context, st time.Time) (models.InstanceSpec, error) {
	args := repo.Called(ctx, st)
	if args.Get(0) != nil {
		return args.Get(0).(models.InstanceSpec), args.Error(1)
	}
	return models.InstanceSpec{}, args.Error(1)
}

func (repo *InstanceSpecRepository) Clear(ctx context.Context, st time.Time) error {
	return repo.Called(ctx, st).Error(0)
}

type RunService struct {
	mock.Mock
}

func (s *RunService) GetScheduledRun(ctx context.Context, namespaceSpec models.NamespaceSpec, jobID models.JobSpec, scheduledAt time.Time) (models.JobRun, error) {
	args := s.Called(ctx, namespaceSpec, jobID, scheduledAt)
	return args.Get(0).(models.JobRun), args.Error(1)
}

func (s *RunService) GetByID(ctx context.Context, jobRunID uuid.UUID) (models.JobRun, models.NamespaceSpec, error) {
	args := s.Called(ctx, jobRunID)
	return args.Get(0).(models.JobRun), args.Get(1).(models.NamespaceSpec), args.Error(2)
}

func (s *RunService) Register(ctx context.Context, namespace models.NamespaceSpec, jobRun models.JobRun, instanceType models.InstanceType, instanceName string) (models.InstanceSpec, error) {
	args := s.Called(ctx, namespace, jobRun, instanceType, instanceName)
	return args.Get(0).(models.InstanceSpec), args.Error(1)
}

func (s *RunService) Compile(ctx context.Context, namespaceSpec models.NamespaceSpec, jobRun models.JobRun, instanceSpec models.InstanceSpec) (*models.JobRunInput, error) {
	args := s.Called(ctx, namespaceSpec, jobRun, instanceSpec)
	return args.Get(0).(*models.JobRunInput), args.Error(1)
}
