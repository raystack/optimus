package mock

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/models"
)

type MonitoringService struct {
	mock.Mock
}

func (srv *MonitoringService) ProcessEvent(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	return srv.Called(ctx, event, namespaceSpec, jobSpec).Error(0)
}
func (srv *MonitoringService) GetJobRunByScheduledAt(ctx context.Context, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec, scheduledAt time.Time) (models.JobRunSpec, error) {
	args := srv.Called(ctx, namespaceSpec, jobSpec, scheduledAt)
	return args.Get(0).(models.JobRunSpec), args.Error(1)
}
func (srv *MonitoringService) GetJobRunByRunID(ctx context.Context, jobRunID uuid.UUID) (models.JobRunSpec, error) {
	args := srv.Called(ctx, jobRunID)
	return args.Get(0).(models.JobRunSpec), args.Error(1)
}

type JobRunMetricsRepository struct {
	mock.Mock
}

func (repo *JobRunMetricsRepository) Update(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	return repo.Called(ctx, event, namespaceSpec, jobSpec).Error(0)
}

func (repo *JobRunMetricsRepository) GetLatestJobRunByScheduledTime(ctx context.Context, scheduledAt string, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) (models.JobRunSpec, error) {
	args := repo.Called(ctx, scheduledAt, namespaceSpec, jobSpec)
	return args.Get(0).(models.JobRunSpec), args.Error(1)
}

func (repo *JobRunMetricsRepository) Get(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) (models.JobRunSpec, error) {
	args := repo.Called(ctx, event, namespaceSpec, jobSpec)
	return args.Get(0).(models.JobRunSpec), args.Error(1)
}

func (repo *JobRunMetricsRepository) GetByID(ctx context.Context, jobRunID uuid.UUID) (models.JobRunSpec, error) {
	args := repo.Called(ctx, jobRunID)
	return args.Get(0).(models.JobRunSpec), args.Error(1)
}

func (repo *JobRunMetricsRepository) Save(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec, slaMissDurationInSec int64, jobDestination string) error {
	return repo.Called(ctx, event, namespaceSpec, jobSpec, slaMissDurationInSec, jobDestination).Error(0)
}

type SensorRunRepository struct {
	mock.Mock
}

func (repo *SensorRunRepository) Save(ctx context.Context, event models.JobEvent, jobRunSpec models.JobRunSpec) error {
	return repo.Called(ctx, event, jobRunSpec).Error(0)
}
func (repo *SensorRunRepository) Update(ctx context.Context, event models.JobEvent, jobRunSpec models.JobRunSpec) error {
	return repo.Called(ctx, event, jobRunSpec).Error(0)
}
func (repo *SensorRunRepository) GetSensorRun(ctx context.Context, jobRunSpec models.JobRunSpec) (models.SensorRunSpec, error) {
	args := repo.Called(ctx, jobRunSpec)
	return args.Get(0).(models.SensorRunSpec), args.Error(1)
}

type HookRunRepository struct {
	mock.Mock
}

func (repo *HookRunRepository) Save(ctx context.Context, event models.JobEvent, jobRunSpec models.JobRunSpec) error {
	return repo.Called(ctx, event, jobRunSpec).Error(0)
}
func (repo *HookRunRepository) Update(ctx context.Context, event models.JobEvent, jobRunSpec models.JobRunSpec) error {
	return repo.Called(ctx, event, jobRunSpec).Error(0)
}
func (repo *HookRunRepository) GetHookRun(ctx context.Context, jobRunSpec models.JobRunSpec) (models.HookRunSpec, error) {
	args := repo.Called(ctx, jobRunSpec)
	return args.Get(0).(models.HookRunSpec), args.Error(1)
}

type TaskRunRepository struct {
	mock.Mock
}

func (repo *TaskRunRepository) Save(ctx context.Context, event models.JobEvent, jobRunSpec models.JobRunSpec) error {
	return repo.Called(ctx, event, jobRunSpec).Error(0)
}

func (repo *TaskRunRepository) Update(ctx context.Context, event models.JobEvent, jobRunSpec models.JobRunSpec) error {
	return repo.Called(ctx, event, jobRunSpec).Error(0)
}

func (repo *TaskRunRepository) GetTaskRun(ctx context.Context, jobRunSpec models.JobRunSpec) (models.TaskRunSpec, error) {
	args := repo.Called(ctx, jobRunSpec)
	return args.Get(0).(models.TaskRunSpec), args.Error(1)
}
