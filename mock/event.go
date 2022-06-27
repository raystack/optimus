package mock

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/models"
)

type MonitoringService struct {
	mock.Mock
}

func (srv *MonitoringService) ProcessEvent(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	return srv.Called(ctx, event, namespaceSpec, jobSpec).Error(0)
}

type JobRunMetricsRepository struct {
	mock.Mock
}

func (repo *JobRunMetricsRepository) Update(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	return repo.Called(ctx, event, namespaceSpec, jobSpec).Error(0)
}

func (repo *JobRunMetricsRepository) GetActiveJobRun(ctx context.Context, scheduledAt string, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) (models.JobRunSpec, error) {
	return models.JobRunSpec{}, repo.Called(ctx, scheduledAt, namespaceSpec, jobSpec).Error(1)
}

func (repo *JobRunMetricsRepository) Get(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) (models.JobRunSpec, error) {
	return models.JobRunSpec{}, repo.Called(ctx, event, namespaceSpec, jobSpec).Error(1)
}

func (repo *JobRunMetricsRepository) Save(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec, slaMissDurationInSec int64) error {
	return repo.Called(ctx, event, namespaceSpec, jobSpec, slaMissDurationInSec).Error(0)
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
	return models.SensorRunSpec{}, repo.Called(ctx, jobRunSpec).Error(1)
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
	return models.HookRunSpec{}, repo.Called(ctx, jobRunSpec).Error(1)
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
	return models.TaskRunSpec{}, repo.Called(ctx, jobRunSpec).Error(1)
}
