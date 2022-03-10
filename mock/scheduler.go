package mock

import (
	"context"
	"time"

	"github.com/odpf/optimus/core/progress"

	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/mock"
)

type Scheduler struct {
	mock.Mock
}

func (ms *Scheduler) VerifyJob(ctx context.Context, namespace models.NamespaceSpec, job models.JobSpec) error {
	args := ms.Called(ctx, namespace, job)
	return args.Error(0)
}

func (ms *Scheduler) ListJobs(ctx context.Context, namespace models.NamespaceSpec, opts models.SchedulerListOptions) ([]models.Job, error) {
	args := ms.Called(ctx, namespace, opts)
	return args.Get(0).([]models.Job), args.Error(1)
}

func (ms *Scheduler) DeployJobs(ctx context.Context, namespace models.NamespaceSpec, jobs []models.JobSpec, obs progress.Observer) error {
	args := ms.Called(ctx, namespace, jobs, obs)
	return args.Error(0)
}

func (ms *Scheduler) DeleteJobs(ctx context.Context, namespace models.NamespaceSpec, jobNames []string, obs progress.Observer) error {
	args := ms.Called(ctx, namespace, jobNames, obs)
	return args.Error(0)
}

func (ms *Scheduler) GetName() string {
	return "mocked"
}

func (ms *Scheduler) Bootstrap(ctx context.Context, projectSpec models.ProjectSpec) error {
	return ms.Called(ctx, projectSpec).Error(0)
}

func (ms *Scheduler) GetJobStatus(ctx context.Context, projSpec models.ProjectSpec, jobName string) ([]models.JobStatus, error) {
	args := ms.Called(ctx, projSpec, jobName)
	return args.Get(0).([]models.JobStatus), args.Error(1)
}

func (ms *Scheduler) Clear(ctx context.Context, projSpec models.ProjectSpec, jobName string, startDate, endDate time.Time) error {
	args := ms.Called(ctx, projSpec, jobName, startDate, endDate)
	return args.Error(0)
}

func (ms *Scheduler) GetJobRunStatus(ctx context.Context, projectSpec models.ProjectSpec, jobName string, startDate time.Time,
	endDate time.Time, batchSize int) ([]models.JobStatus, error) {
	args := ms.Called(ctx, projectSpec, jobName, startDate, endDate, batchSize)
	return args.Get(0).([]models.JobStatus), args.Error(1)
}

func (ms *Scheduler) GetJobRuns(ctx context.Context, projectSpec models.ProjectSpec, param *models.JobQuery) ([]models.JobRun, error) {
	args := ms.Called(ctx, projectSpec, param)
	return args.Get(0).([]models.JobRun), args.Error(1)
}
