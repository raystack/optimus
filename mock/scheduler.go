package mock

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/internal/lib/cron"
	"github.com/odpf/optimus/internal/lib/progress"
	"github.com/odpf/optimus/models"
)

type Scheduler struct {
	mock.Mock
}

func (ms *Scheduler) VerifyJob(ctx context.Context, namespace models.NamespaceSpec, job models.JobSpec) error {
	args := ms.Called(ctx, namespace, job)
	return args.Error(0)
}

func (ms *Scheduler) ListJobs(ctx context.Context, nsDirectoryIdentifier string, namespace models.NamespaceSpec, opts models.SchedulerListOptions) ([]models.Job, error) {
	args := ms.Called(ctx, nsDirectoryIdentifier, namespace, opts)
	return args.Get(0).([]models.Job), args.Error(1)
}

func (ms *Scheduler) DeployJobs(ctx context.Context, namespace models.NamespaceSpec, jobs []models.JobSpec) (models.JobDeploymentDetail, error) {
	args := ms.Called(ctx, namespace, jobs)
	return args.Get(0).(models.JobDeploymentDetail), args.Error(1)
}

func (ms *Scheduler) DeleteJobs(ctx context.Context, nsDirectoryIdentifier string, namespace models.NamespaceSpec, jobNames []string, obs progress.Observer) error {
	args := ms.Called(ctx, nsDirectoryIdentifier, namespace, jobNames, obs)
	return args.Error(0)
}

func (*Scheduler) GetName() string {
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
	endDate time.Time, batchSize int,
) ([]models.JobStatus, error) {
	args := ms.Called(ctx, projectSpec, jobName, startDate, endDate, batchSize)
	return args.Get(0).([]models.JobStatus), args.Error(1)
}

func (ms *Scheduler) GetJobRuns(ctx context.Context, projectSpec models.ProjectSpec, jobQuery *models.JobQuery, jobCron *cron.ScheduleSpec) ([]models.JobRun, error) {
	args := ms.Called(ctx, projectSpec, jobQuery, jobCron)
	return args.Get(0).([]models.JobRun), args.Error(1)
}
