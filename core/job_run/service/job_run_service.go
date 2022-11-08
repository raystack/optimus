package service

import (
	"context"
	"time"

	"github.com/odpf/optimus/core/job_run"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

type JobRepository interface {
	GetJob(ctx context.Context, name tenant.ProjectName, jobName job_run.JobName) (*job_run.Job, error)
}

type JobRunRepository interface {
	GetJobRunByID(ctx context.Context, id job_run.JobRunID) (*job_run.JobRun, error)
	GetJobRunByScheduledAt(ctx context.Context, tnnt tenant.Tenant, name job_run.JobName, scheduledAt time.Time) (*job_run.JobRun, error)
}

type JobInputCompiler interface {
	Compile(ctx context.Context, job *job_run.Job, config job_run.RunConfig, executedAt time.Time) (*job_run.ExecutorInput, error)
}

type JobRunService struct {
	repo     JobRunRepository
	jobRepo  JobRepository
	compiler JobInputCompiler
}

func (s JobRunService) JobRunInput(ctx context.Context, projectName tenant.ProjectName, jobName job_run.JobName, config job_run.RunConfig) (*job_run.ExecutorInput, error) {
	job, err := s.jobRepo.GetJob(ctx, projectName, jobName)
	if err != nil {
		return nil, err
	}

	var jobRun *job_run.JobRun // Only required for executed_at value
	if config.JobRunID.IsEmpty() {
		jobRun, err = s.repo.GetJobRunByScheduledAt(ctx, job.Tenant(), jobName, config.ScheduledAt)
	} else {
		jobRun, err = s.repo.GetJobRunByID(ctx, config.JobRunID)
	}

	var executedAt time.Time
	if err != nil { // Fallback for executed_at to scheduled_at
		if !errors.IsErrorType(err, errors.ErrNotFound) {
			return nil, err
		}
		executedAt = config.ScheduledAt
	} else {
		executedAt = jobRun.StartTime()
	}

	return s.compiler.Compile(ctx, job, config, executedAt)
}

func (s JobRunService) registerNewJobRun(ctx context.Context, tenant tenant.Tenant, event job_run.Event) error {
	jobSpec, err := s.jobRepo.GetJob(ctx, tenant.ProjectName(), event.JobName)
	if err != nil {
		return err
	}
	slaDefinitionInSec, err := jobSpec.SLADuration()
	if err != nil {
		return err
	}
	return m.JobRunMetricsRepository.Save(ctx, event, namespaceSpec, jobSpec, slaDefinitionInSec)
	return nil
}

func (s JobRunService) UpdateJobState(ctx context.Context, tenant tenant.Tenant, event job_run.Event) error {
	switch event.Type {
	case job_run.JobStartEvent:
		return s.registerNewJobRun(ctx, tenant, event)
	case job_run.JobSuccessEvent, job_run.JobFailEvent:
		return s.updateJobRun(ctx, event, namespaceSpec, jobSpec)
	case job_run.TaskStartEvent, job_run.TaskSuccessEvent, job_run.TaskRetryEvent, job_run.TaskFailEvent:
		return s.registerTaskRunEvent(ctx, event, namespaceSpec, jobSpec)
	case job_run.SensorStartEvent, job_run.SensorSuccessEvent, job_run.SensorRetryEvent, job_run.SensorFailEvent:
		return s.registerSensorRunEvent(ctx, event, namespaceSpec, jobSpec)
	case job_run.HookStartEvent, job_run.HookSuccessEvent, job_run.HookRetryEvent, job_run.HookFailEvent:
		return s.registerHookRunEvent(ctx, event, namespaceSpec, jobSpec)
	}
	return nil
}
