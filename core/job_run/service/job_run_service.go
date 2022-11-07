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
	Compile(ctx context.Context, job *job_run.Job, config job_run.RunConfig, executedAt time.Time) (job_run.ExecutorInput, error)
}

type JobRunService struct {
	repo     JobRunRepository
	jobRepo  JobRepository
	compiler JobInputCompiler
}

func (s JobRunService) JobRunInput(ctx context.Context, projectName tenant.ProjectName, jobName job_run.JobName, config job_run.RunConfig) (job_run.ExecutorInput, error) {
	job, err := s.jobRepo.GetJob(ctx, projectName, jobName)
	if err != nil {
		return job_run.ExecutorInput{}, err
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
			return job_run.ExecutorInput{}, err
		}
		executedAt = config.ScheduledAt
	} else {
		executedAt = jobRun.StartTime()
	}

	return s.compiler.Compile(ctx, job, config, executedAt)
}
