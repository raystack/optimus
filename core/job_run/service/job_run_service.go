package service

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/odpf/optimus/core/job_run"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

type JobService interface {
	GetJob(ctx context.Context, name tenant.ProjectName, jobName job_run.JobName) (*job_run.Job, error)
	GetJobDetails(ctx context.Context, name tenant.ProjectName, jobName job_run.JobName) (*job_run.JobWithDetails, error)
}

type JobRunRepository interface {
	// GetJobRunByID get job_run by job_run ID
	GetJobRunByID(ctx context.Context, id job_run.JobRunID) (*job_run.JobRun, error)
	// GetJobRunByScheduledAt get the latest(by created at) job_run for a given Schedule time
	GetJobRunByScheduledAt(ctx context.Context,
		tenant tenant.Tenant,
		name job_run.JobName,
		scheduledAt time.Time) (*job_run.JobRun, error)
	// Create add a new job_run in the DB
	Create(ctx context.Context,
		tenant tenant.Tenant,
		name job_run.JobName,
		scheduledAt time.Time,
		slaDefinitionInSec int64) error
	// Update update an exixting job_run in the DB
	Update(ctx context.Context,
		tenant tenant.Tenant,
		name job_run.JobName,
		scheduledAt time.Time,
		jobRunStatus string,
		endTime time.Time) error

	// GetOperatorRun get operator run
	GetOperatorRun(ctx context.Context,
		operator job_run.OperatorType,
		jobRunId uuid.UUID) (*job_run.OperatorRun, error)

	// CreateOperatorRun create operator run
	CreateOperatorRun(ctx context.Context,
		operator job_run.OperatorType,
		jobRunId uuid.UUID,
		startTime time.Time) error

	// UpdateOperatorRun create operator run
	UpdateOperatorRun(ctx context.Context,
		operator job_run.OperatorType,
		jobRunId uuid.UUID,
		eventTime time.Time,
		state string) error
}

type JobInputCompiler interface {
	Compile(ctx context.Context, job *job_run.Job, config job_run.RunConfig, executedAt time.Time) (*job_run.ExecutorInput, error)
}

type JobRunService struct {
	repo     JobRunRepository
	jobSrvc  JobService
	compiler JobInputCompiler
}

func (s JobRunService) JobRunInput(ctx context.Context, projectName tenant.ProjectName, jobName job_run.JobName, config job_run.RunConfig) (*job_run.ExecutorInput, error) {
	job, err := s.jobSrvc.GetJob(ctx, projectName, jobName)
	if err != nil {
		return nil, err
	}

	// TODO: Use scheduled_at instead of executed_at for computations, for deterministic calculations
	var jobRun *job_run.JobRun
	if config.JobRunID.IsEmpty() {
		jobRun, err = s.repo.GetJobRunByScheduledAt(ctx, job.Tenant, jobName, config.ScheduledAt)
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

func (s JobRunService) registerNewJobRun(ctx context.Context, event job_run.Event) error {
	job, err := s.jobSrvc.GetJobDetails(ctx, event.Tenant.ProjectName(), event.JobName)
	if err != nil {
		return err
	}
	slaDefinitionInSec, err := job.SLADuration()
	if err != nil {
		return err
	}

	scheduledAtTimeStamp, err := time.Parse(job_run.ISODateFormat, event.Values["scheduled_at"].(string))
	if err != nil {
		return err
	}
	return s.repo.Create(ctx,
		event.Tenant,
		event.JobName,
		scheduledAtTimeStamp,
		slaDefinitionInSec)
}

func (s JobRunService) updateJobRun(ctx context.Context, event job_run.Event) error {
	scheduledAtTimeStamp, err := time.Parse(job_run.ISODateFormat, event.Values["scheduled_at"].(string))
	if err != nil {
		return err
	}
	jobRunStatus := event.Values["status"].(string)
	endTime := time.Unix(int64(event.Values["event_time"].(int64)), 0)

	return s.repo.Update(ctx,
		event.Tenant,
		event.JobName,
		scheduledAtTimeStamp,
		jobRunStatus,
		endTime,
	)
}

func (s JobRunService) createOperatorRun(ctx context.Context, event job_run.Event, operatorType job_run.OperatorType) error {
	scheduledAtTimeStamp, err := time.Parse(job_run.ISODateFormat, event.Values["scheduled_at"].(string))
	if err != nil {
		return err
	}

	jobRun, err := s.repo.GetJobRunByScheduledAt(ctx,
		event.Tenant,
		event.JobName,
		scheduledAtTimeStamp)
	if err != nil {
		return err
	}

	startedAtTimeStamp := time.Unix(int64(event.Values["event_time"].(int64)), 0)

	operatorRun, err := s.repo.GetOperatorRun(ctx, operatorType, jobRun.ID)
	if err == nil {
		if !errors.IsErrorType(err, errors.ErrNotFound) {
			return err
		}
	} else {
		if operatorRun.State == job_run.OperatorStateStarted {
			// operator run exists but is not yet finished
			return nil
		}
	}

	return s.repo.CreateOperatorRun(ctx,
		operatorType,
		jobRun.ID,
		startedAtTimeStamp)
}

func (s JobRunService) updateOperatorRun(ctx context.Context, event job_run.Event, operatorType job_run.OperatorType) error {
	scheduledAtTimeStamp, err := time.Parse(job_run.ISODateFormat, event.Values["scheduled_at"].(string))
	if err != nil {
		return err
	}
	jobRun, err := s.repo.GetJobRunByScheduledAt(ctx,
		event.Tenant,
		event.JobName,
		scheduledAtTimeStamp)
	if err != nil {
		return err
	}

	status := event.Values["status"].(string)
	endTime := time.Unix(int64(event.Values["event_time"].(int64)), 0)

	return s.repo.UpdateOperatorRun(ctx,
		operatorType,
		jobRun.ID,
		endTime,
		status)
}

func (s JobRunService) UpdateJobState(ctx context.Context, event job_run.Event) error {
	switch event.Type {
	case job_run.JobStartEvent:
		return s.registerNewJobRun(ctx, event)
	case job_run.JobSuccessEvent, job_run.JobFailEvent:
		return s.updateJobRun(ctx, event)
	case job_run.TaskStartEvent:
		return s.createOperatorRun(ctx, event, job_run.OperatorTask)
	case job_run.TaskSuccessEvent, job_run.TaskRetryEvent, job_run.TaskFailEvent:
		return s.updateOperatorRun(ctx, event, job_run.OperatorTask)
	case job_run.SensorStartEvent:
		return s.createOperatorRun(ctx, event, job_run.OperatorSensor)
	case job_run.SensorSuccessEvent, job_run.SensorRetryEvent, job_run.SensorFailEvent:
		return s.updateOperatorRun(ctx, event, job_run.OperatorSensor)
	case job_run.HookStartEvent:
		return s.createOperatorRun(ctx, event, job_run.OperatorHook)
	case job_run.HookSuccessEvent, job_run.HookRetryEvent, job_run.HookFailEvent:
		return s.updateOperatorRun(ctx, event, job_run.OperatorHook)
	default:
		return errors.InvalidArgument(job_run.EntityEvent, "invalid event type: "+string(event.Type))
	}
}
