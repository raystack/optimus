package service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/salt/log"

	"github.com/odpf/optimus/core/job_run"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/internal/lib/cron"
	"github.com/odpf/optimus/models"
)

type JobRepository interface {
	GetJob(ctx context.Context, name tenant.ProjectName, jobName job_run.JobName) (*job_run.Job, error)
	GetJobDetails(ctx context.Context, projectName tenant.ProjectName, jobName job_run.JobName) (*job_run.JobWithDetails, error)
	GetAll(ctx context.Context, projectName tenant.ProjectName) ([]*job_run.JobWithDetails, error)
}

type JobRunRepository interface {
	GetJobRunByID(ctx context.Context, id job_run.JobRunID) (*job_run.JobRun, error)
	GetJobRunByScheduledAt(ctx context.Context, tenant tenant.Tenant, name job_run.JobName, scheduledAt time.Time) (*job_run.JobRun, error)

	Create(ctx context.Context, tenant tenant.Tenant, name job_run.JobName, scheduledAt time.Time, slaDefinitionInSec int64) error
	Update(ctx context.Context, tenant tenant.Tenant, name job_run.JobName, scheduledAt time.Time, jobRunStatus string, endTime time.Time) error

	GetOperatorRun(ctx context.Context, operator job_run.OperatorType, jobRunId uuid.UUID) (*job_run.OperatorRun, error)
	CreateOperatorRun(ctx context.Context, operator job_run.OperatorType, jobRunID uuid.UUID, startTime time.Time) error
	UpdateOperatorRun(ctx context.Context, operator job_run.OperatorType, jobRunID uuid.UUID, eventTime time.Time, state string) error
}

type JobInputCompiler interface {
	Compile(ctx context.Context, job *job_run.Job, config job_run.RunConfig, executedAt time.Time) (*job_run.ExecutorInput, error)
}

type PriorityResolver interface {
	Resolve(context.Context, []*job_run.JobWithDetails) error
}

type Scheduler interface {
	GetJobRuns(ctx context.Context, t tenant.Tenant, criteria *job_run.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]*job_run.JobRunStatus, error)
	DeployJobs(ctx context.Context, t tenant.Tenant, jobs []*job_run.JobWithDetails) error
	ListJobs(ctx context.Context, t tenant.Tenant) ([]string, error)
	DeleteJobs(ctx context.Context, t tenant.Tenant, jobsToDelete []string) error
}

type JobRunService struct {
	l                log.Logger
	repo             JobRunRepository
	scheduler        Scheduler
	jobRepo          JobRepository
	priorityResolver PriorityResolver
	compiler         JobInputCompiler
}

func (s JobRunService) UploadToScheduler(ctx context.Context, projectName tenant.ProjectName, namespaceName string) error {
	allJobsWithDetails, err := s.jobRepo.GetAll(ctx, projectName)
	//todo: confirm if we need namespace level deployments ?
	if err != nil {
		return err
	}
	// todo: pass logwriter in place of progress observer
	err = s.priorityResolver.Resolve(ctx, allJobsWithDetails)
	if err != nil {
		return err
	}

	jobGroupByTenant := job_run.GroupJobsByTenant(allJobsWithDetails)
	multiError := errors.NewMultiError("ErrorInUploadToScheduler")
	for t, jobs := range jobGroupByTenant {
		if err := s.deployJobsPerNamespace(ctx, t, jobs); err != nil {
			multiError.Append(err)
		}
		s.l.Debug(fmt.Sprintf("namespace %s deployed", namespaceName), "project name", projectName)
	}

	return errors.MultiToError(multiError)
}

func (s JobRunService) deployJobsPerNamespace(ctx context.Context, t tenant.Tenant, jobs []*job_run.JobWithDetails) error {

	err := s.scheduler.DeployJobs(ctx, t, jobs)
	if err != nil {
		return err
	}
	return s.cleanPerNamespace(ctx, t, jobs)
}

func (s JobRunService) cleanPerNamespace(ctx context.Context, t tenant.Tenant, jobs []*job_run.JobWithDetails) error {

	// get all stored job names
	schedulerJobNames, err := s.scheduler.ListJobs(ctx, t)
	if err != nil {
		return err
	}
	jobNamesMap := make(map[string]struct{})
	for _, job := range jobs {
		jobNamesMap[job.Name.String()] = struct{}{}
	}
	var jobsToDelete []string

	for _, schedulerJobName := range schedulerJobNames {
		if _, ok := jobNamesMap[schedulerJobName]; !ok {
			jobsToDelete = append(jobsToDelete, schedulerJobName)
		}
	}
	return s.scheduler.DeleteJobs(ctx, t, jobsToDelete)
}

func (s JobRunService) JobRunInput(ctx context.Context, projectName tenant.ProjectName, jobName job_run.JobName, config job_run.RunConfig) (*job_run.ExecutorInput, error) {
	job, err := s.jobRepo.GetJob(ctx, projectName, jobName)
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

func validateJobQuery(jobQuery *job_run.JobRunsCriteria, jobWithDetails job_run.JobWithDetails) error {
	jobStartDate := jobWithDetails.Schedule.StartDate
	if jobStartDate.IsZero() {
		return fmt.Errorf("job start time not found at DB")
	}
	givenStartDate := jobQuery.StartDate
	givenEndDate := jobQuery.EndDate

	if givenStartDate.Before(jobStartDate) || givenEndDate.Before(jobStartDate) {
		return fmt.Errorf("invalid date range")
	}
	return nil
}

func getExpectedRuns(spec *cron.ScheduleSpec, startTime, endTime time.Time) []*job_run.JobRunStatus {
	var jobRuns []*job_run.JobRunStatus
	start := spec.Next(startTime.Add(-time.Second * 1))
	end := endTime
	exit := spec.Next(end)
	for !start.Equal(exit) {
		jobRuns = append(jobRuns, &job_run.JobRunStatus{
			State:       job_run.StatePending,
			ScheduledAt: start,
		})
		start = spec.Next(start)
	}
	return jobRuns
}

func mergeRuns(expected, actual []*job_run.JobRunStatus) []*job_run.JobRunStatus {
	var mergeRuns []*job_run.JobRunStatus
	m := actualRunMap(actual)
	for _, exp := range expected {
		if act, ok := m[exp.ScheduledAt.UTC().String()]; ok {
			mergeRuns = append(mergeRuns, &act)
		} else {
			mergeRuns = append(mergeRuns, exp)
		}
	}
	return mergeRuns
}

func actualRunMap(runs []*job_run.JobRunStatus) map[string]job_run.JobRunStatus {
	m := map[string]job_run.JobRunStatus{}
	for _, v := range runs {
		m[v.ScheduledAt.UTC().String()] = *v
	}
	return m
}

func filterRuns(runs []*job_run.JobRunStatus, filter map[string]struct{}) []*job_run.JobRunStatus {
	var filteredRuns []*job_run.JobRunStatus
	if len(filter) == 0 {
		return runs
	}
	for _, v := range runs {
		if _, ok := filter[v.State.String()]; ok {
			filteredRuns = append(filteredRuns, v)
		}
	}
	return filteredRuns
}

func createFilterSet(filter []string) map[string]struct{} {
	m := map[string]struct{}{}
	for _, v := range filter {
		m[models.JobRunState(v).String()] = struct{}{}
	}
	return m
}

func (s JobRunService) GetJobRuns(ctx context.Context, projectName tenant.ProjectName, jobName job_run.JobName, criteria *job_run.JobRunsCriteria) ([]*job_run.JobRunStatus, error) {

	jobWithDetails, err := s.jobRepo.GetJobDetails(ctx, projectName, jobName)
	interval := jobWithDetails.Schedule.Interval
	if interval == "" {
		return nil, fmt.Errorf("job interval not found at DB")
	}
	// jobCron
	jobCron, err := cron.ParseCronSchedule(interval)
	if err != nil {
		return nil, fmt.Errorf("unable to parse the interval from DB %w", err)
	}

	if criteria.OnlyLastRun {
		return s.scheduler.GetJobRuns(ctx, jobWithDetails.Job.Tenant, criteria, jobCron)
	}
	// validate job query
	err = validateJobQuery(criteria, *jobWithDetails)
	if err != nil {
		return nil, err
	}
	// get expected runs StartDate and EndDate inclusive
	expectedRuns := getExpectedRuns(jobCron, criteria.StartDate, criteria.EndDate)

	// call to airflow for get runs
	actualRuns, err := s.scheduler.GetJobRuns(ctx, jobWithDetails.Job.Tenant, criteria, jobCron)
	if err != nil {
		return nil, fmt.Errorf("unable to get job runs from airflow %w", err)
	}
	// mergeRuns
	totalRuns := mergeRuns(expectedRuns, actualRuns)

	// filterRuns
	result := filterRuns(totalRuns, createFilterSet(criteria.Filter))

	return result, nil
}

func (s JobRunService) registerNewJobRun(ctx context.Context, event job_run.Event) error {
	job, err := s.jobRepo.GetJobDetails(ctx, event.Tenant.ProjectName(), event.JobName)
	if err != nil {
		return err
	}
	slaDefinitionInSec, err := job.SLADuration() // TODO: add method for sla based on alerts
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
		if operatorRun.State == job_run.StateRunning.String() {
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
