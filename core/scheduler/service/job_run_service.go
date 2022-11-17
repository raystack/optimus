package service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/salt/log"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/internal/lib/cron"
	"github.com/odpf/optimus/models"
)

type JobRepository interface {
	GetJob(ctx context.Context, name tenant.ProjectName, jobName scheduler.JobName) (*scheduler.Job, error)
	GetJobDetails(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName) (*scheduler.JobWithDetails, error)
	GetAll(ctx context.Context, projectName tenant.ProjectName) ([]*scheduler.JobWithDetails, error)
}

type JobRunRepository interface {
	GetByID(ctx context.Context, id scheduler.JobRunID) (*scheduler.JobRun, error)
	GetByScheduledAt(ctx context.Context, tenant tenant.Tenant, name scheduler.JobName, scheduledAt time.Time) (*scheduler.JobRun, error)
	Create(ctx context.Context, tenant tenant.Tenant, name scheduler.JobName, scheduledAt time.Time, slaDefinitionInSec int64) error
	Update(ctx context.Context, jobRunID uuid.UUID, endTime time.Time, jobRunStatus string) error
}

type OperatorRunRepository interface {
	GetOperatorRun(ctx context.Context, operatorName string, operator scheduler.OperatorType, jobRunID uuid.UUID) (*scheduler.OperatorRun, error)
	CreateOperatorRun(ctx context.Context, operatorName string, operator scheduler.OperatorType, jobRunID uuid.UUID, startTime time.Time) error
	UpdateOperatorRun(ctx context.Context, operator scheduler.OperatorType, jobRunID uuid.UUID, eventTime time.Time, state string) error
}
type JobInputCompiler interface {
	Compile(ctx context.Context, job *scheduler.Job, config scheduler.RunConfig, executedAt time.Time) (*scheduler.ExecutorInput, error)
}

type PriorityResolver interface {
	Resolve(context.Context, []*scheduler.JobWithDetails) error
}

type Scheduler interface {
	GetJobRuns(ctx context.Context, t tenant.Tenant, criteria *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error)
	DeployJobs(ctx context.Context, t tenant.Tenant, jobs []*scheduler.JobWithDetails) error
	ListJobs(ctx context.Context, t tenant.Tenant) ([]string, error)
	DeleteJobs(ctx context.Context, t tenant.Tenant, jobsToDelete []string) error
}

type JobRunService struct {
	l                log.Logger
	repo             JobRunRepository
	operatorRunRepo  OperatorRunRepository
	scheduler        Scheduler
	jobRepo          JobRepository
	priorityResolver PriorityResolver
	compiler         JobInputCompiler
}

func (s JobRunService) JobRunInput(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, config scheduler.RunConfig) (*scheduler.ExecutorInput, error) {
	job, err := s.jobRepo.GetJob(ctx, projectName, jobName)
	if err != nil {
		return nil, err
	}

	// TODO: Use scheduled_at instead of executed_at for computations, for deterministic calculations
	var jobRun *scheduler.JobRun
	if config.JobRunID.IsEmpty() {
		jobRun, err = s.repo.GetByScheduledAt(ctx, job.Tenant, jobName, config.ScheduledAt)
	} else {
		jobRun, err = s.repo.GetByID(ctx, config.JobRunID)
	}

	var executedAt time.Time
	if err != nil { // Fallback for executed_at to scheduled_at
		if !errors.IsErrorType(err, errors.ErrNotFound) {
			return nil, err
		}
		executedAt = config.ScheduledAt
	} else {
		executedAt = jobRun.StartTime
	}

	return s.compiler.Compile(ctx, job, config, executedAt)
}

func (s JobRunService) GetJobRuns(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, criteria *scheduler.JobRunsCriteria) ([]*scheduler.JobRunStatus, error) {
	jobWithDetails, err := s.jobRepo.GetJobDetails(ctx, projectName, jobName)
	if err != nil {
		return nil, fmt.Errorf("unable to get job details from DB for jobName: %s, project:%s,  error:%w ", jobName, projectName, err)
	}
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

func getExpectedRuns(spec *cron.ScheduleSpec, startTime, endTime time.Time) []*scheduler.JobRunStatus {
	var jobRuns []*scheduler.JobRunStatus
	start := spec.Next(startTime.Add(-time.Second * 1))
	end := endTime
	exit := spec.Next(end)
	for !start.Equal(exit) {
		jobRuns = append(jobRuns, &scheduler.JobRunStatus{
			State:       scheduler.StatePending,
			ScheduledAt: start,
		})
		start = spec.Next(start)
	}
	return jobRuns
}

func mergeRuns(expected, actual []*scheduler.JobRunStatus) []*scheduler.JobRunStatus {
	var mergeRuns []*scheduler.JobRunStatus
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

func actualRunMap(runs []*scheduler.JobRunStatus) map[string]scheduler.JobRunStatus {
	m := map[string]scheduler.JobRunStatus{}
	for _, v := range runs {
		m[v.ScheduledAt.UTC().String()] = *v
	}
	return m
}

func filterRuns(runs []*scheduler.JobRunStatus, filter map[string]struct{}) []*scheduler.JobRunStatus {
	var filteredRuns []*scheduler.JobRunStatus
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

func validateJobQuery(jobQuery *scheduler.JobRunsCriteria, jobWithDetails scheduler.JobWithDetails) error {
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

func (s JobRunService) registerNewJobRun(ctx context.Context, event scheduler.Event) error {
	job, err := s.jobRepo.GetJobDetails(ctx, event.Tenant.ProjectName(), event.JobName)
	if err != nil {
		return err
	}
	slaDefinitionInSec, err := job.SLADuration() // TODO: add method for sla based on alerts
	if err != nil {
		return err
	}

	scheduledAtTimeStamp, err := time.Parse(scheduler.ISODateFormat, event.Values["scheduled_at"].(string))
	if err != nil {
		return err
	}
	return s.repo.Create(ctx,
		event.Tenant,
		event.JobName,
		scheduledAtTimeStamp,
		slaDefinitionInSec)
}

func (s JobRunService) updateJobRun(ctx context.Context, event scheduler.Event) error {
	scheduledAtTimeStamp, err := time.Parse(scheduler.ISODateFormat, event.Values["scheduled_at"].(string))
	if err != nil {
		return err
	}
	var jobRun *scheduler.JobRun
	jobRun, err = s.repo.GetByScheduledAt(ctx,
		event.Tenant,
		event.JobName,
		scheduledAtTimeStamp)
	if err != nil {
		if !errors.IsErrorType(err, errors.ErrNotFound) {
			return err
		}
		err = s.registerNewJobRun(ctx, event)
		if err != nil {
			return err
		}
		jobRun, err = s.repo.GetByScheduledAt(ctx,
			event.Tenant,
			event.JobName,
			scheduledAtTimeStamp)
		if err != nil {
			return err
		} // todo: ask sandeep should this be done
	}
	jobRunStatus := event.Values["status"].(string)
	endTime := time.Unix(event.Values["event_time"].(int64), 0)

	return s.repo.Update(ctx,
		jobRun.ID,
		endTime,
		jobRunStatus,
	)
}

func (s JobRunService) createOperatorRun(ctx context.Context, event scheduler.Event, operatorType scheduler.OperatorType) error {
	scheduledAtTimeStamp, err := time.Parse(scheduler.ISODateFormat, event.Values["scheduled_at"].(string))
	if err != nil {
		return err
	}

	jobRun, err := s.repo.GetByScheduledAt(ctx,
		event.Tenant,
		event.JobName,
		scheduledAtTimeStamp)
	if err != nil {
		return err
	}

	startedAtTimeStamp := time.Unix(int64(event.Values["event_time"].(int64)), 0)
	operatorName := event.Values[scheduler.OperatorNameKey].(string)

	operatorRun, err := s.operatorRunRepo.GetOperatorRun(ctx, operatorName, operatorType, jobRun.ID)
	if err == nil {
		if !errors.IsErrorType(err, errors.ErrNotFound) {
			return err
		}
	} else {
		if operatorRun.State == scheduler.StateRunning.String() {
			// operator run exists but is not yet finished
			return nil
		}
	}

	return s.operatorRunRepo.CreateOperatorRun(ctx,
		operatorName,
		operatorType,
		jobRun.ID,
		startedAtTimeStamp)
}

func (s JobRunService) updateOperatorRun(ctx context.Context, event scheduler.Event, operatorType scheduler.OperatorType) error {
	scheduledAtTimeStamp, err := time.Parse(scheduler.ISODateFormat, event.Values["scheduled_at"].(string))
	if err != nil {
		return err
	}
	jobRun, err := s.repo.GetByScheduledAt(ctx,
		event.Tenant,
		event.JobName,
		scheduledAtTimeStamp)
	if err != nil {
		return err
	}
	operatorName := event.Values[scheduler.OperatorNameKey].(string)
	operatorRun, err := s.operatorRunRepo.GetOperatorRun(ctx, operatorName, operatorType, jobRun.ID)
	if err != nil {
		return err
		//	todo: should i create a new operator run row here @sandeep
	}
	status := event.Values["status"].(string)
	endTime := time.Unix(event.Values["event_time"].(int64), 0)

	return s.operatorRunRepo.UpdateOperatorRun(ctx,
		operatorType,
		operatorRun.ID,
		endTime,
		status)
}

func (s JobRunService) UpdateJobState(ctx context.Context, event scheduler.Event) error {
	switch event.Type {
	case scheduler.JobStartEvent:
		return s.registerNewJobRun(ctx, event)
	case scheduler.JobSuccessEvent, scheduler.JobFailEvent:
		return s.updateJobRun(ctx, event)
	case scheduler.TaskStartEvent:
		return s.createOperatorRun(ctx, event, scheduler.OperatorTask)
	case scheduler.TaskSuccessEvent, scheduler.TaskRetryEvent, scheduler.TaskFailEvent:
		return s.updateOperatorRun(ctx, event, scheduler.OperatorTask)
	case scheduler.SensorStartEvent:
		return s.createOperatorRun(ctx, event, scheduler.OperatorSensor)
	case scheduler.SensorSuccessEvent, scheduler.SensorRetryEvent, scheduler.SensorFailEvent:
		return s.updateOperatorRun(ctx, event, scheduler.OperatorSensor)
	case scheduler.HookStartEvent:
		return s.createOperatorRun(ctx, event, scheduler.OperatorHook)
	case scheduler.HookSuccessEvent, scheduler.HookRetryEvent, scheduler.HookFailEvent:
		return s.updateOperatorRun(ctx, event, scheduler.OperatorHook)
	default:
		return errors.InvalidArgument(scheduler.EntityEvent, "invalid event type: "+string(event.Type))
	}
}

func NewJobRunService(logger log.Logger, jobRepo JobRepository, scheduler Scheduler) *JobRunService {
	return &JobRunService{
		l:                logger,
		repo:             nil,
		operatorRunRepo:  nil,
		scheduler:        scheduler,
		jobRepo:          jobRepo,
		priorityResolver: nil,
		compiler:         nil,
	}
}
