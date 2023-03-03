package service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/cron"
	"github.com/goto/optimus/internal/telemetry"
)

type metricType string

func (m metricType) String() string {
	return string(m)
}

const (
	scheduleDelay metricType = "schedule_delay"
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
	Update(ctx context.Context, jobRunID uuid.UUID, endTime time.Time, jobRunStatus scheduler.State) error
	UpdateSLA(ctx context.Context, slaObjects []*scheduler.SLAObject) error
	UpdateMonitoring(ctx context.Context, jobRunID uuid.UUID, monitoring map[string]any) error
}

type OperatorRunRepository interface {
	GetOperatorRun(ctx context.Context, operatorName string, operator scheduler.OperatorType, jobRunID uuid.UUID) (*scheduler.OperatorRun, error)
	CreateOperatorRun(ctx context.Context, operatorName string, operator scheduler.OperatorType, jobRunID uuid.UUID, startTime time.Time) error
	UpdateOperatorRun(ctx context.Context, operator scheduler.OperatorType, jobRunID uuid.UUID, eventTime time.Time, state scheduler.State) error
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

func (s *JobRunService) JobRunInput(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, config scheduler.RunConfig) (*scheduler.ExecutorInput, error) {
	job, err := s.jobRepo.GetJob(ctx, projectName, jobName)
	if err != nil {
		return nil, err
	}
	// TODO: Use scheduled_at instead of executed_at for computations, for deterministic calculations
	// Todo: later, always return scheduleTime, for scheduleTimes greater than a given date
	var jobRun *scheduler.JobRun
	if config.JobRunID.IsEmpty() {
		jobRun, err = s.repo.GetByScheduledAt(ctx, job.Tenant, jobName, config.ScheduledAt)
	} else {
		jobRun, err = s.repo.GetByID(ctx, config.JobRunID)
	}
	var executedAt time.Time
	if err != nil { // Fallback for executed_at to scheduled_at
		executedAt = config.ScheduledAt
	} else {
		executedAt = jobRun.StartTime
	}
	return s.compiler.Compile(ctx, job, config, executedAt)
}

func (s *JobRunService) GetJobRuns(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, criteria *scheduler.JobRunsCriteria) ([]*scheduler.JobRunStatus, error) {
	jobWithDetails, err := s.jobRepo.GetJobDetails(ctx, projectName, jobName)
	if err != nil {
		return nil, fmt.Errorf("unable to get job details from DB for jobName: %s, project:%s,  error:%w ", jobName, projectName, err)
	}
	interval := jobWithDetails.Schedule.Interval
	if interval == "" {
		return nil, fmt.Errorf("job schedule interval not found")
	}
	// jobCron
	jobCron, err := cron.ParseCronSchedule(interval)
	if err != nil {
		return nil, fmt.Errorf("unable to parse job cron interval %w", err)
	}

	if criteria.OnlyLastRun {
		return s.scheduler.GetJobRuns(ctx, jobWithDetails.Job.Tenant, criteria, jobCron)
	}
	// validate job query
	err = validateJobQuery(criteria, jobWithDetails)
	if err != nil {
		return nil, err
	}
	// get expected runs StartDate and EndDate inclusive
	expectedRuns := getExpectedRuns(jobCron, criteria.StartDate, criteria.EndDate)

	// call to airflow for get runs
	actualRuns, err := s.scheduler.GetJobRuns(ctx, jobWithDetails.Job.Tenant, criteria, jobCron)
	if err != nil {
		s.l.Error(fmt.Sprintf("unable to get job runs from airflow err: %v", err.Error()))
		actualRuns = []*scheduler.JobRunStatus{}
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
		m[v] = struct{}{}
	}
	return m
}

func validateJobQuery(jobQuery *scheduler.JobRunsCriteria, jobWithDetails *scheduler.JobWithDetails) error {
	jobStartDate := jobWithDetails.Schedule.StartDate
	if jobStartDate.IsZero() {
		return fmt.Errorf("job schedule startDate not found in job fetched from DB")
	}
	givenStartDate := jobQuery.StartDate
	givenEndDate := jobQuery.EndDate

	if givenStartDate.Before(jobStartDate) || givenEndDate.Before(jobStartDate) {
		return fmt.Errorf("invalid date range")
	}
	return nil
}

func (s *JobRunService) registerNewJobRun(ctx context.Context, tenant tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) error {
	job, err := s.jobRepo.GetJobDetails(ctx, tenant.ProjectName(), jobName)
	if err != nil {
		return err
	}
	slaDefinitionInSec, err := job.SLADuration()
	if err != nil {
		return err
	}
	telemetry.NewGauge("total_jobs_running", map[string]string{
		"project":   tenant.ProjectName().String(),
		"namespace": tenant.NamespaceName().String(),
	}).Inc()
	err = s.repo.Create(ctx, tenant, jobName, scheduledAt, slaDefinitionInSec)
	if err != nil {
		return err
	}

	telemetry.NewCounter("scheduler_operator_durations_seconds", map[string]string{
		"project":   tenant.ProjectName().String(),
		"namespace": tenant.NamespaceName().String(),
		"type":      scheduleDelay.String(),
	}).Add(float64(time.Now().Unix() - scheduledAt.Unix()))
	return nil
}

func (s *JobRunService) getJobRunByScheduledAt(ctx context.Context, tenant tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) (*scheduler.JobRun, error) {
	var jobRun *scheduler.JobRun
	jobRun, err := s.repo.GetByScheduledAt(ctx, tenant, jobName, scheduledAt)
	if err != nil {
		if !errors.IsErrorType(err, errors.ErrNotFound) {
			return nil, err
		}
		err = s.registerNewJobRun(ctx, tenant, jobName, scheduledAt)
		if err != nil {
			return nil, err
		}
		jobRun, err = s.repo.GetByScheduledAt(ctx, tenant, jobName, scheduledAt)
		if err != nil {
			return nil, err
		}
	}
	return jobRun, nil
}

func (s *JobRunService) updateJobRun(ctx context.Context, event *scheduler.Event) error {
	var jobRun *scheduler.JobRun
	jobRun, err := s.getJobRunByScheduledAt(ctx, event.Tenant, event.JobName, event.JobScheduledAt)
	if err != nil {
		return err
	}
	for _, state := range scheduler.TaskEndStates {
		if event.Status == state {
			// this can go negative, because it is possible that when we deploy certain job have already started,
			// and the very first events we get are that of task end states, to handle this, we should treat the lowest
			// value as the base value.
			telemetry.NewGauge("total_jobs_running", map[string]string{
				"project":   event.Tenant.ProjectName().String(),
				"namespace": event.Tenant.NamespaceName().String(),
			}).Dec()
			break
		}
	}
	if err := s.repo.Update(ctx, jobRun.ID, event.EventTime, event.Status); err != nil {
		return err
	}
	monitoringValues := s.getMonitoringValues(event)
	return s.repo.UpdateMonitoring(ctx, jobRun.ID, monitoringValues)
}

func (*JobRunService) getMonitoringValues(event *scheduler.Event) map[string]any {
	var output map[string]any
	if value, ok := event.Values["monitoring"]; ok && value != nil {
		output, _ = value.(map[string]any)
	}
	return output
}

func (s *JobRunService) updateJobRunSLA(ctx context.Context, event *scheduler.Event) error {
	if len(event.SLAObjectList) > 0 {
		return s.repo.UpdateSLA(ctx, event.SLAObjectList)
	}
	return nil
}

func (s *JobRunService) createOperatorRun(ctx context.Context, event *scheduler.Event, operatorType scheduler.OperatorType) error {
	jobRun, err := s.getJobRunByScheduledAt(ctx, event.Tenant, event.JobName, event.JobScheduledAt)
	if err != nil {
		return err
	}
	if operatorType == scheduler.OperatorTask {
		telemetry.NewGauge("count_running_tasks", map[string]string{
			"project":   event.Tenant.ProjectName().String(),
			"namespace": event.Tenant.NamespaceName().String(),
			"type":      event.OperatorName,
		}).Inc()
	}
	return s.operatorRunRepo.CreateOperatorRun(ctx, event.OperatorName, operatorType, jobRun.ID, event.EventTime)
}

func (s *JobRunService) getOperatorRun(ctx context.Context, event *scheduler.Event, operatorType scheduler.OperatorType, jobRunID uuid.UUID) (*scheduler.OperatorRun, error) {
	var operatorRun *scheduler.OperatorRun
	operatorRun, err := s.operatorRunRepo.GetOperatorRun(ctx, event.OperatorName, operatorType, jobRunID)
	if err != nil {
		if !errors.IsErrorType(err, errors.ErrNotFound) {
			return nil, err
		}
		err = s.createOperatorRun(ctx, event, operatorType)
		if err != nil {
			return nil, err
		}
		operatorRun, err = s.operatorRunRepo.GetOperatorRun(ctx, event.OperatorName, operatorType, jobRunID)
		if err != nil {
			return nil, err
		}
	}
	return operatorRun, nil
}

func (s *JobRunService) updateOperatorRun(ctx context.Context, event *scheduler.Event, operatorType scheduler.OperatorType) error {
	jobRun, err := s.getJobRunByScheduledAt(ctx, event.Tenant, event.JobName, event.JobScheduledAt)
	if err != nil {
		return err
	}
	operatorRun, err := s.getOperatorRun(ctx, event, operatorType, jobRun.ID)
	if err != nil {
		return err
	}
	if operatorType == scheduler.OperatorTask {
		for _, state := range scheduler.TaskEndStates {
			if event.Status == state {
				// this can go negative, because it is possible that when we deploy certain job have already started,
				// and the very first events we get are that of task end states, to handle this, we should treat the lowest
				// value as the base value.
				telemetry.NewGauge("count_running_tasks", map[string]string{
					"project":   event.Tenant.ProjectName().String(),
					"namespace": event.Tenant.NamespaceName().String(),
					"type":      event.OperatorName,
				}).Dec()
				break
			}
		}
	}
	err = s.operatorRunRepo.UpdateOperatorRun(ctx, operatorType, operatorRun.ID, event.EventTime, event.Status)
	if err != nil {
		return err
	}
	telemetry.NewCounter("scheduler_operator_durations_seconds", map[string]string{
		"project":   event.Tenant.ProjectName().String(),
		"namespace": event.Tenant.NamespaceName().String(),
		"type":      operatorType.String(),
	}).Add(float64(event.EventTime.Unix() - operatorRun.StartTime.Unix()))
	return nil
}

func (s *JobRunService) trackEvent(event *scheduler.Event) {
	if event.Type.IsOfType(scheduler.EventCategorySLAMiss) {
		s.l.Debug(fmt.Sprintf("received event: %v, jobName: %v , slaPayload: %#v",
			event.Type, event.JobName, event.SLAObjectList))
	} else {
		s.l.Debug(fmt.Sprintf("received event: %v, eventTime: %s, jobName: %v, Operator: %v, schedule: %s, status: %s",
			event.Type, event.EventTime.Format("01/02/06 15:04:05 MST"), event.JobName, event.OperatorName, event.JobScheduledAt.Format("01/02/06 15:04:05 MST"), event.Status))
	}
	telemetry.NewGauge("scheduler_events", map[string]string{
		"project":   event.Tenant.ProjectName().String(),
		"namespace": event.Tenant.NamespaceName().String(),
		"type":      event.Type.String(),
	}).Inc()
}

func (s *JobRunService) UpdateJobState(ctx context.Context, event *scheduler.Event) error {
	s.trackEvent(event)

	switch event.Type {
	case scheduler.SLAMissEvent:
		return s.updateJobRunSLA(ctx, event)
	case scheduler.JobSuccessEvent, scheduler.JobFailureEvent:
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

func NewJobRunService(logger log.Logger, jobRepo JobRepository, jobRunRepo JobRunRepository,
	operatorRunRepo OperatorRunRepository, scheduler Scheduler, resolver PriorityResolver, compiler JobInputCompiler,
) *JobRunService {
	return &JobRunService{
		l:                logger,
		repo:             jobRunRepo,
		operatorRunRepo:  operatorRunRepo,
		scheduler:        scheduler,
		jobRepo:          jobRepo,
		priorityResolver: resolver,
		compiler:         compiler,
	}
}
