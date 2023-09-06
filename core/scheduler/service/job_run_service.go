package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/event"
	"github.com/goto/optimus/core/event/moderator"
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

	metricJobRunEvents = "jobrun_events_total"
)

type JobRepository interface {
	GetJob(ctx context.Context, name tenant.ProjectName, jobName scheduler.JobName) (*scheduler.Job, error)
	GetJobDetails(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName) (*scheduler.JobWithDetails, error)
	GetAll(ctx context.Context, projectName tenant.ProjectName) ([]*scheduler.JobWithDetails, error)
	GetJobs(ctx context.Context, projectName tenant.ProjectName, jobs []string) ([]*scheduler.JobWithDetails, error)
}

type JobRunRepository interface {
	GetByID(ctx context.Context, id scheduler.JobRunID) (*scheduler.JobRun, error)
	GetByScheduledAt(ctx context.Context, tenant tenant.Tenant, name scheduler.JobName, scheduledAt time.Time) (*scheduler.JobRun, error)
	GetByScheduledTimes(ctx context.Context, tenant tenant.Tenant, jobName scheduler.JobName, scheduledTimes []time.Time) ([]*scheduler.JobRun, error)
	Create(ctx context.Context, tenant tenant.Tenant, name scheduler.JobName, scheduledAt time.Time, slaDefinitionInSec int64) error
	Update(ctx context.Context, jobRunID uuid.UUID, endTime time.Time, jobRunStatus scheduler.State) error
	UpdateState(ctx context.Context, jobRunID uuid.UUID, jobRunStatus scheduler.State) error
	UpdateSLA(ctx context.Context, jobName scheduler.JobName, project tenant.ProjectName, scheduledTimes []time.Time) error
	UpdateMonitoring(ctx context.Context, jobRunID uuid.UUID, monitoring map[string]any) error
}

type JobReplayRepository interface {
	GetReplayJobConfig(ctx context.Context, jobTenant tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) (map[string]string, error)
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

type EventHandler interface {
	HandleEvent(moderator.Event)
}

type JobRunService struct {
	l                log.Logger
	repo             JobRunRepository
	replayRepo       JobReplayRepository
	operatorRunRepo  OperatorRunRepository
	eventHandler     EventHandler
	scheduler        Scheduler
	jobRepo          JobRepository
	priorityResolver PriorityResolver
	compiler         JobInputCompiler
}

func (s *JobRunService) JobRunInput(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, config scheduler.RunConfig) (*scheduler.ExecutorInput, error) {
	job, err := s.jobRepo.GetJob(ctx, projectName, jobName)
	if err != nil {
		s.l.Error("error getting job [%s]: %s", jobName, err)
		return nil, err
	}
	// TODO: Use scheduled_at instead of executed_at for computations, for deterministic calculations
	// Todo: later, always return scheduleTime, for scheduleTimes greater than a given date
	var jobRun *scheduler.JobRun
	if config.JobRunID.IsEmpty() {
		s.l.Warn("getting job run by scheduled at")
		jobRun, err = s.repo.GetByScheduledAt(ctx, job.Tenant, jobName, config.ScheduledAt)
	} else {
		s.l.Warn("getting job run by id")
		jobRun, err = s.repo.GetByID(ctx, config.JobRunID)
	}

	var executedAt time.Time
	if err != nil { // Fallback for executed_at to scheduled_at
		executedAt = config.ScheduledAt
		s.l.Warn("suppressed error is encountered when getting job run: %s", err)
	} else {
		executedAt = jobRun.StartTime
	}
	// Additional task config from existing replay
	replayJobConfig, err := s.replayRepo.GetReplayJobConfig(ctx, job.Tenant, job.Name, config.ScheduledAt)
	if err != nil {
		s.l.Error("error getting replay job config from db: %s", err)
		return nil, err
	}
	for k, v := range replayJobConfig {
		job.Task.Config[k] = v
	}

	return s.compiler.Compile(ctx, job, config, executedAt)
}

func (s *JobRunService) GetJobRuns(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, criteria *scheduler.JobRunsCriteria) ([]*scheduler.JobRunStatus, error) {
	jobWithDetails, err := s.jobRepo.GetJobDetails(ctx, projectName, jobName)
	if err != nil {
		msg := fmt.Sprintf("unable to get job details for jobName: %s, project:%s", jobName, projectName)
		s.l.Error(msg)
		return nil, errors.AddErrContext(err, scheduler.EntityJobRun, msg)
	}
	interval := jobWithDetails.Schedule.Interval
	if interval == "" {
		s.l.Error("job schedule interval is empty")
		return nil, errors.InvalidArgument(scheduler.EntityJobRun, "cannot get job runs, job interval is empty")
	}
	jobCron, err := cron.ParseCronSchedule(interval)
	if err != nil {
		msg := fmt.Sprintf("unable to parse job cron interval: %s", err)
		s.l.Error(msg)
		return nil, errors.InternalError(scheduler.EntityJobRun, msg, nil)
	}

	if criteria.OnlyLastRun {
		s.l.Warn("getting last run only")
		return s.scheduler.GetJobRuns(ctx, jobWithDetails.Job.Tenant, criteria, jobCron)
	}
	err = validateJobQuery(criteria, jobWithDetails)
	if err != nil {
		s.l.Error("invalid job query: %s", err)
		return nil, err
	}
	expectedRuns := getExpectedRuns(jobCron, criteria.StartDate, criteria.EndDate)

	actualRuns, err := s.scheduler.GetJobRuns(ctx, jobWithDetails.Job.Tenant, criteria, jobCron)
	if err != nil {
		s.l.Error("unable to get job runs from airflow err: %s", err)
		actualRuns = []*scheduler.JobRunStatus{}
	}
	totalRuns := mergeRuns(expectedRuns, actualRuns)

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
	var merged []*scheduler.JobRunStatus
	m := actualRunMap(actual)
	for _, exp := range expected {
		if act, ok := m[exp.ScheduledAt.UTC().String()]; ok {
			merged = append(merged, &act)
		} else {
			merged = append(merged, exp)
		}
	}
	return merged
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
		return errors.InternalError(scheduler.EntityJobRun, "job schedule startDate not found in job", nil)
	}
	if jobQuery.StartDate.Before(jobStartDate) || jobQuery.EndDate.Before(jobStartDate) {
		return errors.InvalidArgument(scheduler.EntityJobRun, "invalid date range, interval contains dates before job start")
	}
	return nil
}

func (s *JobRunService) registerNewJobRun(ctx context.Context, tenant tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) error {
	job, err := s.jobRepo.GetJobDetails(ctx, tenant.ProjectName(), jobName)
	if err != nil {
		s.l.Error("error getting job details for job [%s]: %s", jobName, err)
		return err
	}
	slaDefinitionInSec, err := job.SLADuration()
	if err != nil {
		s.l.Error("error getting sla duration: %s", err)
		return err
	}
	err = s.repo.Create(ctx, tenant, jobName, scheduledAt, slaDefinitionInSec)
	if err != nil {
		s.l.Error("error creating job run: %s", err)
		return err
	}

	telemetry.NewGauge("jobrun_durations_breakdown_seconds", map[string]string{
		"project":   tenant.ProjectName().String(),
		"namespace": tenant.NamespaceName().String(),
		"job":       jobName.String(),
		"type":      scheduleDelay.String(),
	}).Set(float64(time.Now().Unix() - scheduledAt.Unix()))
	return nil
}

func (s *JobRunService) getJobRunByScheduledAt(ctx context.Context, tenant tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) (*scheduler.JobRun, error) {
	var jobRun *scheduler.JobRun
	jobRun, err := s.repo.GetByScheduledAt(ctx, tenant, jobName, scheduledAt)
	if err != nil {
		if !errors.IsErrorType(err, errors.ErrNotFound) {
			s.l.Error("error getting job run by scheduled at: %s", err)
			return nil, err
		}
		// TODO: consider moving below call outside as the caller is a 'getter'
		err = s.registerNewJobRun(ctx, tenant, jobName, scheduledAt)
		if err != nil {
			s.l.Error("error registering new job run: %s", err)
			return nil, err
		}
		jobRun, err = s.repo.GetByScheduledAt(ctx, tenant, jobName, scheduledAt)
		if err != nil {
			s.l.Error("error getting the registered job run: %s", err)
			return nil, err
		}
	}
	return jobRun, nil
}

func (s *JobRunService) updateJobRun(ctx context.Context, event *scheduler.Event) error {
	var jobRun *scheduler.JobRun
	jobRun, err := s.getJobRunByScheduledAt(ctx, event.Tenant, event.JobName, event.JobScheduledAt)
	if err != nil {
		s.l.Error("error getting job run by schedule time [%s]: %s", event.JobScheduledAt, err)
		return err
	}
	if err := s.repo.Update(ctx, jobRun.ID, event.EventTime, event.Status); err != nil {
		s.l.Error("error updating job run with id [%s]: %s", jobRun.ID, err)
		return err
	}
	jobRun.State = event.Status
	s.raiseJobRunStateChangeEvent(jobRun)
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
	if len(event.SLAObjectList) < 1 {
		return nil
	}
	var scheduleTimesList []time.Time
	for _, SLAObject := range event.SLAObjectList {
		scheduleTimesList = append(scheduleTimesList, SLAObject.JobScheduledAt)
	}
	jobRuns, err := s.repo.GetByScheduledTimes(ctx, event.Tenant, event.JobName, scheduleTimesList)
	if err != nil {
		s.l.Error("error getting job runs by schedule time", err)
		return err
	}

	var slaBreachedJobRunScheduleTimes []time.Time
	var filteredSLAObject []*scheduler.SLAObject
	for _, jobRun := range jobRuns {
		if !jobRun.HasSLABreached() {
			s.l.Error("received sla miss callback for job run that has not breached SLA, jobName: %s, scheduled_at: %s, start_time: %s, end_time: %s, SLA definition: %s",
				jobRun.JobName, jobRun.ScheduledAt.String(), jobRun.StartTime, jobRun.EndTime, time.Second*time.Duration(jobRun.SLADefinition))
			continue
		}
		filteredSLAObject = append(filteredSLAObject, &scheduler.SLAObject{
			JobName:        jobRun.JobName,
			JobScheduledAt: jobRun.ScheduledAt,
		})
		slaBreachedJobRunScheduleTimes = append(slaBreachedJobRunScheduleTimes, jobRun.ScheduledAt)
	}

	event.SLAObjectList = filteredSLAObject

	err = s.repo.UpdateSLA(ctx, event.JobName, event.Tenant.ProjectName(), slaBreachedJobRunScheduleTimes)
	if err != nil {
		s.l.Error("error updating job run sla status", err)
		return err
	}
	telemetry.NewCounter(metricJobRunEvents, map[string]string{
		"project":   event.Tenant.ProjectName().String(),
		"namespace": event.Tenant.NamespaceName().String(),
		"name":      event.JobName.String(),
		"status":    scheduler.SLAMissEvent.String(),
	}).Inc()
	return nil
}

func operatorStartToJobState(operatorType scheduler.OperatorType) (scheduler.State, error) {
	switch operatorType {
	case scheduler.OperatorTask:
		return scheduler.StateInProgress, nil
	case scheduler.OperatorSensor:
		return scheduler.StateWaitUpstream, nil
	case scheduler.OperatorHook:
		return scheduler.StateInProgress, nil
	default:
		return "", errors.InvalidArgument(scheduler.EntityJobRun, "Invalid operator type")
	}
}

func (s *JobRunService) raiseJobRunStateChangeEvent(jobRun *scheduler.JobRun) {
	var schedulerEvent moderator.Event
	var err error
	switch jobRun.State {
	case scheduler.StateWaitUpstream:
		schedulerEvent, err = event.NewJobRunWaitUpstreamEvent(jobRun)
	case scheduler.StateInProgress:
		schedulerEvent, err = event.NewJobRunInProgressEvent(jobRun)
	case scheduler.StateSuccess:
		schedulerEvent, err = event.NewJobRunSuccessEvent(jobRun)
	case scheduler.StateFailed:
		schedulerEvent, err = event.NewJobRunFailedEvent(jobRun)
	default:
		s.l.Error("state [%s] is unrecognized, event is not published", jobRun.State)
		return
	}
	if err != nil {
		s.l.Error("error creating event for job run state change : %s", err)
		return
	}
	s.eventHandler.HandleEvent(schedulerEvent)
	telemetry.NewCounter(metricJobRunEvents, map[string]string{
		"project":   jobRun.Tenant.ProjectName().String(),
		"namespace": jobRun.Tenant.NamespaceName().String(),
		"name":      jobRun.JobName.String(),
		"status":    jobRun.State.String(),
	}).Inc()
}

func (s *JobRunService) createOperatorRun(ctx context.Context, event *scheduler.Event, operatorType scheduler.OperatorType) error {
	jobRun, err := s.getJobRunByScheduledAt(ctx, event.Tenant, event.JobName, event.JobScheduledAt)
	if err != nil {
		s.l.Error("error getting job run by scheduled time [%s]: %s", event.JobScheduledAt, err)
		return err
	}
	jobState, err := operatorStartToJobState(operatorType)
	if err != nil {
		s.l.Error("error converting operator to job state: %s", err)
		return err
	}
	if jobRun.State != jobState {
		err := s.repo.UpdateState(ctx, jobRun.ID, jobState)
		if err != nil {
			s.l.Error("error updating state for job run id [%d] to [%s]: %s", jobRun.ID, jobState, err)
			return err
		}
		jobRun.State = jobState
		s.raiseJobRunStateChangeEvent(jobRun)
	}

	return s.operatorRunRepo.CreateOperatorRun(ctx, event.OperatorName, operatorType, jobRun.ID, event.EventTime)
}

func (s *JobRunService) getOperatorRun(ctx context.Context, event *scheduler.Event, operatorType scheduler.OperatorType, jobRunID uuid.UUID) (*scheduler.OperatorRun, error) {
	var operatorRun *scheduler.OperatorRun
	operatorRun, err := s.operatorRunRepo.GetOperatorRun(ctx, event.OperatorName, operatorType, jobRunID)
	if err != nil {
		if !errors.IsErrorType(err, errors.ErrNotFound) {
			s.l.Error("error getting operator for job run [%s]: %s", jobRunID, err)
			return nil, err
		}
		s.l.Warn("operator is not found, creating it")

		// TODO: consider moving below call outside as the caller is a 'getter'
		err = s.createOperatorRun(ctx, event, operatorType)
		if err != nil {
			s.l.Error("error creating operator run: %s", err)
			return nil, err
		}
		operatorRun, err = s.operatorRunRepo.GetOperatorRun(ctx, event.OperatorName, operatorType, jobRunID)
		if err != nil {
			s.l.Error("error getting the registered operator run: %s", err)
			return nil, err
		}
	}
	return operatorRun, nil
}

func (s *JobRunService) updateOperatorRun(ctx context.Context, event *scheduler.Event, operatorType scheduler.OperatorType) error {
	jobRun, err := s.getJobRunByScheduledAt(ctx, event.Tenant, event.JobName, event.JobScheduledAt)
	if err != nil {
		s.l.Error("error getting job run by scheduled time [%s]: %s", event.JobScheduledAt, err)
		return err
	}
	operatorRun, err := s.getOperatorRun(ctx, event, operatorType, jobRun.ID)
	if err != nil {
		s.l.Error("error getting operator for job run id [%s]: %s", jobRun.ID, err)
		return err
	}
	err = s.operatorRunRepo.UpdateOperatorRun(ctx, operatorType, operatorRun.ID, event.EventTime, event.Status)
	if err != nil {
		s.l.Error("error updating operator run id [%s]: %s", operatorRun.ID, err)
		return err
	}
	telemetry.NewGauge("jobrun_durations_breakdown_seconds", map[string]string{
		"project":   event.Tenant.ProjectName().String(),
		"namespace": event.Tenant.NamespaceName().String(),
		"job":       event.JobName.String(),
		"type":      operatorType.String(),
	}).Set(float64(event.EventTime.Unix() - operatorRun.StartTime.Unix()))
	return nil
}

func (s *JobRunService) trackEvent(event *scheduler.Event) {
	if event.Type.IsOfType(scheduler.EventCategorySLAMiss) {
		jsonSLAObjectList, err := json.Marshal(event.SLAObjectList)
		if err != nil {
			jsonSLAObjectList = []byte("unable to json Marshal SLAObjectList")
		}
		s.l.Info("received job sla_miss event, jobName: %v , slaPayload: %s", event.JobName, string(jsonSLAObjectList))
	} else {
		s.l.Info("received event: %v, eventTime: %s, jobName: %v, Operator: %v, schedule: %s, status: %s",
			event.Type, event.EventTime.Format("01/02/06 15:04:05 MST"), event.JobName, event.OperatorName, event.JobScheduledAt.Format("01/02/06 15:04:05 MST"), event.Status)
	}

	if event.Type == scheduler.SensorStartEvent || event.Type == scheduler.SensorRetryEvent || event.Type == scheduler.SensorSuccessEvent || event.Type == scheduler.SensorFailEvent {
		eventType := strings.TrimPrefix(event.Type.String(), fmt.Sprintf("%s_", scheduler.OperatorSensor))
		telemetry.NewCounter("jobrun_sensor_events_total", map[string]string{
			"project":    event.Tenant.ProjectName().String(),
			"namespace":  event.Tenant.NamespaceName().String(),
			"event_type": eventType,
		}).Inc()
		return
	}
	if event.Type == scheduler.TaskStartEvent || event.Type == scheduler.TaskRetryEvent || event.Type == scheduler.TaskSuccessEvent || event.Type == scheduler.TaskFailEvent {
		eventType := strings.TrimPrefix(event.Type.String(), fmt.Sprintf("%s_", scheduler.OperatorTask))
		telemetry.NewCounter("jobrun_task_events_total", map[string]string{
			"project":    event.Tenant.ProjectName().String(),
			"namespace":  event.Tenant.NamespaceName().String(),
			"event_type": eventType,
			"operator":   event.OperatorName,
		}).Inc()
		return
	}
	if event.Type == scheduler.HookStartEvent || event.Type == scheduler.HookRetryEvent || event.Type == scheduler.HookSuccessEvent || event.Type == scheduler.HookFailEvent {
		eventType := strings.TrimPrefix(event.Type.String(), fmt.Sprintf("%s_", scheduler.OperatorHook))
		telemetry.NewCounter("jobrun_hook_events_total", map[string]string{
			"project":    event.Tenant.ProjectName().String(),
			"namespace":  event.Tenant.NamespaceName().String(),
			"event_type": eventType,
			"operator":   event.OperatorName,
		}).Inc()
	}
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

func NewJobRunService(logger log.Logger, jobRepo JobRepository, jobRunRepo JobRunRepository, replayRepo JobReplayRepository,
	operatorRunRepo OperatorRunRepository, scheduler Scheduler, resolver PriorityResolver, compiler JobInputCompiler, eventHandler EventHandler,
) *JobRunService {
	return &JobRunService{
		l:                logger,
		repo:             jobRunRepo,
		operatorRunRepo:  operatorRunRepo,
		scheduler:        scheduler,
		eventHandler:     eventHandler,
		replayRepo:       replayRepo,
		jobRepo:          jobRepo,
		priorityResolver: resolver,
		compiler:         compiler,
	}
}
