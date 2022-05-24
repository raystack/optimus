package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/odpf/optimus/core/cron"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type JobRunService interface {
	// GetScheduledRun find if already present or create a new scheduled run
	GetScheduledRun(ctx context.Context, namespace models.NamespaceSpec, JobID models.JobSpec, scheduledAt time.Time) (models.JobRun, error)

	// GetByID returns job run, normally gets requested for manual runs
	GetByID(ctx context.Context, JobRunID uuid.UUID) (models.JobRun, models.NamespaceSpec, error)

	// Register creates a new instance in provided job run
	Register(ctx context.Context, namespace models.NamespaceSpec, jobRun models.JobRun, instanceType models.InstanceType, instanceName string) (models.InstanceSpec, error)

	// GetJobRunList returns all the job based given status and date range
	GetJobRunList(ctx context.Context, projectSpec models.ProjectSpec, jobSpec models.JobSpec, jobQuery *models.JobQuery) ([]models.JobRun, error)
}

type jobRunService struct {
	jobRunRepo    store.JobRunRepository
	scheduler     models.SchedulerUnit
	Now           func() time.Time
	pluginService PluginService
}

func (s *jobRunService) GetScheduledRun(ctx context.Context, namespace models.NamespaceSpec, jobSpec models.JobSpec,
	scheduledAt time.Time) (models.JobRun, error) {
	newJobRun := models.JobRun{
		Spec:        jobSpec,
		Trigger:     models.TriggerSchedule,
		Status:      models.RunStatePending,
		ScheduledAt: scheduledAt,
		ExecutedAt:  s.Now(),
	}

	jobRun, _, err := s.jobRunRepo.GetByScheduledAt(ctx, jobSpec.ID, scheduledAt)
	if err != nil && !errors.Is(err, store.ErrResourceNotFound) {
		// When err exists and is not "NotFound"
		return models.JobRun{}, err
	}
	if err == nil {
		// if already exists, use the same id for in place update
		// because job spec might have changed by now, status needs to be reset
		newJobRun.ID = jobRun.ID

		// If existing job run found, use its time.
		// This might be a retry of existing instances and whole pipeline(of instances)
		// would like to inherit same run level variable even though it might be triggered
		// more than once.
		newJobRun.ExecutedAt = jobRun.ExecutedAt
	}
	jobDestinationResponse, err := s.pluginService.GenerateDestination(ctx, jobSpec, namespace)
	if err != nil {
		if !errors.Is(err, ErrDependencyModNotFound) {
			return models.JobRun{}, fmt.Errorf("failed to GenerateDestination for job: %s: %w", jobSpec.Name, err)
		}
	}
	var jobDestination string
	if jobDestinationResponse != nil {
		jobDestination = jobDestinationResponse.URN()
	}
	if err := s.jobRunRepo.Save(ctx, namespace, newJobRun, jobDestination); err != nil {
		return models.JobRun{}, err
	}

	jobRun, _, err = s.jobRunRepo.GetByScheduledAt(ctx, jobSpec.ID, scheduledAt)
	return jobRun, err
}

func (s *jobRunService) GetJobRunList(ctx context.Context, projectSpec models.ProjectSpec, jobSpec models.JobSpec, jobQuery *models.JobQuery) ([]models.JobRun, error) {
	var jobRuns []models.JobRun

	interval := jobSpec.Schedule.Interval
	if interval == "" {
		return jobRuns, errors.New("job interval not found at DB")
	}
	// jobCron
	jobCron, err := cron.ParseCronSchedule(interval)
	if err != nil {
		return jobRuns, fmt.Errorf("unable to parse the interval from DB %w", err)
	}

	if jobQuery.OnlyLastRun {
		return s.scheduler.GetJobRuns(ctx, projectSpec, jobQuery, jobCron)
	}
	// validate job query
	err = validateJobQuery(jobQuery, jobSpec)
	if err != nil {
		return jobRuns, err
	}
	// get expected runs StartDate and EndDate inclusive
	expectedRuns := getExpectedRuns(jobCron, jobQuery.StartDate, jobQuery.EndDate)

	// call to airflow for get runs
	actualRuns, err := s.scheduler.GetJobRuns(ctx, projectSpec, jobQuery, jobCron)
	if err != nil {
		return jobRuns, fmt.Errorf("unable to get job runs from airflow %w", err)
	}
	// mergeRuns
	totalRuns := mergeRuns(expectedRuns, actualRuns)

	// filterRuns
	result := filterRuns(totalRuns, createFilterSet(jobQuery.Filter))

	return result, nil
}

func (s *jobRunService) Register(ctx context.Context, namespace models.NamespaceSpec, jobRun models.JobRun,
	instanceType models.InstanceType, instanceName string) (models.InstanceSpec, error) {
	// clear old run
	for _, instance := range jobRun.Instances {
		if instance.Name == instanceName && instance.Type == instanceType {
			if err := s.jobRunRepo.ClearInstance(ctx, jobRun.ID, instance.Type, instance.Name); err != nil && !errors.Is(err, store.ErrResourceNotFound) {
				return models.InstanceSpec{}, fmt.Errorf("Register: failed to clear instance of job %s: %w", jobRun, err)
			}
			break
		}
	}

	instanceToSave, err := s.prepInstance(ctx, jobRun, instanceType, instanceName, jobRun.ExecutedAt, namespace)
	if err != nil {
		return models.InstanceSpec{}, fmt.Errorf("Register: failed to prepare instance: %w", err)
	}
	if err := s.jobRunRepo.AddInstance(ctx, namespace, jobRun, instanceToSave); err != nil {
		return models.InstanceSpec{}, err
	}

	// get whatever is saved, querying again ensures it was saved correctly
	if jobRun, _, err = s.jobRunRepo.GetByID(ctx, jobRun.ID); err != nil {
		return models.InstanceSpec{}, fmt.Errorf("failed to save instance for %s of %s:%s: %w",
			jobRun, instanceName, instanceType, err)
	}
	return jobRun.GetInstance(instanceName, instanceType)
}

func (s *jobRunService) prepInstance(ctx context.Context, jobRun models.JobRun, instanceType models.InstanceType, instanceName string, executedAt time.Time, namespace models.NamespaceSpec) (models.InstanceSpec, error) {
	var jobDestination string
	dest, err := s.pluginService.GenerateDestination(ctx, jobRun.Spec, namespace)
	if err != nil {
		if !errors.Is(err, ErrDependencyModNotFound) {
			return models.InstanceSpec{}, fmt.Errorf("failed to generate destination for job %s: %w", jobRun.Spec.Name, err)
		}
	}
	if dest != nil {
		jobDestination = dest.Destination
	}

	return models.InstanceSpec{
		Name:       instanceName,
		Type:       instanceType,
		ExecutedAt: executedAt,
		Status:     models.RunStateRunning,
		// append optimus configs based on the values of a specific JobRun eg, jobScheduledTime
		Data: []models.InstanceSpecData{
			{
				Name:  models.ConfigKeyExecutionTime,
				Value: executedAt.Format(models.InstanceScheduledAtTimeLayout),
				Type:  models.InstanceDataTypeEnv,
			},
			{
				Name:  models.ConfigKeyDstart,
				Value: jobRun.Spec.Task.Window.GetStart(jobRun.ScheduledAt).Format(models.InstanceScheduledAtTimeLayout),
				Type:  models.InstanceDataTypeEnv,
			},
			{
				Name:  models.ConfigKeyDend,
				Value: jobRun.Spec.Task.Window.GetEnd(jobRun.ScheduledAt).Format(models.InstanceScheduledAtTimeLayout),
				Type:  models.InstanceDataTypeEnv,
			},
			{
				Name:  models.ConfigKeyDestination,
				Value: jobDestination,
				Type:  models.InstanceDataTypeEnv,
			},
		},
	}, nil
}

func (s *jobRunService) GetByID(ctx context.Context, jobRunID uuid.UUID) (models.JobRun, models.NamespaceSpec, error) {
	return s.jobRunRepo.GetByID(ctx, jobRunID)
}

func NewJobRunService(jobRunRepo store.JobRunRepository, timeFunc func() time.Time, scheduler models.SchedulerUnit, pluginService PluginService) *jobRunService {
	return &jobRunService{
		jobRunRepo:    jobRunRepo,
		Now:           timeFunc,
		scheduler:     scheduler,
		pluginService: pluginService,
	}
}

func validateJobQuery(jobQuery *models.JobQuery, jobSpec models.JobSpec) error {
	jobStartDate := jobSpec.Schedule.StartDate
	if jobStartDate.IsZero() {
		return errors.New("job start time not found at DB")
	}
	givenStartDate := jobQuery.StartDate
	givenEndDate := jobQuery.EndDate

	if givenStartDate.Before(jobStartDate) || givenEndDate.Before(jobStartDate) {
		return errors.New("invalid date range")
	}

	return nil
}

func getExpectedRuns(spec *cron.ScheduleSpec, startTime, endTime time.Time) []models.JobRun {
	var jobRuns []models.JobRun
	start := spec.Next(startTime.Add(-time.Second * 1))
	end := endTime
	exit := spec.Next(end)
	for !start.Equal(exit) {
		jobRuns = append(jobRuns, models.JobRun{
			Status:      models.RunStatePending,
			ScheduledAt: start,
		})
		start = spec.Next(start)
	}
	return jobRuns
}

func mergeRuns(expected, actual []models.JobRun) []models.JobRun {
	var mergeRuns []models.JobRun
	m := actualRunMap(actual)
	for _, exp := range expected {
		if act, ok := m[exp.ScheduledAt.UTC().String()]; ok {
			mergeRuns = append(mergeRuns, act)
		} else {
			mergeRuns = append(mergeRuns, exp)
		}
	}
	return mergeRuns
}

func actualRunMap(runs []models.JobRun) map[string]models.JobRun {
	m := map[string]models.JobRun{}
	for _, v := range runs {
		m[v.ScheduledAt.UTC().String()] = v
	}
	return m
}

func filterRuns(runs []models.JobRun, filter map[string]struct{}) []models.JobRun {
	var filteredRuns []models.JobRun
	if len(filter) == 0 {
		return runs
	}
	for _, v := range runs {
		if _, ok := filter[v.Status.String()]; ok {
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
