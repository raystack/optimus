package run

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/optimus/core/cron"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
	"github.com/odpf/optimus/store"
)

const (

	// these configs can be used as macros in task/hook config and job assets
	// ConfigKeyDstart start of the execution window
	ConfigKeyDstart = "DSTART"
	// ConfigKeyDend end of the execution window
	ConfigKeyDend = "DEND"
	// ConfigKeyExecutionTime time when the job started executing, this gets shared across all
	// task and hooks of a job instance
	ConfigKeyExecutionTime = "EXECUTION_TIME"
	// ConfigKeyDestination is destination urn
	ConfigKeyDestination = "JOB_DESTINATION"
)

type SpecRepoFactory interface {
	New() store.JobRunRepository
}

type Service struct {
	repoFac        SpecRepoFactory
	secretService  service.SecretService
	scheduler      models.SchedulerUnit
	Now            func() time.Time
	templateEngine models.TemplateEngine
}

func (s *Service) Compile(ctx context.Context, namespace models.NamespaceSpec, jobRun models.JobRun, instanceSpec models.InstanceSpec) (
	*models.JobRunInput, error) {
	secrets, err := s.secretService.GetSecrets(ctx, namespace)
	if err != nil {
		return nil, err
	}
	return NewContextManager(namespace, secrets, jobRun, s.templateEngine).Generate(instanceSpec)
}

func (s *Service) GetScheduledRun(ctx context.Context, namespace models.NamespaceSpec, jobSpec models.JobSpec,
	scheduledAt time.Time) (models.JobRun, error) {
	newJobRun := models.JobRun{
		Spec:        jobSpec,
		Trigger:     models.TriggerSchedule,
		Status:      models.RunStatePending,
		ScheduledAt: scheduledAt,
		ExecutedAt:  s.Now(),
	}

	repo := s.repoFac.New()
	jobRun, _, err := repo.GetByScheduledAt(ctx, jobSpec.ID, scheduledAt)
	if err == nil || err == store.ErrResourceNotFound {
		// create a new instance if it does not already exists
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
		if err := repo.Save(ctx, namespace, newJobRun); err != nil {
			return models.JobRun{}, err
		}
	} else {
		return models.JobRun{}, err
	}

	jobRun, _, err = repo.GetByScheduledAt(ctx, jobSpec.ID, scheduledAt)
	return jobRun, err
}

func (s *Service) GetJobRunList(ctx context.Context, projectSpec models.ProjectSpec, jobSpec models.JobSpec, jobQuery *models.JobQuery) ([]models.JobRun, error) {
	//get expected runs
	var jobRuns []models.JobRun
	jobStartDate := jobSpec.Schedule.StartDate
	jobInterval := jobSpec.Schedule.Interval

	givenStartDate := jobQuery.StartDate
	givenEndDate := jobQuery.EndDate

	if givenStartDate.Before(jobStartDate) || givenEndDate.Before(jobStartDate) {
		return jobRuns, errors.New("invalid date range")
	}

	sch, err := cron.ParseCronSchedule(jobInterval)
	if err != nil {
		return jobRuns, fmt.Errorf("unable to parse the interval from DB %w", err)
	}

	duration := sch.Interval(time.Now())
	executionStartDate := givenStartDate.Add(-duration)
	executionEndDate := givenEndDate.Add(-duration)

	expectedRuns := buildExpectedRun(sch, executionStartDate, executionEndDate)

	//modify the date at query according on execution date
	jobQuery.StartDate = executionStartDate
	jobQuery.EndDate = executionEndDate
	//call to airflow for get runs
	actualRuns, err := s.scheduler.GetJobRuns(ctx, projectSpec, jobQuery)
	if err != nil {
		return jobRuns, fmt.Errorf("unable to get job runs from airflow %w", err)
	}
	//merge
	runs := merge(expectedRuns, actualRuns)
	//filter
	return filter(runs, filterMap(jobQuery.Filter)), nil
}

func (s *Service) Register(ctx context.Context, namespace models.NamespaceSpec, jobRun models.JobRun,
	instanceType models.InstanceType, instanceName string) (models.InstanceSpec, error) {
	jobRunRepo := s.repoFac.New()

	// clear old run
	for _, instance := range jobRun.Instances {
		if instance.Name == instanceName && instance.Type == instanceType {
			if err := jobRunRepo.ClearInstance(ctx, jobRun.ID, instance.Type, instance.Name); err != nil && !errors.Is(err, store.ErrResourceNotFound) {
				return models.InstanceSpec{}, fmt.Errorf("Register: failed to clear instance of job %s: %w", jobRun, err)
			}
			break
		}
	}

	instanceToSave, err := s.prepInstance(jobRun, instanceType, instanceName, jobRun.ExecutedAt)
	if err != nil {
		return models.InstanceSpec{}, fmt.Errorf("Register: failed to prepare instance: %w", err)
	}
	if err := jobRunRepo.AddInstance(ctx, namespace, jobRun, instanceToSave); err != nil {
		return models.InstanceSpec{}, err
	}

	// get whatever is saved, querying again ensures it was saved correctly
	if jobRun, _, err = jobRunRepo.GetByID(ctx, jobRun.ID); err != nil {
		return models.InstanceSpec{}, fmt.Errorf("failed to save instance for %s of %s:%s: %w",
			jobRun, instanceName, instanceType, err)
	}
	return jobRun.GetInstance(instanceName, instanceType)
}

func (s *Service) prepInstance(jobRun models.JobRun, instanceType models.InstanceType,
	instanceName string, executedAt time.Time) (models.InstanceSpec, error) {
	var jobDestination string
	if jobRun.Spec.Task.Unit.DependencyMod != nil {
		jobDestinationResponse, err := jobRun.Spec.Task.Unit.DependencyMod.GenerateDestination(context.TODO(), models.GenerateDestinationRequest{
			Config: models.PluginConfigs{}.FromJobSpec(jobRun.Spec.Task.Config),
			Assets: models.PluginAssets{}.FromJobSpec(jobRun.Spec.Assets),
		})
		if err != nil {
			return models.InstanceSpec{}, fmt.Errorf("failed to generate destination for job %s: %w", jobRun.Spec.Name, err)
		}
		jobDestination = jobDestinationResponse.Destination
	}

	return models.InstanceSpec{
		Name:       instanceName,
		Type:       instanceType,
		ExecutedAt: executedAt,
		Status:     models.RunStateRunning,
		// append optimus configs based on the values of a specific JobRun eg, jobScheduledTime
		Data: []models.InstanceSpecData{
			{
				Name:  ConfigKeyExecutionTime,
				Value: executedAt.Format(models.InstanceScheduledAtTimeLayout),
				Type:  models.InstanceDataTypeEnv,
			},
			{
				Name:  ConfigKeyDstart,
				Value: jobRun.Spec.Task.Window.GetStart(jobRun.ScheduledAt).Format(models.InstanceScheduledAtTimeLayout),
				Type:  models.InstanceDataTypeEnv,
			},
			{
				Name:  ConfigKeyDend,
				Value: jobRun.Spec.Task.Window.GetEnd(jobRun.ScheduledAt).Format(models.InstanceScheduledAtTimeLayout),
				Type:  models.InstanceDataTypeEnv,
			},
			{
				Name:  ConfigKeyDestination,
				Value: jobDestination,
				Type:  models.InstanceDataTypeEnv,
			},
		},
	}, nil
}

func (s *Service) GetByID(ctx context.Context, JobRunID uuid.UUID) (models.JobRun, models.NamespaceSpec, error) {
	return s.repoFac.New().GetByID(ctx, JobRunID)
}

func NewService(repoFac SpecRepoFactory, secretService service.SecretService, timeFunc func() time.Time, scheduler models.SchedulerUnit, te models.TemplateEngine) *Service {
	return &Service{
		repoFac:        repoFac,
		secretService:  secretService,
		Now:            timeFunc,
		scheduler:      scheduler,
		templateEngine: te,
	}
}

func buildExpectedRun(spec *cron.ScheduleSpec, startTime time.Time, endTime time.Time) []models.JobRun {
	var jobRuns []models.JobRun
	start := spec.Next(startTime)
	end := spec.Next(endTime)
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

func merge(expected []models.JobRun, actual []models.JobRun) []models.JobRun {
	var list []models.JobRun
	if len(expected) < len(actual) {
		list = expected
	} else {
		list = actual
	}
	for index, value := range list {
		if actual[index].ScheduledAt.Equal(value.ScheduledAt) {
			value.Status = actual[index].Status
		}
	}
	return expected
}

func filter(runs []models.JobRun, filter map[string]struct{}) []models.JobRun {
	var filteredRuns []models.JobRun
	for _, v := range runs {
		if _, ok := filter[v.Status.String()]; ok {
			filteredRuns = append(filteredRuns, v)
		}
	}
	return filteredRuns
}

func filterMap(filter []string) map[string]struct{} {
	m := map[string]struct{}{}
	for _, v := range filter {
		m[v] = struct{}{}
	}
	return m
}
