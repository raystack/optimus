package run

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
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
	if err == nil || errors.Is(err, store.ErrResourceNotFound) {
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

func (s *Service) GetByID(ctx context.Context, jobRunID uuid.UUID) (models.JobRun, models.NamespaceSpec, error) {
	return s.repoFac.New().GetByID(ctx, jobRunID)
}

func NewService(repoFac SpecRepoFactory, secretService service.SecretService, timeFunc func() time.Time, te models.TemplateEngine) *Service {
	return &Service{
		repoFac:        repoFac,
		secretService:  secretService,
		Now:            timeFunc,
		templateEngine: te,
	}
}
