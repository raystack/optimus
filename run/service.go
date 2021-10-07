package run

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/pkg/errors"

	"github.com/odpf/optimus/models"
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
	Now            func() time.Time
	templateEngine models.TemplateEngine
}

func (s *Service) Compile(namespace models.NamespaceSpec, jobRun models.JobRun, instanceSpec models.InstanceSpec) (
	envMap map[string]string, fileMap map[string]string, err error) {
	return NewContextManager(namespace, jobRun, s.templateEngine).Generate(instanceSpec)
}

func (s *Service) GetScheduledRun(namespace models.NamespaceSpec, jobSpec models.JobSpec,
	scheduledAt time.Time) (models.JobRun, error) {
	newJobRun := models.JobRun{
		Spec:        jobSpec,
		Trigger:     models.TriggerSchedule,
		Status:      models.RunStatePending,
		ScheduledAt: scheduledAt,
		Instances:   nil,
	}

	repo := s.repoFac.New()
	jobRun, _, err := repo.GetByScheduledAt(jobSpec.ID, scheduledAt)
	if err == nil || err == store.ErrResourceNotFound {
		// create a new instance if it does not already exists
		if err == nil {
			// if already exists, use the same id for in place update
			// because job spec might have changed by now, status needs to be reset
			newJobRun.ID = jobRun.ID
			newJobRun.Instances = jobRun.Instances
		}
		if err := repo.Save(namespace, newJobRun); err != nil {
			return models.JobRun{}, err
		}
	} else {
		return models.JobRun{}, err
	}

	jobRun, _, err = repo.GetByScheduledAt(jobSpec.ID, scheduledAt)
	return jobRun, err
}

func (s *Service) Register(namespace models.NamespaceSpec, jobRun models.JobRun,
	instanceType models.InstanceType, instanceName string) (models.InstanceSpec, error) {
	executedAt := s.Now()
	if len(jobRun.Instances) > 0 {
		// Extract execution time which will be shared across all job run instances.
		// Note(kushsharma): This is not the best way to do this, a better inter task communication
		// mechanism should be in place at job run level which each instance can use
		// at least as a key/value map for temporary storage. This kv map will be
		// available to all the instances
		executedAt = jobRun.Instances[0].ExecutedAt
	}
	instanceToSave, err := s.prepInstance(jobRun, instanceType, instanceName, executedAt)
	if err != nil {
		return models.InstanceSpec{}, errors.Wrap(err, "Register: failed to prepare instance")
	}

	jobRunRepo := s.repoFac.New()
	switch instanceType {
	case models.InstanceTypeTask:
		// clear and save fresh
		if err := jobRunRepo.ClearInstance(jobRun.ID, instanceType, instanceName); err != nil && !errors.Is(err, store.ErrResourceNotFound) {
			return models.InstanceSpec{}, errors.Wrapf(err, "Register: failed to clear instance of job %s", jobRun)
		}
		if err := jobRunRepo.AddInstance(namespace, jobRun, instanceToSave); err != nil {
			return models.InstanceSpec{}, err
		}
	case models.InstanceTypeHook:
		exists := false
		// store only if not already exists
		for _, instance := range jobRun.Instances {
			if instance.Name == instanceName && instance.Type == instanceType {
				exists = true
				break
			}
		}
		if !exists {
			if err := jobRunRepo.AddInstance(namespace, jobRun, instanceToSave); err != nil {
				return models.InstanceSpec{}, err
			}
		}
	default:
		return models.InstanceSpec{}, errors.Errorf("invalid instance type: %s", instanceType)
	}

	// get whatever is saved, querying again ensures it was saved correctly
	if jobRun, _, err = jobRunRepo.GetByID(jobRun.ID); err != nil {
		return models.InstanceSpec{}, errors.Wrapf(err, "failed to save instance for %s of %s:%s",
			jobRun, instanceName, instanceType)
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
			return models.InstanceSpec{}, errors.Wrapf(err, "failed to generate destination for job %s", jobRun.Spec.Name)
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

func (s *Service) GetByID(JobRunID uuid.UUID) (models.JobRun, models.NamespaceSpec, error) {
	return s.repoFac.New().GetByID(JobRunID)
}

func NewService(repoFac SpecRepoFactory, timeFunc func() time.Time, te models.TemplateEngine) *Service {
	return &Service{
		repoFac:        repoFac,
		Now:            timeFunc,
		templateEngine: te,
	}
}
