package instance

import (
	"time"

	"github.com/pkg/errors"
	"gorm.io/gorm"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

const (
	ConfigKeyDstart        = "DSTART"
	ConfigKeyDend          = "DEND"
	ConfigKeyExecutionTime = "EXECUTION_TIME"
)

type InstanceSpecRepoFactory interface {
	New(models.JobSpec) store.InstanceSpecRepository
}

type Service struct {
	repoFac InstanceSpecRepoFactory
	Now     func() time.Time
}

func (s *Service) Register(jobSpec models.JobSpec, scheduledAt time.Time,
	instanceType models.InstanceType) (models.InstanceSpec, error) {
	jobRunRepo := s.repoFac.New(jobSpec)
	instanceToSave := s.prepInstance(jobSpec, scheduledAt)

	switch instanceType {
	case models.InstanceTypeTransformation:
		// clear and save fresh
		if err := jobRunRepo.Clear(scheduledAt); err != nil {
			return models.InstanceSpec{}, errors.Wrapf(err, "failed to clear instance of job %s",
				scheduledAt.String())
		}
		if err := jobRunRepo.Save(instanceToSave); err != nil {
			return models.InstanceSpec{}, err
		}
	case models.InstanceTypeHook:
		// store only if not already exists
		_, err := jobRunRepo.GetByScheduledAt(scheduledAt)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			if err := jobRunRepo.Save(instanceToSave); err != nil {
				return models.InstanceSpec{}, err
			}
		} else if err != nil {
			return models.InstanceSpec{}, err
		}

	default:
		return models.InstanceSpec{}, errors.Errorf("invalid instance type: %s", instanceType)
	}

	// get whatever is saved, querying again ensures it was saved correctly
	instanceSpec, err := jobRunRepo.GetByScheduledAt(scheduledAt)
	if err != nil {
		return models.InstanceSpec{}, errors.Wrapf(err, "failed to save instance scheduled at: %s", scheduledAt.String())
	}
	return instanceSpec, nil
}

func (s *Service) prepInstance(jobSpec models.JobSpec, scheduledAt time.Time) models.InstanceSpec {
	return models.InstanceSpec{
		Job:         jobSpec,
		ScheduledAt: scheduledAt,
		State:       models.InstanceStateRunning,

		// append optimus configs based on the values of a specific JobRun eg, jobScheduledTime
		Data: []models.InstanceSpecData{
			{
				Name:  ConfigKeyExecutionTime,
				Value: s.Now().Format(models.InstanceScheduledAtTimeLayout),
				Type:  models.InstanceDataTypeEnv,
			},
			{
				Name:  ConfigKeyDstart,
				Value: jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
				Type:  models.InstanceDataTypeEnv,
			},
			{
				Name:  ConfigKeyDend,
				Value: jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
				Type:  models.InstanceDataTypeEnv,
			},
		},
	}
}

func NewService(repoFac InstanceSpecRepoFactory, timeFunc func() time.Time) *Service {
	return &Service{
		repoFac: repoFac,
		Now:     timeFunc,
	}
}
