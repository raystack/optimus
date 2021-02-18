package instance

import (
	"time"

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

func (s *Service) Register(jobSpec models.JobSpec, scheduledAt time.Time) (models.InstanceSpec, error) {
	jobRunRepo := s.repoFac.New(jobSpec)
	if err := jobRunRepo.Save(models.InstanceSpec{
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
	}); err != nil {
		return models.InstanceSpec{}, err
	}

	// get whatever is saved
	instanceSpec, err := jobRunRepo.GetByScheduledAt(scheduledAt)
	if err != nil {
		return models.InstanceSpec{}, err
	}
	return instanceSpec, nil
}

// Clear will not delete the record but will reset all the run details
func (s *Service) Clear(jobSpec models.JobSpec, scheduledAt time.Time) error {
	jobRunRepo := s.repoFac.New(jobSpec)
	return jobRunRepo.Clear(scheduledAt)
}

func NewService(repoFac InstanceSpecRepoFactory, timeFunc func() time.Time) *Service {
	return &Service{
		repoFac: repoFac,
		Now:     timeFunc,
	}
}
