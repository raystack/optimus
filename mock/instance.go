package mock

import (
	"time"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/stretchr/testify/mock"
)

type InstanceSpecRepoFactory struct {
	mock.Mock
}

func (repo *InstanceSpecRepoFactory) New(spec models.JobSpec) store.InstanceSpecRepository {
	args := repo.Called(spec)
	return args.Get(0).(store.InstanceSpecRepository)
}

// InstanceSpecRepository to store mock instance specs
type InstanceSpecRepository struct {
	mock.Mock
}

func (repo *InstanceSpecRepository) Save(t models.InstanceSpec) error {
	return repo.Called(t).Error(0)
}

func (repo *InstanceSpecRepository) GetByScheduledAt(st time.Time) (models.InstanceSpec, error) {
	args := repo.Called(st)
	if args.Get(0) != nil {
		return args.Get(0).(models.InstanceSpec), args.Error(1)
	}
	return models.InstanceSpec{}, args.Error(1)
}

func (repo *InstanceSpecRepository) Clear(st time.Time) error {
	return repo.Called(st).Error(0)
}

type InstanceService struct {
	mock.Mock
}

func (s *InstanceService) Compile(nsSpec models.NamespaceSpec, jobSpec models.JobSpec, instanceSpec models.InstanceSpec, runType models.InstanceType, runName string) (envMap map[string]string, fileMap map[string]string, err error) {
	args := s.Called(nsSpec, jobSpec, instanceSpec, runType, runName)
	return args.Get(0).(map[string]string), args.Get(1).(map[string]string), args.Error(2)
}

func (s *InstanceService) Register(jobSpec models.JobSpec, scheduledAt time.Time,
	taskType models.InstanceType) (models.InstanceSpec, error) {
	args := s.Called(jobSpec, scheduledAt, taskType)
	return args.Get(0).(models.InstanceSpec), args.Error(1)
}
