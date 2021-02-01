package mock

import (
	"github.com/stretchr/testify/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"time"
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

func (s *InstanceService) Register(jobSpec models.JobSpec, scheduledAt time.Time) (models.InstanceSpec, error) {
	args := s.Called(jobSpec, scheduledAt)
	return args.Get(0).(models.InstanceSpec), args.Error(1)
}

func (s *InstanceService) Clear(jobSpec models.JobSpec, scheduledAt time.Time) error {
	return s.Called(jobSpec, scheduledAt).Error(0)
}
