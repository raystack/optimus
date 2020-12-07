package mock

import (
	"github.com/stretchr/testify/mock"
	"github.com/odpf/optimus/models"
)

type JobSpecRepository struct {
	mock.Mock
}

func (repo *JobSpecRepository) Save(t models.JobInput) error {
	return repo.Called(t).Error(0)
}

func (repo *JobSpecRepository) GetByName(name string) (models.JobSpec, error) {
	args := repo.Called(name)
	if args.Get(0) != nil {
		return args.Get(0).(models.JobSpec), args.Error(1)
	}
	return models.JobSpec{}, args.Error(1)
}

func (repo *JobSpecRepository) GetAll() ([]models.JobSpec, error) {
	args := repo.Called()
	if args.Get(0) != nil {
		return args.Get(0).([]models.JobSpec), args.Error(1)
	}
	return []models.JobSpec{}, args.Error(1)
}

type JobSpecFactory struct {
	mock.Mock
}

func (fac *JobSpecFactory) CreateJobSpec(inputs models.JobInput) (models.JobSpec, error) {
	args := fac.Called(inputs)
	return args.Get(0).(models.JobSpec), args.Error(1)
}

type JobService struct {
	mock.Mock
}

// CreateJob constructs a DAG and commits it to a storage
func (srv *JobService) CreateJob(inputs models.JobInput) error {
	return srv.Called(inputs).Error(0)
}
