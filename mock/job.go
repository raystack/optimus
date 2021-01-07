package mock

import (
	"github.com/stretchr/testify/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/odpf/optimus/store/local"
)

type JobSpecRepoFactory struct {
	mock.Mock
}

func (repo *JobSpecRepoFactory) New(proj models.ProjectSpec) store.JobSpecRepository {
	return repo.Called(proj).Get(0).(store.JobSpecRepository)
}

type JobSpecRepository struct {
	mock.Mock
}

func (repo *JobSpecRepository) Save(t models.JobSpec) error {
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

type JobConfigLocalFactory struct {
	mock.Mock
}

func (fac *JobConfigLocalFactory) New(inputs models.JobSpec) (local.Job, error) {
	args := fac.Called(inputs)
	return args.Get(0).(local.Job), args.Error(1)
}

type JobService struct {
	mock.Mock
}

// CreateJob constructs a DAG and commits it to a storage
func (srv *JobService) CreateJob(inputs models.JobSpec) error {
	return srv.Called(inputs).Error(0)
}

type DependencyResolver struct {
	mock.Mock
}

func (srv *DependencyResolver) Resolve(jobSpecs []models.JobSpec) ([]models.JobSpec, error) {
	args := srv.Called(jobSpecs)
	return args.Get(0).([]models.JobSpec), args.Error(1)
}
