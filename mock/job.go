package mock

import (
	"github.com/stretchr/testify/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/odpf/optimus/store/local"
)

// JobSpecRepoFactory to store raw specs
type JobSpecRepoFactory struct {
	mock.Mock
}

func (repo *JobSpecRepoFactory) New(proj models.ProjectSpec) store.JobSpecRepository {
	return repo.Called(proj).Get(0).(store.JobSpecRepository)
}

// JobSpecRepoFactory to store raw specs
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

func (repo *JobSpecRepository) Delete(name string) error {
	return repo.Called(name).Error(0)
}

func (repo *JobSpecRepository) GetAll() ([]models.JobSpec, error) {
	args := repo.Called()
	if args.Get(0) != nil {
		return args.Get(0).([]models.JobSpec), args.Error(1)
	}
	return []models.JobSpec{}, args.Error(1)
}

// JobRepoFactory to store compiled specs
type JobRepoFactory struct {
	mock.Mock
}

func (repo *JobRepoFactory) New(proj models.ProjectSpec) (store.JobRepository, error) {
	args := repo.Called(proj)
	return args.Get(0).(store.JobRepository), args.Error(1)
}

// JobRepository to store compiled specs

type JobRepository struct {
	mock.Mock
}

func (repo *JobRepository) Save(t models.Job) error {
	return repo.Called(t).Error(0)
}

func (repo *JobRepository) GetByName(name string) (models.Job, error) {
	args := repo.Called(name)
	return args.Get(0).(models.Job), args.Error(1)
}

func (repo *JobRepository) GetAll() ([]models.Job, error) {
	args := repo.Called()
	return args.Get(0).([]models.Job), args.Error(1)
}

func (repo *JobRepository) Delete(name string) error {
	args := repo.Called(name)
	return args.Error(0)
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

type Compiler struct {
	mock.Mock
}

func (srv *Compiler) Compile(jobSpec models.JobSpec, proj models.ProjectSpec) (models.Job, error) {
	args := srv.Called(jobSpec, proj)
	return args.Get(0).(models.Job), args.Error(1)
}

type DependencyResolver struct {
	mock.Mock
}

func (srv *DependencyResolver) Resolve(jobSpecs []models.JobSpec) ([]models.JobSpec, error) {
	args := srv.Called(jobSpecs)
	return args.Get(0).([]models.JobSpec), args.Error(1)
}

type PriorityResolver struct {
	mock.Mock
}

func (srv *PriorityResolver) Resolve(jobSpecs []models.JobSpec) ([]models.JobSpec, error) {
	args := srv.Called(jobSpecs)
	return args.Get(0).([]models.JobSpec), args.Error(1)
}
