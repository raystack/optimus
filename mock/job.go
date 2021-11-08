package mock

import (
	"context"

	"github.com/google/uuid"

	"github.com/odpf/optimus/job"

	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/odpf/optimus/store/local"
	"github.com/stretchr/testify/mock"
)

// ProjectJobSpecRepoFactory to manage job specs at project level
type ProjectJobSpecRepoFactory struct {
	mock.Mock
}

func (repo *ProjectJobSpecRepoFactory) New(proj models.ProjectSpec) store.ProjectJobSpecRepository {
	return repo.Called(proj).Get(0).(store.ProjectJobSpecRepository)
}

// JobSpecRepoFactory to store raw specs
type ProjectJobSpecRepository struct {
	mock.Mock
}

func (repo *ProjectJobSpecRepository) GetByName(ctx context.Context, name string) (models.JobSpec, models.NamespaceSpec, error) {
	args := repo.Called(ctx, name)
	if args.Get(0) != nil {
		return args.Get(0).(models.JobSpec), args.Get(1).(models.NamespaceSpec), args.Error(2)
	}
	return models.JobSpec{}, models.NamespaceSpec{}, args.Error(1)
}

func (repo *ProjectJobSpecRepository) GetByNameForProject(ctx context.Context, job, project string) (models.JobSpec, models.ProjectSpec, error) {
	args := repo.Called(ctx, job, project)
	if args.Get(0) != nil {
		return args.Get(0).(models.JobSpec), args.Get(1).(models.ProjectSpec), args.Error(2)
	}
	return models.JobSpec{}, models.ProjectSpec{}, args.Error(1)
}

func (repo *ProjectJobSpecRepository) GetAll(ctx context.Context) ([]models.JobSpec, error) {
	args := repo.Called(ctx)
	if args.Get(0) != nil {
		return args.Get(0).([]models.JobSpec), args.Error(1)
	}
	return []models.JobSpec{}, args.Error(1)
}

func (repo *ProjectJobSpecRepository) GetAllWithNamespace(ctx context.Context) (map[string][]string, error) {
	args := repo.Called(ctx)
	if args.Get(0) != nil {
		return args.Get(0).(map[string][]string), args.Error(1)
	}
	return map[string][]string{}, args.Error(1)
}

func (repo *ProjectJobSpecRepository) GetByDestination(ctx context.Context, dest string) (models.JobSpec, models.ProjectSpec, error) {
	args := repo.Called(ctx, dest)
	if args.Get(0) != nil {
		return args.Get(0).(models.JobSpec), args.Get(1).(models.ProjectSpec), args.Error(2)
	}
	return models.JobSpec{}, models.ProjectSpec{}, args.Error(2)
}

func (repo *ProjectJobSpecRepository) GetJobNamespaces(ctx context.Context) (map[string][]string, error) {
	args := repo.Called(ctx)
	if args.Get(0) != nil {
		return args.Get(0).(map[string][]string), args.Error(1)
	}
	return map[string][]string{}, args.Error(1)
}

// JobSpecRepoFactory to store raw specs at namespace level
type JobSpecRepoFactory struct {
	mock.Mock
}

func (repo *JobSpecRepoFactory) New(namespace models.NamespaceSpec) job.SpecRepository {
	return repo.Called(namespace).Get(0).(job.SpecRepository)
}

// JobSpecRepoFactory to store raw specs
type JobSpecRepository struct {
	mock.Mock
}

func (repo *JobSpecRepository) Save(ctx context.Context, t models.JobSpec) error {
	return repo.Called(ctx, t).Error(0)
}

func (repo *JobSpecRepository) GetByName(ctx context.Context, name string) (models.JobSpec, error) {
	args := repo.Called(ctx, name)
	if args.Get(0) != nil {
		return args.Get(0).(models.JobSpec), args.Error(1)
	}
	return models.JobSpec{}, args.Error(1)
}

func (repo *JobSpecRepository) Delete(ctx context.Context, name string) error {
	return repo.Called(ctx, name).Error(0)
}

func (repo *JobSpecRepository) GetAll(ctx context.Context) ([]models.JobSpec, error) {
	args := repo.Called(ctx)
	if args.Get(0) != nil {
		return args.Get(0).([]models.JobSpec), args.Error(1)
	}
	return []models.JobSpec{}, args.Error(1)
}

func (repo *JobSpecRepository) GetByDestination(ctx context.Context, dest string) (models.JobSpec, models.ProjectSpec, error) {
	args := repo.Called(ctx, dest)
	if args.Get(0) != nil {
		return args.Get(0).(models.JobSpec), args.Get(1).(models.ProjectSpec), args.Error(2)
	}
	return models.JobSpec{}, models.ProjectSpec{}, args.Error(2)
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

func (srv *JobService) Create(ctx context.Context, spec2 models.NamespaceSpec, spec models.JobSpec) error {
	args := srv.Called(ctx, spec, spec2)
	return args.Error(0)
}

func (srv *JobService) GetByName(ctx context.Context, s string, spec models.NamespaceSpec) (models.JobSpec, error) {
	args := srv.Called(ctx, s, spec)
	return args.Get(0).(models.JobSpec), args.Error(1)
}

func (srv *JobService) KeepOnly(ctx context.Context, spec models.NamespaceSpec, specs []models.JobSpec, observer progress.Observer) error {
	args := srv.Called(ctx, spec, specs)
	return args.Error(0)
}

func (srv *JobService) GetAll(ctx context.Context, spec models.NamespaceSpec) ([]models.JobSpec, error) {
	args := srv.Called(ctx, spec)
	return args.Get(0).([]models.JobSpec), args.Error(1)
}

func (srv *JobService) GetByNameForProject(ctx context.Context, s string, spec models.ProjectSpec) (models.JobSpec, models.NamespaceSpec, error) {
	args := srv.Called(ctx, s, spec)
	return args.Get(0).(models.JobSpec), args.Get(1).(models.NamespaceSpec), args.Error(2)
}

func (srv *JobService) Sync(ctx context.Context, spec models.NamespaceSpec, observer progress.Observer) error {
	args := srv.Called(ctx, spec, observer)
	return args.Error(0)
}

func (j *JobService) GetTaskDependencies(ctx context.Context, namespaceSpec models.NamespaceSpec, spec models.JobSpec) (models.JobSpecTaskDestination,
	models.JobSpecTaskDependencies, error) {
	args := j.Called(ctx, namespaceSpec, spec)
	return args.Get(0).(models.JobSpecTaskDestination), args.Get(1).(models.JobSpecTaskDependencies), args.Error(2)
}

func (j *JobService) Check(ctx context.Context, namespaceSpec models.NamespaceSpec, specs []models.JobSpec, observer progress.Observer) error {
	args := j.Called(ctx, namespaceSpec, specs, observer)
	return args.Error(0)
}

func (j *JobService) Delete(ctx context.Context, c models.NamespaceSpec, job models.JobSpec) error {
	args := j.Called(ctx, c, job)
	return args.Error(0)
}

func (j *JobService) ReplayDryRun(ctx context.Context, replayRequest models.ReplayRequest) (models.ReplayPlan, error) {
	args := j.Called(ctx, replayRequest)
	return args.Get(0).(models.ReplayPlan), args.Error(1)
}

func (j *JobService) Replay(ctx context.Context, replayRequest models.ReplayRequest) (string, error) {
	args := j.Called(ctx, replayRequest)
	return args.Get(0).(string), args.Error(1)
}

func (j *JobService) GetReplayStatus(ctx context.Context, replayRequest models.ReplayRequest) (models.ReplayState, error) {
	args := j.Called(ctx, replayRequest)
	return args.Get(0).(models.ReplayState), args.Error(1)
}

func (j *JobService) GetReplayList(ctx context.Context, projectUUID uuid.UUID) ([]models.ReplaySpec, error) {
	args := j.Called(ctx, projectUUID)
	return args.Get(0).([]models.ReplaySpec), args.Error(1)
}

func (j *JobService) Run(ctx context.Context, ns models.NamespaceSpec, js []models.JobSpec, obs progress.Observer) error {
	args := j.Called(ctx, ns, js, obs)
	return args.Error(0)
}

func (j *JobService) GetByDestination(ctx context.Context, projectSpec models.ProjectSpec, destination string) (models.JobSpec, error) {
	args := j.Called(projectSpec, destination)
	return args.Get(0).(models.JobSpec), args.Error(1)
}

func (j *JobService) GetDownstream(ctx context.Context, projectSpec models.ProjectSpec, jobName string, allowedDownstream string) ([]models.JobSpec, error) {
	args := j.Called(ctx, projectSpec, jobName, allowedDownstream)
	return args.Get(0).([]models.JobSpec), args.Error(1)
}

type DependencyResolver struct {
	mock.Mock
}

func (srv *DependencyResolver) Resolve(ctx context.Context, projectSpec models.ProjectSpec,
	jobSpec models.JobSpec, obs progress.Observer) (models.JobSpec, error) {
	args := srv.Called(ctx, projectSpec, jobSpec, obs)
	return args.Get(0).(models.JobSpec), args.Error(1)
}

type PriorityResolver struct {
	mock.Mock
}

func (srv *PriorityResolver) Resolve(ctx context.Context, jobSpecs []models.JobSpec) ([]models.JobSpec, error) {
	args := srv.Called(ctx, jobSpecs)
	return args.Get(0).([]models.JobSpec), args.Error(1)
}

type EventService struct {
	mock.Mock
}

func (e *EventService) Register(ctx context.Context, spec models.NamespaceSpec, spec2 models.JobSpec, event models.JobEvent) error {
	return e.Called(ctx, spec, spec2, event).Error(0)
}

type Notifier struct {
	mock.Mock
}

func (n *Notifier) Close() error {
	return n.Called().Error(0)
}

func (n *Notifier) Notify(ctx context.Context, attr models.NotifyAttrs) error {
	return n.Called(ctx, attr).Error(0)
}
