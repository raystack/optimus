package mock

import (
	"context"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/odpf/optimus/store/local"
)

// ProjectJobSpecRepoFactory to manage job specs at project level
type ProjectJobSpecRepoFactory struct {
	mock.Mock
}

func (repo *ProjectJobSpecRepoFactory) New(proj models.ProjectSpec) store.ProjectJobSpecRepository {
	return repo.Called(proj).Get(0).(store.ProjectJobSpecRepository)
}

// ProjectJobSpecRepository to store raw specs
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

func (repo *ProjectJobSpecRepository) GetByIDs(ctx context.Context, jobIDs []uuid.UUID) ([]models.JobSpec, error) {
	args := repo.Called(ctx, jobIDs)
	return args.Get(0).([]models.JobSpec), args.Error(1)
}

func (repo *ProjectJobSpecRepository) GetByNameForProject(ctx context.Context, jobName, project string) (models.JobSpec, models.ProjectSpec, error) {
	args := repo.Called(ctx, jobName, project)
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

func (repo *ProjectJobSpecRepository) GetByDestination(ctx context.Context, dest string) ([]store.ProjectJobPair, error) {
	args := repo.Called(ctx, dest)
	if args.Get(0) != nil {
		return args.Get(0).([]store.ProjectJobPair), args.Error(1)
	}
	return nil, args.Error(1)
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

func (repo *JobSpecRepository) Save(ctx context.Context, t models.JobSpec, jobDestination string) error {
	return repo.Called(ctx, t, jobDestination).Error(0)
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

func (srv *JobService) Create(ctx context.Context, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) (*models.JobSpec, error) {
	args := srv.Called(ctx, namespaceSpec, jobSpec)
	return args.Get(0).(*models.JobSpec), args.Error(1)
}

func (srv *JobService) BulkCreate(ctx context.Context, namespaceSpec models.NamespaceSpec, jobSpecs []models.JobSpec, observers progress.Observer) []models.JobSpec {
	args := srv.Called(ctx, namespaceSpec, jobSpecs, observers)
	return args.Get(0).([]models.JobSpec)
}

func (srv *JobService) GetByName(ctx context.Context, s string, spec models.NamespaceSpec) (models.JobSpec, error) {
	args := srv.Called(ctx, s, spec)
	return args.Get(0).(models.JobSpec), args.Error(1)
}

func (srv *JobService) KeepOnly(ctx context.Context, spec models.NamespaceSpec, specs []models.JobSpec, _ progress.Observer) error {
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

func (srv *JobService) GetTaskDependencies(ctx context.Context, namespaceSpec models.NamespaceSpec, spec models.JobSpec) (models.JobSpecTaskDestination,
	models.JobSpecTaskDependencies, error) {
	args := srv.Called(ctx, namespaceSpec, spec)
	return args.Get(0).(models.JobSpecTaskDestination), args.Get(1).(models.JobSpecTaskDependencies), args.Error(2)
}

func (srv *JobService) Check(ctx context.Context, namespaceSpec models.NamespaceSpec, specs []models.JobSpec, observer progress.Observer) error {
	args := srv.Called(ctx, namespaceSpec, specs, observer)
	return args.Error(0)
}

func (srv *JobService) Delete(ctx context.Context, c models.NamespaceSpec, jobSpec models.JobSpec) error {
	args := srv.Called(ctx, c, jobSpec)
	return args.Error(0)
}

func (srv *JobService) BulkDelete(ctx context.Context, namespaceSpec models.NamespaceSpec, jobSpecs []models.JobSpec, observers progress.Observer) {
	_ = srv.Called(ctx, namespaceSpec, jobSpecs, observers)
}

func (srv *JobService) Run(ctx context.Context, ns models.NamespaceSpec, js []models.JobSpec, obs progress.Observer) error {
	args := srv.Called(ctx, ns, js, obs)
	return args.Error(0)
}

func (srv *JobService) GetByDestination(_ context.Context, projectSpec models.ProjectSpec, destination string) (models.JobSpec, error) {
	args := srv.Called(projectSpec, destination)
	return args.Get(0).(models.JobSpec), args.Error(1)
}

func (srv *JobService) GetDownstream(ctx context.Context, projectSpec models.ProjectSpec, jobName string) ([]models.JobSpec, error) {
	args := srv.Called(ctx, projectSpec, jobName)
	return args.Get(0).([]models.JobSpec), args.Error(1)
}

func (srv *JobService) Refresh(ctx context.Context, projectName string, namespaceNames, jobNames []string, observer progress.Observer) error {
	args := srv.Called(ctx, projectName, namespaceNames, jobNames, observer)
	return args.Error(0)
}

func (srv *JobService) Deploy(ctx context.Context, projectName, namespaceName string, jobSpecs []models.JobSpec, observers progress.Observer) (models.DeploymentID, error) {
	args := srv.Called(ctx, projectName, namespaceName, jobSpecs, observers)
	return args.Get(0).(models.DeploymentID), args.Error(1)
}

func (srv *JobService) GetDeployment(ctx context.Context, deployID models.DeploymentID) (models.JobDeployment, error) {
	args := srv.Called(ctx, deployID)
	return args.Get(0).(models.JobDeployment), args.Error(1)
}

type DependencyResolver struct {
	mock.Mock
}

func (srv *DependencyResolver) Resolve(ctx context.Context, projectSpec models.ProjectSpec,
	jobSpec models.JobSpec, obs progress.Observer) (models.JobSpec, error) {
	args := srv.Called(ctx, projectSpec, jobSpec, obs)
	return args.Get(0).(models.JobSpec), args.Error(1)
}

func (srv *DependencyResolver) Persist(ctx context.Context, jobSpec models.JobSpec) error {
	args := srv.Called(ctx, jobSpec)
	return args.Error(0)
}

func (srv *DependencyResolver) FetchJobSpecsWithJobDependencies(ctx context.Context, projectSpec models.ProjectSpec) ([]models.JobSpec, error) {
	args := srv.Called(ctx, projectSpec)
	return args.Get(0).([]models.JobSpec), args.Error(1)
}

func (srv *DependencyResolver) FetchHookWithDependencies(jobSpec models.JobSpec) []models.JobSpecHook {
	args := srv.Called(jobSpec)
	return args.Get(0).([]models.JobSpecHook)
}

type PriorityResolver struct {
	mock.Mock
}

func (srv *PriorityResolver) Resolve(ctx context.Context, jobSpecs []models.JobSpec, po progress.Observer) ([]models.JobSpec, error) {
	args := srv.Called(ctx, jobSpecs, po)
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

// JobDependencyRepository to store job dependencies
type JobDependencyRepository struct {
	mock.Mock
}

func (repo *JobDependencyRepository) Save(ctx context.Context, projectID models.ProjectID, jobID uuid.UUID, dependency models.JobSpecDependency) error {
	args := repo.Called(ctx, projectID, jobID, dependency)
	return args.Error(0)
}

func (repo *JobDependencyRepository) GetAll(ctx context.Context, projectID models.ProjectID) ([]models.JobIDDependenciesPair, error) {
	args := repo.Called(ctx, projectID)
	return args.Get(0).([]models.JobIDDependenciesPair), args.Error(1)
}

func (repo *JobDependencyRepository) DeleteByJobID(ctx context.Context, jobID uuid.UUID) error {
	args := repo.Called(ctx, jobID)
	return args.Error(0)
}

type Deployer struct {
	mock.Mock
}

func (d *Deployer) Deploy(ctx context.Context, deployRequest models.JobDeployment) error {
	args := d.Called(ctx, deployRequest)
	return args.Error(0)
}

type DeployManager struct {
	mock.Mock
}

func (d *DeployManager) Deploy(ctx context.Context, projectSpec models.ProjectSpec) (models.DeploymentID, error) {
	args := d.Called(ctx, projectSpec)
	return args.Get(0).(models.DeploymentID), args.Error(1)
}

func (d *DeployManager) GetStatus(ctx context.Context, deployID models.DeploymentID) (models.JobDeployment, error) {
	args := d.Called(ctx, deployID)
	return args.Get(0).(models.JobDeployment), args.Error(1)
}

func (d *DeployManager) Init() {
	d.Called()
}

// JobDeploymentRepository to store job deployments
type JobDeploymentRepository struct {
	mock.Mock
}

func (repo *JobDeploymentRepository) Save(ctx context.Context, deployment models.JobDeployment) error {
	args := repo.Called(ctx, deployment)
	return args.Error(0)
}

func (repo *JobDeploymentRepository) GetByID(ctx context.Context, deployID models.DeploymentID) (models.JobDeployment, error) {
	args := repo.Called(ctx, deployID)
	return args.Get(0).(models.JobDeployment), args.Error(1)
}

func (repo *JobDeploymentRepository) GetByStatusAndProjectID(ctx context.Context, jobDeploymentStatus models.JobDeploymentStatus,
	projectID models.ProjectID) (models.JobDeployment, error) {
	args := repo.Called(ctx, jobDeploymentStatus, projectID)
	return args.Get(0).(models.JobDeployment), args.Error(1)
}

func (repo *JobDeploymentRepository) Update(ctx context.Context, deploymentSpec models.JobDeployment) error {
	args := repo.Called(ctx, deploymentSpec)
	return args.Error(0)
}

func (repo *JobDeploymentRepository) GetFirstExecutableRequest(ctx context.Context) (models.JobDeployment, error) {
	args := repo.Called(ctx)
	return args.Get(0).(models.JobDeployment), args.Error(1)
}

func (repo *JobDeploymentRepository) GetByStatus(ctx context.Context, status models.JobDeploymentStatus) ([]models.JobDeployment, error) {
	args := repo.Called(ctx, status)
	return args.Get(0).([]models.JobDeployment), args.Error(1)
}
