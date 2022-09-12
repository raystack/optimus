package mock

import (
	"context"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/api/writer"
	"github.com/odpf/optimus/core/progress"
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

// ProjectJobSpecRepository is an autogenerated mock type for the ProjectJobSpecRepository type
type ProjectJobSpecRepository struct {
	mock.Mock
}

// GetAll provides a mock function with given fields: _a0
func (_m *ProjectJobSpecRepository) GetAll(_a0 context.Context) ([]models.JobSpec, error) {
	ret := _m.Called(_a0)

	var r0 []models.JobSpec
	if rf, ok := ret.Get(0).(func(context.Context) []models.JobSpec); ok {
		r0 = rf(_a0)
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).([]models.JobSpec)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetByIDs provides a mock function with given fields: _a0, _a1
func (_m *ProjectJobSpecRepository) GetByIDs(_a0 context.Context, _a1 []uuid.UUID) ([]models.JobSpec, error) {
	ret := _m.Called(_a0, _a1)

	var r0 []models.JobSpec
	if rf, ok := ret.Get(0).(func(context.Context, []uuid.UUID) []models.JobSpec); ok {
		r0 = rf(_a0, _a1)
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).([]models.JobSpec)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, []uuid.UUID) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetByName provides a mock function with given fields: _a0, _a1
func (_m *ProjectJobSpecRepository) GetByName(_a0 context.Context, _a1 string) (models.JobSpec, models.NamespaceSpec, error) {
	ret := _m.Called(_a0, _a1)

	var r0 models.JobSpec
	if rf, ok := ret.Get(0).(func(context.Context, string) models.JobSpec); ok {
		r0 = rf(_a0, _a1)
	} else {
		r0 = ret.Get(0).(models.JobSpec)
	}

	var r1 models.NamespaceSpec
	if rf, ok := ret.Get(1).(func(context.Context, string) models.NamespaceSpec); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Get(1).(models.NamespaceSpec)
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(context.Context, string) error); ok { // nolint: gomnd
		r2 = rf(_a0, _a1)
	} else {
		r2 = ret.Error(2) // nolint:gomnd
	}

	return r0, r1, r2
}

// GetByNameForProject provides a mock function with given fields: ctx, projectName, jobName
func (_m *ProjectJobSpecRepository) GetByNameForProject(ctx context.Context, projectName string, jobName string) (models.JobSpec, models.ProjectSpec, error) {
	ret := _m.Called(ctx, projectName, jobName)

	var r0 models.JobSpec
	if rf, ok := ret.Get(0).(func(context.Context, string, string) models.JobSpec); ok {
		r0 = rf(ctx, projectName, jobName)
	} else {
		r0 = ret.Get(0).(models.JobSpec)
	}

	var r1 models.ProjectSpec
	if rf, ok := ret.Get(1).(func(context.Context, string, string) models.ProjectSpec); ok {
		r1 = rf(ctx, projectName, jobName)
	} else {
		r1 = ret.Get(1).(models.ProjectSpec)
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(context.Context, string, string) error); ok { // nolint:gomnd
		r2 = rf(ctx, projectName, jobName)
	} else {
		r2 = ret.Error(2) // nolint:gomnd
	}

	return r0, r1, r2
}

// GetJobNamespaces provides a mock function with given fields: ctx
func (_m *ProjectJobSpecRepository) GetJobNamespaces(ctx context.Context) (map[string][]string, error) {
	ret := _m.Called(ctx)

	var r0 map[string][]string
	if rf, ok := ret.Get(0).(func(context.Context) map[string][]string); ok {
		r0 = rf(ctx)
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).(map[string][]string)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(ctx)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type mockConstructorTestingTNewProjectJobSpecRepository interface {
	mock.TestingT
	Cleanup(func())
}

// NewProjectJobSpecRepository creates a new instance of ProjectJobSpecRepository. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewProjectJobSpecRepository(t mockConstructorTestingTNewProjectJobSpecRepository) *ProjectJobSpecRepository {
	mock := &ProjectJobSpecRepository{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// JobSpecRepository is an autogenerated mock type for the JobSpecRepository type
type JobSpecRepository struct {
	mock.Mock
}

func (_m *JobSpecRepository) GetDependentJobsInferred(ctx context.Context, jobSpec *models.JobSpec) ([]models.JobSpec, error) {
	args := _m.Called(ctx, jobSpec)
	return args.Get(0).([]models.JobSpec), args.Error(1)
}

func (_m *JobSpecRepository) GetDependentJobsStatic(ctx context.Context, jobSpec *models.JobSpec) ([]models.JobSpec, error) {
	args := _m.Called(ctx, jobSpec)
	return args.Get(0).([]models.JobSpec), args.Error(1)
}

// GetAllByProjectID provides a mock function with given fields: _a0, _a1
func (_m *JobSpecRepository) GetAllByProjectID(_a0 context.Context, _a1 models.ProjectID) ([]models.JobSpec, error) {
	ret := _m.Called(_a0, _a1)

	var r0 []models.JobSpec
	if rf, ok := ret.Get(0).(func(context.Context, models.ProjectID) []models.JobSpec); ok {
		r0 = rf(_a0, _a1)
	}
	if ret.Get(0) != nil {
		r0 = ret.Get(0).([]models.JobSpec)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, models.ProjectID) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetDependentJobs provides a mock function with given fields: _a0, _a1
func (_m *JobSpecRepository) GetDependentJobs(_a0 context.Context, _a1 *models.JobSpec) ([]models.JobSpec, error) {
	ret := _m.Called(_a0, _a1)

	var r0 []models.JobSpec
	if rf, ok := ret.Get(0).(func(context.Context, *models.JobSpec) []models.JobSpec); ok {
		r0 = rf(_a0, _a1)
	}
	if ret.Get(0) != nil {
		r0 = ret.Get(0).([]models.JobSpec)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *models.JobSpec) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetInferredDependenciesPerJobID provides a mock function with given fields: _a0, _a1
func (_m *JobSpecRepository) GetInferredDependenciesPerJobID(_a0 context.Context, _a1 models.ProjectID) (map[uuid.UUID][]models.JobSpec, error) {
	ret := _m.Called(_a0, _a1)

	var r0 map[uuid.UUID][]models.JobSpec
	if rf, ok := ret.Get(0).(func(context.Context, models.ProjectID) map[uuid.UUID][]models.JobSpec); ok {
		r0 = rf(_a0, _a1)
	}
	if ret.Get(0) != nil {
		r0 = ret.Get(0).(map[uuid.UUID][]models.JobSpec)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, models.ProjectID) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetJobByName provides a mock function with given fields: _a0, _a1
func (_m *JobSpecRepository) GetJobByName(_a0 context.Context, _a1 string) ([]models.JobSpec, error) {
	ret := _m.Called(_a0, _a1)

	var r0 []models.JobSpec
	if rf, ok := ret.Get(0).(func(context.Context, string) []models.JobSpec); ok {
		r0 = rf(_a0, _a1)
	}
	if ret.Get(0) != nil {
		r0 = ret.Get(0).([]models.JobSpec)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetJobByResourceDestination provides a mock function with given fields: _a0, _a1
func (_m *JobSpecRepository) GetJobByResourceDestination(_a0 context.Context, _a1 string) (models.JobSpec, error) {
	ret := _m.Called(_a0, _a1)

	var r0 models.JobSpec
	if rf, ok := ret.Get(0).(func(context.Context, string) models.JobSpec); ok {
		r0 = rf(_a0, _a1)
	} else {
		r0 = ret.Get(0).(models.JobSpec)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetStaticDependenciesPerJobID provides a mock function with given fields: _a0, _a1
func (_m *JobSpecRepository) GetStaticDependenciesPerJobID(_a0 context.Context, _a1 models.ProjectID) (map[uuid.UUID][]models.JobSpec, error) {
	ret := _m.Called(_a0, _a1)

	var r0 map[uuid.UUID][]models.JobSpec
	if rf, ok := ret.Get(0).(func(context.Context, models.ProjectID) map[uuid.UUID][]models.JobSpec); ok {
		r0 = rf(_a0, _a1)
	}
	if ret.Get(0) != nil {
		r0 = ret.Get(0).(map[uuid.UUID][]models.JobSpec)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, models.ProjectID) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type mockConstructorTestingTNewJobSpecRepository interface {
	mock.TestingT
	Cleanup(func())
}

// NewJobSpecRepository creates a new instance of JobSpecRepository. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewJobSpecRepository(t mockConstructorTestingTNewJobSpecRepository) *JobSpecRepository {
	mock := &JobSpecRepository{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// NamespaceJobSpecRepoFactory to store raw specs at namespace level
type NamespaceJobSpecRepoFactory struct {
	mock.Mock
}

func (repo *NamespaceJobSpecRepoFactory) New(namespace models.NamespaceSpec) store.NamespaceJobSpecRepository {
	return repo.Called(namespace).Get(0).(store.NamespaceJobSpecRepository)
}

// NamespaceJobSpecRepoFactory to store raw specs
type NamespaceJobSpecRepository struct {
	mock.Mock
}

func (repo *NamespaceJobSpecRepository) Save(ctx context.Context, t models.JobSpec, jobDestination string) error {
	return repo.Called(ctx, t, jobDestination).Error(0)
}

func (repo *NamespaceJobSpecRepository) GetByName(ctx context.Context, name string) (models.JobSpec, error) {
	args := repo.Called(ctx, name)
	if args.Get(0) != nil {
		return args.Get(0).(models.JobSpec), args.Error(1)
	}
	return models.JobSpec{}, args.Error(1)
}

func (repo *NamespaceJobSpecRepository) Delete(ctx context.Context, id uuid.UUID) error {
	return repo.Called(ctx, id).Error(0)
}

func (repo *NamespaceJobSpecRepository) GetAll(ctx context.Context) ([]models.JobSpec, error) {
	args := repo.Called(ctx)
	if args.Get(0) != nil {
		return args.Get(0).([]models.JobSpec), args.Error(1)
	}
	return []models.JobSpec{}, args.Error(1)
}

func (repo *NamespaceJobSpecRepository) GetByDestination(ctx context.Context, dest string) (models.JobSpec, models.ProjectSpec, error) {
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

func (srv *JobService) GetByFilter(ctx context.Context, filter models.JobSpecFilter) ([]models.JobSpec, error) {
	args := srv.Called(ctx, filter)
	return args.Get(0).([]models.JobSpec), args.Error(1)
}

func (srv *JobService) Create(ctx context.Context, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) (models.JobSpec, error) {
	args := srv.Called(ctx, namespaceSpec, jobSpec)
	return args.Get(0).(models.JobSpec), args.Error(1)
}

func (srv *JobService) GetByName(ctx context.Context, s string, spec models.NamespaceSpec) (models.JobSpec, error) {
	args := srv.Called(ctx, s, spec)
	return args.Get(0).(models.JobSpec), args.Error(1)
}

func (srv *JobService) GetAll(ctx context.Context, spec models.NamespaceSpec) ([]models.JobSpec, error) {
	args := srv.Called(ctx, spec)
	return args.Get(0).([]models.JobSpec), args.Error(1)
}

func (srv *JobService) GetByNameForProject(ctx context.Context, s string, spec models.ProjectSpec) (models.JobSpec, models.NamespaceSpec, error) {
	args := srv.Called(ctx, s, spec)
	return args.Get(0).(models.JobSpec), args.Get(1).(models.NamespaceSpec), args.Error(2)
}

func (srv *JobService) GetTaskDependencies(ctx context.Context, namespaceSpec models.NamespaceSpec, spec models.JobSpec) (models.JobSpecTaskDestination,
	models.JobSpecTaskDependencies, error) {
	args := srv.Called(ctx, namespaceSpec, spec)
	return args.Get(0).(models.JobSpecTaskDestination), args.Get(1).(models.JobSpecTaskDependencies), args.Error(2)
}

func (srv *JobService) Check(ctx context.Context, namespaceSpec models.NamespaceSpec, specs []models.JobSpec, logWriter writer.LogWriter) error {
	args := srv.Called(ctx, namespaceSpec, specs, logWriter)
	return args.Error(0)
}

func (srv *JobService) Delete(ctx context.Context, c models.NamespaceSpec, jobSpec models.JobSpec) error {
	args := srv.Called(ctx, c, jobSpec)
	return args.Error(0)
}

func (srv *JobService) Run(ctx context.Context, ns models.NamespaceSpec, js []models.JobSpec) (models.JobDeploymentDetail, error) {
	args := srv.Called(ctx, ns, js)
	return args.Get(0).(models.JobDeploymentDetail), args.Error(1)
}

func (srv *JobService) GetByDestination(_ context.Context, projectSpec models.ProjectSpec, destination string) (models.JobSpec, error) {
	args := srv.Called(projectSpec, destination)
	return args.Get(0).(models.JobSpec), args.Error(1)
}

func (srv *JobService) GetDownstream(ctx context.Context, projectSpec models.ProjectSpec, jobName string) ([]models.JobSpec, error) {
	args := srv.Called(ctx, projectSpec, jobName)
	return args.Get(0).([]models.JobSpec), args.Error(1)
}

func (srv *JobService) ResolveDependecy(ctx context.Context, resourceDestinations []string, currentSpec models.JobSpec, logWriter writer.LogWriter) ([]models.JobSpec, error) {
	args := srv.Called(ctx, resourceDestinations, currentSpec, logWriter)
	return args.Get(0).([]models.JobSpec), args.Error(1)
}

func (srv *JobService) Refresh(ctx context.Context, projectName string, namespaceNames, jobNames []string, logWriter writer.LogWriter) (models.DeploymentID, error) {
	args := srv.Called(ctx, projectName, namespaceNames, jobNames, logWriter)
	return args.Get(0).(models.DeploymentID), args.Error(1)
}

func (srv *JobService) Deploy(ctx context.Context, projectName, namespaceName string, jobSpecs []models.JobSpec, logWriter writer.LogWriter) (models.DeploymentID, error) {
	args := srv.Called(ctx, projectName, namespaceName, jobSpecs, logWriter)
	return args.Get(0).(models.DeploymentID), args.Error(1)
}

func (srv *JobService) GetDeployment(ctx context.Context, deployID models.DeploymentID) (models.JobDeployment, error) {
	args := srv.Called(ctx, deployID)
	return args.Get(0).(models.JobDeployment), args.Error(1)
}
func (srv *JobService) GetByJobName(ctx context.Context, jobName string) (models.JobSpec, error) {
	args := srv.Called(ctx, jobName)
	return args.Get(0).(models.JobSpec), args.Error(1)
}
func (srv *JobService) GetByResourceDestination(ctx context.Context, resourceDestination string) (models.JobSpec, error) {
	args := srv.Called(ctx, resourceDestination)
	return args.Get(0).(models.JobSpec), args.Error(1)
}

func (srv *JobService) CreateAndDeploy(ctx context.Context, namespaceSpec models.NamespaceSpec, jobSpec []models.JobSpec, logWriter writer.LogWriter) (models.DeploymentID, error) {
	args := srv.Called(ctx, namespaceSpec, jobSpec, logWriter)
	return args.Get(0).(models.DeploymentID), args.Error(1)
}

func (srv *JobService) GetDownstreamJobs(ctx context.Context, currentSpec models.JobSpec) ([]models.JobSpec, error) {
	args := srv.Called(ctx, currentSpec)
	return args.Get(0).([]models.JobSpec), args.Error(1)
}

func (srv *JobService) EnrichUpstreamJobs(ctx context.Context, currentSpec models.JobSpec, jobSources []string, logWriter writer.LogWriter) (models.JobSpec, []models.UnknownDependency, error) {
	args := srv.Called(ctx, currentSpec, jobSources, logWriter)
	return args.Get(0).(models.JobSpec), args.Get(1).([]models.UnknownDependency), args.Error(2)
}

func (srv *JobService) IsJobDestinationDuplicate(ctx context.Context, jobSpec models.JobSpec) (string, error) {
	args := srv.Called(ctx, jobSpec)
	return args.Get(0).(string), args.Error(1)
}

// ExternalDependencyResolver is an autogenerated mock type for the ExternalDependencyResolver type
type ExternalDependencyResolver struct {
	mock.Mock
}

// FetchInferredExternalDependenciesPerJobName provides a mock function with given fields: ctx, unresolvedDependenciesPerJobName
func (_m *ExternalDependencyResolver) FetchInferredExternalDependenciesPerJobName(ctx context.Context, unresolvedDependenciesPerJobName map[string][]models.UnresolvedJobDependency) (map[string]models.ExternalDependency, error) {
	ret := _m.Called(ctx, unresolvedDependenciesPerJobName)

	var r0 map[string]models.ExternalDependency
	if rf, ok := ret.Get(0).(func(context.Context, map[string][]models.UnresolvedJobDependency) map[string]models.ExternalDependency); ok {
		r0 = rf(ctx, unresolvedDependenciesPerJobName)
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).(map[string]models.ExternalDependency)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, map[string][]models.UnresolvedJobDependency) error); ok {
		r1 = rf(ctx, unresolvedDependenciesPerJobName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// FetchStaticExternalDependenciesPerJobName provides a mock function with given fields: ctx, unresolvedDependenciesPerJobName
func (_m *ExternalDependencyResolver) FetchStaticExternalDependenciesPerJobName(ctx context.Context, unresolvedDependenciesPerJobName map[string][]models.UnresolvedJobDependency) (map[string]models.ExternalDependency, []models.UnknownDependency, error) {
	ret := _m.Called(ctx, unresolvedDependenciesPerJobName)

	var r0 map[string]models.ExternalDependency
	if rf, ok := ret.Get(0).(func(context.Context, map[string][]models.UnresolvedJobDependency) map[string]models.ExternalDependency); ok {
		r0 = rf(ctx, unresolvedDependenciesPerJobName)
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).(map[string]models.ExternalDependency)
	}

	var r1 []models.UnknownDependency
	if rf, ok := ret.Get(1).(func(context.Context, map[string][]models.UnresolvedJobDependency) []models.UnknownDependency); ok {
		r1 = rf(ctx, unresolvedDependenciesPerJobName)
	} else if ret.Get(1) != nil {
		r1 = ret.Get(1).([]models.UnknownDependency)
	}

	var r2 error
	errIndex := 2
	if rf, ok := ret.Get(errIndex).(func(context.Context, map[string][]models.UnresolvedJobDependency) error); ok {
		r2 = rf(ctx, unresolvedDependenciesPerJobName)
	} else {
		r2 = ret.Error(errIndex)
	}

	return r0, r1, r2
}

type mockConstructorTestingTNewExternalDependencyResolver interface {
	mock.TestingT
	Cleanup(func())
}

// NewExternalDependencyResolver creates a new instance of ExternalDependencyResolver. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewExternalDependencyResolver(t mockConstructorTestingTNewExternalDependencyResolver) *ExternalDependencyResolver {
	mock := &ExternalDependencyResolver{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// DependencyResolver is an autogenerated mock type for the DependencyResolver type
type DependencyResolver struct {
	mock.Mock
}

func (_m *DependencyResolver) EnrichUpstreamJobs(ctx context.Context, subjectJobSpec models.JobSpec,
	upstreamDestinations []string, logWriter writer.LogWriter) (models.JobSpec, []models.UnknownDependency, error) {
	args := _m.Called(ctx, subjectJobSpec, upstreamDestinations, logWriter)
	return args.Get(0).(models.JobSpec), args.Get(1).([]models.UnknownDependency), args.Error(2)
}

// GetJobSpecsWithDependencies provides a mock function with given fields: ctx, projectID
func (_m *DependencyResolver) GetJobSpecsWithDependencies(ctx context.Context, projectID models.ProjectID) ([]models.JobSpec, []models.UnknownDependency, error) {
	ret := _m.Called(ctx, projectID)

	var r0 []models.JobSpec
	if rf, ok := ret.Get(0).(func(context.Context, models.ProjectID) []models.JobSpec); ok {
		r0 = rf(ctx, projectID)
	}
	if ret.Get(0) != nil {
		r0 = ret.Get(0).([]models.JobSpec)
	}

	var r1 []models.UnknownDependency
	if rf, ok := ret.Get(1).(func(context.Context, models.ProjectID) []models.UnknownDependency); ok {
		r1 = rf(ctx, projectID)
	}
	if ret.Get(1) != nil {
		r1 = ret.Get(1).([]models.UnknownDependency)
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(context.Context, models.ProjectID) error); ok { //nolint:gomnd
		r2 = rf(ctx, projectID)
	} else {
		r2 = ret.Error(2) //nolint:gomnd
	}

	return r0, r1, r2
}

func (_m *DependencyResolver) GetJobsByResourceDestinations(ctx context.Context, resourceDestinations []string, subjectJobName string, logWriter writer.LogWriter) ([]models.JobSpec, error) {
	args := _m.Called(ctx, resourceDestinations, subjectJobName, logWriter)
	return args.Get(0).([]models.JobSpec), args.Error(1)
}

// Resolve provides a mock function with given fields: ctx, projectSpec, jobSpec, observer
func (_m *DependencyResolver) Resolve(ctx context.Context, projectSpec models.ProjectSpec, jobSpec models.JobSpec, observer progress.Observer) (models.JobSpec, error) {
	ret := _m.Called(ctx, projectSpec, jobSpec, observer)

	var r0 models.JobSpec
	if rf, ok := ret.Get(0).(func(context.Context, models.ProjectSpec, models.JobSpec, progress.Observer) models.JobSpec); ok {
		r0 = rf(ctx, projectSpec, jobSpec, observer)
	} else {
		r0 = ret.Get(0).(models.JobSpec)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, models.ProjectSpec, models.JobSpec, progress.Observer) error); ok {
		r1 = rf(ctx, projectSpec, jobSpec, observer)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ResolveStaticDependencies provides a mock function with given fields: ctx, jobSpec, projectSpec, projectJobSpecRepo
func (_m *DependencyResolver) ResolveStaticDependencies(ctx context.Context, jobSpec models.JobSpec, projectSpec models.ProjectSpec, projectJobSpecRepo store.ProjectJobSpecRepository) (map[string]models.JobSpecDependency, error) {
	ret := _m.Called(ctx, jobSpec, projectSpec, projectJobSpecRepo)

	var r0 map[string]models.JobSpecDependency
	if rf, ok := ret.Get(0).(func(context.Context, models.JobSpec, models.ProjectSpec, store.ProjectJobSpecRepository) map[string]models.JobSpecDependency); ok {
		r0 = rf(ctx, jobSpec, projectSpec, projectJobSpecRepo)
	}
	if ret.Get(0) != nil {
		r0 = ret.Get(0).(map[string]models.JobSpecDependency)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, models.JobSpec, models.ProjectSpec, store.ProjectJobSpecRepository) error); ok {
		r1 = rf(ctx, jobSpec, projectSpec, projectJobSpecRepo)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type mockConstructorTestingTNewDependencyResolver interface {
	mock.TestingT
	Cleanup(func())
}

// NewDependencyResolver creates a new instance of DependencyResolver. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewDependencyResolver(t mockConstructorTestingTNewDependencyResolver) *DependencyResolver {
	mock := &DependencyResolver{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
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

// JobSourceRepository is an autogenerated mock type for the JobSourceRepository type
type JobSourceRepository struct {
	mock.Mock
}

// DeleteByJobID provides a mock function with given fields: _a0, _a1
func (_m *JobSourceRepository) DeleteByJobID(_a0 context.Context, _a1 uuid.UUID) error {
	ret := _m.Called(_a0, _a1)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, uuid.UUID) error); ok {
		r0 = rf(_a0, _a1)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetAll provides a mock function with given fields: _a0, _a1
func (_m *JobSourceRepository) GetAll(_a0 context.Context, _a1 models.ProjectID) ([]models.JobSource, error) {
	ret := _m.Called(_a0, _a1)

	var r0 []models.JobSource
	if rf, ok := ret.Get(0).(func(context.Context, models.ProjectID) []models.JobSource); ok {
		r0 = rf(_a0, _a1)
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).([]models.JobSource)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, models.ProjectID) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetByResourceURN provides a mock function with given fields: _a0, _a1
func (_m *JobSourceRepository) GetByResourceURN(_a0 context.Context, _a1 string) ([]models.JobSource, error) {
	ret := _m.Called(_a0, _a1)

	var r0 []models.JobSource
	if rf, ok := ret.Get(0).(func(context.Context, string) []models.JobSource); ok {
		r0 = rf(_a0, _a1)
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).([]models.JobSource)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetResourceURNsPerJobID provides a mock function with given fields: _a0, _a1
func (_m *JobSourceRepository) GetResourceURNsPerJobID(_a0 context.Context, _a1 models.ProjectID) (map[uuid.UUID][]string, error) {
	ret := _m.Called(_a0, _a1)

	var r0 map[uuid.UUID][]string
	if rf, ok := ret.Get(0).(func(context.Context, models.ProjectID) map[uuid.UUID][]string); ok {
		r0 = rf(_a0, _a1)
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).(map[uuid.UUID][]string)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, models.ProjectID) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Save provides a mock function with given fields: ctx, projectID, jobID, jobSourceURNs
func (_m *JobSourceRepository) Save(ctx context.Context, projectID models.ProjectID, jobID uuid.UUID, jobSourceURNs []string) error {
	ret := _m.Called(ctx, projectID, jobID, jobSourceURNs)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, models.ProjectID, uuid.UUID, []string) error); ok {
		r0 = rf(ctx, projectID, jobID, jobSourceURNs)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

type mockConstructorTestingTNewJobSourceRepository interface {
	mock.TestingT
	Cleanup(func())
}

// NewJobSourceRepository creates a new instance of JobSourceRepository. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewJobSourceRepository(t mockConstructorTestingTNewJobSourceRepository) *JobSourceRepository {
	mock := &JobSourceRepository{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// ResourceManager is an autogenerated mock type for the ResourceManager type
type ResourceManager struct {
	mock.Mock
}

// GetOptimusDependencies provides a mock function with given fields: _a0, _a1
func (_m *ResourceManager) GetOptimusDependencies(_a0 context.Context, _a1 models.UnresolvedJobDependency) ([]models.OptimusDependency, error) {
	ret := _m.Called(_a0, _a1)

	var r0 []models.OptimusDependency
	if rf, ok := ret.Get(0).(func(context.Context, models.UnresolvedJobDependency) []models.OptimusDependency); ok {
		r0 = rf(_a0, _a1)
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).([]models.OptimusDependency)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, models.UnresolvedJobDependency) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type mockConstructorTestingTNewResourceManager interface {
	mock.TestingT
	Cleanup(func())
}

// NewResourceManager creates a new instance of ResourceManager. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewResourceManager(t mockConstructorTestingTNewResourceManager) *ResourceManager {
	mock := &ResourceManager{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
