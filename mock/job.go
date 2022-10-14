package mock

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/api/writer"
	"github.com/odpf/optimus/internal/lib/progress"
	"github.com/odpf/optimus/models"
)

// JobSpecRepository is an autogenerated mock type for the JobSpecRepository type
type JobSpecRepository struct {
	mock.Mock
}

// DeleteByID provides a mock function with given fields: ctx, id
func (_m *JobSpecRepository) DeleteByID(ctx context.Context, id uuid.UUID) error {
	ret := _m.Called(ctx, id)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, uuid.UUID) error); ok {
		r0 = rf(ctx, id)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetAllByProjectName provides a mock function with given fields: ctx, projectName
func (_m *JobSpecRepository) GetAllByProjectName(ctx context.Context, projectName string) ([]models.JobSpec, error) {
	ret := _m.Called(ctx, projectName)

	var r0 []models.JobSpec
	if rf, ok := ret.Get(0).(func(context.Context, string) []models.JobSpec); ok {
		r0 = rf(ctx, projectName)
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).([]models.JobSpec)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, projectName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetAllByProjectNameAndNamespaceName provides a mock function with given fields: ctx, projectName, namespaceName
func (_m *JobSpecRepository) GetAllByProjectNameAndNamespaceName(ctx context.Context, projectName string, namespaceName string) ([]models.JobSpec, error) {
	ret := _m.Called(ctx, projectName, namespaceName)

	var r0 []models.JobSpec
	if rf, ok := ret.Get(0).(func(context.Context, string, string) []models.JobSpec); ok {
		r0 = rf(ctx, projectName, namespaceName)
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).([]models.JobSpec)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, string) error); ok {
		r1 = rf(ctx, projectName, namespaceName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetByNameAndProjectName provides a mock function with given fields: ctx, name, projectName
func (_m *JobSpecRepository) GetByNameAndProjectName(ctx context.Context, name string, projectName string) (models.JobSpec, error) {
	ret := _m.Called(ctx, name, projectName)

	var r0 models.JobSpec
	if rf, ok := ret.Get(0).(func(context.Context, string, string) models.JobSpec); ok {
		r0 = rf(ctx, name, projectName)
	} else {
		r0 = ret.Get(0).(models.JobSpec)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, string) error); ok {
		r1 = rf(ctx, name, projectName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetByResourceDestinationURN provides a mock function with given fields: ctx, resourceDestinationURN
func (_m *JobSpecRepository) GetByResourceDestinationURN(ctx context.Context, resourceDestinationURN string) ([]models.JobSpec, error) {
	ret := _m.Called(ctx, resourceDestinationURN)

	var r0 []models.JobSpec
	if rf, ok := ret.Get(0).(func(context.Context, string) []models.JobSpec); ok {
		r0 = rf(ctx, resourceDestinationURN)
	} else {
		r0 = ret.Get(0).([]models.JobSpec)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, resourceDestinationURN)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetDependentJobs provides a mock function with given fields: ctx, jobName, projectName, resourceDestinationURN
func (_m *JobSpecRepository) GetDependentJobs(ctx context.Context, jobName string, projectName string, resourceDestinationURN string) ([]models.JobSpec, error) {
	ret := _m.Called(ctx, jobName, projectName, resourceDestinationURN)

	var r0 []models.JobSpec
	if rf, ok := ret.Get(0).(func(context.Context, string, string, string) []models.JobSpec); ok {
		r0 = rf(ctx, jobName, projectName, resourceDestinationURN)
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).([]models.JobSpec)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, string, string) error); ok {
		r1 = rf(ctx, jobName, projectName, resourceDestinationURN)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetInferredDependenciesPerJobID provides a mock function with given fields: ctx, projectName
func (_m *JobSpecRepository) GetInferredDependenciesPerJobID(ctx context.Context, projectName string) (map[uuid.UUID][]models.JobSpec, error) {
	ret := _m.Called(ctx, projectName)

	var r0 map[uuid.UUID][]models.JobSpec
	if rf, ok := ret.Get(0).(func(context.Context, string) map[uuid.UUID][]models.JobSpec); ok {
		r0 = rf(ctx, projectName)
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).(map[uuid.UUID][]models.JobSpec)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, projectName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetStaticDependenciesPerJobID provides a mock function with given fields: ctx, projectName
func (_m *JobSpecRepository) GetStaticDependenciesPerJobID(ctx context.Context, projectName string) (map[uuid.UUID][]models.JobSpec, error) {
	ret := _m.Called(ctx, projectName)

	var r0 map[uuid.UUID][]models.JobSpec
	if rf, ok := ret.Get(0).(func(context.Context, string) map[uuid.UUID][]models.JobSpec); ok {
		r0 = rf(ctx, projectName)
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).(map[uuid.UUID][]models.JobSpec)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, projectName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Save provides a mock function with given fields: ctx, jobSpec
func (_m *JobSpecRepository) Save(ctx context.Context, jobSpec models.JobSpec) error {
	ret := _m.Called(ctx, jobSpec)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, models.JobSpec) error); ok {
		r0 = rf(ctx, jobSpec)
	} else {
		r0 = ret.Error(0)
	}

	return r0
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

type JobService struct {
	mock.Mock
}

func (srv *JobService) GetByFilter(ctx context.Context, filter models.JobSpecFilter) ([]models.JobSpec, error) {
	args := srv.Called(ctx, filter)
	return args.Get(0).([]models.JobSpec), args.Error(1)
}

// GetExternalJobRuns provides a mock function with given fields: ctx,  host, jobName, projectName, startDate, endDate,filter
func (srv *JobService) GetExternalJobRuns(ctx context.Context, host, jobName, projectName string, startDate, endDate time.Time, filter []string) ([]models.JobRun, error) {
	args := srv.Called(ctx, host, jobName, projectName, startDate, endDate, filter)
	return args.Get(0).([]models.JobRun), args.Error(1)
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

func (srv *JobService) GetJobBasicInfo(ctx context.Context, spec models.JobSpec) models.JobBasicInfo {
	args := srv.Called(ctx, spec)
	return args.Get(0).(models.JobBasicInfo)
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

func (srv *JobService) GetEnrichedUpstreamJobSpec(ctx context.Context, currentSpec models.JobSpec, jobSources []string, logWriter writer.LogWriter) (models.JobSpec, []models.UnknownDependency, error) {
	args := srv.Called(ctx, currentSpec, jobSources, logWriter)
	return args.Get(0).(models.JobSpec), args.Get(1).([]models.UnknownDependency), args.Error(2)
}

func (srv *JobService) GetDownstreamJobs(ctx context.Context, jobName, resourceDestinationURN, projectName string) ([]models.JobSpec, error) {
	args := srv.Called(ctx, jobName, resourceDestinationURN, projectName)
	return args.Get(0).([]models.JobSpec), args.Error(1)
}
func (srv *JobService) GetJobNamesWithDuplicateDestination(ctx context.Context, jobFullName, resourceDestination string) (string, error) {
	args := srv.Called(ctx, jobFullName, resourceDestination)
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

// GetExternalJobRuns provides a mock function with given fields: ctx,  host, jobName, projectName, startDate, endDate,filter
func (_m *ExternalDependencyResolver) GetExternalJobRuns(ctx context.Context, host, jobName, projectName string, startDate, endDate time.Time, filter []string) ([]models.JobRun, error) {
	args := _m.Called(ctx, host, jobName, projectName, startDate, endDate, filter)
	return args.Get(0).([]models.JobRun), args.Error(1)
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

// DependencyResolver is an autogenerated mock type for the DependencyResolver type
type DependencyResolver struct {
	mock.Mock
}

func (_m *DependencyResolver) GetEnrichedUpstreamJobSpec(ctx context.Context, subjectJobSpec models.JobSpec,
	upstreamDestinations []string, logWriter writer.LogWriter) (models.JobSpec, []models.UnknownDependency, error) {
	args := _m.Called(ctx, subjectJobSpec, upstreamDestinations, logWriter)
	return args.Get(0).(models.JobSpec), args.Get(1).([]models.UnknownDependency), args.Error(2)
}

// GetStaticDependencies provides a mock function with given fields: ctx, projectName
func (_m *DependencyResolver) GetStaticDependencies(ctx context.Context, jobSpec models.JobSpec, projectSpec models.ProjectSpec) (map[string]models.JobSpecDependency, error) {
	args := _m.Called(ctx, jobSpec, projectSpec)
	return args.Get(0).(map[string]models.JobSpecDependency), args.Error(1)
}

// GetExternalJobRuns provides a mock function with given fields: ctx,  host, jobName, projectName, startDate, endDate,filter
func (_m *DependencyResolver) GetExternalJobRuns(ctx context.Context, host, jobName, projectName string, startDate, endDate time.Time, filter []string) ([]models.JobRun, error) {
	args := _m.Called(ctx, host, jobName, projectName, startDate, endDate, filter)
	return args.Get(0).([]models.JobRun), args.Error(1)
}

// GetJobSpecsWithDependencies provides a mock function with given fields: ctx, projectName
func (_m *DependencyResolver) GetJobSpecsWithDependencies(ctx context.Context, projectName string) ([]models.JobSpec, []models.UnknownDependency, error) {
	ret := _m.Called(ctx, projectName)

	var r0 []models.JobSpec
	if rf, ok := ret.Get(0).(func(context.Context, string) []models.JobSpec); ok {
		r0 = rf(ctx, projectName)
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).([]models.JobSpec)
	}

	var r1 []models.UnknownDependency
	if rf, ok := ret.Get(1).(func(context.Context, string) []models.UnknownDependency); ok {
		r1 = rf(ctx, projectName)
	} else if ret.Get(1) != nil {
		r1 = ret.Get(1).([]models.UnknownDependency)
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(context.Context, string) error); ok { //nolint: gomnd
		r2 = rf(ctx, projectName)
	} else {
		r2 = ret.Error(2) //nolint: gomnd
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

func (repo *JobDeploymentRepository) GetAndUpdateExecutableRequests(ctx context.Context, limit int) ([]models.JobDeployment, error) {
	args := repo.Called(ctx, limit)
	return args.Get(0).([]models.JobDeployment), args.Error(1)
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

// GetResourceURNsPerJobID provides a mock function with given fields: ctx, projectName
func (_m *JobSourceRepository) GetResourceURNsPerJobID(ctx context.Context, projectName string) (map[uuid.UUID][]string, error) {
	ret := _m.Called(ctx, projectName)

	var r0 map[uuid.UUID][]string
	if rf, ok := ret.Get(0).(func(context.Context, string) map[uuid.UUID][]string); ok {
		r0 = rf(ctx, projectName)
	} else if ret.Get(0) != nil {
		r0 = ret.Get(0).(map[uuid.UUID][]string)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, projectName)
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

// GetExternalJobRuns provides a mock function with given fields: ctx,  host, jobName, projectName, startDate, endDate,filter
func (_m *ResourceManager) GetExternalJobRuns(ctx context.Context, host, jobName, projectName string, startDate, endDate time.Time, filter []string) ([]models.JobRun, error) {
	args := _m.Called(ctx, host, jobName, projectName, startDate, endDate, filter)
	return args.Get(0).([]models.JobRun), args.Error(1)
}

// GetHost provides a mock function
func (_m *ResourceManager) GetHost() string {
	args := _m.Called()
	return args.Get(0).(string)
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
