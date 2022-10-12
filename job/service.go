package job

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/kushsharma/parallel"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/odpf/optimus/api/writer"
	"github.com/odpf/optimus/internal/lib/progress"
	"github.com/odpf/optimus/internal/lib/tree"
	"github.com/odpf/optimus/internal/store"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
)

const (
	ConcurrentTicketPerSec = 40
	ConcurrentLimit        = 600

	MetricDependencyResolutionStatus  = "status"
	MetricDependencyResolutionSucceed = "succeed"
	MetricDependencyResolutionFailed  = "failed"
)

var (
	errDependencyResolution = fmt.Errorf("dependency resolution")

	resolveDependencyGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "runtime_job_dependency",
		Help: "Number of job dependency resolution succeed/failed",
	},
		[]string{MetricDependencyResolutionStatus},
	)

	resolveDependencyHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "runtime_job_dependency_histogram",
		Help: "Duration of resolving job dependency",
	})
)

// DependencyResolver compiles static and runtime dependencies
// TODO: when refactoring, we need to rethink about renaming it
type DependencyResolver interface {
	Resolve(ctx context.Context, projectSpec models.ProjectSpec, jobSpec models.JobSpec, observer progress.Observer) (models.JobSpec, error)
	GetJobSpecsWithDependencies(ctx context.Context, projectName string) ([]models.JobSpec, []models.UnknownDependency, error)
}

type Deployer interface {
	Deploy(context.Context, models.JobDeployment) error
}

// NamespaceRepoFactory is used to store job specs
type NamespaceRepoFactory interface {
	New(spec models.ProjectSpec) store.NamespaceRepository
}

type ReplayManager interface {
	Init()
	Replay(context.Context, models.ReplayRequest) (models.ReplayResult, error)
	GetReplay(context.Context, uuid.UUID) (models.ReplaySpec, error)
	GetReplayList(ctx context.Context, projectID models.ProjectID) ([]models.ReplaySpec, error)
	GetRunStatus(ctx context.Context, projectSpec models.ProjectSpec, startDate, endDate time.Time,
		jobName string) ([]models.JobStatus, error)
}

// Service compiles all jobs with its dependencies, priority
// and other properties. Finally, it syncs the jobs with corresponding
// store
type Service struct {
	dependencyResolver DependencyResolver
	priorityResolver   PriorityResolver
	replayManager      ReplayManager
	projectService     service.ProjectService
	namespaceService   service.NamespaceService
	jobSpecRepository  store.JobSpecRepository
	deployManager      DeployManager

	// scheduler for managing batch scheduled jobs
	batchScheduler models.SchedulerUnit

	// scheduler for managing one time executable jobs
	manualScheduler models.SchedulerUnit

	pluginService service.PluginService

	jobSourceRepo store.JobSourceRepository
}

// NewService creates a new instance of JobService, requiring
// the necessary dependencies as arguments
func NewService(batchScheduler models.SchedulerUnit,
	manualScheduler models.SchedulerUnit, dependencyResolver DependencyResolver, priorityResolver PriorityResolver,
	replayManager ReplayManager, namespaceService service.NamespaceService,
	projectService service.ProjectService, deployManager DeployManager, pluginService service.PluginService,
	jobSpecRepository store.JobSpecRepository,
	jobSourceRepository store.JobSourceRepository,
) *Service {
	return &Service{
		batchScheduler:     batchScheduler,
		manualScheduler:    manualScheduler,
		dependencyResolver: dependencyResolver,
		priorityResolver:   priorityResolver,
		replayManager:      replayManager,
		namespaceService:   namespaceService,
		projectService:     projectService,
		deployManager:      deployManager,
		jobSpecRepository:  jobSpecRepository,
		jobSourceRepo:      jobSourceRepository,
		pluginService:      pluginService,
	}
}

// Create constructs a Job for a namespace and commits it to the store
func (srv *Service) Create(ctx context.Context, namespace models.NamespaceSpec, spec models.JobSpec) (models.JobSpec, error) {
	jobDestinationResponse, err := srv.pluginService.GenerateDestination(ctx, spec, namespace)
	if err != nil {
		if !errors.Is(err, service.ErrDependencyModNotFound) {
			return models.JobSpec{}, fmt.Errorf("failed to GenerateDestination for job: %s: %w", spec.Name, err)
		}
	}
	var jobDestination string
	if jobDestinationResponse != nil {
		jobDestination = jobDestinationResponse.URN()
	}
	spec.NamespaceSpec = namespace
	spec.ResourceDestination = jobDestination
	if err := srv.jobSpecRepository.Save(ctx, spec); err != nil {
		return models.JobSpec{}, fmt.Errorf("failed to save job: %s: %w", spec.Name, err)
	}

	result, err := srv.jobSpecRepository.GetByNameAndProjectName(ctx, spec.Name, spec.GetProjectSpec().Name)
	if err != nil {
		return models.JobSpec{}, fmt.Errorf("failed to fetch job on create: %s: %w", spec.Name, err)
	}

	return result, nil
}

func (srv *Service) bulkCreate(ctx context.Context, namespace models.NamespaceSpec, jobSpecs []models.JobSpec, logWriter writer.LogWriter) []models.JobSpec {
	result := []models.JobSpec{}
	var op string

	successCreate, successModify, failureCreate, failureModify := 0, 0, 0, 0
	for _, jobSpec := range jobSpecs {
		jobSpecCreated, err := srv.Create(ctx, namespace, jobSpec)
		if err != nil {
			if jobSpec.ID == uuid.Nil {
				failureCreate++
				op = "create"
			} else {
				failureModify++
				op = "modify"
			}
			errMsg := fmt.Sprintf("[%s] error '%s': failed to %s job, %s", namespace.Name, jobSpec.Name, op, err.Error())
			logWriter.Write(writer.LogLevelError, errMsg)

			continue
		}

		if jobSpec.ID == uuid.Nil {
			successCreate++
			op = "created"
		} else {
			successModify++
			op = "modified"
		}
		successMsg := fmt.Sprintf("[%s] info '%s': job %s", namespace.Name, jobSpec.Name, op)
		logWriter.Write(writer.LogLevelInfo, successMsg)

		result = append(result, jobSpecCreated)
	}

	if failureCreate > 0 {
		errMsg := fmt.Sprintf("[%s] Created %d/%d jobs", namespace.Name, successCreate, successCreate+failureCreate)
		logWriter.Write(writer.LogLevelError, errMsg)
	} else {
		successMsg := fmt.Sprintf("[%s] Created %d jobs", namespace.Name, successCreate)
		logWriter.Write(writer.LogLevelInfo, successMsg)
	}

	if failureModify > 0 {
		errMsg := fmt.Sprintf("[%s] Modifyd %d/%d jobs", namespace.Name, successModify, successModify+failureModify)
		logWriter.Write(writer.LogLevelError, errMsg)
	} else {
		successMsg := fmt.Sprintf("[%s] Modifyd %d jobs", namespace.Name, successModify)
		logWriter.Write(writer.LogLevelInfo, successMsg)
	}

	return result
}

// GetByName fetches a Job by name for a specific namespace
// TODO: replace namespace with project name
func (srv *Service) GetByName(ctx context.Context, name string, namespace models.NamespaceSpec) (models.JobSpec, error) {
	jobSpec, err := srv.jobSpecRepository.GetByNameAndProjectName(ctx, name, namespace.ProjectSpec.Name)
	if err != nil {
		return models.JobSpec{}, fmt.Errorf("failed to retrieve job: %w", err)
	}
	return jobSpec, nil
}

func (srv *Service) GetByFilter(ctx context.Context, filter models.JobSpecFilter) ([]models.JobSpec, error) {
	if filter.ResourceDestination != "" {
		jobSpecs, err := srv.jobSpecRepository.GetByResourceDestinationURN(ctx, filter.ResourceDestination)
		if err != nil {
			if errors.Is(err, store.ErrResourceNotFound) {
				return []models.JobSpec{}, nil
			}
			return nil, err
		}
		return jobSpecs, nil
	}
	if filter.ProjectName != "" {
		if filter.JobName == "" {
			return srv.jobSpecRepository.GetAllByProjectName(ctx, filter.ProjectName)
		}
		jobSpec, err := srv.jobSpecRepository.GetByNameAndProjectName(ctx, filter.JobName, filter.ProjectName)
		if err != nil {
			if errors.Is(err, store.ErrResourceNotFound) {
				return []models.JobSpec{}, nil
			}
			return nil, err
		}
		return []models.JobSpec{jobSpec}, nil
	}
	return nil, fmt.Errorf("filters not specified")
}

// GetByNameForProject fetches a Job by name for a specific project
// TODO: replace project spec with project name, and remove namespace from return
func (srv *Service) GetByNameForProject(ctx context.Context, name string, proj models.ProjectSpec) (models.JobSpec, models.NamespaceSpec, error) {
	jobSpec, err := srv.jobSpecRepository.GetByNameAndProjectName(ctx, name, proj.Name)
	if err != nil {
		return models.JobSpec{}, models.NamespaceSpec{}, fmt.Errorf("failed to retrieve job: %w", err)
	}
	return jobSpec, jobSpec.NamespaceSpec, nil
}

// TODO: use project name and namespace name instead
func (srv *Service) GetAll(ctx context.Context, namespace models.NamespaceSpec) ([]models.JobSpec, error) {
	jobSpecs, err := srv.jobSpecRepository.GetAllByProjectNameAndNamespaceName(ctx, namespace.ProjectSpec.Name, namespace.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve jobs: %w", err)
	}
	return jobSpecs, nil
}

// Check if job specifications are valid
func (srv *Service) Check(ctx context.Context, namespace models.NamespaceSpec, jobSpecs []models.JobSpec, logWriter writer.LogWriter) (err error) {
	for i := range jobSpecs {
		// remove manual dependencies as they needs to be resolved
		jobSpecs[i].Dependencies = map[string]models.JobSpecDependency{}
	}

	runner := parallel.NewRunner(parallel.WithTicket(ConcurrentTicketPerSec), parallel.WithLimit(ConcurrentLimit))
	for _, jSpec := range jobSpecs {
		runner.Add(func(currentSpec models.JobSpec, lw writer.LogWriter) func() (interface{}, error) {
			return func() (interface{}, error) {
				// check dependencies
				var specCheckErrors error
				_, err := srv.pluginService.GenerateDependencies(ctx, currentSpec, namespace, true)
				if err != nil {
					if !errors.Is(err, service.ErrDependencyModNotFound) {
						errMsg := fmt.Sprintf("check for job failed: %s, reason: %s", currentSpec.Name, fmt.Sprintf("dependency resolution: %s\n", err.Error()))
						lw.Write(writer.LogLevelError, errMsg)
						specCheckErrors = multierror.Append(specCheckErrors, fmt.Errorf("%s %s: %w", errDependencyResolution.Error(), currentSpec.Name, err))
					}
				}

				// check compilation
				if err := srv.batchScheduler.VerifyJob(ctx, namespace, currentSpec); err != nil {
					errMsg := fmt.Sprintf("check for job failed: %s, reason: %s", currentSpec.Name, fmt.Sprintf("compilation: %s\n", err.Error()))
					lw.Write(writer.LogLevelError, errMsg)
					specCheckErrors = multierror.Append(specCheckErrors, fmt.Errorf("failed to compile %s: %w", currentSpec.Name, err))
				}

				if specCheckErrors == nil {
					successMsg := fmt.Sprintf("check for job passed: %s", currentSpec.Name)
					lw.Write(writer.LogLevelInfo, successMsg)
				}
				return nil, specCheckErrors
			}
		}(jSpec, logWriter))
	}
	for _, result := range runner.Run() {
		if result.Err != nil {
			err = multierror.Append(err, result.Err)
		}
	}
	return err
}

func (srv *Service) GetJobBasicInfo(ctx context.Context, jobSpec models.JobSpec) models.JobBasicInfo {
	var jobBasicInfo models.JobBasicInfo
	dest, err := srv.pluginService.GenerateDestination(ctx, jobSpec, jobSpec.NamespaceSpec)
	if err != nil {
		jobBasicInfo.Log.Write(writer.LogLevelError, fmt.Sprintf("unable to generate destination, err: %v", err))
	}
	if dest != nil {
		destination := models.JobSpecTaskDestination{
			Destination: dest.Destination,
			Type:        dest.Type,
		}
		jobSpec.ResourceDestination = destination.URN()
		jobBasicInfo.Destination = destination.URN()
	}
	deps, err := srv.pluginService.GenerateDependencies(ctx, jobSpec, jobSpec.NamespaceSpec, false)
	if err != nil {
		jobBasicInfo.Log.Write(writer.LogLevelError, fmt.Sprintf("failed to generate job sources, err: %v", err))
	} else {
		if deps != nil {
			jobBasicInfo.JobSource = deps.Dependencies
		} else {
			jobBasicInfo.Log.Write(writer.LogLevelInfo, "no job sources detected")
		}
	}
	srv.Check(ctx, jobSpec.NamespaceSpec, []models.JobSpec{jobSpec}, &jobBasicInfo.Log)

	if jobSpec.Behavior.CatchUp {
		jobBasicInfo.Log.Write(writer.LogLevelWarning, "catchup is enabled")
	}
	if dupDestJobNames, err := srv.IsJobDestinationDuplicate(ctx, jobSpec); err != nil {
		// todo : check on this
		jobBasicInfo.Log.Write(writer.LogLevelError, "could not perform duplicate job destination check, err: "+err.Error())
	} else if dupDestJobNames != "" {
		jobBasicInfo.Log.Write(writer.LogLevelWarning, "job already exists with same Destination: "+jobSpec.ResourceDestination+" existing jobNames: "+dupDestJobNames)
	}
	jobBasicInfo.Spec = jobSpec

	return jobBasicInfo
}

func (srv *Service) GetTaskDependencies(ctx context.Context, namespace models.NamespaceSpec, jobSpec models.JobSpec) (models.JobSpecTaskDestination,
	models.JobSpecTaskDependencies, error) {
	destination := models.JobSpecTaskDestination{}
	dependencies := models.JobSpecTaskDependencies{}

	dest, err := srv.pluginService.GenerateDestination(ctx, jobSpec, namespace)
	if err != nil {
		return destination, dependencies, err
	}

	if dest != nil {
		destination.Destination = dest.Destination
		destination.Type = dest.Type
	}

	deps, err := srv.pluginService.GenerateDependencies(ctx, jobSpec, namespace, false)
	if err != nil {
		return destination, dependencies, fmt.Errorf("failed to generate dependencies: %w", err)
	}
	if deps != nil {
		dependencies = deps.Dependencies
	}

	return destination, dependencies, nil
}

// Delete deletes a job spec from all spec repos
func (srv *Service) Delete(ctx context.Context, namespace models.NamespaceSpec, jobSpec models.JobSpec) error {
	dependentJobNames, err := srv.getDependentJobNames(ctx, jobSpec)
	if err != nil {
		return err
	}

	if len(dependentJobNames) > 0 {
		return fmt.Errorf("cannot delete job %s since it's dependency of other jobs: %s", jobSpec.Name, strings.Join(dependentJobNames, ","))
	}

	// delete jobs from internal store
	if err := srv.jobSpecRepository.DeleteByID(ctx, jobSpec.ID); err != nil {
		return fmt.Errorf("failed to delete spec: %s: %w", jobSpec.Name, err)
	}

	// delete from batch scheduler
	namespaceIdentifiers := []string{
		namespace.ID.String(), // old, kept for folder cleanup, to be removed after complete migration of name space folder #cleaup
		namespace.Name,
	}
	for _, nsDirectoryIdentifier := range namespaceIdentifiers {
		err = srv.batchScheduler.DeleteJobs(ctx, nsDirectoryIdentifier, namespace, []string{jobSpec.Name}, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (srv *Service) bulkDelete(ctx context.Context, namespace models.NamespaceSpec, jobSpecsToDelete []models.JobSpec, logWriter writer.LogWriter) {
	success, failure := 0, 0
	for _, jobSpec := range jobSpecsToDelete {
		dependentJobNames, err := srv.getDependentJobNames(ctx, jobSpec)
		if err != nil {
			failure++
			errMsg := fmt.Sprintf("[%s] error '%s': failed to delete job, %s", namespace.Name, jobSpec.Name, err.Error())
			logWriter.Write(writer.LogLevelError, errMsg)
			continue
		}
		if len(dependentJobNames) > 0 {
			failure++
			err = fmt.Errorf("cannot delete job %s since it's dependency of other jobs: %s", jobSpec.Name, strings.Join(dependentJobNames, ","))
			errMsg := fmt.Sprintf("[%s] error '%s': failed to delete job, %s", namespace.Name, jobSpec.Name, err.Error())
			logWriter.Write(writer.LogLevelError, errMsg)
			continue
		}
		if err := srv.jobSpecRepository.DeleteByID(ctx, jobSpec.ID); err != nil {
			failure++
			errMsg := fmt.Sprintf("[%s] error '%s': failed to delete job, %s", namespace.Name, jobSpec.Name, err.Error())
			logWriter.Write(writer.LogLevelError, errMsg)
			continue
		}

		success++
		successMsg := fmt.Sprintf("[%s] info '%s': job deleted", namespace.Name, jobSpec.Name)
		logWriter.Write(writer.LogLevelInfo, successMsg)
	}

	if failure > 0 {
		errMsg := fmt.Sprintf("[%s] Deleted %d/%d jobs", namespace.Name, success, success+failure)
		logWriter.Write(writer.LogLevelError, errMsg)
	} else {
		successMsg := fmt.Sprintf("[%s] Deleted %d jobs", namespace.Name, success)
		logWriter.Write(writer.LogLevelInfo, successMsg)
	}
}

// TODO: we only need project name
func (srv *Service) GetDependencyResolvedSpecs(ctx context.Context, proj models.ProjectSpec, progressObserver progress.Observer) (resolvedSpecs []models.JobSpec, resolvedErrors error) {
	// fetch all jobs since dependency resolution happens for all jobs in a project, not just for a namespace
	jobSpecs, err := srv.jobSpecRepository.GetAllByProjectName(ctx, proj.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve jobs: %w", err)
	}
	srv.notifyProgress(progressObserver, &models.ProgressJobSpecFetch{})

	// generate a reverse map for namespace
	jobsToNamespace := srv.getMappedJobNameToNamespaceName(jobSpecs)
	// resolve specs in parallel
	runner := parallel.NewRunner(parallel.WithTicket(ConcurrentTicketPerSec), parallel.WithLimit(ConcurrentLimit))
	for _, jobSpec := range jobSpecs {
		runner.Add(func(currentSpec models.JobSpec) func() (interface{}, error) {
			return func() (interface{}, error) {
				resolvedSpec, err := srv.dependencyResolver.Resolve(ctx, proj, currentSpec, progressObserver)
				if err != nil {
					return nil, fmt.Errorf("%s: %s/%s: %w", errDependencyResolution, jobsToNamespace[currentSpec.Name], currentSpec.Name, err)
				}
				return resolvedSpec, nil
			}
		}(jobSpec))
	}

	for _, state := range runner.Run() {
		if state.Err != nil {
			resolvedErrors = multierror.Append(resolvedErrors, state.Err)
		} else {
			resolvedSpecs = append(resolvedSpecs, state.Val.(models.JobSpec))
		}
	}
	return resolvedSpecs, resolvedErrors
}

// do other jobs depend on this jobSpec
func (srv *Service) getDependentJobNames(ctx context.Context, jobSpec models.JobSpec) ([]string, error) {
	// inferred and static dependents
	dependentJobs, err := srv.jobSpecRepository.GetDependentJobs(ctx, jobSpec.Name, jobSpec.GetProjectSpec().Name, jobSpec.ResourceDestination)
	if err != nil {
		return nil, fmt.Errorf("unable to check dependents of job %s", jobSpec.Name)
	}
	jobNames := make([]string, len(dependentJobs))
	for i, job := range dependentJobs {
		jobNames[i] = job.Name
	}
	return jobNames, nil
}

func (srv *Service) GetByDestination(ctx context.Context, projectSpec models.ProjectSpec, destination string) (models.JobSpec, error) {
	// generate job spec using datastore destination. if a destination can be owned by multiple jobs, need to change to list
	jobSpecs, err := srv.jobSpecRepository.GetByResourceDestinationURN(ctx, destination)
	if err != nil {
		return models.JobSpec{}, err
	}
	for _, jobSpec := range jobSpecs {
		if jobSpec.NamespaceSpec.ProjectSpec.Name == projectSpec.Name {
			return jobSpec, nil
		}
	}
	return models.JobSpec{}, store.ErrResourceNotFound
}

func (srv *Service) GetDownstream(ctx context.Context, projectSpec models.ProjectSpec, rootJobName string) ([]models.JobSpec, error) {
	jobSpecMap, err := srv.prepareJobSpecMap(ctx, projectSpec)
	if err != nil {
		return nil, err
	}

	rootJobSpec, found := jobSpecMap[rootJobName]
	if !found {
		return nil, fmt.Errorf("couldn't find any job with name %s", rootJobName)
	}

	dagTree := tree.NewMultiRootTree()
	dagTree.AddNode(tree.NewTreeNode(rootJobSpec))
	rootInstance, err := populateDownstreamDAGs(dagTree, rootJobSpec, jobSpecMap)
	if err != nil {
		return nil, err
	}

	var jobSpecs []models.JobSpec
	for _, node := range rootInstance.GetAllNodes() {
		// ignore the root
		if node.GetName() != rootInstance.GetName() {
			jobSpecs = append(jobSpecs, node.Data.(models.JobSpec))
		}
	}
	return jobSpecs, nil
}

func (srv *Service) prepareJobSpecMap(ctx context.Context, projectSpec models.ProjectSpec) (map[string]models.JobSpec, error) {
	// resolve dependency of all jobs in given project
	jobSpecs, err := srv.GetDependencyResolvedSpecs(ctx, projectSpec, nil)
	if err != nil {
		return nil, err
	}

	jobSpecMap := make(map[string]models.JobSpec)
	for _, currSpec := range jobSpecs {
		jobSpecMap[currSpec.Name] = currSpec
	}

	return jobSpecMap, nil
}

func (srv *Service) prepareNamespaceJobSpecMap(ctx context.Context, projectSpec models.ProjectSpec) (map[string]string, error) {
	jobSpecs, err := srv.jobSpecRepository.GetAllByProjectName(ctx, projectSpec.Name)
	if err != nil {
		return nil, err
	}
	return srv.getMappedJobNameToNamespaceName(jobSpecs), err
}

func filterNode(parentNode *tree.TreeNode, dependents []*tree.TreeNode, allowedDownstream []string, jobNamespaceMap map[string]string) *tree.TreeNode {
	for _, dep := range dependents {
		// if dep is not within allowed namespace, skip this dependency
		isAuthorized := false
		for _, namespace := range allowedDownstream {
			if namespace == models.AllNamespace || namespace == jobNamespaceMap[dep.GetName()] {
				isAuthorized = true
				break
			}
		}
		if !isAuthorized {
			continue
		}

		// if dep is within allowed namespace, add the node to parent
		depNode := tree.NewTreeNode(dep.Data)

		// check for the dependent
		depNode = filterNode(depNode, dep.Dependents, allowedDownstream, jobNamespaceMap)

		// add the complete node
		parentNode.AddDependent(depNode)
	}
	return parentNode
}

func listIgnoredJobs(rootInstance, rootFilteredTree *tree.TreeNode) []string {
	allowedNodesMap := make(map[string]*tree.TreeNode)
	for _, allowedNode := range rootFilteredTree.GetAllNodes() {
		allowedNodesMap[allowedNode.GetName()] = allowedNode
	}

	ignoredJobsMap := make(map[string]bool)
	for _, node := range rootInstance.GetAllNodes() {
		if _, ok := allowedNodesMap[node.GetName()]; !ok {
			ignoredJobsMap[node.GetName()] = true
		}
	}

	var ignoredJobs []string
	for jobName := range ignoredJobsMap {
		ignoredJobs = append(ignoredJobs, jobName)
	}

	return ignoredJobs
}

func (*Service) notifyProgress(po progress.Observer, event progress.Event) {
	if po == nil {
		return
	}
	po.Notify(event)
}

// remove items present in from
func setSubtract(from, remove []string) []string {
	removeMap := make(map[string]bool)
	for _, item := range remove {
		removeMap[item] = true
	}

	res := make([]string, 0)
	for _, fromKey := range from {
		if _, exists := removeMap[fromKey]; !exists {
			res = append(res, fromKey)
		}
	}

	return res
}

func (srv *Service) Run(ctx context.Context, nsSpec models.NamespaceSpec,
	jobSpecs []models.JobSpec) (models.JobDeploymentDetail, error) {
	// Note(kush.sharma): ideally we should resolve dependencies & priorities
	// before passing it to be deployed but as the used scheduler doesn't support
	// it yet to use them appropriately, I am not doing it to avoid unnecessary
	// processing
	return srv.manualScheduler.DeployJobs(ctx, nsSpec, jobSpecs)
}

func populateDownstreamDAGs(dagTree *tree.MultiRootTree, jobSpec models.JobSpec, jobSpecMap map[string]models.JobSpec) (*tree.TreeNode, error) {
	for _, childSpec := range jobSpecMap {
		childNode := findOrCreateDAGNode(dagTree, childSpec)
		for _, depDAG := range childSpec.Dependencies {
			isExternal := false
			parentSpec, ok := jobSpecMap[depDAG.Job.Name]
			if !ok {
				if depDAG.Type == models.JobSpecDependencyTypeIntra {
					return nil, fmt.Errorf("%s: %w", depDAG.Job.Name, ErrJobSpecNotFound)
				}
				// when the dependency of a jobSpec belong to some other tenant or is external, the jobSpec won't
				// be available in jobSpecs []models.JobSpec object (which is tenant specific)
				// so we'll add a dummy JobSpec for that cross tenant/external dependency.
				parentSpec = models.JobSpec{Name: depDAG.Job.Name, Dependencies: make(map[string]models.JobSpecDependency)}
				isExternal = true
			}
			parentNode := findOrCreateDAGNode(dagTree, parentSpec)
			parentNode.AddDependent(childNode)
			dagTree.AddNode(parentNode)

			if isExternal {
				// dependency that are outside current project will be considered as root because
				// optimus don't know dependencies of those external parents
				dagTree.MarkRoot(parentNode)
			}
		}

		if len(childSpec.Dependencies) == 0 {
			dagTree.MarkRoot(childNode)
		}
	}

	if err := dagTree.ValidateCyclic(); err != nil {
		return nil, err
	}

	// since we are adding the rootNode at start, it will always be present
	rootNode, _ := dagTree.GetNodeByName(jobSpec.Name)

	return rootNode, nil
}

// Refresh fetches all the requested jobs, resolves its dependencies, assign proper priority weights,
// compile all jobs in the project and upload them to the destination store.
func (srv *Service) Refresh(ctx context.Context, projectName string, namespaceNames []string, jobNames []string,
	logWriter writer.LogWriter) (models.DeploymentID, error) {
	projectSpec, err := srv.projectService.Get(ctx, projectName)
	if err != nil {
		return models.DeploymentID(uuid.Nil), err
	}

	// get job specs as requested
	jobSpecs, err := srv.fetchJobSpecs(ctx, projectSpec, namespaceNames, jobNames, logWriter)
	if err != nil {
		return models.DeploymentID(uuid.Nil), err
	}

	// resolve dependency and persist
	srv.identifyAndPersistJobSources(ctx, projectSpec, jobSpecs, logWriter)
	successMsg := "info: dependencies resolved"
	logWriter.Write(writer.LogLevelInfo, successMsg)

	deployID, err := srv.deployManager.Deploy(ctx, projectSpec)
	if err != nil {
		return models.DeploymentID(uuid.Nil), err
	}

	return deployID, nil
}

func (srv *Service) fetchJobSpecs(ctx context.Context, projectSpec models.ProjectSpec,
	namespaceNames []string, jobNames []string, logWriter writer.LogWriter) (jobSpecs []models.JobSpec, err error) {
	defer logWriter.Write(writer.LogLevelInfo, "fetching job specs")

	if len(jobNames) > 0 {
		return srv.fetchSpecsForGivenJobNames(ctx, projectSpec, jobNames)
	} else if len(namespaceNames) > 0 {
		return srv.fetchAllJobSpecsForGivenNamespaces(ctx, projectSpec, namespaceNames)
	}
	return srv.jobSpecRepository.GetAllByProjectName(ctx, projectSpec.Name)
}

func (srv *Service) fetchAllJobSpecsForGivenNamespaces(ctx context.Context, projectSpec models.ProjectSpec, namespaceNames []string) ([]models.JobSpec, error) {
	var jobSpecs []models.JobSpec
	for _, namespaceName := range namespaceNames {
		namespaceSpec, err := srv.namespaceService.Get(ctx, projectSpec.Name, namespaceName)
		if err != nil {
			return nil, err
		}
		specs, err := srv.GetAll(ctx, namespaceSpec)
		if err != nil {
			return nil, err
		}
		jobSpecs = append(jobSpecs, specs...)
	}
	return jobSpecs, nil
}

func (srv *Service) fetchSpecsForGivenJobNames(ctx context.Context, projectSpec models.ProjectSpec, jobNames []string) ([]models.JobSpec, error) {
	var jobSpecs []models.JobSpec
	for _, name := range jobNames {
		jobSpec, _, err := srv.GetByNameForProject(ctx, name, projectSpec)
		if err != nil {
			return nil, err
		}
		jobSpecs = append(jobSpecs, jobSpec)
	}
	return jobSpecs, nil
}

func (srv *Service) IsJobDestinationDuplicate(ctx context.Context, jobSpec models.JobSpec) (string, error) {
	jobsWithSameDestination, err := srv.jobSpecRepository.GetByResourceDestinationURN(ctx, jobSpec.ResourceDestination)
	if err != nil {
		if errors.Is(err, store.ErrResourceNotFound) {
			return "", nil
		}
		return "", err
	}
	var duplicateJobNames []string
	for _, dupJobSpec := range jobsWithSameDestination {
		if dupJobSpec.GetFullName() == jobSpec.GetFullName() {
			// this is the same job from the DB. hence not an issues
			continue
		}
		duplicateJobNames = append(duplicateJobNames, dupJobSpec.GetFullName())
	}
	return strings.Join(duplicateJobNames, ", "), nil
}

func (srv *Service) identifyAndPersistJobSources(ctx context.Context, projectSpec models.ProjectSpec,
	jobSpecs []models.JobSpec, logWriter writer.LogWriter) {
	start := time.Now()
	defer resolveDependencyHistogram.Observe(time.Since(start).Seconds())

	// resolve specs in parallel
	runner := parallel.NewRunner(parallel.WithTicket(ConcurrentTicketPerSec), parallel.WithLimit(ConcurrentLimit))
	for _, jobSpec := range jobSpecs {
		runner.Add(func(currentSpec models.JobSpec) func() (interface{}, error) {
			return func() (interface{}, error) {
				namespaceName := currentSpec.NamespaceSpec.Name
				specVal := []string{currentSpec.Name, namespaceName}
				jobSourceURNs, err := srv.identify(ctx, currentSpec, projectSpec)
				if err != nil {
					return specVal, err
				}
				if len(jobSourceURNs) == 0 {
					return specVal, nil
				}
				err = srv.jobSourceRepo.Save(ctx, projectSpec.ID, currentSpec.ID, jobSourceURNs)
				if err != nil {
					err = fmt.Errorf("error persisting job sources for job %s: %w", currentSpec.Name, err)
				}
				return specVal, err
			}
		}(jobSpec))
	}

	failure, success := 0, 0
	for _, state := range runner.Run() {
		specVal := state.Val.([]string)
		jobName, namespaceName := specVal[0], specVal[1]
		if state.Err != nil {
			failure++
			errMsg := fmt.Sprintf("[%s] error '%s': failed to resolve dependency, %s", namespaceName, jobName, state.Err.Error())
			logWriter.Write(writer.LogLevelError, errMsg)
		} else {
			success++
			successMsg := fmt.Sprintf("[%s] info '%s': dependency is successfully resolved", namespaceName, jobName)
			logWriter.Write(writer.LogLevelInfo, successMsg)
		}
	}

	if failure > 0 {
		errMsg := fmt.Sprintf("Resolved dependencies of %d/%d jobs.", success, success+failure)
		logWriter.Write(writer.LogLevelError, errMsg)
	} else {
		successMsg := fmt.Sprintf("Resolved dependency of %d jobs.", success)
		logWriter.Write(writer.LogLevelInfo, successMsg)
	}

	resolveDependencyGauge.With(prometheus.Labels{MetricDependencyResolutionStatus: MetricDependencyResolutionSucceed}).Set(float64(success))
	resolveDependencyGauge.With(prometheus.Labels{MetricDependencyResolutionStatus: MetricDependencyResolutionFailed}).Set(float64(failure))
}

func (srv *Service) identify(ctx context.Context, currentSpec models.JobSpec, projectSpec models.ProjectSpec) ([]string, error) {
	namespace := currentSpec.NamespaceSpec
	namespace.ProjectSpec = projectSpec // TODO: Temp fix to to get secrets from project
	resp, err := srv.pluginService.GenerateDependencies(ctx, currentSpec, namespace, false)
	if err != nil {
		if !errors.Is(err, service.ErrDependencyModNotFound) {
			return nil, fmt.Errorf("%s: %s: %w", errDependencyResolution, currentSpec.Name, err)
		}
		return nil, nil
	}
	return resp.Dependencies, nil
}

// Deploy only the modified jobs (created or updated)
func (srv *Service) Deploy(ctx context.Context, projectName string, namespaceName string, jobSpecs []models.JobSpec, logWriter writer.LogWriter) (models.DeploymentID, error) {
	// Get namespace spec
	namespaceSpec, err := srv.namespaceService.Get(ctx, projectName, namespaceName)
	if err != nil {
		return models.DeploymentID(uuid.Nil), err
	}

	createdJobs, modifiedJobs, deletedJobs, err := srv.getJobsDiff(ctx, namespaceSpec, jobSpecs)
	if err != nil {
		return models.DeploymentID(uuid.Nil), err
	}

	createdAndModifiedJobs := createdJobs
	createdAndModifiedJobs = append(createdAndModifiedJobs, modifiedJobs...)
	savedJobs := srv.bulkCreate(ctx, namespaceSpec, createdAndModifiedJobs, logWriter)

	srv.bulkDelete(ctx, namespaceSpec, deletedJobs, logWriter)

	// Resolve inferred dependency
	if len(savedJobs) > 0 {
		srv.identifyAndPersistJobSources(ctx, namespaceSpec.ProjectSpec, savedJobs, logWriter)
	}

	// Deploy through deploy manager
	deployID, err := srv.deployManager.Deploy(ctx, namespaceSpec.ProjectSpec)
	if err != nil {
		return models.DeploymentID(uuid.Nil), err
	}

	successMsg := fmt.Sprintf("[%s] Deployment request created with ID: %s", namespaceName, deployID.UUID().String())
	logWriter.Write(writer.LogLevelInfo, successMsg)

	return deployID, nil
}

func (srv *Service) getJobsDiff(ctx context.Context, namespace models.NamespaceSpec, requestedJobSpecs []models.JobSpec) ([]models.JobSpec, []models.JobSpec, []models.JobSpec, error) {
	existingJobSpecs, err := srv.jobSpecRepository.GetAllByProjectNameAndNamespaceName(ctx, namespace.ProjectSpec.Name, namespace.Name)
	if err != nil {
		return nil, nil, nil, err
	}

	existingJobSpecMap := map[string]models.JobSpec{}
	for _, jobSpec := range existingJobSpecs {
		existingJobSpecMap[jobSpec.Name] = jobSpec
	}

	requestedJobSpecMap := map[string]models.JobSpec{}
	for _, jobSpec := range requestedJobSpecs {
		requestedJobSpecMap[jobSpec.Name] = jobSpec
	}

	createdJobSpecs, modifiedJobSpecs := srv.getModifiedJobs(existingJobSpecMap, requestedJobSpecMap)
	deletedJobSpecs := srv.getDeletedJobs(existingJobSpecMap, requestedJobSpecMap)

	return createdJobSpecs, modifiedJobSpecs, deletedJobSpecs, nil
}

func (srv *Service) getModifiedJobs(existingJobSpecs, requestedJobSpecs map[string]models.JobSpec) ([]models.JobSpec, []models.JobSpec) {
	createdJobSpecs := []models.JobSpec{}
	modifiedJobSpecs := []models.JobSpec{}

	for jobName, requestedJobSpec := range requestedJobSpecs {
		if existingJobSpec, ok := existingJobSpecs[jobName]; !ok {
			createdJobSpecs = append(createdJobSpecs, requestedJobSpec)
		} else if !srv.jobSpecEqual(requestedJobSpec, existingJobSpec) {
			requestedJobSpec.ID = existingJobSpec.ID
			modifiedJobSpecs = append(modifiedJobSpecs, requestedJobSpec)
		}
	}

	return createdJobSpecs, modifiedJobSpecs
}

func (*Service) getDeletedJobs(existingJobSpecs, requestedJobSpecs map[string]models.JobSpec) []models.JobSpec {
	deletedJobSpecs := []models.JobSpec{}

	for jobName, existingJobSpec := range existingJobSpecs {
		if _, ok := requestedJobSpecs[jobName]; !ok {
			deletedJobSpecs = append(deletedJobSpecs, existingJobSpec)
		}
	}

	return deletedJobSpecs
}

func (*Service) jobSpecEqual(js1, js2 models.JobSpec) bool {
	js2.ID = js1.ID
	js2.NamespaceSpec = js1.NamespaceSpec
	js2.ResourceDestination = js1.ResourceDestination

	jobSpecHash1 := getHash(fmt.Sprintf("%v", js1))
	jobSpecHash2 := getHash(fmt.Sprintf("%v", js2))

	return jobSpecHash1 == jobSpecHash2
}

func getHash(val string) string {
	h := sha256.New()
	h.Write([]byte(val))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (srv *Service) GetDeployment(ctx context.Context, deployID models.DeploymentID) (models.JobDeployment, error) {
	return srv.deployManager.GetStatus(ctx, deployID)
}

func (srv *Service) CreateAndDeploy(ctx context.Context, namespaceSpec models.NamespaceSpec, jobSpecs []models.JobSpec, logWriter writer.LogWriter) (models.DeploymentID, error) {
	// validate job spec
	if err := srv.Check(ctx, namespaceSpec, jobSpecs, logWriter); err != nil {
		return models.DeploymentID{}, status.Errorf(codes.Internal, "spec validation failed\n%s", err.Error())
	}

	jobSpecs = srv.bulkCreate(ctx, namespaceSpec, jobSpecs, logWriter)

	if len(jobSpecs) > 0 {
		srv.identifyAndPersistJobSources(ctx, namespaceSpec.ProjectSpec, jobSpecs, logWriter)
	}

	logWriter.Write(writer.LogLevelInfo, "info: dependencies resolved")

	return srv.deployManager.Deploy(ctx, namespaceSpec.ProjectSpec)
}

func (*Service) getMappedJobNameToNamespaceName(jobSpecs []models.JobSpec) map[string]string {
	output := make(map[string]string)
	for _, j := range jobSpecs {
		output[j.Name] = j.NamespaceSpec.Name
	}
	return output
}
