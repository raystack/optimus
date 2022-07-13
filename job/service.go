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

	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/core/progress/sender"
	"github.com/odpf/optimus/core/tree"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
	"github.com/odpf/optimus/store"
)

const (
	// PersistJobPrefix is used to keep the job during sync even if they are not in source repo
	PersistJobPrefix string = "__"

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
	ResolveStaticDependencies(ctx context.Context, jobSpec models.JobSpec, projectSpec models.ProjectSpec, projectJobSpecRepo store.ProjectJobSpecRepository) (map[string]models.JobSpecDependency, error)

	GetJobSpecsWithDependencies(ctx context.Context, projectID models.ProjectID) ([]models.JobSpec, error)
}

type Deployer interface {
	Deploy(context.Context, models.JobDeployment) error
}

// NamespaceJobSpecRepoFactory is used to manage job specs at namespace level
type NamespaceJobSpecRepoFactory interface {
	New(spec models.NamespaceSpec) store.NamespaceJobSpecRepository
}

// ProjectJobSpecRepoFactory is used to manage job specs at project level
type ProjectJobSpecRepoFactory interface {
	New(proj models.ProjectSpec) store.ProjectJobSpecRepository
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
	namespaceJobSpecRepoFactory NamespaceJobSpecRepoFactory
	dependencyResolver          DependencyResolver
	priorityResolver            PriorityResolver
	projectJobSpecRepoFactory   ProjectJobSpecRepoFactory
	replayManager               ReplayManager
	projectService              service.ProjectService
	namespaceService            service.NamespaceService
	jobSpecRepository           store.JobSpecRepository
	deployManager               DeployManager

	// scheduler for managing batch scheduled jobs
	batchScheduler models.SchedulerUnit

	// scheduler for managing one time executable jobs
	manualScheduler models.SchedulerUnit

	pluginService service.PluginService

	jobSourceRepo store.JobSourceRepository
}

// Create constructs a Job for a namespace and commits it to the store
func (srv *Service) Create(ctx context.Context, namespace models.NamespaceSpec, spec models.JobSpec) (models.JobSpec, error) {
	jobRepo := srv.namespaceJobSpecRepoFactory.New(namespace)
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
	if err := jobRepo.Save(ctx, spec, jobDestination); err != nil {
		return models.JobSpec{}, fmt.Errorf("failed to save job: %s: %w", spec.Name, err)
	}

	result, err := jobRepo.GetByName(ctx, spec.Name)
	if err != nil {
		return models.JobSpec{}, fmt.Errorf("failed to fetch job on create: %s: %w", spec.Name, err)
	}

	return result, nil
}

func (srv *Service) bulkCreate(ctx context.Context, namespace models.NamespaceSpec, jobSpecs []models.JobSpec, logSender sender.LogStatus) []models.JobSpec {
	result := []models.JobSpec{}
	for _, jobSpec := range jobSpecs {
		jobSpecCreated, err := srv.Create(ctx, namespace, jobSpec)
		if err != nil {
			op := "modify"
			if jobSpec.ID == uuid.Nil {
				op = "create"
			}
			warnMsg := fmt.Sprintf("[%s] error '%s': failed to %s job, %s", namespace.Name, jobSpec.Name, op, err.Error())
			sender.SendWarningMessage(logSender, warnMsg)

			continue
		}

		op := "modified"
		if jobSpec.ID == uuid.Nil {
			op = "created"
		}
		successMsg := fmt.Sprintf("[%s] info '%s': job %s", namespace.Name, jobSpec.Name, op)
		sender.SendSuccessMessage(logSender, successMsg)

		result = append(result, jobSpecCreated)
	}

	return result
}

// GetByName fetches a Job by name for a specific namespace
func (srv *Service) GetByName(ctx context.Context, name string, namespace models.NamespaceSpec) (models.JobSpec, error) {
	jobSpec, err := srv.namespaceJobSpecRepoFactory.New(namespace).GetByName(ctx, name)
	if err != nil {
		return models.JobSpec{}, fmt.Errorf("failed to retrieve job: %w", err)
	}
	return jobSpec, nil
}

func (srv *Service) GetByFilter(ctx context.Context, filter models.JobSpecFilter) ([]models.JobSpec, error) {
	if filter.ResourceDestination != "" {
		jobSpec, err := srv.jobSpecRepository.GetJobByResourceDestination(ctx, filter.ResourceDestination)
		if err != nil {
			return nil, err
		}
		return []models.JobSpec{jobSpec}, nil
	}
	if filter.ProjectName != "" {
		projSpec, err := srv.projectService.Get(ctx, filter.ProjectName)
		if err != nil {
			return nil, fmt.Errorf("not able to find project: %w", err)
		}
		projectJobSpecRepo := srv.projectJobSpecRepoFactory.New(projSpec)
		if filter.JobName != "" {
			jobSpec, _, err := projectJobSpecRepo.GetByName(ctx, filter.JobName)
			if err != nil {
				return nil, err
			}
			return []models.JobSpec{jobSpec}, nil
		}
		jobSpecs, err := projectJobSpecRepo.GetAll(ctx)
		if err != nil {
			return []models.JobSpec{}, err
		}
		return jobSpecs, nil
	}
	if filter.JobName != "" {
		jobSpecs, err := srv.jobSpecRepository.GetJobByName(ctx, filter.JobName)
		if err != nil {
			return nil, err
		}
		return jobSpecs, nil
	}
	return nil, fmt.Errorf("filters not specified")
}

// GetByNameForProject fetches a Job by name for a specific project
func (srv *Service) GetByNameForProject(ctx context.Context, name string, proj models.ProjectSpec) (models.JobSpec, models.NamespaceSpec, error) {
	jobSpec, namespace, err := srv.projectJobSpecRepoFactory.New(proj).GetByName(ctx, name)
	if err != nil {
		return models.JobSpec{}, models.NamespaceSpec{}, fmt.Errorf("failed to retrieve job: %w", err)
	}
	return jobSpec, namespace, nil
}

func (srv *Service) GetAll(ctx context.Context, namespace models.NamespaceSpec) ([]models.JobSpec, error) {
	jobSpecs, err := srv.namespaceJobSpecRepoFactory.New(namespace).GetAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve jobs: %w", err)
	}
	return jobSpecs, nil
}

// Check if job specifications are valid
func (srv *Service) Check(ctx context.Context, namespace models.NamespaceSpec, jobSpecs []models.JobSpec, obs progress.Observer) (err error) {
	for i := range jobSpecs {
		// remove manual dependencies as they needs to be resolved
		jobSpecs[i].Dependencies = map[string]models.JobSpecDependency{}
	}

	runner := parallel.NewRunner(parallel.WithTicket(ConcurrentTicketPerSec), parallel.WithLimit(ConcurrentLimit))
	for _, jSpec := range jobSpecs {
		runner.Add(func(currentSpec models.JobSpec) func() (interface{}, error) {
			return func() (interface{}, error) {
				// check dependencies
				_, err := srv.pluginService.GenerateDependencies(ctx, currentSpec, namespace, true)
				if err != nil {
					if !errors.Is(err, service.ErrDependencyModNotFound) {
						if obs != nil {
							obs.Notify(&models.ProgressJobCheckFailed{Name: currentSpec.Name, Reason: fmt.Sprintf("dependency resolution: %s\n", err.Error())})
						}
						return nil, fmt.Errorf("%s %s: %w", errDependencyResolution.Error(), currentSpec.Name, err)
					}
				}

				// check compilation
				if err := srv.batchScheduler.VerifyJob(ctx, namespace, currentSpec); err != nil {
					if obs != nil {
						obs.Notify(&models.ProgressJobCheckFailed{Name: currentSpec.Name, Reason: fmt.Sprintf("compilation: %s\n", err.Error())})
					}
					return nil, fmt.Errorf("failed to compile %s: %w", currentSpec.Name, err)
				}

				if obs != nil {
					obs.Notify(&models.ProgressJobCheckSuccess{Name: currentSpec.Name})
				}
				return nil, nil
			}
		}(jSpec))
	}
	for _, result := range runner.Run() {
		if result.Err != nil {
			err = multierror.Append(err, result.Err)
		}
	}
	return err
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
	isDependency, err := srv.isDependency(ctx, jobSpec)
	if err != nil {
		return err
	}

	if isDependency {
		// TODO: Ideally should include list of jobs that are using the requested job in the error message
		return fmt.Errorf("cannot delete job %s since it's dependency of other job", jobSpec.Name)
	}

	// delete jobs from internal store
	namespaceJobSpecRepo := srv.namespaceJobSpecRepoFactory.New(namespace)
	if err := namespaceJobSpecRepo.Delete(ctx, jobSpec.ID); err != nil {
		return fmt.Errorf("failed to delete spec: %s: %w", jobSpec.Name, err)
	}

	// delete from batch scheduler
	return srv.batchScheduler.DeleteJobs(ctx, namespace, []string{jobSpec.Name}, nil)
}

func (srv *Service) bulkDelete(ctx context.Context, namespace models.NamespaceSpec, jobSpecsToDelete []models.JobSpec,
	logSender sender.LogStatus) {
	namespaceJobSpecRepo := srv.namespaceJobSpecRepoFactory.New(namespace)
	for _, jobSpec := range jobSpecsToDelete {
		isDependency, err := srv.isDependency(ctx, jobSpec)
		if err != nil {
			warnMsg := fmt.Sprintf("[%s] error '%s': failed to delete job, %s", namespace.Name, jobSpec.Name, err.Error())
			sender.SendWarningMessage(logSender, warnMsg)
			continue
		}
		if isDependency {
			// TODO: Ideally should include list of jobs that are using the requested job in the error message
			err = fmt.Errorf("cannot delete job %s since it's dependency of other job", jobSpec.Name)
			warnMsg := fmt.Sprintf("[%s] error '%s': failed to delete job, %s", namespace.Name, jobSpec.Name, err.Error())
			sender.SendWarningMessage(logSender, warnMsg)
			continue
		}
		if err := namespaceJobSpecRepo.Delete(ctx, jobSpec.ID); err != nil {
			warnMsg := fmt.Sprintf("[%s] error '%s': failed to delete job, %s", namespace.Name, jobSpec.Name, err.Error())
			sender.SendWarningMessage(logSender, warnMsg)
			continue
		}

		successMsg := fmt.Sprintf("[%s] info '%s': job deleted", namespace.Name, jobSpec.Name)
		sender.SendSuccessMessage(logSender, successMsg)
	}
}

// Sync fetches all the jobs that belong to a project, resolves its dependencies
// assign proper priority weights, compiles it and uploads it to the destination
// store.
// It syncs the internal store state with destination batch batchScheduler by deleting
// what is not needed anymore
func (srv *Service) Sync(ctx context.Context, namespace models.NamespaceSpec, progressObserver progress.Observer) error {
	projectJobSpecRepo := srv.projectJobSpecRepoFactory.New(namespace.ProjectSpec)
	jobSpecs, err := srv.GetDependencyResolvedSpecs(ctx, namespace.ProjectSpec, projectJobSpecRepo, progressObserver)
	if err != nil {
		// if err is caused during dependency resolution in a job spec that belong to
		// different namespace then the current, on which this operation is being performed,
		// then don't treat this as error
		var merrs *multierror.Error
		if !errors.As(err, &merrs) {
			return err
		}
		var newErr error
		for _, cerr := range merrs.Errors {
			if strings.Contains(cerr.Error(), errDependencyResolution.Error()) {
				if !strings.Contains(cerr.Error(), namespace.Name) {
					continue
				}
			}
			newErr = multierror.Append(newErr, cerr)
		}
		if newErr != nil {
			return newErr
		}
	}
	srv.notifyProgress(progressObserver, &models.ProgressJobDependencyResolutionFinished{})

	jobSpecs, err = srv.priorityResolver.Resolve(ctx, jobSpecs, progressObserver)
	if err != nil {
		return err
	}
	srv.notifyProgress(progressObserver, &models.ProgressJobPriorityWeightAssign{})

	jobSpecs, err = srv.filterJobSpecForNamespace(ctx, projectJobSpecRepo, jobSpecs, namespace)
	if err != nil {
		return err
	}

	if err := srv.batchScheduler.DeployJobs(ctx, namespace, jobSpecs, progressObserver); err != nil {
		return err
	}

	// get all stored job names
	schedulerJobs, err := srv.batchScheduler.ListJobs(ctx, namespace, models.SchedulerListOptions{OnlyName: true})
	if err != nil {
		return err
	}
	var destJobNames []string
	for _, j := range schedulerJobs {
		destJobNames = append(destJobNames, j.Name)
	}

	// filter what we need to keep/delete
	var sourceJobNames []string
	for _, jobSpec := range jobSpecs {
		sourceJobNames = append(sourceJobNames, jobSpec.Name)
	}
	jobsToDelete := setSubtract(destJobNames, sourceJobNames)
	jobsToDelete = jobDeletionFilter(jobsToDelete)
	if len(jobsToDelete) > 0 {
		if err := srv.batchScheduler.DeleteJobs(ctx, namespace, jobsToDelete, progressObserver); err != nil {
			return err
		}
	}
	return nil
}

// filterJobSpecForNamespace returns only job specs of a given namespace
func (srv *Service) filterJobSpecForNamespace(ctx context.Context, projectJobSpecRepo store.ProjectJobSpecRepository,
	jobSpecs []models.JobSpec, namespace models.NamespaceSpec) ([]models.JobSpec, error) {
	namespaceJobSpecNames, err := projectJobSpecRepo.GetJobNamespaces(ctx)
	if err != nil {
		return nil, err
	}

	var filteredJobSpecs []models.JobSpec
	for _, jobSpec := range jobSpecs {
		if srv.ifPresentInNamespace(namespaceJobSpecNames[namespace.Name], jobSpec.Name) {
			filteredJobSpecs = append(filteredJobSpecs, jobSpec)
		}
	}
	return filteredJobSpecs, nil
}

func (srv *Service) GetDependencyResolvedSpecs(ctx context.Context, proj models.ProjectSpec, projectJobSpecRepo store.ProjectJobSpecRepository,
	progressObserver progress.Observer) (resolvedSpecs []models.JobSpec, resolvedErrors error) {
	// fetch all jobs since dependency resolution happens for all jobs in a project, not just for a namespace
	jobSpecs, err := projectJobSpecRepo.GetAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve jobs: %w", err)
	}
	srv.notifyProgress(progressObserver, &models.ProgressJobSpecFetch{})

	namespaceToJobs, err := projectJobSpecRepo.GetJobNamespaces(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve namespace to job mapping: %w", err)
	}
	// generate a reverse map for namespace
	jobsToNamespace := map[string]string{}
	for ns, jobNames := range namespaceToJobs {
		for _, jobName := range jobNames {
			jobsToNamespace[jobName] = ns
		}
	}

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

func (srv *Service) isDependency(ctx context.Context, jobSpec models.JobSpec) (bool, error) {
	// inferred and static dependents
	dependentJobs, err := srv.jobSpecRepository.GetDependentJobs(ctx, &jobSpec)
	if err != nil {
		return false, fmt.Errorf("unable to check dependents of job %s", jobSpec.Name)
	}
	return len(dependentJobs) > 0, nil
}

func (srv *Service) GetByDestination(ctx context.Context, projectSpec models.ProjectSpec, destination string) (models.JobSpec, error) {
	// generate job spec using datastore destination. if a destination can be owned by multiple jobs, need to change to list
	jobSpec, err := srv.jobSpecRepository.GetJobByResourceDestination(ctx, destination)
	if err != nil {
		return models.JobSpec{}, err
	}
	if jobSpec.NamespaceSpec.ProjectSpec.Name == projectSpec.Name {
		return jobSpec, nil
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
	projectJobSpecRepo := srv.projectJobSpecRepoFactory.New(projectSpec)

	// resolve dependency of all jobs in given project
	jobSpecs, err := srv.GetDependencyResolvedSpecs(ctx, projectSpec, projectJobSpecRepo, nil)
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
	projectJobSpecRepo := srv.projectJobSpecRepoFactory.New(projectSpec)
	namespaceJobSpecMap, err := projectJobSpecRepo.GetJobNamespaces(ctx)
	if err != nil {
		return nil, err
	}

	jobNamespaceMap := make(map[string]string)
	for namespace, jobNames := range namespaceJobSpecMap {
		for _, jobName := range jobNames {
			jobNamespaceMap[jobName] = namespace
		}
	}

	return jobNamespaceMap, err
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

func (*Service) ifPresentInNamespace(jobSpecNames []string, jobSpecToFind string) bool {
	for _, jName := range jobSpecNames {
		if jName == jobSpecToFind {
			return true
		}
	}
	return false
}

// jobDeletionFilter helps in keeping created dags even if they are not in source repo
func jobDeletionFilter(dagNames []string) []string {
	filtered := make([]string, 0)
	for _, dag := range dagNames {
		if strings.HasPrefix(dag, PersistJobPrefix) {
			continue
		}

		filtered = append(filtered, dag)
	}

	return filtered
}

func (srv *Service) Run(ctx context.Context, nsSpec models.NamespaceSpec,
	jobSpecs []models.JobSpec, observer progress.Observer) error {
	// Note(kush.sharma): ideally we should resolve dependencies & priorities
	// before passing it to be deployed but as the used scheduler doesn't support
	// it yet to use them appropriately, I am not doing it to avoid unnecessary
	// processing
	return srv.manualScheduler.DeployJobs(ctx, nsSpec, jobSpecs, observer)
}

// NewService creates a new instance of JobService, requiring
// the necessary dependencies as arguments
func NewService(namespaceJobSpecRepoFactory NamespaceJobSpecRepoFactory, batchScheduler models.SchedulerUnit,
	manualScheduler models.SchedulerUnit, dependencyResolver DependencyResolver, priorityResolver PriorityResolver,
	projectJobSpecRepoFactory ProjectJobSpecRepoFactory,
	replayManager ReplayManager, namespaceService service.NamespaceService,
	projectService service.ProjectService, deployManager DeployManager, pluginService service.PluginService,
	jobSpecRepository store.JobSpecRepository,
	jobSourceRepository store.JobSourceRepository,
) *Service {
	return &Service{
		namespaceJobSpecRepoFactory: namespaceJobSpecRepoFactory,
		batchScheduler:              batchScheduler,
		manualScheduler:             manualScheduler,
		dependencyResolver:          dependencyResolver,
		priorityResolver:            priorityResolver,
		projectJobSpecRepoFactory:   projectJobSpecRepoFactory,
		replayManager:               replayManager,
		namespaceService:            namespaceService,
		projectService:              projectService,
		deployManager:               deployManager,
		jobSpecRepository:           jobSpecRepository,
		jobSourceRepo:               jobSourceRepository,
		pluginService:               pluginService,
	}
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

	if err := dagTree.IsCyclic(); err != nil {
		return nil, err
	}

	// since we are adding the rootNode at start, it will always be present
	rootNode, _ := dagTree.GetNodeByName(jobSpec.Name)

	return rootNode, nil
}

// Refresh fetches all the requested jobs, resolves its dependencies, assign proper priority weights,
// compile all jobs in the project and upload them to the destination store.
func (srv *Service) Refresh(ctx context.Context, projectName string, namespaceNames []string, jobNames []string,
	progressObserver progress.Observer) error {
	projectSpec, err := srv.projectService.Get(ctx, projectName)
	if err != nil {
		return err
	}

	// get job specs as requested
	jobSpecs, err := srv.fetchJobSpecs(ctx, projectSpec, namespaceNames, jobNames, progressObserver)
	if err != nil {
		return err
	}

	// resolve dependency and persist
	srv.identifyAndPersistJobSources(ctx, projectSpec, jobSpecs, progressObserver)
	srv.notifyProgress(progressObserver, &models.ProgressJobDependencyResolutionFinished{})

	deployID, err := srv.deployManager.Deploy(ctx, projectSpec)
	if err != nil {
		return err
	}

	srv.notifyProgress(progressObserver, &models.ProgressJobDeploymentRequestCreated{DeployID: deployID})
	return nil
}

func (srv *Service) fetchJobSpecs(ctx context.Context, projectSpec models.ProjectSpec,
	namespaceNames []string, jobNames []string, progressObserver progress.Observer) (jobSpecs []models.JobSpec, err error) {
	defer srv.notifyProgress(progressObserver, &models.ProgressJobSpecFetch{})

	if len(jobNames) > 0 {
		return srv.fetchSpecsForGivenJobNames(ctx, projectSpec, jobNames)
	} else if len(namespaceNames) > 0 {
		return srv.fetchAllJobSpecsForGivenNamespaces(ctx, projectSpec, namespaceNames)
	}
	return srv.fetchAllJobSpecsForAProject(ctx, projectSpec)
}

func (srv *Service) fetchAllJobSpecsForAProject(ctx context.Context, projectSpec models.ProjectSpec) ([]models.JobSpec, error) {
	var jobSpecs []models.JobSpec
	projectJobSpecRepo := srv.projectJobSpecRepoFactory.New(projectSpec)
	jobSpecs, err := projectJobSpecRepo.GetAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve jobs: %w", err)
	}
	return jobSpecs, nil
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

func (srv *Service) identifyAndPersistJobSources(ctx context.Context, projectSpec models.ProjectSpec,
	namespaceName string, jobSpecs []models.JobSpec, logSender sender.LogStatus) {
	start := time.Now()
	defer resolveDependencyHistogram.Observe(time.Since(start).Seconds())

	// resolve specs in parallel
	runner := parallel.NewRunner(parallel.WithTicket(ConcurrentTicketPerSec), parallel.WithLimit(ConcurrentLimit))
	for _, jobSpec := range jobSpecs {
		runner.Add(func(currentSpec models.JobSpec) func() (interface{}, error) {
			return func() (interface{}, error) {
				jobSourceURNs, err := srv.identify(ctx, currentSpec, projectSpec)
				if err != nil {
					return currentSpec.Name, err
				}
				err = srv.jobSourceRepo.Save(ctx, projectSpec.ID, currentSpec.ID, jobSourceURNs)
				if err != nil {
					err = fmt.Errorf("error persisting job sources for job %s: %w", currentSpec.Name, err)
				}
				return currentSpec.Name, err
			}
		}(jobSpec))
	}

	failure, success := 0, 0
	for _, state := range runner.Run() {
		if state.Err != nil {
			failure++
			warnMsg := fmt.Sprintf("[%s] error '%s': failed to resolve dependency, %s", namespaceName, state.Val, state.Err.Error())
			sender.SendWarningMessage(logSender, warnMsg)
		} else {
			success++
			successMsg := fmt.Sprintf("[%s] info '%s': dependency is successfully resolved", namespaceName, state.Val)
			sender.SendSuccessMessage(logSender, successMsg)
		}
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
	}
	return resp.Dependencies, nil
}

// Deploy only the modified jobs (created or updated)
func (srv *Service) Deploy(ctx context.Context, projectName string, namespaceName string, jobSpecs []models.JobSpec, logSender sender.LogStatus) (models.DeploymentID, error) {
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
	savedJobs := srv.bulkCreate(ctx, namespaceSpec, createdAndModifiedJobs, logSender)

	srv.bulkDelete(ctx, namespaceSpec, deletedJobs, logSender)

	// Resolve inferred dependency
	if len(savedJobs) > 0 {
		srv.identifyAndPersistJobSources(ctx, namespaceSpec.ProjectSpec, namespaceName, savedJobs, logSender)
	}

	// Deploy through deploy manager
	deployID, err := srv.deployManager.Deploy(ctx, namespaceSpec.ProjectSpec)
	if err != nil {
		return models.DeploymentID(uuid.Nil), err
	}

	return deployID, nil
}

func (srv *Service) getJobsDiff(ctx context.Context, namespace models.NamespaceSpec, requestedJobSpecs []models.JobSpec) ([]models.JobSpec, []models.JobSpec, []models.JobSpec, error) {
	namespaceJobSpecRepo := srv.namespaceJobSpecRepoFactory.New(namespace)
	existingJobSpecs, err := namespaceJobSpecRepo.GetAll(ctx)
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
