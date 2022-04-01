package job

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/kushsharma/parallel"

	"github.com/odpf/optimus/core/progress"
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
)

var errDependencyResolution = fmt.Errorf("dependency resolution")

type AssetCompiler func(jobSpec models.JobSpec, scheduledAt time.Time) (models.JobAssets, error)

// DependencyResolver compiles static and runtime dependencies
type DependencyResolver interface {
	Resolve(ctx context.Context, projectSpec models.ProjectSpec, jobSpec models.JobSpec, namespaceName string, observer progress.Observer) (models.JobSpec, error)
}

// SpecRepoFactory is used to manage job specs at namespace level
type SpecRepoFactory interface {
	New(spec models.NamespaceSpec) SpecRepository
}

// ProjectJobSpecRepoFactory is used to manage job specs at project level
type ProjectJobSpecRepoFactory interface {
	New(proj models.ProjectSpec) store.ProjectJobSpecRepository
}

// NamespaceRepoFactory is used to store job specs
type NamespaceRepoFactory interface {
	New(spec models.ProjectSpec) store.NamespaceRepository
}

// ReplaySpecRepoFactory is used to manage replay spec objects from store
type ReplaySpecRepoFactory interface {
	New() store.ReplaySpecRepository
}

// ProjectRepoFactory is used to manage projects from store
type ProjectRepoFactory interface {
	New() store.ProjectRepository
}

type ReplayManager interface {
	Init()
	Replay(context.Context, models.ReplayRequest) (models.ReplayResult, error)
	GetReplay(context.Context, uuid.UUID) (models.ReplaySpec, error)
	GetReplayList(ctx context.Context, projectID uuid.UUID) ([]models.ReplaySpec, error)
	GetRunStatus(ctx context.Context, projectSpec models.ProjectSpec, startDate, endDate time.Time,
		jobName string) ([]models.JobStatus, error)
}

// Service compiles all jobs with its dependencies, priority and
// and other properties. Finally, it syncs the jobs with corresponding
// store
type Service struct {
	jobSpecRepoFactory        SpecRepoFactory
	dependencyResolver        DependencyResolver
	priorityResolver          PriorityResolver
	projectJobSpecRepoFactory ProjectJobSpecRepoFactory
	replayManager             ReplayManager

	// scheduler for managing batch scheduled jobs
	batchScheduler models.SchedulerUnit

	// scheduler for managing one time executable jobs
	manualScheduler models.SchedulerUnit

	Now           func() time.Time
	assetCompiler AssetCompiler
	pluginService service.PluginService
}

// Create constructs a Job for a namespace and commits it to the store
func (srv *Service) Create(ctx context.Context, namespace models.NamespaceSpec, spec models.JobSpec) error {
	jobRepo := srv.jobSpecRepoFactory.New(namespace)
	if err := jobRepo.Save(ctx, spec); err != nil {
		return fmt.Errorf("failed to save job: %s: %w", spec.Name, err)
	}
	return nil
}

// GetByName fetches a Job by name for a specific namespace
func (srv *Service) GetByName(ctx context.Context, name string, namespace models.NamespaceSpec) (models.JobSpec, error) {
	jobSpec, err := srv.jobSpecRepoFactory.New(namespace).GetByName(ctx, name)
	if err != nil {
		return models.JobSpec{}, fmt.Errorf("failed to retrieve job: %w", err)
	}
	return jobSpec, nil
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
	jobSpecs, err := srv.jobSpecRepoFactory.New(namespace).GetAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve jobs: %w", err)
	}
	return jobSpecs, nil
}

// Check if job specifications are valid
func (srv *Service) Check(ctx context.Context, namespace models.NamespaceSpec, jobSpecs []models.JobSpec, obs progress.Observer) (err error) {
	for i, jSpec := range jobSpecs {
		// compile assets
		if jobSpecs[i].Assets, err = srv.assetCompiler(jSpec, srv.Now()); err != nil {
			return fmt.Errorf("asset compilation: %w", err)
		}

		// remove manual dependencies as they needs to be resolved
		jobSpecs[i].Dependencies = map[string]models.JobSpecDependency{}
	}

	runner := parallel.NewRunner(parallel.WithTicket(ConcurrentTicketPerSec), parallel.WithLimit(ConcurrentLimit))
	for _, jSpec := range jobSpecs {
		runner.Add(func(currentSpec models.JobSpec) func() (interface{}, error) {
			return func() (interface{}, error) {
				// check dependencies
				if currentSpec.Task.Unit.DependencyMod != nil {
					if _, err := currentSpec.Task.Unit.DependencyMod.GenerateDependencies(context.TODO(), models.GenerateDependenciesRequest{
						Config:  models.PluginConfigs{}.FromJobSpec(currentSpec.Task.Config),
						Assets:  models.PluginAssets{}.FromJobSpec(currentSpec.Assets),
						Project: namespace.ProjectSpec,
						PluginOptions: models.PluginOptions{
							DryRun: true,
						},
					}); err != nil {
						if obs != nil {
							obs.Notify(&EventJobCheckFailed{Name: currentSpec.Name, Reason: fmt.Sprintf("dependency resolution: %s\n", err.Error())})
						}
						return nil, fmt.Errorf("%s %s: %w", errDependencyResolution.Error(), currentSpec.Name, err)
					}
				}

				// check compilation
				if err := srv.batchScheduler.VerifyJob(ctx, namespace, currentSpec); err != nil {
					if obs != nil {
						obs.Notify(&EventJobCheckFailed{Name: currentSpec.Name, Reason: fmt.Sprintf("compilation: %s\n", err.Error())})
					}
					return nil, fmt.Errorf("failed to compile %s: %w", currentSpec.Name, err)
				}

				if obs != nil {
					obs.Notify(&EventJobCheckSuccess{Name: currentSpec.Name})
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

	// compile assets before generating dependencies
	if jobSpec.Assets, err = srv.assetCompiler(jobSpec, srv.Now()); err != nil {
		return destination, dependencies, fmt.Errorf("asset compilation: %w", err)
	}

	deps, err := srv.pluginService.GenerateDependencies(ctx, jobSpec, namespace)
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
	if err := srv.isJobDeletable(ctx, namespace.ProjectSpec, jobSpec); err != nil {
		return err
	}
	jobSpecRepo := srv.jobSpecRepoFactory.New(namespace)

	// delete from internal store
	if err := jobSpecRepo.Delete(ctx, jobSpec.Name); err != nil {
		return fmt.Errorf("failed to delete spec: %s: %w", jobSpec.Name, err)
	}

	// delete from batch scheduler
	return srv.batchScheduler.DeleteJobs(ctx, namespace, []string{jobSpec.Name}, nil)
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
		if errors.As(err, &merrs) {
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
		} else {
			return err
		}
	}
	srv.notifyProgress(progressObserver, &EventJobSpecDependencyResolve{})

	jobSpecs, err = srv.priorityResolver.Resolve(ctx, jobSpecs, progressObserver)
	if err != nil {
		return err
	}
	srv.notifyProgress(progressObserver, &EventJobPriorityWeightAssign{})

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

// KeepOnly only keeps the provided jobSpecs in argument and deletes rest from spec repository
func (srv *Service) KeepOnly(ctx context.Context, namespace models.NamespaceSpec, specsToKeep []models.JobSpec, progressObserver progress.Observer) error {
	jobSpecRepo := srv.jobSpecRepoFactory.New(namespace)
	jobSpecs, err := jobSpecRepo.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch specs for namespace %s: %w", namespace.Name, err)
	}
	var specsPresentNames []string
	for _, jobSpec := range jobSpecs {
		specsPresentNames = append(specsPresentNames, jobSpec.Name)
	}

	var specsToKeepNames []string
	for _, jobSpec := range specsToKeep {
		specsToKeepNames = append(specsToKeepNames, jobSpec.Name)
	}

	// filter what we need to keep/delete
	jobsToDelete := setSubtract(specsPresentNames, specsToKeepNames)
	jobsToDelete = jobDeletionFilter(jobsToDelete)

	for _, jobName := range jobsToDelete {
		// delete raw spec
		if err := jobSpecRepo.Delete(ctx, jobName); err != nil {
			return fmt.Errorf("failed to delete spec: %s: %w", jobName, err)
		}
		srv.notifyProgress(progressObserver, &EventSavedJobDelete{jobName})
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
	srv.notifyProgress(progressObserver, &EventJobSpecFetch{})

	// compile assets first
	for i, jSpec := range jobSpecs {
		if jobSpecs[i].Assets, err = srv.assetCompiler(jSpec, srv.Now()); err != nil {
			return nil, fmt.Errorf("asset compilation: %w", err)
		}
	}

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
				resolvedSpec, err := srv.dependencyResolver.Resolve(ctx, proj, currentSpec, jobsToNamespace[currentSpec.Name], progressObserver)
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

// isJobDeletable determines if a given job is deletable or not
func (srv *Service) isJobDeletable(ctx context.Context, projectSpec models.ProjectSpec, jobSpec models.JobSpec) error {
	// check if this job spec is dependency of any other job spec
	projectJobSpecRepo := srv.projectJobSpecRepoFactory.New(projectSpec)
	depsResolvedJobSpecs, err := srv.GetDependencyResolvedSpecs(ctx, projectSpec, projectJobSpecRepo, nil)
	if err != nil {
		return err
	}
	for _, resolvedJobSpec := range depsResolvedJobSpecs {
		for depJobSpecName := range resolvedJobSpec.Dependencies {
			if depJobSpecName == jobSpec.Name {
				return fmt.Errorf("cannot delete job %s since it's dependency of job %s", jobSpec.Name,
					resolvedJobSpec.Name)
			}
		}
	}

	return nil
}

func (srv *Service) GetByDestination(ctx context.Context, projectSpec models.ProjectSpec, destination string) (models.JobSpec, error) {
	// generate job spec using datastore destination. if a destination can be owned by multiple jobs, need to change to list
	projectJobSpecRepo := srv.projectJobSpecRepoFactory.New(projectSpec)
	projectJobPairs, err := projectJobSpecRepo.GetByDestination(ctx, destination)
	if err != nil {
		return models.JobSpec{}, err
	}
	for _, p := range projectJobPairs {
		if p.Project.Name == projectSpec.Name {
			return p.Job, nil
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

func (srv *Service) notifyProgress(po progress.Observer, event progress.Event) {
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

func (srv *Service) ifPresentInNamespace(jobSpecNames []string, jobSpecToFind string) bool {
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
func NewService(jobSpecRepoFactory SpecRepoFactory, batchScheduler models.SchedulerUnit,
	manualScheduler models.SchedulerUnit, assetCompiler AssetCompiler,
	dependencyResolver DependencyResolver, priorityResolver PriorityResolver,
	projectJobSpecRepoFactory ProjectJobSpecRepoFactory,
	replayManager ReplayManager, pluginService service.PluginService,
) *Service {
	return &Service{
		jobSpecRepoFactory:        jobSpecRepoFactory,
		batchScheduler:            batchScheduler,
		manualScheduler:           manualScheduler,
		dependencyResolver:        dependencyResolver,
		priorityResolver:          priorityResolver,
		projectJobSpecRepoFactory: projectJobSpecRepoFactory,
		replayManager:             replayManager,

		assetCompiler: assetCompiler,
		pluginService: pluginService,
		Now:           time.Now,
	}
}

type (
	// EventJobSpecFetch represents a specification being
	// read from the storage
	EventJobSpecFetch struct{}

	// EventJobSpecDependencyResolve represents dependencies are being
	// successfully resolved
	EventJobSpecDependencyResolve struct{}

	// EventJobSpecUnknownDependencyUsed represents a job spec has used
	// dependencies which are unknown/unresolved
	EventJobSpecUnknownDependencyUsed struct {
		Job        string
		Dependency string
	}

	// EventSavedJobDelete signifies that a raw
	// job from a repository is being deleted
	EventSavedJobDelete struct{ Name string }

	// EventJobPriorityWeightAssign signifies that a
	// job is being assigned a priority weight
	EventJobPriorityWeightAssign struct{}
	// EventJobPriorityWeightAssignmentFailed signifies that a
	// job is failed during priority weight assignment
	EventJobPriorityWeightAssignmentFailed struct {
		Err error
	}

	// job check events
	EventJobCheckFailed struct {
		Name   string
		Reason string
	}
	EventJobCheckSuccess struct {
		Name string
	}
)

func (e *EventJobSpecFetch) String() string {
	return "fetching job specs"
}

func (e *EventSavedJobDelete) String() string {
	return fmt.Sprintf("deleting: %s", e.Name)
}

func (e *EventJobPriorityWeightAssign) String() string {
	return "assigned priority weights"
}

func (e *EventJobPriorityWeightAssignmentFailed) String() string {
	return fmt.Sprintf("failed priority weight assignment: %v", e.Err)
}

func (e *EventJobSpecDependencyResolve) String() string {
	return "dependencies resolved"
}

func (e *EventJobSpecUnknownDependencyUsed) String() string {
	return fmt.Sprintf("could not find registered destination '%s' during compiling dependencies for the provided job %s", e.Dependency, e.Job)
}

func (e *EventJobCheckFailed) String() string {
	return fmt.Sprintf("check for job failed: %s, reason: %s", e.Name, e.Reason)
}

func (e *EventJobCheckSuccess) String() string {
	return fmt.Sprintf("check for job passed: %s", e.Name)
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
