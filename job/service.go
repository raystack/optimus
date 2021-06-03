package job

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"

	"github.com/kushsharma/parallel"
	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/meta"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/pkg/errors"
)

const (
	//PersistJobPrefix is used to keep the job during sync even if they are not in source repo
	PersistJobPrefix string = "__"

	ConcurrentTicketPerSec = 40
	ConcurrentLimit        = 600
)

type AssetCompiler func(jobSpec models.JobSpec, scheduledAt time.Time) (models.JobAssets, error)

// DependencyResolver compiles static and runtime dependencies
type DependencyResolver interface {
	Resolve(projectSpec models.ProjectSpec, projectJobSpecRepo store.ProjectJobSpecRepository,
		jobSpec models.JobSpec, observer progress.Observer) (models.JobSpec, error)
}

// JobSpecRepoFactory is used to manage job specs at namespace level
type JobSpecRepoFactory interface {
	New(spec models.NamespaceSpec) store.JobSpecRepository
}

// ProjectJobSpecRepoFactory is used to manage job specs at project level
type ProjectJobSpecRepoFactory interface {
	New(proj models.ProjectSpec) store.ProjectJobSpecRepository
}

// NamespaceRepoFactory is used to store job specs
type NamespaceRepoFactory interface {
	New(spec models.ProjectSpec) store.NamespaceRepository
}

// JobRepoFactory is used to store compiled jobs
type JobRepoFactory interface {
	New(context.Context, models.ProjectSpec) (store.JobRepository, error)
}

// Service compiles all jobs with its dependencies, priority and
// and other properties. Finally, it syncs the jobs with corresponding
// store
type Service struct {
	jobSpecRepoFactory        JobSpecRepoFactory
	compiler                  models.JobCompiler
	jobRepoFactory            JobRepoFactory
	dependencyResolver        DependencyResolver
	priorityResolver          PriorityResolver
	metaSvcFactory            meta.MetaSvcFactory
	projectJobSpecRepoFactory ProjectJobSpecRepoFactory

	Now           func() time.Time
	assetCompiler AssetCompiler
}

// Create constructs a Job for a namespace and commits it to the store
func (srv *Service) Create(namespace models.NamespaceSpec, spec models.JobSpec) error {
	jobRepo := srv.jobSpecRepoFactory.New(namespace)
	if err := jobRepo.Save(spec); err != nil {
		return errors.Wrapf(err, "failed to save job: %s", spec.Name)
	}
	return nil
}

// GetByName fetches a Job by name for a specific namespace
func (srv *Service) GetByName(name string, namespace models.NamespaceSpec) (models.JobSpec, error) {
	jobSpec, err := srv.jobSpecRepoFactory.New(namespace).GetByName(name)
	if err != nil {
		return models.JobSpec{}, errors.Wrapf(err, "failed to retrieve job")
	}
	return jobSpec, nil
}

// GetByNameForProject fetches a Job by name for a specific project
func (srv *Service) GetByNameForProject(name string, proj models.ProjectSpec) (models.JobSpec, models.NamespaceSpec, error) {
	jobSpec, namespace, err := srv.projectJobSpecRepoFactory.New(proj).GetByName(name)
	if err != nil {
		return models.JobSpec{}, models.NamespaceSpec{}, errors.Wrapf(err, "failed to retrieve job")
	}
	return jobSpec, namespace, nil
}

func (srv *Service) GetAll(namespace models.NamespaceSpec) ([]models.JobSpec, error) {
	jobSpecs, err := srv.jobSpecRepoFactory.New(namespace).GetAll()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to retrieve jobs")
	}
	return jobSpecs, nil
}

// Dump takes a jobSpec of a project, resolves dependencies, priorities and returns the compiled Job
func (srv *Service) Dump(namespace models.NamespaceSpec, jobSpec models.JobSpec) (models.Job, error) {
	projectJobSpecRepo := srv.projectJobSpecRepoFactory.New(namespace.ProjectSpec)
	jobSpecs, err := srv.getDependencyResolvedSpecs(namespace.ProjectSpec, projectJobSpecRepo, nil)
	if err != nil {
		return models.Job{}, err
	}

	// resolve priority of all jobSpecs
	jobSpecs, err = srv.priorityResolver.Resolve(jobSpecs)
	if err != nil {
		return models.Job{}, err
	}

	// get our input job from the request. since a job name is unique at a project level,
	// we can simply do the comparison by name
	var resolvedJobSpec models.JobSpec
	for _, jSpec := range jobSpecs {
		if jSpec.Name == jobSpec.Name {
			resolvedJobSpec = jSpec
		}
	}
	if resolvedJobSpec.Name == "" {
		return models.Job{}, errors.Errorf("missing job during compile %s", jobSpec.Name)
	}

	compiledJob, err := srv.compiler.Compile(namespace, resolvedJobSpec)
	if err != nil {
		return models.Job{}, errors.Wrapf(err, "failed to compile %s", resolvedJobSpec.Name)
	}
	return compiledJob, nil
}

// Check if job specifications are valid
func (srv *Service) Check(namespace models.NamespaceSpec, jobSpecs []models.JobSpec, obs progress.Observer) (err error) {
	for i, jSpec := range jobSpecs {
		// compile assets
		if jobSpecs[i].Assets, err = srv.assetCompiler(jSpec, srv.Now()); err != nil {
			return errors.Wrap(err, "asset compilation")
		}

		// remove manual dependencies as they needs to be resolved
		jobSpecs[i].Dependencies = map[string]models.JobSpecDependency{}
	}

	runner := parallel.NewRunner(parallel.WithTicket(ConcurrentTicketPerSec), parallel.WithLimit(ConcurrentLimit))
	for _, jSpec := range jobSpecs {
		runner.Add(func(currentSpec models.JobSpec) func() (interface{}, error) {
			return func() (interface{}, error) {
				// check dependencies
				if _, err := currentSpec.Task.Unit.GenerateTaskDependencies(context.TODO(), models.GenerateTaskDependenciesRequest{
					Config:  models.TaskPluginConfigs{}.FromJobSpec(currentSpec.Task.Config),
					Assets:  models.TaskPluginAssets{}.FromJobSpec(currentSpec.Assets),
					Project: namespace.ProjectSpec,
					PluginOptions: models.PluginOptions{
						DryRun: true,
					},
				}); err != nil {
					obs.Notify(&EventJobCheckFailed{Name: currentSpec.Name, Reason: fmt.Sprintf("dependency resolution: %s\n", err.Error())})
					return nil, errors.Wrapf(err, "failed to resolve dependencies %s", currentSpec.Name)
				}

				// check compilation
				if _, err := srv.compiler.Compile(namespace, currentSpec); err != nil {
					obs.Notify(&EventJobCheckFailed{Name: currentSpec.Name, Reason: fmt.Sprintf("compilation: %s\n", err.Error())})
					return nil, errors.Wrapf(err, "failed to compile %s", currentSpec.Name)
				}

				obs.Notify(&EventJobCheckSuccess{Name: currentSpec.Name})
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

// Delete deletes a job spec from all spec repos
func (srv *Service) Delete(ctx context.Context, namespace models.NamespaceSpec, jobSpec models.JobSpec) error {
	if err := srv.isJobDeletable(namespace.ProjectSpec, jobSpec); err != nil {
		return err
	}

	jobSpecRepo := srv.jobSpecRepoFactory.New(namespace)
	if err := jobSpecRepo.Delete(jobSpec.Name); err != nil {
		return errors.Wrapf(err, "failed to delete spec: %s", jobSpec.Name)
	}

	if err := srv.Sync(ctx, namespace, nil); err != nil {
		return err
	}

	return nil
}

// Sync fetches all the jobs that belong to a project, resolves its dependencies
// assign proper priority weights, compiles it and uploads it to the destination
// store
func (srv *Service) Sync(ctx context.Context, namespace models.NamespaceSpec, progressObserver progress.Observer) error {
	projectJobSpecRepo := srv.projectJobSpecRepoFactory.New(namespace.ProjectSpec)
	jobSpecs, err := srv.getDependencyResolvedSpecs(namespace.ProjectSpec, projectJobSpecRepo, progressObserver)
	if err != nil {
		return err
	}
	srv.notifyProgress(progressObserver, &EventJobSpecDependencyResolve{})

	jobSpecs, err = srv.priorityResolver.Resolve(jobSpecs)
	if err != nil {
		return err
	}
	srv.notifyProgress(progressObserver, &EventJobPriorityWeightAssign{})

	jobSpecs, err = srv.filterJobSpecForNamespace(jobSpecs, namespace)
	if err != nil {
		return err
	}

	jobRepo, err := srv.jobRepoFactory.New(ctx, namespace.ProjectSpec)
	if err != nil {
		return err
	}

	if err = srv.uploadSpecs(ctx, jobSpecs, jobRepo, namespace, progressObserver); err != nil {
		return err
	}

	if err = srv.publishMetadata(namespace, jobSpecs, progressObserver); err != nil {
		return err
	}

	// get all the stored job names
	destJobNames, err := jobRepo.ListNames(ctx, namespace)
	if err != nil {
		return err
	}

	// filter what we need to keep/delete
	var sourceJobNames []string
	for _, jobSpec := range jobSpecs {
		sourceJobNames = append(sourceJobNames, jobSpec.Name)
	}
	jobsToDelete := setSubstract(destJobNames, sourceJobNames)
	jobsToDelete = jobDeletionFilter(jobsToDelete)
	for _, dagName := range jobsToDelete {
		// delete compiled spec
		if err := jobRepo.Delete(ctx, namespace, dagName); err != nil {
			return err
		}
		srv.notifyProgress(progressObserver, &EventJobRemoteDelete{dagName})
	}
	return nil
}

// KeepOnly only keeps the provided jobSpecs in argument and deletes rest from spec repository
func (srv *Service) KeepOnly(namespace models.NamespaceSpec, specsToKeep []models.JobSpec, progressObserver progress.Observer) error {
	jobSpecRepo := srv.jobSpecRepoFactory.New(namespace)
	jobSpecs, err := jobSpecRepo.GetAll()
	if err != nil {
		return errors.Wrapf(err, "failed to fetch specs for namespace %s", namespace.Name)
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
	jobsToDelete := setSubstract(specsPresentNames, specsToKeepNames)
	jobsToDelete = jobDeletionFilter(jobsToDelete)

	for _, jobName := range jobsToDelete {
		// delete raw spec
		if err := jobSpecRepo.Delete(jobName); err != nil {
			return errors.Wrapf(err, "failed to delete spec: %s", jobName)
		}
		srv.notifyProgress(progressObserver, &EventSavedJobDelete{jobName})
	}
	return nil
}

// filterJobSpecForNamespace returns only job specs of a given namespace
func (srv *Service) filterJobSpecForNamespace(jobSpecs []models.JobSpec, namespace models.NamespaceSpec) ([]models.JobSpec, error) {
	jobSpecRepo := srv.jobSpecRepoFactory.New(namespace)
	namespaceJobSpecs, err := jobSpecRepo.GetAll()
	if err != nil {
		return nil, err
	}
	var namespaceJobSpecNames []string
	for _, jSpec := range namespaceJobSpecs {
		namespaceJobSpecNames = append(namespaceJobSpecNames, jSpec.Name)
	}

	var filteredJobSpecs []models.JobSpec
	for _, jobSpec := range jobSpecs {
		if srv.ifPresentInJobSpec(namespaceJobSpecNames, jobSpec.Name) {
			filteredJobSpecs = append(filteredJobSpecs, jobSpec)
		}
	}
	return filteredJobSpecs, nil
}

func (srv *Service) getDependencyResolvedSpecs(proj models.ProjectSpec, projectJobSpecRepo store.ProjectJobSpecRepository,
	progressObserver progress.Observer) (resolvedSpecs []models.JobSpec, resolvedErrors error) {
	// fetch all jobs since dependency resolution happens for all jobs in a project, not just for a namespace
	jobSpecs, err := projectJobSpecRepo.GetAll()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to retrieve jobs")
	}
	srv.notifyProgress(progressObserver, &EventJobSpecFetch{})

	// compile assets first
	for i, jSpec := range jobSpecs {
		if jobSpecs[i].Assets, err = srv.assetCompiler(jSpec, srv.Now()); err != nil {
			return nil, errors.Wrap(err, "asset compilation")
		}
	}

	// resolve specs in parallel
	runner := parallel.NewRunner(parallel.WithTicket(ConcurrentTicketPerSec), parallel.WithLimit(ConcurrentLimit))
	for _, jobSpec := range jobSpecs {
		runner.Add(func(currentSpec models.JobSpec) func() (interface{}, error) {
			return func() (interface{}, error) {
				resolvedSpec, err := srv.dependencyResolver.Resolve(proj, projectJobSpecRepo, currentSpec, progressObserver)
				if err != nil {
					return nil, errors.Wrapf(err, "failed to resolve dependency for %s", currentSpec.Name)
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

// uploadSpecs compiles a Job and uploads it to the destination store
func (srv *Service) uploadSpecs(ctx context.Context, jobSpecs []models.JobSpec, jobRepo store.JobRepository,
	namespace models.NamespaceSpec, progressObserver progress.Observer) error {
	runner := parallel.NewRunner(parallel.WithTicket(ConcurrentTicketPerSec))
	for _, jobSpec := range jobSpecs {
		runner.Add(func(currentSpec models.JobSpec) func() (interface{}, error) {
			return func() (interface{}, error) {
				compiledJob, err := srv.compiler.Compile(namespace, currentSpec)
				if err != nil {
					return nil, err
				}
				srv.notifyProgress(progressObserver, &EventJobSpecCompile{
					Name: currentSpec.Name,
				})

				if err = jobRepo.Save(ctx, compiledJob); err != nil {
					return nil, err
				}
				return nil, nil
			}
		}(jobSpec))
	}

	for runIdx, state := range runner.Run() {
		srv.notifyProgress(progressObserver, &EventJobUpload{
			Job: jobSpecs[runIdx],
			Err: state.Err,
		})
	}
	return nil
}

func (srv *Service) publishMetadata(namespace models.NamespaceSpec, jobSpecs []models.JobSpec,
	progressObserver progress.Observer) error {
	if srv.metaSvcFactory == nil {
		return nil
	}

	metadataJobService := srv.metaSvcFactory.New()
	if err := metadataJobService.Publish(namespace, jobSpecs, progressObserver); err != nil {
		return err
	}
	return nil
}

// isJobDeletable determines if a given job is deletable or not
func (srv *Service) isJobDeletable(projectSpec models.ProjectSpec, jobSpec models.JobSpec) error {
	// check if this job spec is dependency of any other job spec
	projectJobSpecRepo := srv.projectJobSpecRepoFactory.New(projectSpec)
	depsResolvedJobSpecs, err := srv.getDependencyResolvedSpecs(projectSpec, projectJobSpecRepo, nil)
	if err != nil {
		return err
	}
	for _, resolvedJobSpec := range depsResolvedJobSpecs {
		for depJobSpecName := range resolvedJobSpec.Dependencies {
			if depJobSpecName == jobSpec.Name {
				return errors.Errorf("cannot delete job %s since it's dependency of job %s", jobSpec.Name,
					resolvedJobSpec.Name)
			}
		}
	}

	return nil
}

func (srv *Service) notifyProgress(po progress.Observer, event progress.Event) {
	if po == nil {
		return
	}
	po.Notify(event)
}

func setSubstract(left []string, right []string) []string {
	rightMap := make(map[string]struct{})
	for _, item := range right {
		rightMap[item] = struct{}{}
	}

	res := make([]string, 0)
	for _, leftKey := range left {
		_, exists := rightMap[leftKey]
		if !exists {
			res = append(res, leftKey)
		}
	}

	return res
}

func (srv *Service) ifPresentInJobSpec(jobSpecNames []string, jobSpecToFind string) bool {
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

// NewService creates a new instance of JobService, requiring
// the necessary dependencies as arguments
func NewService(jobSpecRepoFactory JobSpecRepoFactory, jobRepoFact JobRepoFactory,
	compiler models.JobCompiler, assetCompiler AssetCompiler, dependencyResolver DependencyResolver,
	priorityResolver PriorityResolver, metaSvcFactory meta.MetaSvcFactory,
	projectJobSpecRepoFactory ProjectJobSpecRepoFactory,
) *Service {
	return &Service{
		jobSpecRepoFactory:        jobSpecRepoFactory,
		jobRepoFactory:            jobRepoFact,
		compiler:                  compiler,
		dependencyResolver:        dependencyResolver,
		priorityResolver:          priorityResolver,
		metaSvcFactory:            metaSvcFactory,
		projectJobSpecRepoFactory: projectJobSpecRepoFactory,

		assetCompiler: assetCompiler,
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

	// EventJobSpecCompile represents a specification
	// being compiled to a Job
	EventJobSpecCompile struct{ Name string }

	// EventJobUpload represents the compiled Job
	// being uploaded
	EventJobUpload struct {
		Job models.JobSpec
		Err error
	}

	// EventJobRemoteDelete signifies that a
	// compiled job from a remote repository is being deleted
	EventJobRemoteDelete struct{ Name string }

	// EventSavedJobDelete signifies that a raw
	// job from a repository is being deleted
	EventSavedJobDelete struct{ Name string }

	// EventJobPriorityWeightAssign signifies that a
	// job is being assigned a priority weight
	EventJobPriorityWeightAssign struct{}

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
	return fmt.Sprintf("fetching job specs")
}

func (e *EventJobSpecCompile) String() string {
	return fmt.Sprintf("compiling: %s", e.Name)
}

func (e *EventJobUpload) String() string {
	if e.Err != nil {
		return fmt.Sprintf("uploading: %s, failed with error): %s", e.Job.Name, e.Err.Error())
	}
	return fmt.Sprintf("uploaded: %s", e.Job.Name)
}

func (e *EventJobRemoteDelete) String() string {
	return fmt.Sprintf("deleting: %s", e.Name)
}

func (e *EventSavedJobDelete) String() string {
	return fmt.Sprintf("deleting: %s", e.Name)
}

func (e *EventJobPriorityWeightAssign) String() string {
	return fmt.Sprintf("assigned priority weights")
}

func (e *EventJobSpecDependencyResolve) String() string {
	return fmt.Sprintf("dependencies resolved")
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
