package job

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/kushsharma/parallel"
	"github.com/pkg/errors"
	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/meta"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
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
	Resolve(projectSpec models.ProjectSpec, jobSpecRepo store.JobSpecRepository,
		jobSpec models.JobSpec, observer progress.Observer) (models.JobSpec, error)
}

// JobSpecRepoFactory is used to store job specs
type JobSpecRepoFactory interface {
	New(models.ProjectSpec) store.JobSpecRepository
}

// JobRepoFactory is used to store compiled jobs
type JobRepoFactory interface {
	New(context.Context, models.ProjectSpec) (store.JobRepository, error)
}

// Service compiles all jobs with its dependencies, priority and
// and other properties. Finally, it syncs the jobs with corresponding
// store
type Service struct {
	jobSpecRepoFactory JobSpecRepoFactory
	compiler           models.JobCompiler
	jobRepoFactory     JobRepoFactory
	dependencyResolver DependencyResolver
	priorityResolver   PriorityResolver
	metaSvcFactory     meta.MetaSvcFactory

	Now           func() time.Time
	assetCompiler AssetCompiler
}

// CreateJob constructs a Job and commits it to store
func (srv *Service) Create(spec models.JobSpec, proj models.ProjectSpec) error {
	jobRepo := srv.jobSpecRepoFactory.New(proj)
	if err := jobRepo.Save(spec); err != nil {
		return errors.Wrapf(err, "failed to save job: %s", spec.Name)
	}
	return nil
}

func (srv *Service) GetByName(name string, proj models.ProjectSpec) (models.JobSpec, error) {
	jobSpec, err := srv.jobSpecRepoFactory.New(proj).GetByName(name)
	if err != nil {
		return models.JobSpec{}, errors.Wrapf(err, "failed to retrive job")
	}
	return jobSpec, nil
}

func (srv *Service) GetAll(proj models.ProjectSpec) ([]models.JobSpec, error) {
	jobSpecs, err := srv.jobSpecRepoFactory.New(proj).GetAll()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to retrive jobs")
	}
	return jobSpecs, nil
}

// Check if job specifications are valid
func (srv *Service) Check(projSpec models.ProjectSpec, jobSpecs []models.JobSpec, obs progress.Observer) (err error) {
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
		currentSpec := jSpec
		runner.Add(func() (interface{}, error) {
			// check dependencies
			if _, err := currentSpec.Task.Unit.GenerateDependencies(models.GenerateDependenciesRequest{
				Config:  currentSpec.Task.Config,
				Assets:  currentSpec.Assets.ToMap(),
				Project: projSpec,
				UnitOptions: models.UnitOptions{
					DryRun: true,
				},
			}); err != nil {
				obs.Notify(&EventJobCheckFailed{Name: currentSpec.Name, Reason: fmt.Sprintf("dependency resolution: %s\n", err.Error())})
				return nil, errors.Wrapf(err, "failed to resolve dependencies %s", currentSpec.Name)
			}

			// check compilation
			if _, err := srv.compiler.Compile(currentSpec, projSpec); err != nil {
				obs.Notify(&EventJobCheckFailed{Name: currentSpec.Name, Reason: fmt.Sprintf("compilation: %s\n", err.Error())})
				return nil, errors.Wrapf(err, "failed to compile %s", currentSpec.Name)
			}

			obs.Notify(&EventJobCheckSuccess{Name: currentSpec.Name})
			return nil, nil
		})
	}
	for _, result := range runner.Run() {
		if result.Err != nil {
			err = multierror.Append(err, result.Err)
		}
	}
	return err
}

// Dump takes a jobSpec of a project, resolves dependencies,priorities and returns the compiled Job
func (srv *Service) Dump(projSpec models.ProjectSpec, jobSpec models.JobSpec) (models.Job, error) {
	jobSpecRepo := srv.jobSpecRepoFactory.New(projSpec)
	jobSpecs, err := srv.getDependencyResolvedSpecs(projSpec, jobSpecRepo, nil)
	if err != nil {
		return models.Job{}, err
	}

	// resolve priority of all jobSpecs
	jobSpecs, err = srv.priorityResolver.Resolve(jobSpecs)
	if err != nil {

	}

	// get our input job from the request
	var resolvedJobSpec models.JobSpec
	for _, jSpec := range jobSpecs {
		if jSpec.Name == jobSpec.Name {
			resolvedJobSpec = jSpec
		}
	}
	if resolvedJobSpec.Name == "" {
		return models.Job{}, errors.Errorf("missing job during compile %s", jobSpec.Name)
	}

	// compile
	compiledJob, err := srv.compiler.Compile(resolvedJobSpec, projSpec)
	if err != nil {
		return models.Job{}, errors.Wrapf(err, "failed to compile %s", resolvedJobSpec.Name)
	}
	return compiledJob, nil
}

// Sync fetches all the jobs that belong to a project, resolves its dependencies
// assign proper priority weights, compiles it and uploads it to the destination
// store
func (srv *Service) Sync(ctx context.Context, proj models.ProjectSpec, progressObserver progress.Observer) error {
	jobSpecRepo := srv.jobSpecRepoFactory.New(proj)

	jobSpecs, err := srv.getDependencyResolvedSpecs(proj, jobSpecRepo, progressObserver)
	if err != nil {
		return err
	}
	srv.notifyProgress(progressObserver, &EventJobSpecDependencyResolve{})

	jobSpecs, err = srv.priorityResolver.Resolve(jobSpecs)
	if err != nil {
		return err
	}
	srv.notifyProgress(progressObserver, &EventJobPriorityWeightAssign{})

	jobRepo, err := srv.jobRepoFactory.New(ctx, proj)
	if err != nil {
		return err
	}

	if err = srv.uploadSpecs(ctx, jobSpecs, jobRepo, proj, progressObserver); err != nil {
		return err
	}

	if err = srv.publishMetadata(proj, jobSpecs, progressObserver); err != nil {
		return err
	}

	// get all the stored job names
	destJobNames, err := jobRepo.ListNames(ctx)
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
		if err := jobRepo.Delete(ctx, dagName); err != nil {
			return err
		}
		srv.notifyProgress(progressObserver, &EventJobRemoteDelete{dagName})
	}
	return nil
}

func (srv *Service) getDependencyResolvedSpecs(proj models.ProjectSpec, jobSpecRepo store.JobSpecRepository,
	progressObserver progress.Observer) (resolvedSpecs []models.JobSpec, resolvedErrors error) {

	// fetch all
	jobSpecs, err := jobSpecRepo.GetAll()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to retrive jobs")
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
		currentSpec := jobSpec
		runner.Add(func() (interface{}, error) {
			resolvedSpec, err := srv.dependencyResolver.Resolve(proj, jobSpecRepo, currentSpec, progressObserver)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to resolve dependency for %s", currentSpec.Name)
			}
			return resolvedSpec, nil
		})
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
	proj models.ProjectSpec, progressObserver progress.Observer) error {

	runner := parallel.NewRunner(parallel.WithTicket(ConcurrentTicketPerSec))
	for _, jobSpec := range jobSpecs {
		currentSpec := jobSpec
		runner.Add(func() (interface{}, error) {
			compiledJob, err := srv.compiler.Compile(currentSpec, proj)
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
		})
	}

	for runIdx, state := range runner.Run() {
		srv.notifyProgress(progressObserver, &EventJobUpload{
			Job: jobSpecs[runIdx],
			Err: state.Err,
		})
	}
	return nil
}

func (srv *Service) publishMetadata(proj models.ProjectSpec, jobSpecs []models.JobSpec, progressObserver progress.Observer) error {
	if srv.metaSvcFactory == nil {
		return nil
	}

	metadataJobService := srv.metaSvcFactory.New()
	if err := metadataJobService.Publish(proj, jobSpecs, progressObserver); err != nil {
		return err
	}
	return nil
}

func (srv *Service) KeepOnly(proj models.ProjectSpec, specsToKeep []models.JobSpec, progressObserver progress.Observer) error {
	jobSpecRepo := srv.jobSpecRepoFactory.New(proj)
	jobSpecs, err := jobSpecRepo.GetAll()
	if err != nil {
		return errors.Wrapf(err, "failed to fetch specs for project %s", proj.Name)
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
) *Service {
	return &Service{
		jobSpecRepoFactory: jobSpecRepoFactory,
		jobRepoFactory:     jobRepoFact,
		compiler:           compiler,
		dependencyResolver: dependencyResolver,
		priorityResolver:   priorityResolver,
		metaSvcFactory:     metaSvcFactory,

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
