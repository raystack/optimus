package job

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

const (
	//PersistJobPrefix is used to keep the job during sync even if they are not in source repo
	PersistJobPrefix string = "__"
)

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

// upload compiles a Job and uploads it to the destination store
func (srv *Service) upload(jobSpec models.JobSpec, jobRepo store.JobRepository, proj models.ProjectSpec, progressObserver progress.Observer) error {
	compiledJob, err := srv.compiler.Compile(jobSpec, proj)
	if err != nil {
		return err
	}
	srv.notifyProgress(progressObserver, &EventJobSpecCompile{
		Name: jobSpec.Name,
	})

	if err = jobRepo.Save(compiledJob); err != nil {
		return err
	}

	return nil
}

// Dump takes a jobSpec of a project, resolves dependencies.priorities and returns the compiled Job
func (srv *Service) Dump(projSpec models.ProjectSpec, jobSpec models.JobSpec) (models.Job, error) {
	jobSpecRepo := srv.jobSpecRepoFactory.New(projSpec)
	jobSpecs, err := jobSpecRepo.GetAll()
	if err != nil {
		return models.Job{}, errors.Wrapf(err, "failed to retrive jobs")
	}

	// resolve dependencies
	for idx, jSpec := range jobSpecs {
		if jobSpecs[idx], err = srv.dependencyResolver.Resolve(projSpec, jobSpecRepo, jSpec, nil); err != nil {
			return models.Job{}, errors.Wrapf(err, "failed to resolve dependencies %s", jSpec.Name)
		}
	}

	// resolve priority of all jobSpecs
	jobSpecs, err = srv.priorityResolver.Resolve(jobSpecs)

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
func (srv *Service) Sync(proj models.ProjectSpec, progressObserver progress.Observer) error {
	jobSpecRepo := srv.jobSpecRepoFactory.New(proj)
	jobSpecs, err := jobSpecRepo.GetAll()
	if err != nil {
		return errors.Wrapf(err, "failed to retrive jobs")
	}
	srv.notifyProgress(progressObserver, &EventJobSpecFetch{})

	var dependencyErrors error
	for idx, jobSpec := range jobSpecs {
		if jobSpecs[idx], err = srv.dependencyResolver.Resolve(proj, jobSpecRepo, jobSpec, progressObserver); err != nil {
			dependencyErrors = multierror.Append(dependencyErrors, err)
		}
	}
	if dependencyErrors != nil {
		return dependencyErrors
	}
	srv.notifyProgress(progressObserver, &EventJobSpecDependencyResolve{})

	jobSpecs, err = srv.priorityResolver.Resolve(jobSpecs)
	if err != nil {
		return err
	}
	srv.notifyProgress(progressObserver, &EventJobPriorityWeightAssign{})

	jobRepo, err := srv.jobRepoFactory.New(context.Background(), proj)
	if err != nil {
		return err
	}

	var sourceJobNames []string
	for _, jobSpec := range jobSpecs {
		if err = srv.upload(jobSpec, jobRepo, proj, progressObserver); err != nil {
			srv.notifyProgress(progressObserver, &EventJobUpload{
				Job: jobSpec,
				Err: err,
			})
		} else {
			srv.notifyProgress(progressObserver, &EventJobUpload{
				Job: jobSpec,
			})
		}

		sourceJobNames = append(sourceJobNames, jobSpec.Name)
	}

	// get all the stored jobs
	jobs, err := jobRepo.GetAll()
	if err != nil {
		return err
	}

	// filter what we need to keep/delete
	var destjobNames []string
	for _, job := range jobs {
		destjobNames = append(destjobNames, job.Name)
	}
	jobsToDelete := setSubstract(destjobNames, sourceJobNames)
	jobsToDelete = jobDeletionFilter(jobsToDelete)

	for _, dagName := range jobsToDelete {
		// delete compiled spec
		if err := jobRepo.Delete(dagName); err != nil {
			return err
		}
		srv.notifyProgress(progressObserver, &EventJobRemoteDelete{dagName})
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
	compiler models.JobCompiler, dependencyResolver DependencyResolver,
	priorityResolver PriorityResolver,
) *Service {
	return &Service{
		jobSpecRepoFactory: jobSpecRepoFactory,
		jobRepoFactory:     jobRepoFact,
		compiler:           compiler,
		dependencyResolver: dependencyResolver,
		priorityResolver:   priorityResolver,
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
