package job

import (
	"github.com/pkg/errors"
	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

var (
	ErrUnknownDependency            = errors.New("unknown local dependency")
	UnknownRuntimeDependencyMessage = "could not find registered destination '%s' during compiling dependencies for the provided job '%s', " +
		"please check if the source is correct, " +
		"if it is and want this to be ignored as dependency, " +
		"check docs how this can be done in used transformation task"
)

type dependencyResolver struct{}

// Resolve resolves all kind of dependencies (inter/intra project, static deps) of a given JobSpec
func (r *dependencyResolver) Resolve(projectSpec models.ProjectSpec, projectJobSpecRepo store.ProjectJobSpecRepository,
	jobSpec models.JobSpec, observer progress.Observer) (models.JobSpec, error) {

	// resolve inter/intra dependencies inferred by optimus
	jobSpec, err := r.resolveInferredDependencies(jobSpec, projectSpec, projectJobSpecRepo, observer)
	if err != nil {
		return models.JobSpec{}, err
	}

	// resolve statically defined dependencies
	jobSpec, err = r.resolveStaticDependencies(jobSpec, projectSpec, projectJobSpecRepo)
	if err != nil {
		return models.JobSpec{}, err
	}

	// resolve inter hook dependencies
	jobSpec, err = r.resolveHookDependencies(jobSpec)
	if err != nil {
		return models.JobSpec{}, err
	}

	return jobSpec, nil
}

func (r *dependencyResolver) resolveInferredDependencies(jobSpec models.JobSpec, projectSpec models.ProjectSpec,
	projectJobSpecRepo store.ProjectJobSpecRepository, observer progress.Observer) (models.JobSpec, error) {

	// get destinations of dependencies, assets should be
	jobDependenciesDestination, err := jobSpec.Task.Unit.GenerateDependencies(
		models.GenerateDependenciesRequest{
			Config:  jobSpec.Task.Config,
			Assets:  jobSpec.Assets.ToMap(),
			Project: projectSpec,
		},
	)
	if err != nil {
		return models.JobSpec{}, err
	}

	// get job spec of these destinations and append to current jobSpec
	for _, depDestination := range jobDependenciesDestination.Dependencies {
		depSpec, depProj, err := projectJobSpecRepo.GetByDestination(depDestination)
		if err != nil {
			if err == store.ErrResourceNotFound {
				// should not fail for unknown dependency
				r.notifyProgress(observer, &EventJobSpecUnknownDependencyUsed{Job: jobSpec.Name, Dependency: depDestination})
				continue
			}
			return jobSpec, errors.Wrap(err, "runtime dependency evaluation failed")
		}

		// determine the type of dependency
		dep := models.JobSpecDependency{Job: &depSpec, Project: &depProj}
		dep.Type = r.getJobSpecDependencyType(dep, projectSpec.Name)
		jobSpec.Dependencies[depSpec.Name] = dep
	}

	return jobSpec, nil
}

func (r *dependencyResolver) getJobSpecDependencyType(dependency models.JobSpecDependency, currentJobSpecProject string) models.JobSpecDependencyType {
	if dependency.Project.Name == currentJobSpecProject {
		return models.JobSpecDependencyTypeIntra
	}
	return models.JobSpecDependencyTypeInter
}

// update named (explicit/static) dependencies if unresolved with its spec model
// this can normally happen when reading specs from a store[local/postgres]
func (r *dependencyResolver) resolveStaticDependencies(jobSpec models.JobSpec, projectSpec models.ProjectSpec, projectJobSpecRepo store.ProjectJobSpecRepository) (models.JobSpec, error) {
	// update static dependencies if unresolved with its spec model
	for depName, depSpec := range jobSpec.Dependencies {
		if depSpec.Job == nil {
			job, _, err := projectJobSpecRepo.GetByName(depName)
			if err != nil {
				return models.JobSpec{}, errors.Wrapf(err, "%s for job %s", ErrUnknownDependency, depName)
			}
			depSpec.Job = &job
			depSpec.Project = &projectSpec
			jobSpec.Dependencies[depName] = depSpec
		}
	}

	return jobSpec, nil
}

// hooks can be dependent on each other inside a job spec, this will populate
// the local array that points to its dependent hook
func (r *dependencyResolver) resolveHookDependencies(jobSpec models.JobSpec) (models.JobSpec, error) {
	for hookIdx, jobHook := range jobSpec.Hooks {
		jobHook.DependsOn = nil
		for _, depends := range jobHook.Unit.DependsOn() {
			dependentHook, err := jobSpec.GetHookByName(depends)
			if err == nil {
				jobHook.DependsOn = append(jobHook.DependsOn, &dependentHook)
			}
		}
		jobSpec.Hooks[hookIdx] = jobHook
	}
	return jobSpec, nil
}

func (r *dependencyResolver) notifyProgress(observer progress.Observer, e progress.Event) {
	if observer == nil {
		return
	}
	observer.Notify(e)
}

// NewDependencyResolver creates a new instance of Resolver
func NewDependencyResolver() *dependencyResolver {
	return &dependencyResolver{}
}
