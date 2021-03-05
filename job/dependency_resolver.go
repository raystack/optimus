package job

import (
	"github.com/pkg/errors"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

var (
	ErrUnknownDependency = errors.New("unknown dependency")
)

type dependencyResolver struct {
}

// Resolve resolves all kind of dependencies (inter/intra project, static deps) of a given JobSpec
func (r *dependencyResolver) Resolve(projectSpec models.ProjectSpec, jobSpecRepo store.JobSpecRepository, jobSpec models.JobSpec) (models.JobSpec, error) {
	// resolve inter/intra dependencies inferred by optimus
	jobSpec, err := r.resolveInferredDependencies(jobSpec, projectSpec, jobSpecRepo)
	if err != nil {
		return models.JobSpec{}, err
	}

	// resolve statically defined dependencies
	jobSpec, err = r.resolveStaticDependencies(jobSpec, projectSpec, jobSpecRepo)
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

func (r *dependencyResolver) resolveInferredDependencies(jobSpec models.JobSpec, projectSpec models.ProjectSpec, jobSpecRepo store.JobSpecRepository) (models.JobSpec, error) {
	// get destinations of dependencies
	jobDependenciesDestination, err := jobSpec.Task.Unit.GenerateDependencies(models.UnitData{
		Config: jobSpec.Task.Config,
		Assets: jobSpec.Assets.ToMap(),
	})
	if err != nil {
		return models.JobSpec{}, err
	}

	// get job spec of these destinations and append to current jobSpec
	for _, depDestination := range jobDependenciesDestination {
		depSpec, depProj, err := jobSpecRepo.GetByDestination(depDestination)
		if err != nil {
			return models.JobSpec{}, errors.Wrapf(err, "could not find destination %s", depDestination)
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
func (r *dependencyResolver) resolveStaticDependencies(jobSpec models.JobSpec, projectSpec models.ProjectSpec, jobSpecRepo store.JobSpecRepository) (models.JobSpec, error) {
	// update static dependencies if unresolved with its spec model
	for depName, depSpec := range jobSpec.Dependencies {
		if depSpec.Job == nil {
			job, err := jobSpecRepo.GetByName(depName)
			if err != nil {
				return models.JobSpec{}, errors.Wrapf(err, "%s for job %s", ErrUnknownDependency, depName)
			}
			depSpec.Job = &job
			// currently we allow only intra project static dependencies, so resolve project to current project,
			// and dependency type to Intra.
			depSpec.Project = &projectSpec
			depSpec.Type = models.JobSpecDependencyTypeIntra
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
		for _, depends := range jobHook.Unit.GetDependsOn() {
			dependentHook, err := jobSpec.GetHookByName(depends)
			if err == nil {
				jobHook.DependsOn = append(jobHook.DependsOn, &dependentHook)
			}
		}
		jobSpec.Hooks[hookIdx] = jobHook
	}
	return jobSpec, nil
}

// NewDependencyResolver creates a new instance of Resolver
func NewDependencyResolver() *dependencyResolver {
	return &dependencyResolver{}
}
