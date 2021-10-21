package job

import (
	"context"
	"strings"

	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/pkg/errors"
)

var (
	ErrUnknownLocalDependency        = errors.New("unknown local dependency")
	ErrUnknownCrossProjectDependency = errors.New("unknown cross project dependency")
	UnknownRuntimeDependencyMessage  = "could not find registered destination '%s' during compiling dependencies for the provided job '%s', " +
		"please check if the source is correct, " +
		"if it is and want this to be ignored as dependency, " +
		"check docs how this can be done in used transformation task"
)

type dependencyResolver struct {
	projectJobSpecRepoFactory ProjectJobSpecRepoFactory
}

// Resolve resolves all kind of dependencies (inter/intra project, static deps) of a given JobSpec
func (r *dependencyResolver) Resolve(ctx context.Context, projectSpec models.ProjectSpec, jobSpec models.JobSpec,
	observer progress.Observer) (models.JobSpec, error) {
	if ctx.Err() != nil {
		return models.JobSpec{}, ctx.Err()
	}

	projectJobSpecRepo := r.projectJobSpecRepoFactory.New(projectSpec)
	// resolve inter/intra dependencies inferred by optimus
	jobSpec, err := r.resolveInferredDependencies(ctx, jobSpec, projectSpec, projectJobSpecRepo, observer)
	if err != nil {
		return models.JobSpec{}, err
	}

	// resolve statically defined dependencies
	jobSpec, err = r.resolveStaticDependencies(ctx, jobSpec, projectSpec, projectJobSpecRepo)
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

func (r *dependencyResolver) resolveInferredDependencies(ctx context.Context, jobSpec models.JobSpec, projectSpec models.ProjectSpec,
	projectJobSpecRepo store.ProjectJobSpecRepository, observer progress.Observer) (models.JobSpec, error) {
	// get destinations of dependencies, assets should be dependent on
	var jobDependencies []string
	if jobSpec.Task.Unit.DependencyMod != nil {
		resp, err := jobSpec.Task.Unit.DependencyMod.GenerateDependencies(ctx, models.GenerateDependenciesRequest{
			Config:  models.PluginConfigs{}.FromJobSpec(jobSpec.Task.Config),
			Assets:  models.PluginAssets{}.FromJobSpec(jobSpec.Assets),
			Project: projectSpec,
		})
		if err != nil {
			return models.JobSpec{}, err
		}
		jobDependencies = resp.Dependencies
	}

	// get job spec of these destinations and append to current jobSpec
	for _, depDestination := range jobDependencies {
		depSpec, depProj, err := projectJobSpecRepo.GetByDestination(ctx, depDestination)
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
func (r *dependencyResolver) resolveStaticDependencies(ctx context.Context, jobSpec models.JobSpec, projectSpec models.ProjectSpec,
	projectJobSpecRepo store.ProjectJobSpecRepository) (models.JobSpec, error) {
	// update static dependencies if unresolved with its spec model
	for depName, depSpec := range jobSpec.Dependencies {
		if depSpec.Job == nil {
			switch depSpec.Type {
			case models.JobSpecDependencyTypeIntra:
				{
					job, _, err := projectJobSpecRepo.GetByName(ctx, depName)
					if err != nil {
						return models.JobSpec{}, errors.Wrapf(err, "%s for job %s", ErrUnknownLocalDependency, depName)
					}
					depSpec.Job = &job
					depSpec.Project = &projectSpec
					jobSpec.Dependencies[depName] = depSpec
				}
			case models.JobSpecDependencyTypeInter:
				{
					// extract project name
					depParts := strings.SplitN(depName, "/", 2)
					if len(depParts) != 2 {
						return models.JobSpec{}, errors.Errorf("%s dependency should be in 'project_name/job_name' format: %s", models.JobSpecDependencyTypeInter, depName)
					}
					projectName := depParts[0]
					jobName := depParts[1]
					job, proj, err := projectJobSpecRepo.GetByNameForProject(ctx, projectName, jobName)
					if err != nil {
						return models.JobSpec{}, errors.Wrapf(err, "%s for job %s", ErrUnknownCrossProjectDependency, depName)
					}
					depSpec.Job = &job
					depSpec.Project = &proj
					jobSpec.Dependencies[depName] = depSpec
				}
			default:
				return models.JobSpec{}, errors.Errorf("unsupported dependency type: %s", depSpec.Type)
			}
		}
	}

	return jobSpec, nil
}

// hooks can be dependent on each other inside a job spec, this will populate
// the local array that points to its dependent hook
func (r *dependencyResolver) resolveHookDependencies(jobSpec models.JobSpec) (models.JobSpec, error) {
	for hookIdx, jobHook := range jobSpec.Hooks {
		jobHook.DependsOn = nil
		for _, depends := range jobHook.Unit.Info().DependsOn {
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
func NewDependencyResolver(projectJobSpecRepoFactory ProjectJobSpecRepoFactory) *dependencyResolver {
	return &dependencyResolver{
		projectJobSpecRepoFactory: projectJobSpecRepoFactory,
	}
}
