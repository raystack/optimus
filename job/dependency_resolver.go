package job

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
	"github.com/odpf/optimus/store"
)

var (
	ErrUnknownLocalDependency        = errors.New("unknown local dependency")
	ErrUnknownCrossProjectDependency = errors.New("unknown cross project dependency")
	UnknownRuntimeDependencyMessage  = "could not find registered destination '%s' during compiling dependencies for the provided job '%s', " +
		"please check if the source is correct, " +
		"if it is and want this to be ignored as dependency, " +
		"check docs how this can be done in used transformation task"
)

const InterJobDependencyNameSections = 2

type dependencyResolver struct {
	jobSpecRepo   store.JobSpecRepository
	jobSourceRepo store.JobSourceRepository
	pluginService service.PluginService

	// TODO: will be deprecated along with Resolve method deprecation
	projectJobSpecRepoFactory ProjectJobSpecRepoFactory
}

// NewDependencyResolver creates a new instance of Resolver
func NewDependencyResolver(
	jobSpecRepo store.JobSpecRepository,
	jobSourceRepo store.JobSourceRepository,
	pluginService service.PluginService,
	projectJobSpecRepoFactory ProjectJobSpecRepoFactory,
) DependencyResolver {
	return &dependencyResolver{
		jobSpecRepo:               jobSpecRepo,
		jobSourceRepo:             jobSourceRepo,
		pluginService:             pluginService,
		projectJobSpecRepoFactory: projectJobSpecRepoFactory,
	}
}

// Resolve resolves all kind of dependencies (inter/intra project, static deps) of a given JobSpec
// TODO: this method will be deprecated
func (d *dependencyResolver) Resolve(ctx context.Context, projectSpec models.ProjectSpec, jobSpec models.JobSpec,
	observer progress.Observer) (models.JobSpec, error) {
	if ctx.Err() != nil {
		return models.JobSpec{}, ctx.Err()
	}

	projectJobSpecRepo := d.projectJobSpecRepoFactory.New(projectSpec)
	// resolve inter/intra dependencies inferred by optimus
	jobSpec, err := d.resolveInferredDependencies(ctx, jobSpec, projectSpec, observer)
	if err != nil {
		return models.JobSpec{}, err
	}

	// resolve statically defined dependencies
	jobSpec, err = d.resolveStaticDependencies(ctx, jobSpec, projectSpec, projectJobSpecRepo)
	if err != nil {
		return models.JobSpec{}, err
	}

	// resolve inter hook dependencies
	jobSpec = d.resolveHookDependencies(jobSpec)

	return jobSpec, nil
}

// ResolveStaticDependencies return named (explicit/static) dependencies that unresolved with its spec model
// this is normally happen when reading specs from a store[local/postgres]
// unresolved dependencies will no longer exist in the map
// TODO: if we have field `projectJobFactory`, we might not need the `projectJobSpecRepository` parameter
func (*dependencyResolver) ResolveStaticDependencies(ctx context.Context, jobSpec models.JobSpec, projectSpec models.ProjectSpec,
	projectJobSpecRepo store.ProjectJobSpecRepository) (map[string]models.JobSpecDependency, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	if reflect.ValueOf(jobSpec).IsZero() {
		return nil, errors.New("job spec is empty")
	}
	if reflect.ValueOf(projectSpec).IsZero() {
		return nil, errors.New("project spec is empty")
	}
	if projectJobSpecRepo == nil {
		return nil, errors.New("project job spec repo is nil")
	}

	resolvedJobSpecDependencies := make(map[string]models.JobSpecDependency)
	for depName, depSpec := range jobSpec.Dependencies {
		if depSpec.Job == nil {
			switch depSpec.Type {
			case models.JobSpecDependencyTypeIntra:
				job, _, err := projectJobSpecRepo.GetByName(ctx, depName)
				if err != nil {
					return nil, fmt.Errorf("%s for job %s: %w", ErrUnknownLocalDependency, depName, err)
				}
				depSpec.Job = &job
				depSpec.Project = &projectSpec
				resolvedJobSpecDependencies[depName] = depSpec
			case models.JobSpecDependencyTypeInter:
				// extract project name
				depParts := strings.SplitN(depName, "/", InterJobDependencyNameSections)
				if len(depParts) != InterJobDependencyNameSections {
					return nil, fmt.Errorf("%s dependency should be in 'project_name/job_name' format: %s", models.JobSpecDependencyTypeInter, depName)
				}
				projectName := depParts[0]
				jobName := depParts[1]
				job, proj, err := projectJobSpecRepo.GetByNameForProject(ctx, projectName, jobName)
				if err != nil {
					return nil, fmt.Errorf("%s for job %s: %w", ErrUnknownCrossProjectDependency, depName, err)
				}
				depSpec.Job = &job
				depSpec.Project = &proj
				resolvedJobSpecDependencies[depName] = depSpec
			default:
				return nil, fmt.Errorf("unsupported dependency type: %s", depSpec.Type)
			}
		}
	}
	return resolvedJobSpecDependencies, nil
}

// TODO: this method will be deprecated (should be refactored to separate responsibility)
func (d *dependencyResolver) resolveInferredDependencies(ctx context.Context, jobSpec models.JobSpec, projectSpec models.ProjectSpec,
	observer progress.Observer) (models.JobSpec, error) {
	// get destinations of dependencies, assets should be dependent on
	namespace := jobSpec.NamespaceSpec
	namespace.ProjectSpec = projectSpec // TODO: Temp fix to to get secrets from project
	resp, err := d.pluginService.GenerateDependencies(ctx, jobSpec, namespace, false)
	if err != nil {
		if !errors.Is(err, service.ErrDependencyModNotFound) {
			return models.JobSpec{}, err
		}
	}
	if resp == nil || len(resp.Dependencies) == 0 {
		return jobSpec, nil
	}

	jobDependencies := resp.Dependencies
	if err := d.jobSourceRepo.Save(ctx, projectSpec.ID, jobSpec.ID, jobDependencies); err != nil {
		return models.JobSpec{}, fmt.Errorf("error persisting job sources for job %s: %w", jobSpec.Name, err)
	}

	// get job spec of these destinations and append to current jobSpec
	for _, depDestination := range jobDependencies {
		dependencyJobSpec, err := d.jobSpecRepo.GetJobByResourceDestination(ctx, depDestination)
		if err != nil {
			if errors.Is(err, store.ErrResourceNotFound) {
				// should not fail for unknown dependency, its okay to not have a upstream job
				// registered in optimus project and still refer to them in our job
				d.notifyProgress(observer, &models.ProgressJobSpecUnknownDependencyUsed{Job: jobSpec.Name, Dependency: depDestination})
				continue
			}
			return jobSpec, fmt.Errorf("runtime dependency evaluation failed: %w", err)
		}
		dep := extractDependency(dependencyJobSpec, projectSpec)
		jobSpec.Dependencies[dep.Job.Name] = dep
	}

	return jobSpec, nil
}

// extractDependency extracts tries to find the upstream dependency from multiple matches
// type of dependency is decided based on if the job belongs to same project or other
// Note(kushsharma): correct way to do this is by creating a unique destination for each job
// this will require us to either change the database schema or add some kind of
// unique literal convention
func extractDependency(dependencyJobSpec models.JobSpec, projectSpec models.ProjectSpec) models.JobSpecDependency {
	dep := models.JobSpecDependency{
		Job:     &dependencyJobSpec,
		Project: &dependencyJobSpec.NamespaceSpec.ProjectSpec,
		Type:    models.JobSpecDependencyTypeIntra,
	}

	if dep.Project.Name != projectSpec.Name {
		// if doesn't belong to same project, this is inter
		dep.Type = models.JobSpecDependencyTypeInter
	}
	return dep
}

// TODO: this method will be deprecated and replaced with ResolveStaticDependencies
func (*dependencyResolver) resolveStaticDependencies(ctx context.Context, jobSpec models.JobSpec, projectSpec models.ProjectSpec,
	projectJobSpecRepo store.ProjectJobSpecRepository) (models.JobSpec, error) {
	// update static dependencies if unresolved with its spec model
	for depName, depSpec := range jobSpec.Dependencies {
		if depSpec.Job == nil {
			switch depSpec.Type {
			case models.JobSpecDependencyTypeIntra:
				job, _, err := projectJobSpecRepo.GetByName(ctx, depName)
				if err != nil {
					return models.JobSpec{}, fmt.Errorf("%s for job %s: %w", ErrUnknownLocalDependency, depName, err)
				}
				depSpec.Job = &job
				depSpec.Project = &projectSpec
				jobSpec.Dependencies[depName] = depSpec
			case models.JobSpecDependencyTypeInter:
				// extract project name
				depParts := strings.SplitN(depName, "/", InterJobDependencyNameSections)
				if len(depParts) != InterJobDependencyNameSections {
					return models.JobSpec{}, fmt.Errorf("%s dependency should be in 'project_name/job_name' format: %s", models.JobSpecDependencyTypeInter, depName)
				}
				projectName := depParts[0]
				jobName := depParts[1]
				job, proj, err := projectJobSpecRepo.GetByNameForProject(ctx, projectName, jobName)
				if err != nil {
					return models.JobSpec{}, fmt.Errorf("%s for job %s: %w", ErrUnknownCrossProjectDependency, depName, err)
				}
				depSpec.Job = &job
				depSpec.Project = &proj
				jobSpec.Dependencies[depName] = depSpec
			default:
				return models.JobSpec{}, fmt.Errorf("unsupported dependency type: %s", depSpec.Type)
			}
		}
	}
	return jobSpec, nil
}

// hooks can be dependent on each other inside a job spec, this will populate
// the local array that points to its dependent hook
func (*dependencyResolver) resolveHookDependencies(jobSpec models.JobSpec) models.JobSpec {
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
	return jobSpec
}

func (d *dependencyResolver) GetJobSpecsWithDependencies(ctx context.Context, projectID models.ProjectID) ([]models.JobSpec, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	jobSpecs, err := d.jobSpecRepo.GetAllByProjectID(ctx, projectID)
	if err != nil {
		return nil, err
	}
	staticDependenciesPerJob, err := d.jobSpecRepo.GetStaticDependenciesPerJob(ctx, projectID)
	if err != nil {
		return nil, err
	}
	inferredDependenciesPerJob, err := d.jobSpecRepo.GetInferredDependenciesPerJob(ctx, projectID)
	if err != nil {
		return nil, err
	}

	//externalInferredDependenciesPerJob, err := externalDependencyResolver{}.FetchExternalInferredDependenciesPerJobName(ctx, projectID)
	//if err != nil {
	//	return nil, err
	//}
	//
	//externalStaticDependenciesPerJob, unknownDependencies, err := externalDependencyResolver{}.FetchExternalStaticDependenciesPerJobName(ctx, projectID)
	//if err != nil {
	//	return nil, err
	//}
	/*
		Deployer:
			- DependencyResolver.GetJobSpecsWithDependency() ([]JobSpec, []UnknownDependency, error)
					- GetJobs(ProjectID)
					- FetchInferredDependencies(ProjectID) (map[jobName]JobSpecs)
					- FetchStaticDependencies(ProjectID) (map[jobName]JobSpecs)
					- ExternalDependencyResolver.FetchExternalInferredDependencies(ProjectID) (map[jobName]ExternalDependency)
						- Query to get the Unknown inferred dependencies
						- Try to fetch from API
					- ExternalDependencyResolver.FetchExternalStaticDependencies(ProjectID) (map[jobName]ExternalDependency)
						- Query to get the Unknown inferred dependencies
						- Try to fetch from API
						- If not found at all, will put it in the unknownDependency
					- Iterate JobSpecs
						- Enrich with internal dependencies
						- Enrich with external dependencies
						- Enrich with hook dependencies
			- Check the UnknownDependency
				- Issue in Static	-> Not skip the jobspec (for now)
				- Update the JobDeployment.Detail
			- PriorityResolver.Resolve
			- Deploy

			100 jobs
			- 95 can be deployed
			- 5 having issues

			If there is a single job dependency or job deployment failed, will mark the status as failed, but complete the deployment.

				[]UnknownDependency {	-> only static
					JobName
					Value		-> Name or ResourceURN
					Type		-> [Job/ResourceURN]
				}





			ExternalDependencyResolver.Getting inferred dependencies []ExternalDependencies
				- Iteration of each of resource managers
				- Init resolver based on Type
					- OptimusDependencyResolver []OptimusDependency
						Responsibility:
						- GetExternalInferredDependencies
							- Get the unknown inferred dependencies from DB (ProjectID) (map[JobName]DependencyResourceURN)
							- Create the filter
							- Get the job specs from the client
								Warning: Some inferred dependencies might not be found.
										 Job deployment should not be skipped, but users need to be informed.
							- Convert the job specs to the OptimusDependency
							- Return the OptimusDependency

					- Other resource manager Resolver
		- api interactions abstraction
	*/

	for i := 0; i < len(jobSpecs); i++ {
		staticDependencies := staticDependenciesPerJob[jobSpecs[i].ID]
		inferredDependencies := inferredDependenciesPerJob[jobSpecs[i].ID]

		var dependencies []models.JobSpec
		dependencies = append(dependencies, staticDependencies...)
		dependencies = append(dependencies, inferredDependencies...)

		jobSpecs[i].Dependencies = d.groupDependencies(dependencies)
		jobSpecs[i].Hooks = d.fetchHookWithDependencies(jobSpecs[i])
	}
	return jobSpecs, nil
}

func (*dependencyResolver) groupDependencies(dependencyJobSpecs []models.JobSpec) map[string]models.JobSpecDependency {
	output := make(map[string]models.JobSpecDependency)
	for _, spec := range dependencyJobSpecs {
		projectSpec := spec.GetProjectSpec()
		jobSpec := spec

		key := projectSpec.Name + "/" + jobSpec.Name
		output[key] = models.JobSpecDependency{
			Project: &projectSpec,
			Job:     &jobSpec,
		}
	}
	return output
}

func (*dependencyResolver) fetchHookWithDependencies(jobSpec models.JobSpec) []models.JobSpecHook {
	var hooks []models.JobSpecHook
	for _, jobHook := range jobSpec.Hooks {
		jobHook.DependsOn = nil
		for _, depends := range jobHook.Unit.Info().DependsOn {
			dependentHook, err := jobSpec.GetHookByName(depends)
			if err == nil {
				jobHook.DependsOn = append(jobHook.DependsOn, &dependentHook)
			}
		}
		hooks = append(hooks, jobHook)
	}
	return hooks
}

func (*dependencyResolver) notifyProgress(observer progress.Observer, e progress.Event) {
	if observer == nil {
		return
	}
	observer.Notify(e)
}
