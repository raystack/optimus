package job

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"

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
	projectJobSpecRepoFactory ProjectJobSpecRepoFactory
	pluginService             service.PluginService
	dependencyRepo            store.JobDependencyRepository
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
	jobSpec = r.resolveHookDependencies(jobSpec)

	return jobSpec, nil
}

// Persist resolve inter/intra dependencies inferred by optimus and persist
func (r *dependencyResolver) Persist(ctx context.Context, jobSpec models.JobSpec) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// delete from dependency table
	if err := r.dependencyRepo.DeleteByJobID(ctx, jobSpec.ID); err != nil {
		return err
	}

	for _, dependency := range jobSpec.Dependencies {
		// insert the new ones
		err := r.dependencyRepo.Save(ctx, jobSpec.GetProjectSpec().ID, jobSpec.ID, dependency)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *dependencyResolver) FetchJobSpecsWithJobDependencies(ctx context.Context, projectSpec models.ProjectSpec, observer progress.Observer) ([]models.JobSpec, error) {
	projectJobSpecRepo := r.projectJobSpecRepoFactory.New(projectSpec)
	jobSpecs, err := projectJobSpecRepo.GetAll(ctx)
	if err != nil {
		return nil, err
	}
	r.notifyProgress(observer, &models.ProgressJobSpecFetch{})

	// fetch all dependencies
	dependencies, err := r.dependencyRepo.GetAll(ctx, projectSpec.ID)
	if err != nil {
		return nil, err
	}
	r.notifyProgress(observer, &models.ProgressJobDependencyFetch{})

	// fetch inter project dependencies job specs
	externalJobSpecs, err := r.getExternalProjectJobSpecs(ctx, dependencies)
	if err != nil {
		return nil, err
	}

	// create job spec map
	jobSpecMap := createJobSpecMap(jobSpecs, externalJobSpecs)

	// enrich
	return r.enrichJobSpecsWithJobDependencies(jobSpecs, dependencies, jobSpecMap)
}

func (*dependencyResolver) enrichJobSpecsWithJobDependencies(jobSpecs []models.JobSpec, jobDependencies []models.JobIDDependenciesPair,
	jobSpecMap map[uuid.UUID]models.JobSpec) ([]models.JobSpec, error) {
	var enrichedJobSpecs []models.JobSpec
	jobSpecAndDependenciesMap := models.JobIDDependenciesPairs(jobDependencies).GetJobDependencyMap()
	for _, jobSpec := range jobSpecs {
		dependencies := jobSpecAndDependenciesMap[jobSpec.ID]
		enrichedJobSpec := enrichSingleJobSpecWithDependencies(dependencies, jobSpec, jobSpecMap)
		enrichedJobSpecs = append(enrichedJobSpecs, enrichedJobSpec)
	}
	return enrichedJobSpecs, nil
}

func enrichSingleJobSpecWithDependencies(dependencies []models.JobIDDependenciesPair,
	jobSpec models.JobSpec, jobSpecMap map[uuid.UUID]models.JobSpec) models.JobSpec {
	if len(dependencies) > 0 {
		jobSpec.Dependencies = make(map[string]models.JobSpecDependency)
	}

	for _, dep := range dependencies {
		dependentJob := jobSpecMap[dep.DependentJobID]
		dependentProject := dep.DependentProject
		jobSpec.Dependencies[dependentJob.Name] = models.JobSpecDependency{
			Project: &dependentProject,
			Job:     &dependentJob,
			Type:    dep.Type,
		}
	}
	return jobSpec
}

func createJobSpecMap(jobSpecs []models.JobSpec, externalProjectJobSpecs []models.JobSpec) map[uuid.UUID]models.JobSpec {
	jobSpecMap := make(map[uuid.UUID]models.JobSpec)
	for _, jobSpec := range append(externalProjectJobSpecs, jobSpecs...) {
		jobSpecMap[jobSpec.ID] = jobSpec
	}
	return jobSpecMap
}

func (r *dependencyResolver) getExternalProjectJobSpecs(ctx context.Context, jobDependencies []models.JobIDDependenciesPair) ([]models.JobSpec, error) {
	var externalJobSpecs []models.JobSpec

	externalProjectAndDependenciesMap := models.JobIDDependenciesPairs(jobDependencies).GetExternalProjectAndDependenciesMap()
	for _, dependencies := range externalProjectAndDependenciesMap {
		specs, err := r.getJobSpecsPerExternalProject(ctx, dependencies)
		if err != nil {
			return nil, err
		}
		externalJobSpecs = append(externalJobSpecs, specs...)
	}
	return externalJobSpecs, nil
}

func (r *dependencyResolver) getJobSpecsPerExternalProject(ctx context.Context, dependencies []models.JobIDDependenciesPair) ([]models.JobSpec, error) {
	var dependencyJobIDs []uuid.UUID
	for _, dependency := range dependencies {
		dependencyJobIDs = append(dependencyJobIDs, dependency.DependentJobID)
	}

	projectJobSpecRepo := r.projectJobSpecRepoFactory.New(dependencies[0].DependentProject)
	specs, err := projectJobSpecRepo.GetByIDs(ctx, dependencyJobIDs)
	if err != nil {
		return nil, err
	}
	return specs, nil
}

func (r *dependencyResolver) resolveInferredDependencies(ctx context.Context, jobSpec models.JobSpec, projectSpec models.ProjectSpec,
	projectJobSpecRepo store.ProjectJobSpecRepository, observer progress.Observer) (models.JobSpec, error) {
	// get destinations of dependencies, assets should be dependent on
	var jobDependencies []string
	resp, err := r.pluginService.GenerateDependencies(ctx, jobSpec, jobSpec.NamespaceSpec, false)
	if err != nil {
		if !errors.Is(err, service.ErrDependencyModNotFound) {
			return models.JobSpec{}, err
		}
	}
	if resp != nil {
		jobDependencies = resp.Dependencies
	}

	// get job spec of these destinations and append to current jobSpec
	for _, depDestination := range jobDependencies {
		projectJobPairs, err := projectJobSpecRepo.GetByDestination(ctx, depDestination)
		if err != nil && !errors.Is(err, store.ErrResourceNotFound) {
			return jobSpec, fmt.Errorf("runtime dependency evaluation failed: %w", err)
		}
		if len(projectJobPairs) == 0 {
			// should not fail for unknown dependency, its okay to not have a upstream job
			// registered in optimus project and still refer to them in our job
			r.notifyProgress(observer, &models.ProgressJobSpecUnknownDependencyUsed{Job: jobSpec.Name, Dependency: depDestination})
			continue
		}
		dep := extractDependency(projectJobPairs, projectSpec)
		jobSpec.Dependencies[dep.Job.Name] = dep
	}

	return jobSpec, nil
}

// extractDependency extracts tries to find the upstream dependency from multiple matches
// type of dependency is decided based on if the job belongs to same project or other
// Note(kushsharma): correct way to do this is by creating a unique destination for each job
// this will require us to either change the database schema or add some kind of
// unique literal convention
func extractDependency(projectJobPairs []store.ProjectJobPair, projectSpec models.ProjectSpec) models.JobSpecDependency {
	var dep models.JobSpecDependency
	if len(projectJobPairs) == 1 {
		dep = models.JobSpecDependency{
			Job:     &projectJobPairs[0].Job,
			Project: &projectJobPairs[0].Project,
			Type:    models.JobSpecDependencyTypeIntra,
		}

		if projectJobPairs[0].Project.Name != projectSpec.Name {
			// if doesn't belong to same project, this is inter
			dep.Type = models.JobSpecDependencyTypeInter
		}
		return dep
	}

	// multiple projects found, this should not happen ideally and we should make
	// each destination unique, but now this has happened, give higher priority
	// to current project if found or choose any from the rest
	for _, pair := range projectJobPairs {
		if pair.Project.Name == projectSpec.Name {
			return models.JobSpecDependency{
				Job:     &pair.Job,
				Project: &pair.Project,
				Type:    models.JobSpecDependencyTypeIntra,
			}
		}
	}

	// dependency doesn't belong to this project, choose any(first)
	return models.JobSpecDependency{
		Job:     &projectJobPairs[0].Job,
		Project: &projectJobPairs[0].Project,
		Type:    models.JobSpecDependencyTypeInter,
	}
}

// update named (explicit/static) dependencies if unresolved with its spec model
// this can normally happen when reading specs from a store[local/postgres]
func (*dependencyResolver) resolveStaticDependencies(ctx context.Context, jobSpec models.JobSpec, projectSpec models.ProjectSpec,
	projectJobSpecRepo store.ProjectJobSpecRepository) (models.JobSpec, error) {
	// update static dependencies if unresolved with its spec model
	for depName, depSpec := range jobSpec.Dependencies {
		if depSpec.Job == nil {
			switch depSpec.Type {
			case models.JobSpecDependencyTypeIntra:
				{
					job, _, err := projectJobSpecRepo.GetByName(ctx, depName)
					if err != nil {
						return models.JobSpec{}, fmt.Errorf("%s for job %s: %w", ErrUnknownLocalDependency, depName, err)
					}
					depSpec.Job = &job
					depSpec.Project = &projectSpec
					jobSpec.Dependencies[depName] = depSpec
				}
			case models.JobSpecDependencyTypeInter:
				{
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
				}
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

func (*dependencyResolver) FetchHookWithDependencies(jobSpec models.JobSpec) []models.JobSpecHook {
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

// NewDependencyResolver creates a new instance of Resolver
func NewDependencyResolver(projectJobSpecRepoFactory ProjectJobSpecRepoFactory,
	dependencyRepo store.JobDependencyRepository,
	pluginService service.PluginService) *dependencyResolver {
	return &dependencyResolver{
		projectJobSpecRepoFactory: projectJobSpecRepoFactory,
		dependencyRepo:            dependencyRepo,
		pluginService:             pluginService,
	}
}
